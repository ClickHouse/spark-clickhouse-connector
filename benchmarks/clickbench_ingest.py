#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
ClickBench ingest job. Reads Parquet from S3, writes to a partitioned MergeTree
via spark-clickhouse-connector.

Configuration is read entirely from environment variables. The same script
runs locally (master=local[*], spark.eventLog.dir on disk) and on EMR
(Spark 3.5 on EMR 7.x, spark.eventLog.dir on S3) - the difference is supplied
via spark-submit --conf, not here.

Required env vars:
  CH_HOST, CH_PORT, CH_PROTOCOL, CH_USER, CH_PASSWORD,
  CH_DATABASE, CH_TABLE, INPUT_PARQUET_GLOB, RUN_ID
"""
import os
import sys
from pyspark.sql import SparkSession


REQUIRED = (
    "CH_HOST", "CH_PORT", "CH_PROTOCOL", "CH_USER", "CH_PASSWORD",
    "CH_DATABASE", "CH_TABLE", "INPUT_PARQUET_GLOB", "RUN_ID",
)


def load_config(env):
    # Use `k not in env` rather than `not env.get(k)` so that legitimate empty
    # values (e.g. an empty CH_PASSWORD for a passwordless local user) are not
    # rejected.
    missing = [k for k in REQUIRED if k not in env]
    if missing:
        sys.exit(f"missing required env vars: {missing}")
    try:
        int(env["CH_PORT"])
    except ValueError:
        sys.exit(f"CH_PORT must be an integer, got: {env['CH_PORT']}")
    return {k: env[k] for k in REQUIRED}


def main():
    cfg = load_config(os.environ)

    spark = (
        SparkSession.builder
        .appName(f"ClickBenchIngest:{cfg['RUN_ID']}")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", cfg["CH_HOST"])
        .config("spark.sql.catalog.clickhouse.protocol", cfg["CH_PROTOCOL"])
        .config("spark.sql.catalog.clickhouse.http_port", cfg["CH_PORT"])
        .config("spark.sql.catalog.clickhouse.user", cfg["CH_USER"])
        .config("spark.sql.catalog.clickhouse.password", cfg["CH_PASSWORD"])
        .config("spark.sql.catalog.clickhouse.database", cfg["CH_DATABASE"])
        .config("spark.sql.catalog.clickhouse.option.ssl",
                str(cfg["CH_PROTOCOL"] == "https").lower())
        # Force synchronous inserts. CH Cloud defaults to async_insert=1, which
        # masks the connector's real per-insert behaviour and makes runs
        # non-comparable across services. The `option.clickhouse_setting_*`
        # prefix passes through as a server-level query setting.
        .config("spark.sql.catalog.clickhouse.option.clickhouse_setting_async_insert", "0")
        .config("spark.sql.catalog.clickhouse.option.clickhouse_setting_wait_for_async_insert", "0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[clickbench_ingest] run_id={cfg['RUN_ID']}")
    print(f"[clickbench_ingest] reading: {cfg['INPUT_PARQUET_GLOB']}")
    df = spark.read.parquet(cfg["INPUT_PARQUET_GLOB"])

    target = f"clickhouse.{cfg['CH_DATABASE']}.{cfg['CH_TABLE']}"
    print(f"[clickbench_ingest] writing to: {target}")
    # No manual repartition - the connector reads the CH table's partition
    # transform via ExprUtils.toSparkTransformOpt and Spark's V2 write planner
    # adds the appropriate Exchange. With PARTITION BY toYear(EventDate) the
    # connector's identity-mapping patch gives Spark IdentityTransform(EventDate),
    # which Spark uses for partition-aware writes.
    df.writeTo(target).append()

    spark.stop()


if __name__ == "__main__":
    main()
