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
  CH_HOST, CH_PORT, CH_PROTOCOL, CH_USER,
  CH_DATABASE, CH_TABLE, INPUT_PARQUET_GLOB, RUN_ID

Password resolution:
  CH_SECRET_ID  AWS Secrets Manager secret holding the CH password, read at
                runtime via the instance profile (used on EMR - the password is
                never passed through the env or the spark-submit args).
  CH_PASSWORD   fallback used only when CH_SECRET_ID is unset (local runs; may
                be empty for a passwordless local ClickHouse).
"""
import json
import os
import sys
from pyspark.sql import SparkSession


REQUIRED = (
    "CH_HOST", "CH_PORT", "CH_PROTOCOL", "CH_USER",
    "CH_DATABASE", "CH_TABLE", "INPUT_PARQUET_GLOB", "RUN_ID",
)


def resolve_password(env):
    secret_id = env.get("CH_SECRET_ID")
    if secret_id:
        import boto3  # only on the EMR path; not installed in the local venv
        region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or "us-east-1"
        value = boto3.client("secretsmanager", region_name=region) \
            .get_secret_value(SecretId=secret_id)["SecretString"]
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return value
        if isinstance(parsed, dict) and "password" in parsed:
            return parsed["password"]
        return value
    if "CH_PASSWORD" in env:
        return env["CH_PASSWORD"]
    sys.exit("set CH_SECRET_ID (Secrets Manager) or CH_PASSWORD (local)")


def load_config(env):
    missing = [k for k in REQUIRED if k not in env]
    if missing:
        sys.exit(f"missing required env vars: {missing}")
    try:
        int(env["CH_PORT"])
    except ValueError:
        sys.exit(f"CH_PORT must be an integer, got: {env['CH_PORT']}")
    cfg = {k: env[k] for k in REQUIRED}
    cfg["CH_PASSWORD"] = resolve_password(env)
    return cfg


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
    df.writeTo(target).append()

    spark.stop()


if __name__ == "__main__":
    main()
