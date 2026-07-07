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

Connector operating config under test (plan §6.9; version-controlled in the
workflow env, forwarded via emr_submit.sh appMasterEnv). Defaulted here to the
known operating point so a bare invocation still runs the pinned config:
  BATCH_SIZE         rows per insert batch -> spark.clickhouse.write.batchSize
                     (default 100000)
  ASYNC_INSERT       server async_insert mode, '0' | '1' (default '0')
  SOCKET_TIMEOUT_MS  CH Java client socket/read timeout in ms (default 300000);
                     legacy-orchestrator parity, fails a stalled insert fast
                     instead of hanging forever (see load_operating_config)

Write parallelism is RECORD-ONLY, not forced: the write stage keeps its natural
input-split parallelism (repartitioning here would inject a full shuffle of the
dataset inside the timed window — a trend-breaking workload change, not
hygiene). The observed write-stage partition count is printed in a parseable
line and, when WRITE_PARALLELISM_META_S3_URI is set, uploaded there as a
one-line sidecar so the workflow can echo the OBSERVED value into
runtime['write_parallelism']:
  WRITE_PARALLELISM_META_S3_URI  optional s3://<bucket>/<key> that receives the
                                 observed partition count (best-effort; a
                                 failure to upload never fails the ingest)

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


def load_operating_config(env):
    """Connector operating config under test (plan §6.9). Version-controlled in
    the workflow env; defaulted here to the known operating point so a bare run
    still exercises the pinned config."""
    batch_size = env.get("BATCH_SIZE", "100000")
    async_insert = env.get("ASYNC_INSERT", "0")
    # socket_timeout hang protection (legacy-orchestrator parity). The retired S3
    # orchestrator carried socket_timeout=300000; the repo ingest script had
    # dropped it. Without a read/socket timeout the CH Java Client V2 insert path
    # can hang FOREVER when the target service stalls mid-insert (observed on a
    # slow target — MEMORY.md b_cluster_insert_hang): the write task never returns
    # and the whole run wedges. 300000 ms (5 min) fails the task fast instead, so
    # the run surfaces a failure rather than hanging until the workflow timeout.
    # Env override SOCKET_TIMEOUT_MS; default 300000 (the client's own default is
    # only 30000, too short for large batches, hence the explicit 300000).
    socket_timeout_ms = env.get("SOCKET_TIMEOUT_MS", "300000")
    try:
        if int(batch_size) < 1:
            sys.exit(f"BATCH_SIZE must be >= 1, got: {batch_size}")
    except ValueError:
        sys.exit(f"BATCH_SIZE must be an integer, got: {batch_size}")
    if async_insert not in ("0", "1"):
        sys.exit(f"ASYNC_INSERT must be '0' or '1', got: {async_insert}")
    try:
        if int(socket_timeout_ms) < 1:
            sys.exit(f"SOCKET_TIMEOUT_MS must be >= 1, got: {socket_timeout_ms}")
    except ValueError:
        sys.exit(f"SOCKET_TIMEOUT_MS must be an integer (ms), got: {socket_timeout_ms}")
    return {
        "BATCH_SIZE": batch_size,
        "ASYNC_INSERT": async_insert,
        "SOCKET_TIMEOUT_MS": socket_timeout_ms,
    }


def report_observed_parallelism(observed, env):
    """Surface the OBSERVED write-stage partition count (record-only — nothing
    forces it). Printed for the step log, and uploaded as a one-line S3 sidecar
    when WRITE_PARALLELISM_META_S3_URI is set so the workflow can put the
    observed value into runtime['write_parallelism']. Best-effort: a sidecar
    failure must never fail the ingest."""
    print(f"[clickbench_ingest] observed_write_parallelism={observed}")
    uri = env.get("WRITE_PARALLELISM_META_S3_URI")
    if not uri:
        return
    try:
        import boto3  # EMR path; the instance profile grants CI-bucket access
        bucket, key = uri.removeprefix("s3://").split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key,
                                      Body=f"{observed}\n".encode())
        print(f"[clickbench_ingest] wrote observed parallelism sidecar: {uri}")
    except Exception as e:  # noqa: BLE001 - deliberately non-fatal
        print(f"[clickbench_ingest] WARN: sidecar upload failed ({e}); "
              f"runtime['write_parallelism'] will fall back to 'input-splits'")


def main():
    cfg = load_config(os.environ)
    op = load_operating_config(os.environ)

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
        # socket_timeout hang protection (legacy-orchestrator parity). This is a
        # CH Java Client OPTION (not a server setting): the connector maps
        # `option.<name>` catalog props into the client's option map keyed by
        # com.clickhouse.client.config.ClickHouseClientOption — SOCKET_TIMEOUT's
        # key is exactly "socket_timeout" (verified against clickhouse-client
        # 0.9.5: getKey()=="socket_timeout", client default 30000 ms). Value in
        # milliseconds. See load_operating_config for the hang failure mode.
        .config("spark.sql.catalog.clickhouse.option.socket_timeout",
                op["SOCKET_TIMEOUT_MS"])
        # Batch size (rows per insert) under test — connector write conf.
        .config("spark.clickhouse.write.batchSize", op["BATCH_SIZE"])
        # async_insert mode under test. wait_for_async_insert kept coupled so a
        # sync run (async=0) is a clean synchronous insert.
        .config("spark.sql.catalog.clickhouse.option.clickhouse_setting_async_insert",
                op["ASYNC_INSERT"])
        .config("spark.sql.catalog.clickhouse.option.clickhouse_setting_wait_for_async_insert",
                op["ASYNC_INSERT"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[clickbench_ingest] run_id={cfg['RUN_ID']}")
    print(f"[clickbench_ingest] operating config: batch_size={op['BATCH_SIZE']} "
          f"async_insert={op['ASYNC_INSERT']} socket_timeout_ms={op['SOCKET_TIMEOUT_MS']}")
    print(f"[clickbench_ingest] reading: {cfg['INPUT_PARQUET_GLOB']}")
    df = spark.read.parquet(cfg["INPUT_PARQUET_GLOB"])

    # Write parallelism is record-only (see module docstring): no repartition —
    # the write stage runs at the input-split partition count (task concurrency
    # is then bounded by executor slots). Observe it immediately before the
    # write and surface it for runtime['write_parallelism'].
    observed_parallelism = df.rdd.getNumPartitions()
    report_observed_parallelism(observed_parallelism, os.environ)

    target = f"clickhouse.{cfg['CH_DATABASE']}.{cfg['CH_TABLE']}"
    print(f"[clickbench_ingest] writing to: {target} "
          f"(write-stage partitions={observed_parallelism})")
    df.writeTo(target).append()

    spark.stop()


if __name__ == "__main__":
    main()
