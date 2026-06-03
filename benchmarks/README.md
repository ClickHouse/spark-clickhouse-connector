# ClickBench Load Test

Scheduled benchmark that ingests ClickBench `hits_*.parquet` (~70 GB, ~100M rows)
into a partitioned ClickHouse MergeTree via `spark-clickhouse-connector` on a
transient EMR cluster, then pushes Spark + ClickHouse metrics into a `perf.metrics`
table for trend analysis.

## How it's built

- **Job**: `benchmarks/clickbench_ingest.py` (PySpark). Same script runs locally
  and on EMR — config comes from env vars.
- **Connector**: built via Gradle (`:clickhouse-spark-runtime-3.5_2.12:shadowJar`)
  and passed to Spark via `--jars`. Spark 3.5 / Scala 2.12 (matches EMR 7.13.0;
  EMR doesn't ship Spark 4 yet).
- **Metrics**: ClickHouse pulls the Spark event log directly from S3 via the
  `s3()` table function. CH-side numbers come from the target's system tables
  (`query_log`, `metric_log`, `part_log`) via `remoteSecure()` on the
  native-secure port 9440 (the target is ClickHouse Cloud).
- **Credentials**: the Spark job reads the target ClickHouse password from AWS
  Secrets Manager at runtime, so it never appears in the EMR step args or the
  node environment. Before submit the workflow creates the `CH_SECRET_ID` secret
  from `CLICKBENCH_TARGET_CH_PASSWORD` only if it doesn't already exist (an
  existing secret is left as-is — rotate it in Secrets Manager directly).
  Requires the OIDC role to allow `secretsmanager:DescribeSecret` /
  `CreateSecret` and the EMR instance profile to allow
  `secretsmanager:GetSecretValue`.

## Testing model

EMR is the source of truth. There is no JUnit-style test suite. Iteration runs
use `workflow_dispatch` with a 1-file glob (~5 min on EMR).

If you change `clickbench_ingest.py` or `10_insert_from_event_log.sql`, fire a
`workflow_dispatch` smoke before merging.

## Required GitHub Actions secrets

| Secret                                  | Purpose                                         |
| --------------------------------------- | ----------------------------------------------- |
| `CLICKBENCH_AWS_ROLE_ARN`               | OIDC role assumed by the workflow               |
| `CLICKBENCH_S3_BUCKET`                  | Bucket for jars / scripts / event logs / EMR logs |
| `CLICKBENCH_EMR_SUBNET_ID`              | VPC subnet for EMR ENIs                         |
| `CLICKBENCH_EMR_KEY_NAME`               | EC2 key-pair name for EMR nodes                 |
| `CLICKBENCH_EMR_SERVICE_ROLE`           | EMR service role                                |
| `CLICKBENCH_EMR_INSTANCE_PROFILE`       | EC2 instance profile for EMR core nodes         |
| `CLICKBENCH_TARGET_CH_HOST`             | Target ClickHouse hostname                      |
| `CLICKBENCH_TARGET_CH_PORT`             | Target ClickHouse HTTPS port (8443)             |
| `CLICKBENCH_TARGET_CH_USER`             | Target CH user (write to `clickbench.hits`)     |
| `CLICKBENCH_TARGET_CH_PASSWORD`         | Target CH password                              |
| `CLICKBENCH_METRICS_CH_HOST`            | Metrics CH hostname                             |
| `CLICKBENCH_METRICS_CH_USER`            | Metrics CH user (write to `perf.*`)             |
| `CLICKBENCH_METRICS_CH_PASSWORD`        | Metrics CH password                             |

## First-time setup

The scheduled workflow runs `benchmarks/scripts/bootstrap_schema.py` on every
invocation, so the target / metrics tables are auto-created the first time
the workflow fires. To apply the schema by hand (fresh CH service, or local
ad-hoc work), run the same script — it's idempotent (`CREATE … IF NOT EXISTS`):

```bash
pip install clickhouse-connect
TARGET_CH_HOST=<host>  TARGET_CH_USER=<u>  TARGET_CH_PASSWORD=<p> \
METRICS_CH_HOST=<host> METRICS_CH_USER=<u> METRICS_CH_PASSWORD=<p> \
  python benchmarks/scripts/bootstrap_schema.py
```

(The target and metrics services are currently the same, so the two host/user/
password trios point at the same place.)

## Manual run

```bash
gh workflow run clickbench-load-test.yml \
  -f input_parquet_glob='s3a://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_0.parquet'
```

Single-file glob = cheap smoke (~5 min job; ~$1, since the ~8-min cluster
bootstrap dominates). Omit the flag for the full run.

## Local iteration

```bash
# 1. Build the shaded connector runtime jar (needs JDK 17 on JAVA_HOME).
./gradlew -Dspark_binary_version=3.5 -Dscala_binary_version=2.12 \
  :clickhouse-spark-runtime-3.5_2.12:shadowJar

# 2. Start a local ClickHouse (passwordless default user).
docker run -d --name ch-local -p 18123:8123 -p 19000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:latest

# 3. Apply DDL.
until curl -sS http://localhost:18123/ping > /dev/null; do sleep 1; done
for f in benchmarks/sql/clickbench/0*.sql benchmarks/sql/perf/0*.sql; do
  curl -sS --fail-with-body -X POST 'http://localhost:18123/' --data-binary @"$f"
done

# 4. Install pyspark 3.5 in a venv (one-time).
python3 -m venv benchmarks/.local-run/venv
benchmarks/.local-run/venv/bin/pip install 'pyspark==3.5.6' pyarrow

# 5. Download a single ClickBench file (~120 MB, one-time).
aws s3 cp --no-sign-request \
  s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_0.parquet \
  benchmarks/.local-run/hits_0.parquet

# 6. Submit.
REPO=$(pwd)
# Spark's event-log writer requires the dir to already exist; it won't create it.
mkdir -p "$REPO/benchmarks/.local-run/spark-events"
# Resolve the venv's pyspark dir without hardcoding the python minor version.
PYSPARK_HOME=$(echo "$REPO"/benchmarks/.local-run/venv/lib/python*/site-packages/pyspark)
JAR=$(ls spark-3.5/clickhouse-spark-runtime/build/libs/clickhouse-spark-runtime-3.5_2.12-*.jar | head -1)
export SPARK_HOME=$PYSPARK_HOME
export PYSPARK_PYTHON=$REPO/benchmarks/.local-run/venv/bin/python
CH_HOST=localhost CH_PORT=18123 CH_PROTOCOL=http CH_USER=default CH_PASSWORD='' \
CH_DATABASE=clickbench CH_TABLE=hits \
INPUT_PARQUET_GLOB="file://$REPO/benchmarks/.local-run/hits_0.parquet" \
RUN_ID=local-1 \
$PYSPARK_HOME/bin/spark-submit --master 'local[*]' --jars "$JAR" \
  --conf spark.driver.bindAddress=127.0.0.1 --conf spark.driver.host=127.0.0.1 \
  --conf spark.eventLog.enabled=true \
  --conf "spark.eventLog.dir=file://$REPO/benchmarks/.local-run/spark-events" \
  --conf spark.eventLog.logStageExecutorMetrics=true \
  benchmarks/clickbench_ingest.py
```

## Validating the parser SQL locally

Run locally (no rolling event log configured), Spark 3.5 writes a single flat
file `benchmarks/.local-run/spark-events/local-<timestamp>` — not the rolling
`eventlog_v2_<appId>/events_N_<appId>` directory tree EMR/Spark 4 produces. To
validate `10_insert_from_event_log.sql` against it:

```bash
LOG=$(find benchmarks/.local-run/spark-events -type f | head -1)
docker cp "$LOG" ch-local:/var/lib/clickhouse/user_files/spark_event_log.jsonl
# On macOS Docker the copied file is owned by the host UID; CH cannot read it
# until we re-own it as the clickhouse user inside the container:
docker exec -u 0 ch-local chown clickhouse:clickhouse \
  /var/lib/clickhouse/user_files/spark_event_log.jsonl

# Swap the s3(...) call for file(...) and bind the run_id parameter inline.
sed -e "s|s3([^)]*)|file('spark_event_log.jsonl', JSONAsString, 'json String')|" \
    -e "s|{run_id:String}|'local-validate'|g" \
    benchmarks/sql/perf/10_insert_from_event_log.sql > /tmp/parse_local.sql
curl -sS --fail-with-body -X POST 'http://localhost:18123/' --data-binary @/tmp/parse_local.sql

curl -sS 'http://localhost:18123/' --data-binary "
  SELECT metric_name, value, unit FROM perf.metrics
  WHERE run_id='local-validate' ORDER BY metric_name FORMAT PrettyCompactMonoBlock"
```

Expected: 13 rows (11 `base` + 2 `derived` throughput metrics), all non-negative.
`peak_jvm_heap_bytes` and `peak_jvm_offheap_bytes` come back `0` under `local[*]`
(executor memory metrics aren't captured locally) — that's expected, not a failure.

## Reading results

```sql
SELECT run_id, metric_name, value, unit
FROM perf.metrics
WHERE run_id IN (SELECT run_id FROM perf.runs ORDER BY run_started_at DESC LIMIT 5)
ORDER BY run_id, metric_name;
```

## Truncating between runs

Each scheduled / `workflow_dispatch` run truncates `clickbench.hits` so row counts
and metrics describe that run only. The truncate step runs after the schema
bootstrap but before EMR is provisioned, so if it fails no cluster is launched.

Override with a repository variable `SKIP_TRUNCATE=1` if you want to accumulate
data across runs (e.g. for steady-state experiments).

For manual local runs, run the truncate first:

```bash
TARGET_CH_HOST=<host> TARGET_CH_USER=<user> TARGET_CH_PASSWORD=<pwd> \
  CH_DATABASE=clickbench CH_TABLE=hits \
  python benchmarks/scripts/truncate_target.py
```

