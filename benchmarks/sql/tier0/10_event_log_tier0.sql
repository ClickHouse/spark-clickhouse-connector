--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- TIER 0 (benchmark-v2-plan §3 / §5) event-log capture — the Null-engine
-- connector-ceiling variant of benchmarks/sql/perf/10_insert_from_event_log.sql.
-- Parses the SAME Spark event log (the ingest workload is byte-identical to
-- Tier 1; only the target differs — an ENGINE=Null table (clickbench.hits_null)
-- on the SAME Cloud target as Tier 1, redesigned 2026-07-07 from the original
-- Docker-on-EMR-master instrument) into perf.metrics via run_metrics_sql.py,
-- tagged with the Tier 0
-- run_id (<pair_id>-<arm>-t0).  Parameters ({name:Type}) are bound by
-- run_metrics_sql.py.
--
-- WHY A SEPARATE FILE (contract §2.2): the throughput headline is renamed
--   throughput_rows_per_sec -> null_rows_per_sec
--   throughput_mb_per_sec   -> null_mb_per_sec
-- so a Tier-0 client-ceiling number is NEVER stored under the Tier-1 server-bound
-- name (contract §2.2: neither pipeline may store its headline under the aliased
-- name; the cross-connector view aliases them, it does not rename the store).
-- Every OTHER metric keeps its IDENTICAL Tier-1 spelling (cpu_seconds_per_Mrows,
-- serialize_seconds_per_Mrows, write_wait_share, jvm_gc_time_share,
-- peak_jvm_heap_bytes, peak_jvm_offheap_bytes, ...): tier separation is carried
-- by the row's run_id -> perf.runs join (contract §1.2 dataset-integrity rule),
-- NOT by mangling the metric names.  The ACCOUNTING RULE (successful task
-- attempts only) is identical to perf/10 — see that file's header.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH
  events AS (
    SELECT json
    -- 4th arg (session token) is empty for long-term IAM keys, set for STS creds.
    FROM s3({event_log_uri:String}, {aws_access_key:String}, {aws_secret_key:String}, {aws_session_token:String}, 'JSONAsString', 'json String')
  ),
  task_ends AS (
    SELECT
      JSONExtractInt(json, 'Task Metrics', 'Executor CPU Time')                 AS cpu_ns,
      JSONExtractInt(json, 'Task Metrics', 'JVM GC Time')                       AS gc_ms,
      JSONExtractInt(json, 'Task Metrics', 'Output Metrics', 'Bytes Written')   AS out_bytes,
      JSONExtractInt(json, 'Task Metrics', 'Output Metrics', 'Records Written') AS out_rows,
      JSONExtractString(json, 'Task End Reason', 'Reason')                      AS end_reason_str
    FROM events
    WHERE JSONExtractString(json, 'Event') = 'SparkListenerTaskEnd'
  ),
  stage_completed AS (
    SELECT
      JSONExtractInt(json, 'Stage Info', 'Submission Time') AS submit_ms,
      JSONExtractInt(json, 'Stage Info', 'Completion Time') AS complete_ms
    FROM events
    WHERE JSONExtractString(json, 'Event') = 'SparkListenerStageCompleted'
  ),
  stage_exec_metrics AS (
    SELECT
      JSONExtractInt(json, 'Executor Metrics', 'JVMHeapMemory')    AS heap_bytes,
      JSONExtractInt(json, 'Executor Metrics', 'JVMOffHeapMemory') AS offheap_bytes
    FROM events
    WHERE JSONExtractString(json, 'Event') = 'SparkListenerStageExecutorMetrics'
  ),
  task_accum AS (
    SELECT
      arraySum(arrayMap(
        x -> toInt64OrZero(JSONExtractString(x, 'Update')),
        arrayFilter(x -> JSONExtractString(x, 'Name') = 'total time of serialization',
          JSONExtractArrayRaw(json, 'Task Info', 'Accumulables')))) AS serialize_ms,
      arraySum(arrayMap(
        x -> toInt64OrZero(JSONExtractString(x, 'Update')),
        arrayFilter(x -> JSONExtractString(x, 'Name') = 'total time of writing',
          JSONExtractArrayRaw(json, 'Task Info', 'Accumulables')))) AS write_ms
    FROM events
    WHERE JSONExtractString(json, 'Event') = 'SparkListenerTaskEnd'
      AND JSONExtractString(json, 'Task End Reason', 'Reason') = 'Success'
  ),
  base AS (
    SELECT 'e2e_duration' AS metric_name, 'seconds' AS unit,
           (max(complete_ms) - min(submit_ms)) / 1000.0 AS value
    FROM stage_completed
    UNION ALL
    -- Successful attempts only (see perf/10 header ACCOUNTING RULE).
    SELECT 'write_rows', 'count',
           toFloat64(sumIf(out_rows, end_reason_str = 'Success'))
    FROM task_ends
    UNION ALL
    SELECT 'write_bytes_serialized', 'bytes',
           toFloat64(sumIf(out_bytes, end_reason_str = 'Success'))
    FROM task_ends
    UNION ALL
    SELECT 'executor_cpu_time_total', 'seconds',
           sumIf(cpu_ns, end_reason_str = 'Success') / 1e9
    FROM task_ends
    UNION ALL
    SELECT 'jvm_gc_time_total', 'seconds',
           sumIf(gc_ms, end_reason_str = 'Success') / 1000.0
    FROM task_ends
    UNION ALL
    SELECT 'task_count', 'count',
           toFloat64(count())
    FROM task_ends
    UNION ALL
    SELECT 'task_failed_count', 'count',
           toFloat64(countIf(end_reason_str != 'Success'))
    FROM task_ends
    UNION ALL
    SELECT 'peak_jvm_heap_bytes', 'bytes',
           toFloat64(max(heap_bytes))
    FROM stage_exec_metrics
    UNION ALL
    SELECT 'peak_jvm_offheap_bytes', 'bytes',
           toFloat64(max(offheap_bytes))
    FROM stage_exec_metrics
    UNION ALL
    SELECT 'serialize_time_total', 'seconds', sum(serialize_ms) / 1000.0 FROM task_accum
    UNION ALL
    SELECT 'write_time_total', 'seconds', sum(write_ms) / 1000.0 FROM task_accum
  ),
  base_pivot AS (
    SELECT
      anyIf(value, metric_name = 'e2e_duration')            AS e2e_duration,
      anyIf(value, metric_name = 'write_rows')              AS write_rows,
      anyIf(value, metric_name = 'write_bytes_serialized')  AS write_bytes_serialized,
      anyIf(value, metric_name = 'executor_cpu_time_total') AS executor_cpu_time_total,
      anyIf(value, metric_name = 'jvm_gc_time_total')       AS jvm_gc_time_total,
      anyIf(value, metric_name = 'serialize_time_total')    AS serialize_time_total,
      anyIf(value, metric_name = 'write_time_total')        AS write_time_total
    FROM base
  ),
  derived AS (
    -- TIER-0 HEADLINE RENAME (contract §2.2): null_rows_per_sec / null_mb_per_sec,
    -- NOT throughput_* — this is the connector ceiling against a Null target.
    SELECT 'null_rows_per_sec' AS metric_name, 'rows/s' AS unit,
           write_rows / nullIf(e2e_duration, 0) AS value
    FROM base_pivot
    UNION ALL
    SELECT 'null_mb_per_sec' AS metric_name, 'MB/s' AS unit,
           (write_bytes_serialized / (1024 * 1024)) / nullIf(e2e_duration, 0) AS value
    FROM base_pivot
    UNION ALL
    -- Client-cost metrics (§5): IDENTICAL Tier-1 spellings — tier separation is
    -- via run_id (contract §1.2), not the metric name.
    SELECT 'cpu_seconds_per_Mrows', 's/Mrows',
           executor_cpu_time_total / nullIf(write_rows / 1e6, 0)
    FROM base_pivot
    UNION ALL
    SELECT 'serialize_seconds_per_Mrows', 's/Mrows',
           serialize_time_total / nullIf(write_rows / 1e6, 0)
    FROM base_pivot
    UNION ALL
    SELECT 'write_wait_share', 'ratio',
           write_time_total
           / nullIf(write_time_total + serialize_time_total + executor_cpu_time_total, 0)
    FROM base_pivot
    UNION ALL
    SELECT 'jvm_gc_time_share', 'ratio',
           jvm_gc_time_total / nullIf(executor_cpu_time_total, 0)
    FROM base_pivot
  )
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM base
UNION ALL
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM derived;
