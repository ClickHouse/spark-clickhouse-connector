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
-- Parses the Spark event log into perf.metrics.
-- Parameters ({name:Type}) are bound by run_metrics_sql.py.
--
-- ACCOUNTING RULE (benchmark-v2-plan §6.4): write-volume and cost sums count
-- ONLY successful task attempts. A Spark task that fails is retried; both the
-- failed attempt and its successful retry emit a SparkListenerTaskEnd, and the
-- failed attempt still reports the partial Records/Bytes Written and CPU it did
-- before dying. Summing all attempts double-counts retried work, inflating
-- write_rows past the rows actually delivered to the target and polluting every
-- derived cost-per-row metric. `end_reason_str` (Task End Reason.Reason) is
-- 'Success' exactly for committed attempts, so every volume/cost aggregate below
-- filters on it. task_count stays a count of ALL attempts (raw scheduler
-- activity); task_failed_count is the count of non-Success attempts and feeds the
-- validity guard (§6.4 item 10: task_failed_count > 0 flags a run non-comparable).
--
-- Metric definitions emitted here:
--   e2e_duration [s]             wall-clock span of the write stage(s)
--   write_rows [count]           rows written by SUCCESSFUL task attempts only
--   write_bytes_serialized [B]   bytes written by SUCCESSFUL attempts only
--   executor_cpu_time_total [s]  executor CPU over SUCCESSFUL attempts only
--   jvm_gc_time_total [s]        JVM GC time over SUCCESSFUL attempts only
--   serialize_time_total [s]     connector serialization time, SUCCESSFUL only
--   write_time_total [s]         connector wire-write time, SUCCESSFUL only
--   task_count [count]           ALL task attempts (successful + failed)
--   task_failed_count [count]    non-Success task attempts (validity guard)
--   peak_jvm_heap_bytes [B]      peak executor JVM heap (StageExecutorMetrics)
--   peak_jvm_offheap_bytes [B]   peak executor JVM off-heap
--   throughput_rows_per_sec      write_rows / e2e_duration
--   throughput_mb_per_sec        write_bytes_serialized / e2e_duration
-- Derived client-cost / stability metrics (benchmark-v2-plan §5, Tier 0/1):
--   cpu_seconds_per_Mrows [s/Mrows]
--       executor_cpu_time_total / (write_rows / 1e6). Purest client-regression
--       signal: compute cost to push a million rows. Uses corrected sums so
--       retried work cannot deflate it.
--   serialize_seconds_per_Mrows [s/Mrows]
--       serialize_time_total / (write_rows / 1e6). Isolates format-conversion
--       cost from the rest of client CPU.
--   write_wait_share [ratio, 0..1]
--       write_time_total / (write_time_total + serialize_time_total +
--       executor_cpu_time_total). Share of client task-time spent waiting on the
--       wire vs computing; separates protocol/network changes from CPU changes.
--       NB: write_time/serialize_time are summed task-time accumulables, so this
--       is a task-time decomposition (not wall-clock), consistent across arms.
--   jvm_gc_time_share [ratio, 0..1]
--       jvm_gc_time_total / executor_cpu_time_total. Allocation-pressure signal;
--       a classic serializer-regression fingerprint (GC climbs relative to CPU).

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
  -- Connector custom metrics land as accumulables keyed by their DISPLAY name.
  -- Summing each task's "Update" gives total task-time (not wall-clock).
  -- Same successful-attempts-only rule as task_ends (see header): the
  -- connector's serialize/write accumulables are also emitted by failed
  -- attempts, so filter them to committed tasks.
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
    -- Successful attempts only: a failed/retried attempt double-counts the rows
    -- it wrote before dying (see header ACCOUNTING RULE).
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
    -- task_count is intentionally ALL attempts (raw scheduler activity), unlike
    -- the volume/cost sums above.
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
  -- pivot base into one row so derived metrics can reference values by name.
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
    SELECT 'throughput_rows_per_sec' AS metric_name, 'rows/s' AS unit,
           write_rows / nullIf(e2e_duration, 0) AS value
    FROM base_pivot
    UNION ALL
    SELECT 'throughput_mb_per_sec' AS metric_name, 'MB/s' AS unit,
           (write_bytes_serialized / (1024 * 1024)) / nullIf(e2e_duration, 0) AS value
    FROM base_pivot
    UNION ALL
    -- Client-cost metrics (§5). All divide by the CORRECTED, successful-attempts-
    -- only write_rows, so retries cannot deflate cost-per-row. write_rows/1e6 =
    -- millions of rows; a NULL result (write_rows=0) simply omits the row.
    SELECT 'cpu_seconds_per_Mrows', 's/Mrows',
           executor_cpu_time_total / nullIf(write_rows / 1e6, 0)
    FROM base_pivot
    UNION ALL
    SELECT 'serialize_seconds_per_Mrows', 's/Mrows',
           serialize_time_total / nullIf(write_rows / 1e6, 0)
    FROM base_pivot
    UNION ALL
    -- Fraction of client task-time spent waiting on the wire vs computing.
    SELECT 'write_wait_share', 'ratio',
           write_time_total
           / nullIf(write_time_total + serialize_time_total + executor_cpu_time_total, 0)
    FROM base_pivot
    UNION ALL
    -- Allocation-pressure signal: GC time relative to executor CPU.
    SELECT 'jvm_gc_time_share', 'ratio',
           jvm_gc_time_total / nullIf(executor_cpu_time_total, 0)
    FROM base_pivot
  )
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM base
UNION ALL
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM derived;
