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
  ),
  base AS (
    SELECT 'e2e_duration' AS metric_name, 'seconds' AS unit,
           (max(complete_ms) - min(submit_ms)) / 1000.0 AS value
    FROM stage_completed
    UNION ALL
    SELECT 'write_rows', 'count',
           toFloat64(sum(out_rows))
    FROM task_ends
    UNION ALL
    SELECT 'write_bytes_serialized', 'bytes',
           toFloat64(sum(out_bytes))
    FROM task_ends
    UNION ALL
    SELECT 'executor_cpu_time_total', 'seconds',
           sum(cpu_ns) / 1e9
    FROM task_ends
    UNION ALL
    SELECT 'jvm_gc_time_total', 'seconds',
           sum(gc_ms) / 1000.0
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
  -- pivot base into one row so derived metrics can reference values by name.
  base_pivot AS (
    SELECT
      anyIf(value, metric_name = 'e2e_duration')           AS e2e_duration,
      anyIf(value, metric_name = 'write_rows')             AS write_rows,
      anyIf(value, metric_name = 'write_bytes_serialized') AS write_bytes_serialized
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
  )
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM base
UNION ALL
SELECT {run_id:String} AS run_id, metric_name, unit, toFloat64(value) AS value FROM derived;
