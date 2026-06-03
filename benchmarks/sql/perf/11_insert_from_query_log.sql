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
-- Parameters ({name:Type}) are bound by run_metrics_sql.py.
--
-- remoteSecure()/9440 reaches the ClickHouse Cloud target (no plain 9000);
-- for self-hosted use remote()/9000. Single-replica only; for multi-replica
-- swap in clusterAllReplicas('default', system.X).
--
-- Async inserts: when async_insert=1 (CH Cloud default) each batch logs an
-- 'Insert' receipt (written_rows=0) plus an 'AsyncInsertFlush' (the real
-- write); when async_insert=0 only 'Insert' exists, carrying the rows. So
-- write-volume metrics UNION both kinds on `written_rows > 0` to stay
-- mode-agnostic, while Insert-only metrics (count, latency, memory, CPU)
-- cover the receipt. has(tables) excludes CH-Cloud-internal billing inserts.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  -- ====== From `Insert` receipts (what the connector sees) ======
  SELECT 'ch_insert_count' AS metric_name, 'count' AS unit, toFloat64(count()) AS value
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_insert_duration_p50_ms', 'ms', quantile(0.50)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_insert_duration_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_peak_memory_usage_bytes', 'bytes', toFloat64(max(memory_usage))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_avg_memory_per_insert_bytes', 'bytes', toFloat64(avg(memory_usage))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- Network + CPU on the receipt side: bytes the connector sent, server CPU spent parsing.
  SELECT 'ch_network_receive_bytes', 'bytes',
         toFloat64(sum(ProfileEvents['NetworkReceiveBytes']))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_insert_cpu_seconds', 'seconds',
         toFloat64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) / 1e6
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- ====== Real write rows/bytes (mode-agnostic) ======
  -- `written_rows > 0` filter selects:
  --   * AsyncInsertFlush rows when async_insert=1 (Insert receipts have rows=0)
  --   * Insert rows when async_insert=0 (no AsyncInsertFlush events exist)
  -- so the same aggregate works in both modes.
  SELECT 'ch_avg_rows_per_insert', 'rows', toFloat64(avg(written_rows))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_p50_rows_per_insert', 'rows', quantile(0.50)(written_rows)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_avg_uncompressed_bytes_per_insert', 'bytes', toFloat64(avg(written_bytes))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_bytes > 0
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- ch_flush_duration_* describes the duration of the query that actually
  -- wrote data. In async mode that's the AsyncInsertFlush; in sync mode
  -- it's the Insert itself. Filtering on `written_rows > 0` selects the
  -- right one in either mode (in sync mode the flush and the insert
  -- duration coincide).
  SELECT 'ch_flush_duration_p50_ms', 'ms', quantile(0.50)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
  UNION ALL
  SELECT 'ch_flush_duration_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- ====== Windowed error counters ======
  SELECT 'ch_failed_inserts', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- TOO_MANY_PARTS (252) is the canonical "batches too small" error.
  SELECT 'ch_too_many_parts_errors', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND exception_code = 252
    AND has(tables, {table_qualified:String})
  UNION ALL
  -- NETWORK_ERROR (209), SOCKET_TIMEOUT (210) cover Spark-side timeout symptoms.
  SELECT 'ch_socket_errors', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND exception_code IN (209, 210)
    AND has(tables, {table_qualified:String})
);
