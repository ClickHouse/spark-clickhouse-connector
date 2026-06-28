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
-- Parts + merge metrics. The window split is deliberate:
--   [run_start, run_end]    insert-time activity (parts created, ratios).
--   [run_start, settle_end] merge work, most of which happens *after* the
--                           Spark job exits as the merge tree consolidates.
-- {settle_end} comes from wait_for_settle.py; if unset it defaults to
-- {run_end}, which under-counts merge cost.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  -- ====== Parts created during the run (window: run_end) ======
  SELECT 'ch_parts_created' AS metric_name, 'count' AS unit, toFloat64(count()) AS value
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND event_type = 'NewPart'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  -- 1.0 = each insert produced one part; > 1.0 = batches sprayed across CH partitions.
  SELECT 'ch_parts_per_insert', 'ratio',
         (SELECT toFloat64(count())
          FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
            AND event_type = 'NewPart'
            AND database = {ch_database:String} AND table = {ch_table:String})
         /
         greatest(
           (SELECT toFloat64(count())
            FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind = 'Insert'
              AND has(tables, {table_qualified:String})),
           1.0)
  UNION ALL
  -- ====== Merge metrics (window: settle_end) ======
  SELECT 'ch_merge_count', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  SELECT 'ch_merge_total_duration_ms', 'ms', toFloat64(sum(duration_ms))
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  SELECT 'ch_merge_p50_duration_ms', 'ms', quantile(0.50)(duration_ms)
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  SELECT 'ch_merge_p99_duration_ms', 'ms', quantile(0.99)(duration_ms)
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  SELECT 'ch_merge_input_rows', 'rows', toFloat64(sum(read_rows))
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  SELECT 'ch_merge_peak_memory_bytes', 'bytes', toFloat64(max(peak_memory_usage))
  FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND event_type = 'MergeParts'
    AND database = {ch_database:String} AND table = {ch_table:String}
  UNION ALL
  -- Merge amplification: total bytes merges had to read divided by total
  -- bytes inserted. Healthy ~ 2x; with tiny batches can be 10x+.
  -- Denominator unions Insert + AsyncInsertFlush filtered to written_bytes > 0:
  -- in async mode the bytes live on AsyncInsertFlush, in sync mode on Insert.
  SELECT 'ch_merge_amplification', 'ratio',
         (SELECT toFloat64(sum(read_bytes))
          FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
            AND event_type = 'MergeParts'
            AND database = {ch_database:String} AND table = {ch_table:String})
         /
         greatest(
           (SELECT toFloat64(sum(written_bytes))
            FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
              AND written_bytes > 0
              AND has(tables, {table_qualified:String})),
           1.0)
  UNION ALL
  -- Wall-clock between end-of-Spark and merges-quiesced: the post-ingest tail.
  SELECT 'ch_merge_tail_seconds', 'seconds',
         toFloat64(
           dateDiff('second',
             parseDateTimeBestEffort({run_end:String}),
             parseDateTimeBestEffort({settle_end:String})))
  UNION ALL
  -- Sanity-check from metric_log: total merge-thread time (should be in the
  -- same ballpark as ch_merge_total_duration_ms above). ProfileEvent_* in
  -- system.metric_log are cumulative since server start, so use max()-min()
  -- for the window delta, not sum().
  SELECT 'ch_merge_thread_time_ms', 'ms',
         toFloat64(max(ProfileEvent_MergeTotalMilliseconds) - min(ProfileEvent_MergeTotalMilliseconds))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
);
