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
-- Peak server memory over the run window, so memory pressure shows up per run
-- (the per-query/per-merge peak metrics alone don't reveal the server total
-- that MEMORY_LIMIT_EXCEEDED / code 241 inserts hit). Total RSS and query memory
-- come from system.asynchronous_metric_log; the merge component is only in
-- system.metric_log (and can go transiently negative, so it is clamped).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String} AS run_id, metric_name, 'bytes' AS unit, value
FROM (
  SELECT 'ch_peak_server_memory_bytes' AS metric_name,
         toFloat64(max(if(metric = 'MemoryResident', value, 0))) AS value
  FROM remoteSecure({target_addr:String}, system.asynchronous_metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND metric = 'MemoryResident'
  UNION ALL
  SELECT 'ch_peak_queries_memory_bytes',
         toFloat64(max(if(metric = 'QueriesMemoryUsage', value, 0)))
  FROM remoteSecure({target_addr:String}, system.asynchronous_metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND metric = 'QueriesMemoryUsage'
  UNION ALL
  SELECT 'ch_peak_merge_memory_bytes',
         toFloat64(greatest(0, max(CurrentMetric_MergesMutationsMemoryTracking)))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
)
WHERE value > 0;
