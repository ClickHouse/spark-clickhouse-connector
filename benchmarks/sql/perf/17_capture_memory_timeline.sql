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
-- Snapshots the target's server memory over the run window into
-- perf.ch_memory_timeline, so it survives the target's short system-log
-- retention. Total RSS comes from system.asynchronous_metric_log; the merge
-- component (CurrentMetric_MergesMutationsMemoryTracking) is only in
-- system.metric_log, so the two are joined per 10s bucket.

INSERT INTO perf.ch_memory_timeline
SELECT
  {run_id:String} AS run_id,
  a.ts AS ts,
  toUInt64(a.total_rss)            AS total_rss_bytes,
  toUInt64(a.tracked)              AS tracked_bytes,
  toUInt64(greatest(0, ifNull(m.merge_bytes, 0))) AS merge_bytes,  -- the metric can go transiently negative
  toUInt64(a.queries)              AS queries_bytes,
  toUInt64(a.cgroup_used)          AS cgroup_used_bytes
FROM (
  SELECT toStartOfInterval(event_time, INTERVAL 10 SECOND) AS ts,
         max(if(metric = 'MemoryResident', value, 0))    AS total_rss,
         max(if(metric = 'TrackedMemory', value, 0))      AS tracked,
         max(if(metric = 'QueriesMemoryUsage', value, 0)) AS queries,
         max(if(metric = 'CGroupMemoryUsed', value, 0))   AS cgroup_used
  FROM remoteSecure({target_addr:String}, system.asynchronous_metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
    AND metric IN ('MemoryResident', 'TrackedMemory', 'QueriesMemoryUsage', 'CGroupMemoryUsed')
  GROUP BY ts
) AS a
LEFT JOIN (
  SELECT toStartOfInterval(event_time, INTERVAL 10 SECOND) AS ts,
         max(CurrentMetric_MergesMutationsMemoryTracking) AS merge_bytes
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  GROUP BY ts
) AS m USING (ts)
ORDER BY ts;
