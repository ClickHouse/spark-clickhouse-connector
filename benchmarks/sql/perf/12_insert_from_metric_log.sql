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
-- Connection-churn metrics from system.metric_log: ch_connections_per_insert
-- is ~1.0 today (one cold connection per insert) and should drop to << 1.0
-- once the connector caches an HTTP client per executor.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  SELECT 'ch_http_connections_created' AS metric_name, 'count' AS unit,
         toFloat64(max(ProfileEvent_HTTPConnectionsCreated) - min(ProfileEvent_HTTPConnectionsCreated)) AS value
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  SELECT 'ch_http_connections_preserved', 'count',
         toFloat64(max(ProfileEvent_HTTPConnectionsPreserved) - min(ProfileEvent_HTTPConnectionsPreserved))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  -- Ratio: connections opened / inserts. The smoking gun.
  -- 1.0 = one cold connection per insert (current behaviour).
  -- << 1.0 = one connection serves many inserts (target).
  SELECT 'ch_connections_per_insert', 'ratio',
         (SELECT toFloat64(max(ProfileEvent_HTTPConnectionsCreated) - min(ProfileEvent_HTTPConnectionsCreated))
          FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String}))
         /
         greatest(
           (SELECT toFloat64(count())
            FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind = 'Insert'
              -- Scope to our table; without this, CH-Cloud-internal billing
              -- inserts inflate the denominator and deflate the ratio.
              AND has(tables, {table_qualified:String})),
           1.0)
  UNION ALL
  -- Peak concurrent HTTP connections. Verifies the server isn't saturated post-fix.
  SELECT 'ch_http_connections_peak', 'count',
         toFloat64(max(CurrentMetric_HTTPConnection))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  -- Background merge queue: anything > 100 means merges are falling behind the ingest.
  SELECT 'ch_bg_merge_queue_peak', 'count',
         toFloat64(max(CurrentMetric_BackgroundMergesAndMutationsPoolTask))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
);
