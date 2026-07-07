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
-- Pre-run server covariates (plan §6.3). Captured on the target BEFORE the
-- ingest (pre-truncate) so every known environment noise source becomes a
-- filterable fact rather than an unexplained step-change:
--   ch_uptime             server uptime in seconds. A run whose uptime is LOWER
--                         than the previous run's = the service restarted
--                         between runs (cleared memory pressure, cold caches) —
--                         the documented cause of the ~20% throughput jumps.
--   pre_run_rss           resident memory (MemoryResident) at run start — how
--                         much memory pressure the run inherits.
--   pre_run_active_parts  active parts on the target table at run start —
--                         non-zero means the previous run's data / merge debt
--                         is still present (e.g. SKIP_TRUNCATE, or a failed
--                         end-of-run truncate).
--
-- These are point-in-time reads from system.asynchronous_metrics (current
-- snapshot, not the *_log) and system.parts, so they need no time window and
-- run correctly before RUN_END exists. Uptime/RSS are single-valued gauges;
-- any() picks the one row.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String} AS run_id, metric_name, unit, value FROM (
  SELECT 'ch_uptime' AS metric_name, 'seconds' AS unit,
         toFloat64(any(if(metric = 'Uptime', value, NULL))) AS value
  FROM remoteSecure({target_addr:String}, system.asynchronous_metrics, {target_user:String}, {target_password:String})
  WHERE metric = 'Uptime'
  UNION ALL
  SELECT 'pre_run_rss', 'bytes',
         toFloat64(any(if(metric = 'MemoryResident', value, NULL)))
  FROM remoteSecure({target_addr:String}, system.asynchronous_metrics, {target_user:String}, {target_password:String})
  WHERE metric = 'MemoryResident'
  UNION ALL
  SELECT 'pre_run_active_parts', 'count',
         toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.parts, {target_user:String}, {target_password:String})
  WHERE database = {ch_database:String} AND table = {ch_table:String} AND active
);
