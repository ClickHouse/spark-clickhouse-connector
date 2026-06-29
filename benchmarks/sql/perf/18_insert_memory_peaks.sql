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
-- Peak server memory for this run, derived from the timeline 17 just wrote into
-- perf.ch_memory_timeline. Stored as scalar perf.metrics rows so they ride the
-- existing metrics pipeline to the DWH (no separate table/ingestion needed),
-- which lets the dashboard show peak RSS vs the cgroup cap per run.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String} AS run_id, metric_name, 'bytes' AS unit, value
FROM (
  SELECT 'ch_peak_server_memory_bytes'  AS metric_name, toFloat64(max(total_rss_bytes)) AS value FROM perf.ch_memory_timeline WHERE run_id = {run_id:String}
  UNION ALL
  SELECT 'ch_peak_merge_memory_bytes',   toFloat64(max(merge_bytes))     FROM perf.ch_memory_timeline WHERE run_id = {run_id:String}
  UNION ALL
  SELECT 'ch_peak_queries_memory_bytes', toFloat64(max(queries_bytes))   FROM perf.ch_memory_timeline WHERE run_id = {run_id:String}
)
WHERE value > 0;
