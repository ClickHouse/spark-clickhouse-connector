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
-- Stability metric (benchmark-v2-plan §5, Tier 1):
--
--   ingest_rate_stability [CoV, ratio]
--       Coefficient of Variation of the per-minute insert rate:
--         CoV = stddevPop(rows_per_minute) / median(rows_per_minute)
--       where rows_per_minute = sum(written_rows) bucketed by
--       toStartOfMinute(event_time) over this run's rows in perf.ch_inserts.
--       Distinguishes a flat plateau (low CoV) from a sawtooth (high CoV) — the
--       throttling/stall pattern that a run-average throughput number hides.
--       Lower is steadier. Uses stddev/median (not stddev/mean) to match the
--       plan's CoV definition (§7 Tab 3) and stay robust to a few outlier minutes.
--
-- Zero new capture: reads perf.ch_inserts, which 15_capture_raw_inserts.sql
-- populates for this run_id earlier in the same capture loop (files run in
-- numeric order, so 15 lands before 19). Bucketing on event_time (the target's
-- insert time), so the rate reflects server-observed ingestion, not client
-- submission. A run with < 2 populated minute-buckets yields NULL (stddev of a
-- single point / degenerate median) and the row is simply omitted.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH per_minute AS (
  SELECT toStartOfMinute(event_time) AS minute,
         sum(written_rows)           AS rows_per_minute
  FROM perf.ch_inserts
  WHERE run_id = {run_id:String}
  GROUP BY minute
)
SELECT {run_id:String} AS run_id,
       'ingest_rate_stability' AS metric_name,
       'ratio' AS unit,
       toFloat64(stddevPop(rows_per_minute) / nullIf(median(rows_per_minute), 0)) AS value
FROM per_minute
HAVING count() >= 2;
