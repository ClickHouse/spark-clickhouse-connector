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
-- ClickBench load-test dashboard queries
--
-- All queries assume the perf.* schema from benchmarks/sql/perf/.
--
-- Building the dashboard in ClickHouse Cloud (SQL Console -> Dashboards):
--   1. For each `-- @query: NAME` block below, open a new SQL Console tab,
--      paste the SQL, Run, then Save the query under NAME.
--   2. Switch the result to the Chart tab and pick the `-- @viz` type:
--        line / multi-line -> Line  (X = t, Y = the value column[s])
--        histogram         -> Bar   (X = the *_bucket column, Y = n_inserts)
--        table             -> Table
--   3. Dashboards -> New dashboard "ClickBench Load Test" -> Add visualization
--      -> pick each saved query. Arrange by section (A, B, B2, C, then D/E).
--   4. Per-run drill-downs (section D) and the compare query (E) take
--      {run_id:String} / {run_a,run_b} parameters. Add a dashboard-level
--      filter (global filter) named `run_id` populated by:
--        SELECT run_id FROM perf.runs ORDER BY run_started_at DESC
--      and bind each parameterised panel's parameter to that filter, so the
--      whole dashboard follows the selected run (mirrors Grafana's $run_id).
--      Standalone in SQL Console, set the value via the Parameters panel.


-- ============================================================================
-- A. RUN-LEVEL OVERVIEW
-- ============================================================================

-- @query: runs_overview
-- @viz: table
-- Latest N benchmark runs with the headline metrics. The first thing to look
-- at after any benchmark run.
SELECT
  r.run_id,
  r.run_started_at,
  dateDiff('second', r.run_started_at, r.run_ended_at)              AS wall_clock_s,
  toUInt64OrZero(toString(any(if(m.metric_name='write_rows',              m.value, NULL)))) AS write_rows,
  round(any(if(m.metric_name='throughput_rows_per_sec',                   m.value, NULL)), 0) AS rows_per_sec,
  round(any(if(m.metric_name='throughput_mb_per_sec',                     m.value, NULL)), 2) AS mb_per_sec,
  round(any(if(m.metric_name='ch_avg_rows_per_insert',                    m.value, NULL)), 0) AS avg_batch_rows,
  toUInt64OrZero(toString(any(if(m.metric_name='ch_parts_created',        m.value, NULL)))) AS parts_created,
  round(any(if(m.metric_name='ch_connections_per_insert',                 m.value, NULL)), 3) AS conns_per_insert,
  r.connector_version,
  r.notes
FROM perf.runs r
LEFT JOIN perf.metrics m ON r.run_id = m.run_id
GROUP BY r.run_id, r.run_started_at, r.run_ended_at, r.connector_version, r.notes
ORDER BY r.run_started_at DESC
LIMIT 20;


-- ============================================================================
-- B. TREND LINES (one chart per metric, x-axis = run_started_at)
-- ============================================================================

-- @query: trend_throughput_rows_per_sec
-- @viz: line
-- @x: t
-- @y: rows_per_sec
-- Headline throughput trend. Plot every run.
SELECT r.run_started_at AS t, m.value AS rows_per_sec
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'throughput_rows_per_sec'
ORDER BY t;

-- @query: trend_e2e_duration
-- @viz: line
-- @x: t
-- @y: seconds
-- End-to-end wall clock per run. Lower is better.
SELECT r.run_started_at AS t, m.value AS seconds
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'e2e_duration'
ORDER BY t;

-- @query: trend_connections_per_insert
-- @viz: line
-- @x: t
-- @y: ratio
-- Connection-churn signal. Lower = better client cache behaviour.
-- 1.0 = cold connection per insert; << 1.0 = HTTP keep-alive in play.
SELECT r.run_started_at AS t, m.value AS ratio
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_connections_per_insert'
ORDER BY t;

-- @query: trend_batch_size
-- @viz: line
-- @x: t
-- @y: avg_rows_per_insert
-- Average rows per insert (real write rows). Bigger = fewer connections + fewer parts.
SELECT r.run_started_at AS t,
       round(m.value, 0) AS avg_rows_per_insert
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_avg_rows_per_insert'
ORDER BY t;

-- @query: trend_insert_latency
-- @viz: multi-line
-- @x: t
-- @y: p50_s, p99_s
-- Per-insert latency percentiles over time, in SECONDS (source metric is ms).
SELECT r.run_started_at AS t,
       any(if(m.metric_name='ch_insert_duration_p50_ms', m.value, NULL)) / 1000 AS p50_s,
       any(if(m.metric_name='ch_insert_duration_p99_ms', m.value, NULL)) / 1000 AS p99_s
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('ch_insert_duration_p50_ms', 'ch_insert_duration_p99_ms')
GROUP BY t ORDER BY t;

-- @query: trend_parts_created
-- @viz: line
-- @x: t
-- @y: parts
-- Total parts created per run. Lower = fewer merges needed.
SELECT r.run_started_at AS t, m.value AS parts
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_parts_created'
ORDER BY t;

-- @query: trend_merge_total_duration
-- @viz: line
-- @x: t
-- @y: merge_seconds
-- Total wall-clock spent merging. Lower = less server work after ingest.
SELECT r.run_started_at AS t, m.value / 1000 AS merge_seconds
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_merge_total_duration_ms'
ORDER BY t;

-- @query: trend_merge_amplification
-- @viz: line
-- @x: t
-- @y: amplification
-- Read bytes / written bytes during merges. Ideal ~ 2x. > 5x = small batches.
SELECT r.run_started_at AS t, round(m.value, 2) AS amplification
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_merge_amplification'
ORDER BY t;


-- ============================================================================
-- B2. SPARK SIDE (trend lines, x-axis = run_started_at)
-- ============================================================================

-- @query: trend_executor_cpu_time
-- @viz: line
-- @x: t
-- @y: executor_cpu_s
-- Total executor CPU-seconds across all tasks. Tracks the connector's CPU cost.
SELECT r.run_started_at AS t, m.value AS executor_cpu_s
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'executor_cpu_time_total'
ORDER BY t;

-- @query: trend_jvm_gc_time
-- @viz: line
-- @x: t
-- @y: jvm_gc_s
-- Total JVM GC time across tasks. Rising GC = memory pressure on executors.
SELECT r.run_started_at AS t, m.value AS jvm_gc_s
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'jvm_gc_time_total'
ORDER BY t;

-- @query: trend_peak_jvm_memory
-- @viz: multi-line
-- @x: t
-- @y: heap_bytes, offheap_bytes
-- Peak JVM heap and off-heap across executors (bytes). 0 under local[*]; real on EMR.
SELECT r.run_started_at AS t,
       any(if(m.metric_name='peak_jvm_heap_bytes',    m.value, NULL)) AS heap_bytes,
       any(if(m.metric_name='peak_jvm_offheap_bytes', m.value, NULL)) AS offheap_bytes
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('peak_jvm_heap_bytes','peak_jvm_offheap_bytes')
GROUP BY t ORDER BY t;

-- @query: trend_connector_time_split
-- @viz: multi-line
-- @x: t
-- @y: serialize_s, write_s
-- Connector task-time split: serialization vs the insert/write call (seconds).
-- Shows where the connector spends time as batch size / config changes.
SELECT r.run_started_at AS t,
       any(if(m.metric_name='serialize_time_total', m.value, NULL)) AS serialize_s,
       any(if(m.metric_name='write_time_total',     m.value, NULL)) AS write_s
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('serialize_time_total','write_time_total')
GROUP BY t ORDER BY t;

-- @query: trend_tasks
-- @viz: multi-line
-- @x: t
-- @y: task_count, task_failed
-- Spark task count vs failed tasks per run. Any failed > 0 = retries (look at
-- ch_* error counters to see why).
SELECT r.run_started_at AS t,
       any(if(m.metric_name='task_count',        m.value, NULL)) AS task_count,
       any(if(m.metric_name='task_failed_count', m.value, NULL)) AS task_failed
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('task_count','task_failed_count')
GROUP BY t ORDER BY t;


-- ============================================================================
-- C. STRESS / THROTTLING INDICATORS
-- ============================================================================

-- @query: trend_throttling
-- @viz: multi-line
-- @x: t
-- @y: delayed_inserts, rejected_inserts, failed_inserts, too_many_parts
-- Throttle counters. All zero = the merge pool is keeping up. Any non-zero =
-- the server is fighting the ingest.
SELECT r.run_started_at AS t,
       any(if(m.metric_name='ch_delayed_inserts_count',  m.value, NULL)) AS delayed_inserts,
       any(if(m.metric_name='ch_rejected_inserts_count', m.value, NULL)) AS rejected_inserts,
       any(if(m.metric_name='ch_failed_inserts',         m.value, NULL)) AS failed_inserts,
       any(if(m.metric_name='ch_too_many_parts_errors',  m.value, NULL)) AS too_many_parts
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('ch_delayed_inserts_count','ch_rejected_inserts_count',
                        'ch_failed_inserts','ch_too_many_parts_errors')
GROUP BY t ORDER BY t;

-- @query: trend_parts_vs_threshold
-- @viz: multi-line
-- @x: t
-- @y: parts_active_peak, threshold_delay, threshold_throw
-- Peak active parts vs the configured throttle thresholds. If parts_active_peak
-- approaches throttle_threshold_delay (1000), the server starts sleeping inserts.
SELECT r.run_started_at AS t,
       any(if(m.metric_name='ch_parts_active_peak',          m.value, NULL)) AS parts_active_peak,
       any(if(m.metric_name='ch_throttle_threshold_delay',   m.value, NULL)) AS threshold_delay,
       any(if(m.metric_name='ch_throttle_threshold_throw',   m.value, NULL)) AS threshold_throw
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name IN ('ch_parts_active_peak','ch_throttle_threshold_delay','ch_throttle_threshold_throw')
GROUP BY t ORDER BY t;

-- @query: trend_merge_pool_pct
-- @viz: line
-- @x: t
-- @y: percent
-- Merge pool peak utilisation. 100% = the merger is the bottleneck.
SELECT r.run_started_at AS t, round(m.value, 1) AS percent
FROM perf.metrics m JOIN perf.runs r USING (run_id)
WHERE m.metric_name = 'ch_merge_pool_peak_pct'
ORDER BY t;


-- ============================================================================
-- D. PER-RUN DRILL-DOWN (parameterised by {run_id:String})
-- ============================================================================

-- @query: drill_metrics_for_run
-- @viz: table
-- @param: run_id (String)
-- Everything captured for one specific run.
SELECT metric_name,
       argMax(value, recorded_at) AS value,
       any(unit) AS unit
FROM perf.metrics
WHERE run_id = {run_id:String}
GROUP BY metric_name
ORDER BY metric_name;

-- @query: drill_batch_size_distribution
-- @viz: histogram
-- @x: batch_size_bucket
-- @y: n_inserts
-- @param: run_id (String)
-- Histogram of insert batch sizes during a single run. Bimodal = some tail
-- inserts smaller than average (e.g. the last partial batch per Spark task).
SELECT
  floor(written_rows / 1000) * 1000 AS batch_size_bucket,
  count() AS n_inserts
FROM perf.ch_inserts
WHERE run_id = {run_id:String} AND written_rows > 0
GROUP BY batch_size_bucket
ORDER BY batch_size_bucket;

-- @query: drill_insert_latency_distribution
-- @viz: histogram
-- @x: duration_bucket_ms
-- @y: n_inserts
-- @param: run_id (String)
-- Histogram of per-insert flush durations. Long tail = throttling or merge
-- backpressure.
SELECT
  floor(query_duration_ms / 50) * 50 AS duration_bucket_ms,
  count() AS n_inserts
FROM perf.ch_inserts
WHERE run_id = {run_id:String}
GROUP BY duration_bucket_ms
ORDER BY duration_bucket_ms
LIMIT 100;

-- @query: drill_throughput_over_time_within_run
-- @viz: line
-- @x: t
-- @y: rows_written, mb_written
-- @param: run_id (String)
-- Per-second throughput within a single run. Reveals warmup/cooldown shape.
SELECT
  toStartOfInterval(event_time, INTERVAL 10 SECOND) AS t,
  sum(written_rows) AS rows_written,
  sum(written_bytes) / 1024 / 1024 AS mb_written
FROM perf.ch_inserts
WHERE run_id = {run_id:String}
GROUP BY t
ORDER BY t;


-- ============================================================================
-- E. CROSS-RUN COMPARISON (parameterised by 2 run_ids)
-- ============================================================================

-- @query: compare_two_runs
-- @viz: table
-- @param: run_a (String), run_b (String)
-- Side-by-side metric values for any two runs. Plug in run_ids from
-- runs_overview to see what changed.
SELECT
  metric_name,
  any(unit) AS unit,
  argMax(if(run_id = {run_a:String}, value, NULL), recorded_at) AS run_a,
  argMax(if(run_id = {run_b:String}, value, NULL), recorded_at) AS run_b,
  round(
    argMax(if(run_id = {run_a:String}, value, NULL), recorded_at) /
    nullIf(argMax(if(run_id = {run_b:String}, value, NULL), recorded_at), 0),
    3) AS ratio_a_over_b
FROM perf.metrics
WHERE run_id IN ({run_a:String}, {run_b:String})
GROUP BY metric_name
ORDER BY metric_name;
