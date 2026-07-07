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
-- TIER 0 (benchmark-v2-plan §3 / §5) server-side capture — the Docker-CH,
-- ENGINE=Null counterpart of the Tier-1 CH-side SQL (perf/12, perf/18).
--
-- EXECUTION MODEL (differs from every perf/1x file): this is run ON THE EMR
-- MASTER against the LOCAL Docker ClickHouse, via
--   sudo docker exec -i tier0-clickhouse clickhouse-client --multiquery \
--     --param_run_id=... --param_run_start=... --param_run_end=... \
--     --param_table_qualified=tier0.hits
-- (see benchmarks/tier0/capture_tier0_onmaster.sh). It queries the container's
-- OWN system.query_log / system.metric_log directly — NOT remoteSecure() — so
-- there is no target_addr/target_user/target_password here. The container's
-- config.d override (bootstrap_tier0_ch.sh) guarantees these log tables exist.
-- clickhouse-client binds {name:Type} placeholders from --param_<name>; run_id /
-- run_start / run_end / table_qualified are passed positionally-safe (no shell
-- interpolation into the SQL text — injection-safe).
--
-- WINDOWING: the Docker CH accumulates BOTH arms' inserts across the pair (it is
-- never truncated between arms), so — exactly like the Tier-1 CH-side files —
-- every read is windowed `event_time BETWEEN {run_start} AND {run_end}` to the
-- arm's own ingest window.  The pass-level run_start/run_end come from the
-- t0 submit window (RUN_START/RUN_END for the -t0 run_id).
--
-- The insert-count denominator and the target-table scope mirror perf/12 exactly
-- (QueryFinish + query_kind='Insert' + has(tables, {table_qualified})).
--
-- Metrics emitted (contract §2.1):
--   connections_per_insert     [ratio]   (Amendment 2026-07-07; SAME spelling ALL tiers)
--   bytes_on_wire_per_row      [bytes/row]
--   ch_insert_cpu_share_tier0  [percent] (Tier-0-only instrument-health watch)

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (

  -- connections_per_insert: new server connections created / insert count over
  -- the window (metric_log connection counters). Mirrors perf/12's logic and
  -- the pinned contract spelling.
  --
  -- COUNTER CHOICE — VERIFY ON FIRST CLUSTER RUN: perf/12 (Tier 1, against a
  -- Cloud target) reads ProfileEvent_HTTPConnectionsCreated, so this file uses
  -- the IDENTICAL counter for cross-tier comparability. An empirical local test
  -- (real clickhouse-server receiving direct HTTP inserts) showed that on the
  -- RECEIVING server it is ProfileEvent_HTTPServerConnectionsCreated that
  -- increments, while ProfileEvent_HTTPConnectionsCreated (the client-side
  -- OUTBOUND pool counter) stays 0. If the first cluster run reports
  -- connections_per_insert == 0 for Tier 0, switch the two counter references
  -- below to ProfileEvent_HTTPServerConnectionsCreated (server-inbound) and note
  -- the divergence from Tier 1 on the dashboard.
  SELECT 'connections_per_insert' AS metric_name, 'ratio' AS unit,
         (SELECT toFloat64(max(ProfileEvent_HTTPConnectionsCreated) - min(ProfileEvent_HTTPConnectionsCreated))
          FROM system.metric_log
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String}))
         /
         greatest(
           (SELECT toFloat64(count())
            FROM system.query_log
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind = 'Insert'
              AND has(tables, {table_qualified:String})),
           1.0) AS value

  UNION ALL

  -- bytes_on_wire_per_row: server NetworkReceiveBytes on Insert receipts (post-
  -- compression ingress) / sum(written_rows) over the window. VERIFIED on
  -- ENGINE=Null (2026-07-07, real clickhouse-server): a Null-target Insert
  -- populates BOTH query_log.written_rows AND ProfileEvents['NetworkReceiveBytes']
  -- (Null parses the block before discarding), so this denominator is non-zero.
  SELECT 'bytes_on_wire_per_row', 'bytes/row',
         sumIf(ProfileEvents['NetworkReceiveBytes'],
               type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, {table_qualified:String}))
         / nullIf(sumIf(written_rows,
               type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, {table_qualified:String})), 0)
  FROM system.query_log
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})

  UNION ALL

  -- ch_insert_cpu_share_tier0: sum(insert OSCPUVirtualTimeMicroseconds)/1e6 over
  -- the window, divided by wall-clock (run_end - run_start), x100. Instrument-
  -- health watch: if it camps near 100 the Null target is parse-bound and Tier 0
  -- is measuring the server, not the connector (contract §2.1).
  -- NB (verify on first cluster run): OSCPUVirtualTimeMicroseconds was 0 in the
  -- local macOS test (per-thread CPU clocks not exposed on Darwin); it populates
  -- on Linux Docker CH, which is where this always runs.
  SELECT 'ch_insert_cpu_share_tier0', 'percent',
         (sumIf(toFloat64(ProfileEvents['OSCPUVirtualTimeMicroseconds']),
                type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, {table_qualified:String})) / 1e6)
         / nullIf(dateDiff('second',
                    parseDateTimeBestEffort({run_start:String}),
                    parseDateTimeBestEffort({run_end:String})), 0)
         * 100.0
  FROM system.query_log
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
);
