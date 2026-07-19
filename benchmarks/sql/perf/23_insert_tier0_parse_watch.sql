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
-- TIER 0 PARSE-WATCH (redesign 2026-07-07) — MANDATORY on every Cloud-hosted
-- Null Tier-0 row (contract §1.1 / §1.4 / §2.1). Tier 0 now runs against an
-- ENGINE=Null table ON THE CLOUD TARGET (clickbench.hits_null), so its
-- server-side capture is a NORMAL remoteSecure() read of the target's
-- system.query_log — exactly like the perf/1x Tier-1 files — NOT the on-master
-- Docker read that the deleted benchmarks/sql/tier0/12_server_side_tier0.sql did.
-- This file ports that formula to the remoteSecure()/9440 shape.
--
--   ch_insert_cpu_share_tier0 [percent]
--       = sum(insert OSCPUVirtualTimeMicroseconds)/1e6 over the window,
--         divided by wall-clock (run_end - run_start), x100.
--
-- Instrument-health watch: if it camps near 100% the Null target is parse-bound
-- and Tier 0 is measuring the server, not the connector. Because a Cloud-hosted
-- Null target has NO pinned instrument version (tier0_ch_version is omitted;
-- the Cloud version drifts, tracked by the clickhouse_version covariate), this
-- parse-watch is the mandatory guard the contract requires INSTEAD (§1.4). The
-- design deliberately lets Cloud drift enter Tier 0, guarded by this metric
-- (plan §3, user-accepted for symmetry with Kafka's Cloud-hosted Null).
--
-- SCOPING: run_metrics_sql.py binds {table_qualified} from CH_DATABASE.CH_TABLE,
-- which the t0 capture step sets to clickbench.hits_null — so has(tables) scopes
-- the query_log read to the Null table, disjoint from the Tier-1 hits reads that
-- share the same target service and windows. remoteSecure()/9440 reaches the
-- Cloud target (see perf/11 header).
--
-- NB: a Null-target Insert DOES populate query_log.OSCPUVirtualTimeMicroseconds
-- (the server parses the block before discarding it), so the numerator is
-- non-zero (verified 2026-07-07 on ENGINE=Null; see the deleted Docker file's
-- header for the original derivation).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, 'ch_insert_cpu_share_tier0' AS metric_name, 'percent' AS unit,
       (sumIf(toFloat64(ProfileEvents['OSCPUVirtualTimeMicroseconds']),
              type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, {table_qualified:String})) / 1e6)
       / nullIf(dateDiff('second',
                  parseDateTimeBestEffort({run_start:String}),
                  parseDateTimeBestEffort({run_end:String})), 0)
       * 100.0 AS value
FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String});
