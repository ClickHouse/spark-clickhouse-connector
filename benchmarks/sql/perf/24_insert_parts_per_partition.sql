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
-- Per-partition parts_per_insert (benchmark v2 plan §5 Tier-1 catalog,
-- "per-partition parts_per_insert [N] ... First visibility into partition-aware
-- write distribution (post-toYYYYMM)"). Added 2026-07-07 with the toYYYYMM
-- partition switch (plan §9 step 4): with ~12 real partitions the connector's
-- partition-aware write path (repartitionByPartition + localSortByPartition) is
-- exercised non-degenerately, and this file exposes how parts distribute across
-- those partitions — a spray across partitions is the #1 server-cost signal.
--
-- Window + table scoping mirror 13_insert_from_part_log.sql exactly:
--   [run_start, run_end] over system.part_log NewPart events, scoped to the
--   target database.table via remoteSecure(). One perf.metrics row PER PARTITION.
--
-- METRIC-NAME ENCODING (READ THIS):
--   metric_name = 'parts_per_insert.' || <partition value>
--   e.g. 'parts_per_insert.202307', 'parts_per_insert.202308', ...
--   <partition value> is system.part_log.partition (the human-readable partition
--   expression value; for PARTITION BY toYYYYMM(EventDate) this is the YYYYMM
--   integer as a string). This is a SYSTEMATIC, Spark-side-only encoding for
--   dashboard drill-down — it is NOT a cross-connector contract metric name
--   (contract §2 pins scalar names only; the base scalar `parts_per_insert` in
--   13_*.sql remains the contract signal). Consumers that want the per-partition
--   breakdown match the 'parts_per_insert.%' prefix; nothing in the contract or
--   the gated set depends on the individual partition suffixes.
--
-- Value definition (per partition P):
--   parts_per_insert.P = (NewPart events in P over the run window)
--                        / (total inserts into the table over the run window)
--   The denominator is the same table-scoped insert count as 13_*.sql (inserts
--   are not attributable to a single partition — one insert can spray parts
--   across several). So sum over partitions of parts_per_insert.P equals the
--   scalar parts_per_insert from 13_*.sql. unit = 'ratio'.
--
-- Cardinality: ~12 partitions/run (toYYYYMM over the ~12-month hits dataset), so
-- ~12 rows/run — negligible. Tier 1 ONLY: the Null target (Tier 0) creates no
-- parts, so part_log has no NewPart rows there; this file is not wired into the
-- Tier 0 capture list (see .github/actions/run-arm/action.yml).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT
  {run_id:String},
  concat('parts_per_insert.', partition) AS metric_name,
  'ratio' AS unit,
  toFloat64(count())
    /
    greatest(
      (SELECT toFloat64(count())
       FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
       WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
         AND type = 'QueryFinish' AND query_kind = 'Insert'
         AND has(tables, {table_qualified:String})),
      1.0) AS value
FROM remoteSecure({target_addr:String}, system.part_log, {target_user:String}, {target_password:String})
WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  AND event_type = 'NewPart'
  AND database = {ch_database:String} AND table = {ch_table:String}
GROUP BY partition;
