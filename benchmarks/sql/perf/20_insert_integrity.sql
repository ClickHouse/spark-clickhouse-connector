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
-- Post-settle integrity verification (plan §6.1, contract §2.1). We verify the
-- write landed every source row exactly once by comparing target vs SOURCE on
-- BOTH count() and uniqExact(WatchID).
--
-- IMPORTANT: the ClickBench hits dataset itself contains repeated WatchID values
-- (WatchID is not unique in the source). So `count() - uniqExact(WatchID)` on the
-- target is NON-ZERO on a perfectly correct load — computing duplicate_rows that
-- way false-positives every run. duplicate_rows is the target-vs-SOURCE delta.
--
--   rows_delivered    target count() — read via remoteSecure like every other
--                     capture SQL: clickbench.hits lives on the TARGET service,
--                     not on the metrics service this INSERT runs on. The
--                     aggregation pushes to the remote, so count()/uniqExact()
--                     execute target-side.
--   rows_expected     source count(). For the default full glob this is the
--                     constant pinned in the workflow env (SOURCE_ROWS_EXPECTED);
--                     run_metrics_sql.py re-derives it from the input glob via
--                     s3() ONLY for non-default (smoke/override) globs. Per-run
--                     re-derivation of the full source is deliberately avoided:
--                     uniqExact over ~100M rows is a full WatchID scan with
--                     GB-scale hash state — not a "cheap footer read".
--   unique_delivered  target uniqExact(WatchID)
--   unique_expected   source uniqExact(WatchID) (same constant-vs-derived rule,
--                     pinned as SOURCE_UNIQUE_EXPECTED)
--   duplicate_rows    rows_delivered - rows_expected. 0 = every source row landed
--                     exactly once. >0 = a retry re-sent committed work the server
--                     did NOT dedup (extra rows). <0 = rows lost.
--   integrity_ok      1 iff rows_delivered == rows_expected AND
--                     unique_delivered == unique_expected. The workflow reads this
--                     back and FAILS the run on 0 (contract §3: integrity mismatch
--                     fails outright, unlike the flagged-not-failed guards).
--   ch_dedup_dropped_blocks  server-side context: DuplicatedInsertedBlocks
--                     ProfileEvent delta over the window — retried batches the
--                     server absorbed as duplicate-drops (the benign counterpart
--                     to duplicate_rows). Cumulative counter, so window delta is
--                     max()-min(), matching 16's pattern.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH
  delivered AS (
    SELECT count() AS rows_delivered, uniqExact(WatchID) AS unique_delivered
    FROM remoteSecure({target_addr:String}, {ch_database:String}, {ch_table:String}, {target_user:String}, {target_password:String})
  ),
  dedup AS (
    SELECT toFloat64(max(ProfileEvent_DuplicatedInsertedBlocks) -
                     min(ProfileEvent_DuplicatedInsertedBlocks)) AS dropped_blocks
    FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
    WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  )
SELECT {run_id:String} AS run_id, metric_name, unit, value FROM (
  SELECT 'rows_delivered' AS metric_name, 'count' AS unit,
         toFloat64((SELECT rows_delivered FROM delivered)) AS value
  UNION ALL
  SELECT 'rows_expected', 'count',
         {rows_expected:Float64}
  UNION ALL
  SELECT 'unique_delivered', 'count',
         toFloat64((SELECT unique_delivered FROM delivered))
  UNION ALL
  SELECT 'unique_expected', 'count',
         {unique_expected:Float64}
  UNION ALL
  -- Target-vs-source row delta (NOT count-minus-uniqExact — see header).
  SELECT 'duplicate_rows', 'count',
         toFloat64((SELECT rows_delivered FROM delivered)) - {rows_expected:Float64}
  UNION ALL
  SELECT 'integrity_ok', 'bool',
         toFloat64(
           toFloat64((SELECT rows_delivered FROM delivered)) = {rows_expected:Float64}
           AND toFloat64((SELECT unique_delivered FROM delivered)) = {unique_expected:Float64})
  UNION ALL
  SELECT 'ch_dedup_dropped_blocks', 'count',
         greatest(0, (SELECT dropped_blocks FROM dedup))
);
