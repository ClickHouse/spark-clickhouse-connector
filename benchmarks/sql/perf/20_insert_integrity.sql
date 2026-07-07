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
-- Post-settle integrity verification (plan §6.1). We verify the write landed
-- every source row exactly once by comparing target vs SOURCE on BOTH count()
-- and uniqExact(WatchID).
--
-- IMPORTANT: the ClickBench hits dataset itself contains repeated WatchID values
-- (WatchID is not unique in the source). So `count() - uniqExact(WatchID)` on the
-- target is NON-ZERO on a perfectly correct load — computing duplicate_rows that
-- way false-positives every run and zeroes the trend. duplicate_rows must be the
-- target-vs-SOURCE delta instead: how many MORE rows / distinct WatchIDs the
-- target holds than the source actually had.
--
--   rows_delivered    target count()
--   rows_expected     source count() (re-derived from the exact input glob)
--   unique_delivered  target uniqExact(WatchID)
--   unique_expected   source uniqExact(WatchID)
--   duplicate_rows    rows_delivered - rows_expected. 0 = every source row landed
--                     exactly once. >0 = a retry re-sent committed work the server
--                     did NOT dedup (extra rows). <0 = rows lost.
--   integrity_ok      1 iff rows_delivered == rows_expected AND
--                     unique_delivered == unique_expected. The workflow reads this
--                     back and FAILS the run on 0 (plan §6.10: integrity mismatch
--                     fails outright, unlike the flagged-not-failed guards).
--   ch_dedup_dropped_blocks  server-side context: DuplicatedInsertedBlocks
--                     ProfileEvent delta over the window — retried batches the
--                     server absorbed as duplicate-drops (the benign counterpart
--                     to duplicate_rows). Cumulative counter, so window delta is
--                     max()-min(), matching 16's pattern.
--
-- Both source values are re-derived every run from the exact input glob via s3()
-- (Parquet count() reads only footer metadata; uniqExact scans WatchID once),
-- so the check auto-adapts to a single-file smoke glob instead of relying on a
-- hard-coded full-dataset size. The full-dataset reference constants are recorded
-- in the workflow env (SOURCE_ROWS_EXPECTED / SOURCE_UNIQUE_EXPECTED) as a
-- documented cross-check for the nightly full-glob run.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH
  delivered AS (
    SELECT count() AS rows_delivered, uniqExact(WatchID) AS unique_delivered
    FROM {table_qualified:Identifier}
  ),
  expected AS (
    -- Public dataset: NOSIGN. {source_glob} is the s3:// form of the ingest glob.
    SELECT count() AS rows_expected, uniqExact(WatchID) AS unique_expected
    FROM s3({source_glob:String}, NOSIGN, 'Parquet')
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
         toFloat64((SELECT rows_expected FROM expected))
  UNION ALL
  SELECT 'unique_delivered', 'count',
         toFloat64((SELECT unique_delivered FROM delivered))
  UNION ALL
  SELECT 'unique_expected', 'count',
         toFloat64((SELECT unique_expected FROM expected))
  UNION ALL
  -- Target-vs-source row delta (NOT count-minus-uniqExact — see header).
  SELECT 'duplicate_rows', 'count',
         toFloat64((SELECT rows_delivered FROM delivered) - (SELECT rows_expected FROM expected))
  UNION ALL
  SELECT 'integrity_ok', 'bool',
         toFloat64(
           (SELECT rows_delivered FROM delivered) = (SELECT rows_expected FROM expected)
           AND (SELECT unique_delivered FROM delivered) = (SELECT unique_expected FROM expected))
  UNION ALL
  SELECT 'ch_dedup_dropped_blocks', 'count',
         greatest(0, (SELECT dropped_blocks FROM dedup))
);
