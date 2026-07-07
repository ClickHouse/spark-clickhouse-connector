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
-- Post-settle integrity verification (plan §6.1). After merges settle we
-- verify the write actually landed every source row exactly once:
--   rows_delivered        target count()
--   rows_expected         count() of the source parquet glob (ground truth)
--   duplicate_rows        rows_delivered - uniqExact(WatchID); >0 means the
--                         same logical row landed more than once (retry re-sent
--                         committed work and the server did NOT dedup it)
--   integrity_ok          1 iff rows_delivered == rows_expected AND
--                         duplicate_rows == 0; the workflow reads this back and
--                         fails the run on 0 (plan §6.10: integrity mismatch
--                         fails outright, unlike the flagged-not-failed guards)
--   ch_dedup_dropped_blocks  server-side context: DuplicatedInsertedBlocks
--                         ProfileEvent delta over the window — retried batches
--                         the server absorbed as duplicate-drops (the benign
--                         counterpart to duplicate_rows). Cumulative counter, so
--                         window delta is max()-min(), matching 16's pattern.
--
-- rows_expected is counted straight off the source parquet via s3() so the
-- ground truth is re-derived every run from the exact input glob (no hard-coded
-- dataset size that drifts when the glob changes for a smoke run). WatchID is
-- the ClickBench per-event id; uniqExact is exact (not approximate) so a single
-- duplicated row is detectable.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH
  delivered AS (
    SELECT count() AS rows_delivered, uniqExact(WatchID) AS rows_unique
    FROM {table_qualified:Identifier}
  ),
  expected AS (
    -- Public dataset: NOSIGN. {source_glob} is the s3:// form of the ingest
    -- glob; count(*) over Parquet reads only footer row-group metadata, so this
    -- is cheap even over the full 100-file glob.
    SELECT count() AS rows_expected
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
  SELECT 'duplicate_rows', 'count',
         toFloat64((SELECT rows_delivered FROM delivered) - (SELECT rows_unique FROM delivered))
  UNION ALL
  SELECT 'integrity_ok', 'bool',
         toFloat64(
           (SELECT rows_delivered FROM delivered) = (SELECT rows_expected FROM expected)
           AND (SELECT rows_delivered FROM delivered) = (SELECT rows_unique FROM delivered))
  UNION ALL
  SELECT 'ch_dedup_dropped_blocks', 'count',
         greatest(0, (SELECT dropped_blocks FROM dedup))
);
