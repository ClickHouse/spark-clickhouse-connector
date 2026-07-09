-- =============================================================================
-- v2_drill_batch_size_distribution  —  Benchmark v2 Superset virtual dataset (C3)
-- =============================================================================
-- Purpose:
--   Tab-4 run-drill: distribution of per-insert batch sizes (written_rows) as a
--   histogram, one series per arm. Shows whether the two arms are actually
--   pushing comparably-sized batches (a divergent batch-size distribution can
--   explain a throughput/latency delta that is NOT a connector regression).
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 (data foundation — run drill).
-- Spec reference:  merged-dashboard-spec.md §3 / §6 (C3:
--   "v2_drill_batch_size_distribution — bar, series arm").
-- Contract reference:  docs/benchmark-v2-contract.md §1 (arm/pair_id/tier).
-- Added: 2026-07-08 (merged-dashboard build, Phase 0).
--
-- CONNECTOR SCOPING (2026-07-10): kafka rows now share these SAME DWH tables (runs
--   carries a first-class `connector` column; our rows are connector='spark').
--   Scoped to 'spark' so kafka runs never contaminate the Tab-4 drill; cross-
--   connector lives on kafka's Tab 5 per contract §6. FLAGGED NOTE: kafka's first
--   ingested pair carries a pre-correction flagged='true' spelling the flagged
--   predicate (runtime['flagged']='1') would misread as unflagged — connector
--   scoping moots that here; the contract pins '1'.
--
-- Source schema (VERIFIED against 04_create_ch_inserts.sql): the per-insert row
--   count is `written_rows` (UInt64). (Spec sketch guessed written_rows —
--   confirmed correct.)
--
-- Bucketing:  fixed 10k-row buckets (bucket = floor(written_rows/10000)*10000);
--   bucket_rows is the bucket's lower edge, suitable for a bar x-axis. The
--   nightly batch target is 100k (memory file loadtest_batchsize_sweep), so 10k
--   buckets give ~10 bars across the working range while staying stable if the
--   target moves.
--
-- Columns out:  pair_id, tier, arm, bucket_rows (bucket lower edge),
--               insert_count (# inserts in that bucket for that arm).
--
-- Notes:
--   * Successful inserts only (exception_code = 0) — a failed attempt's row
--     count is not a real batch delivered.
--   * pair_id is the RAW runtime['pair_id'] ('' when absent) — the SAME
--     convention as v_runs_enriched, so the Tab-4 native pair filter's value
--     set matches across datasets. Legacy pre-pair rows carry '' and are
--     unselectable by design (pairs did not exist then). No run_id fallback.
--   * Empty ch_inserts / no matching pair -> zero rows, no error.
-- =============================================================================
SELECT
  r.runtime['pair_id']                                  AS pair_id,
  coalesce(nullIf(r.runtime['tier'], ''), '1')          AS tier,
  coalesce(nullIf(r.runtime['arm'], ''),  'head')       AS arm,
  intDiv(ci.written_rows, 10000) * 10000                AS bucket_rows,
  count()                                               AS insert_count
FROM raw_connectors_load_testing.ch_inserts AS ci
INNER JOIN raw_connectors_load_testing.runs AS r ON ci.run_id = r.run_id
WHERE ci.exception_code = 0
  -- kafka rows share these tables since 2026-07-10; Spark dashboard datasets are
  -- connector-scoped; cross-connector lives on kafka's Tab 5 per contract §6.
  AND r.connector = 'spark'
  -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
  -- from all real trends (drill base row is a ch_inserts row with only run_id, so
  -- we exclude on BOTH the joined connector and the 'FIXTURE-' run_id prefix).
  -- (connector='spark' already excludes verdict_fixture — kept as belt-and-braces.)
  AND r.connector != 'verdict_fixture'
  AND NOT startsWith(ci.run_id, 'FIXTURE-')
GROUP BY pair_id, tier, arm, bucket_rows
ORDER BY arm, bucket_rows
