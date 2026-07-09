-- =============================================================================
-- v2_drill_insert_latency_distribution  —  Benchmark v2 Superset virtual dataset (C4)
-- =============================================================================
-- Purpose:
--   Tab-4 run-drill: distribution of per-insert latency (query_duration_ms) as a
--   histogram, one series per arm. The distributional companion to C2's
--   sequence view — shows a shifted median or a fatter tail on one arm at a
--   glance, independent of ordering within the run.
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 (data foundation — run drill).
-- Spec reference:  merged-dashboard-spec.md §3 / §6 (C4:
--   "v2_drill_insert_latency_distribution — bar, series arm").
-- Contract reference:  docs/benchmark-v2-contract.md §1 (arm/pair_id/tier).
-- Added: 2026-07-08 (merged-dashboard build, Phase 0).
--
-- Source schema (VERIFIED against 04_create_ch_inserts.sql): per-insert latency
--   is `query_duration_ms` (UInt64, milliseconds). (Spec sketch guessed
--   query_duration_ms — confirmed correct.)
--
-- Bucketing:  fixed 100ms latency buckets
--   (bucket_ms = floor(query_duration_ms/100)*100), lower edge as the bar
--   x-axis. 100ms granularity resolves the typical insert-latency range without
--   producing hundreds of near-empty bars.
--
-- Columns out:  pair_id, tier, arm, bucket_ms (bucket lower edge),
--               insert_count (# inserts in that bucket for that arm).
--
-- Notes:
--   * Successful inserts only (exception_code = 0); a failed attempt's duration
--     is a timeout/error artifact, not a delivered-batch latency.
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
  intDiv(ci.query_duration_ms, 100) * 100               AS bucket_ms,
  count()                                               AS insert_count
FROM raw_connectors_load_testing.ch_inserts AS ci
INNER JOIN raw_connectors_load_testing.runs AS r ON ci.run_id = r.run_id
WHERE ci.exception_code = 0
  -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
  -- from all real trends (drill base row is a ch_inserts row with only run_id, so
  -- we exclude on BOTH the joined connector and the 'FIXTURE-' run_id prefix).
  AND r.connector != 'verdict_fixture'
  AND NOT startsWith(ci.run_id, 'FIXTURE-')
GROUP BY pair_id, tier, arm, bucket_ms
ORDER BY arm, bucket_ms
