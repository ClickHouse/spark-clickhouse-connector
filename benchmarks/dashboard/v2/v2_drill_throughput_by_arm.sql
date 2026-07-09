-- =============================================================================
-- v2_drill_throughput_by_arm  —  Benchmark v2 Superset virtual dataset (C1)
-- =============================================================================
-- Purpose:
--   Tab-4 run-drill: instantaneous insert throughput over the life of a run,
--   bucketed into 10-second windows, one series per arm (head vs pinned). Lets
--   you see WHERE in the run the two arms diverge (ramp, steady state, tail).
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 (data foundation — run drill).
-- Spec reference:  merged-dashboard-spec.md §3 / §6 (C1: "v2_drill_throughput_
--   by_arm — 10s buckets, series arm"). Native pair_id filter (Tab-4 scope)
--   narrows to one pair; the arm series then shows head vs pinned.
-- Contract reference:  docs/benchmark-v2-contract.md §1 (arm/pair_id/tier).
-- Added: 2026-07-08 (merged-dashboard build, Phase 0).
--
-- Source schema (VERIFIED against benchmarks/sql/perf/04_create_ch_inserts.sql;
--   DWH mirror raw_connectors_load_testing.ch_inserts):
--     run_id String, event_time DateTime, query_duration_ms UInt64,
--     written_rows UInt64, written_bytes UInt64, memory_usage UInt64,
--     network_bytes UInt64, cpu_microseconds UInt64, exception_code Int32.
--   (The spec sketch guessed written_rows / query_duration_ms / event_time —
--    those happen to be the real column names, confirmed here.)
--
-- Columns out:  pair_id, tier, arm, bucket_ts (10s bucket start),
--               rows_per_sec (written_rows in bucket / 10).
--
-- Notes:
--   * Only SUCCESSFUL inserts count toward throughput (exception_code = 0).
--   * Arm/tier come from runs.runtime with the contract coalesce defaults
--     (arm->'head', tier->'1'), so legacy runs still tag as head/1.
--   * pair_id is the RAW runtime['pair_id'] ('' when absent) — the SAME
--     convention as v_runs_enriched, so the Tab-4 native pair filter's value
--     set matches across datasets. Legacy pre-pair rows carry '' and are
--     unselectable by design (pairs did not exist then). No run_id fallback.
--   * Empty ch_inserts (or no matching pair) -> zero rows, no error.
-- =============================================================================
SELECT
  r.runtime['pair_id']                                  AS pair_id,
  coalesce(nullIf(r.runtime['tier'], ''), '1')          AS tier,
  coalesce(nullIf(r.runtime['arm'], ''),  'head')       AS arm,
  toStartOfInterval(ci.event_time, INTERVAL 10 SECOND)  AS bucket_ts,
  sum(ci.written_rows) / 10.0                           AS rows_per_sec
FROM raw_connectors_load_testing.ch_inserts AS ci
INNER JOIN raw_connectors_load_testing.runs AS r ON ci.run_id = r.run_id
WHERE ci.exception_code = 0
GROUP BY pair_id, tier, arm, bucket_ts
ORDER BY bucket_ts, arm
