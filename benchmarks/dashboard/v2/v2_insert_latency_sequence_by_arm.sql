-- =============================================================================
-- v2_insert_latency_sequence_by_arm  —  Benchmark v2 Superset virtual dataset (C2)
-- =============================================================================
-- Purpose:
--   Tab-4 run-drill: per-insert latency plotted against the insert's ordinal
--   position within the run (insert_seq), one series per arm. Reveals latency
--   drift/growth across a run (e.g. a tail that only one arm develops) that a
--   distribution histogram (C4) would flatten. This is the v2 dual-series
--   companion of the legacy 5608 insert_duration_sequence.
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 (data foundation — run drill).
-- Spec reference:  merged-dashboard-spec.md §3 / §6 (C2:
--   "v2_insert_latency_sequence_by_arm — x=insert_seq, series arm"); §1 notes
--   5608 is cloned dual-series for Tab 4.
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
-- Source schema (VERIFIED against 04_create_ch_inserts.sql): run_id, event_time,
--   query_duration_ms, written_rows, ... exception_code. There is NO explicit
--   sequence column, so insert_seq is derived as the row_number over event_time
--   within each run (ties broken by written_bytes for determinism).
--
-- Columns out:  pair_id, tier, arm, insert_seq (1-based), query_duration_ms,
--               exception_code (0 = success; non-zero marks a FAILED insert so
--               charts can distinguish a failure point from a merely-slow one,
--               e.g. via a series/tooltip dimension or a point-style rule).
--
-- Notes:
--   * insert_seq is per-run, so head and pinned each start at 1 and overlay.
--   * All inserts included (failed ones carry latency too, and the sequence
--     matters); exception_code is PROJECTED so the failure/slow distinction is
--     actionable in the chart, not just theoretically filterable.
--   * pair_id is the RAW runtime['pair_id'] ('' when absent) — the SAME
--     convention as v_runs_enriched, so the Tab-4 native pair filter's value
--     set matches across datasets. Legacy pre-pair rows carry '' and are
--     unselectable by design (pairs did not exist then). No run_id fallback.
--   * Empty ch_inserts / no matching pair -> zero rows, no error.
-- =============================================================================
SELECT
  pair_id,
  tier,
  arm,
  row_number() OVER (PARTITION BY run_id ORDER BY event_time, written_bytes) AS insert_seq,
  query_duration_ms,
  exception_code
FROM (
  SELECT
    ci.run_id                                             AS run_id,
    r.runtime['pair_id']                                  AS pair_id,
    coalesce(nullIf(r.runtime['tier'], ''), '1')          AS tier,
    coalesce(nullIf(r.runtime['arm'], ''),  'head')       AS arm,
    ci.event_time                                         AS event_time,
    ci.written_bytes                                      AS written_bytes,
    ci.query_duration_ms                                  AS query_duration_ms,
    ci.exception_code                                     AS exception_code
  FROM raw_connectors_load_testing.ch_inserts AS ci
  INNER JOIN raw_connectors_load_testing.runs AS r ON ci.run_id = r.run_id
  -- kafka rows share these tables since 2026-07-10; Spark dashboard datasets are
  -- connector-scoped; cross-connector lives on kafka's Tab 5 per contract §6.
  WHERE r.connector = 'spark'
  -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
  -- from all real trends (drill base row is a ch_inserts row with only run_id, so
  -- we exclude on BOTH the joined connector and the 'FIXTURE-' run_id prefix).
  -- (connector='spark' already excludes verdict_fixture — kept as belt-and-braces.)
    AND r.connector != 'verdict_fixture'
    AND NOT startsWith(ci.run_id, 'FIXTURE-')
)
ORDER BY arm, insert_seq
