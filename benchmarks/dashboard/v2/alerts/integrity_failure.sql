-- =============================================================================
-- ALERT: integrity failure  ==>  the run FAILS (no headline)
-- =============================================================================
-- Shares the SQL semantics of the Tab 1 "Integrity rows OK" tile
-- (chart "v2: Integrity rows OK" 5697, dataset v2_runs_enriched): that tile shows
-- the latest run's duplicate_rows (0 = OK); this alert surfaces ANY run whose
-- integrity comparison failed.
--
-- Contract: docs/benchmark-v2-contract.md §3.1 — "Integrity mismatch => the run
--   FAILS. If rows_delivered != rows_expected OR duplicate_rows != 0, the run
--   FAILS: it produces NO headline number." Plan §6.1.
--   NB: integrity that could not be COMPUTED is `integrity_unverified` (a FLAG,
--   §1.3), NOT a failure — this alert fires ONLY on a computed mismatch.
--
-- Trips when: a run has a computed integrity mismatch
--   (rows_delivered != rows_expected  OR  unique_delivered != unique_expected
--    OR  duplicate_rows != 0). One row per failing run.
--
-- Channel wiring is a FLAGGED follow-up (open decision 3) — see README.md.
-- Non-empty result set == alert should fire; attach `run_link` per row.
--
-- CONNECTOR SCOPING (2026-07-10): kafka rows now share these SAME DWH tables (runs
--   carries a first-class `connector` column; our rows are connector='spark'). This
--   alert fires on SPARK integrity failures only, so its runs CTE is scoped to
--   'spark'. Cross-connector lives on kafka's Tab 5 per contract §6. FLAGGED NOTE:
--   kafka's first ingested pair carries a pre-correction flagged='true' spelling the
--   flagged predicate (runtime['flagged']='1') would misread as unflagged —
--   connector scoping moots that here; contract pins '1'.
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- Empty today. MUST NOT error on the empty dataset.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    -- contract §3: belt for the fixture rows on a metrics-only join (metrics has
    -- no connector col; fixture identity is run_id 'FIXTURE-*'). Braces is the
    -- connector != 'verdict_fixture' filter on the runs scope below.
    WHERE NOT startsWith(run_id, 'FIXTURE-')
    GROUP BY run_id, metric_name
  ),
  pivot AS (
    SELECT
      run_id,
      max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
      max(if(metric_name = 'rows_delivered',    value, NULL)) AS rows_delivered,
      max(if(metric_name = 'rows_expected',     value, NULL)) AS rows_expected,
      max(if(metric_name = 'unique_delivered',  value, NULL)) AS unique_delivered,
      max(if(metric_name = 'unique_expected',   value, NULL)) AS unique_expected,
      max(if(metric_name = 'duplicate_rows',    value, NULL)) AS duplicate_rows
    FROM m GROUP BY run_id
  ),
  runs AS (
    SELECT
      r.run_id                                          AS run_id,
      r.run_started_at                                  AS run_started_at,
      r.runtime['pair_id']                              AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''),  'head')   AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')      AS tier,
      p.integrity_ok_metric, p.rows_delivered, p.rows_expected,
      p.unique_delivered, p.unique_expected, p.duplicate_rows
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN pivot AS p ON r.run_id = p.run_id
    -- kafka rows share these tables since 2026-07-10; Spark dashboard datasets are
    -- connector-scoped; cross-connector lives on kafka's Tab 5 per contract §6.
    -- This alert fires on SPARK integrity failures only — a kafka mismatch would
    -- cross-fire, so scope positively to 'spark'.
    WHERE r.connector = 'spark'
    -- contract §3: exclude the reserved verdict-fixture connector (a CI truth
    -- table, never a real run) — matches the consumer views' predicate so a
    -- mirrored fixture row can never raise a spurious INTEGRITY_MISMATCH.
    -- (connector='spark' already excludes it — kept as belt-and-braces.)
      AND r.connector != 'verdict_fixture'
  )
SELECT
  run_id,
  run_started_at,
  pair_id,
  arm,
  tier,
  rows_delivered,
  rows_expected,
  unique_delivered,
  unique_expected,
  duplicate_rows,
  'INTEGRITY_MISMATCH — RUN FAILS' AS verdict,
  concat('https://superset.clickhouse-dev.com/superset/dashboard/427/?run_id=', run_id) AS run_link
FROM runs
-- Only runs whose integrity was actually COMPUTED (both sides present); an
-- absent comparison is `integrity_unverified` (a flag), not a failure — excluded.
WHERE rows_delivered IS NOT NULL AND rows_expected IS NOT NULL
  AND unique_delivered IS NOT NULL AND unique_expected IS NOT NULL
  AND (
    rows_delivered != rows_expected
    OR unique_delivered != unique_expected
    OR coalesce(duplicate_rows, 0) != 0
  )
ORDER BY run_started_at DESC
