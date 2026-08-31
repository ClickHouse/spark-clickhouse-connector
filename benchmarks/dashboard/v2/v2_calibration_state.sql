-- =============================================================================
-- v2_calibration_state  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   Tab-1 calibration-state tile. Renders the provisional verdict state the
--   contract requires while the bands are still being learned: contract §3 —
--   "During calibration (fewer than ~20 pairs) verdicts MUST display as
--   provisional ('calibrating, n=X/20'); ... only integrity failures (and the
--   parts TRIPWIRE) alert." This panel computes X (the clean-pair count) and
--   emits the state string + a completion fraction for a Superset big-number /
--   label tile so a viewer knows whether a green "OK" is a calibrated verdict or
--   a provisional one.
--
-- DISPLAY ONLY: this panel gates NOTHING and suppresses NOTHING. It reports the
--   calibration state; the actual "alerts stay unwired until calibrated" behaviour
--   lives with the alert artifacts (band_excursion is the gate). Reading this tile
--   never changes what any other panel shows.
--
-- WHAT COUNTS AS A CALIBRATION PAIR:
--   A CLEAN, gateable two-arm pair — flagged=0 (contract §3: flagged pairs are
--   excluded from calibration BY DEFAULT — the CALIBRATION CONSUMER OBLIGATION in
--   v_pair_ratios.sql) and NOT FAIL-class (outcome != 'failed', not integrity-
--   FAILED), with BOTH arms (head AND pinned) present so a ratio could actually be
--   formed. This mirrors the eligibility of the ratio/band consumers (v_pair_ratios
--   / band_excursion): a pair only advances calibration if it produces a gateable
--   ratio. Counted at the PAIR level (one nightly invocation, contract §1.2), so a
--   pair that ran both tiers counts ONCE — tiers share one pair_id, and the bands
--   are a property of the metric, not the tier (contract §3), so calibration
--   progress is a single shared number, not one per tier.
--
-- CALIBRATION RULE (contract §3): fewer than the target (~20) clean pairs =>
--   'calibrating, n=X/20'; at or above the target => 'calibrated'. The target is a
--   Superset jinja param (default 20, the calibration trailing-window size) in the
--   same style as v2_flag_rate.sql's `trailing_n`.
--
-- CONNECTOR SCOPING (2026-07-10): kafka rows share these SAME DWH tables (runs
--   carries a first-class `connector` column; our rows are connector='spark').
--   Scoped to 'spark'; the verdict-fixture connector (a CI truth table) is moot
--   under connector='spark' and kept excluded belt-and-braces.
--
-- Contract reference:  docs/benchmark-v2-contract.md §3 (calibration rule,
--   provisional verdict display, flagged/FAIL exclusion), §1.2 (pair definition).
--
-- STANDALONE: v_pair_ratios is a Superset VIRTUAL dataset, not a queryable
--   ClickHouse object, so this reads the base DWH mirror tables directly and
--   inlines the same eligibility, so it is copy-paste runnable.
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- Empty today (no two-arm pairs) => clean_pair_count 0, state 'calibrating,
--   n=0/20'. MUST return exactly one row without erroring on empty input.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    -- belt for fixture rows on this metrics-only join (metrics has no connector
    -- col; fixture identity is run_id 'FIXTURE-*').
    WHERE NOT startsWith(run_id, 'FIXTURE-')
    GROUP BY run_id, metric_name
  ),
  runs_scoped AS (
    SELECT
      r.run_id                                          AS run_id,
      r.runtime['pair_id']                              AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''), 'head')    AS arm,
      (r.runtime['flagged'] = '1')                      AS flagged,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome,
      p.integrity_ok_metric,
      p.rows_delivered, p.rows_expected, p.unique_delivered, p.unique_expected
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN (
      -- max(if(...NULL)) NOT maxIf (0.0-on-absence would look integrity-FAILED).
      SELECT
        run_id,
        max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
        max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
        max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
        max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
        max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected
      FROM m GROUP BY run_id
    ) AS p ON r.run_id = p.run_id
    WHERE r.connector = 'spark'
      AND r.connector != 'verdict_fixture'
  ),
  -- CLEAN, non-FAIL-class runs (flagged=0; outcome!='failed'; not integrity-
  -- FAILED, integrity-UNKNOWN allowed via coalesce(...,1)).
  eligible AS (
    SELECT run_id, pair_id, arm
    FROM runs_scoped
    WHERE pair_id != ''
      AND flagged = 0
      AND outcome != 'failed'
      AND coalesce(
        multiIf(
          integrity_ok_metric IS NOT NULL, integrity_ok_metric = 1,
          rows_delivered IS NOT NULL AND rows_expected IS NOT NULL
            AND unique_delivered IS NOT NULL AND unique_expected IS NOT NULL,
            (rows_delivered = rows_expected AND unique_delivered = unique_expected),
          NULL
        ),
        1
      ) != 0
  ),
  -- One row per pair_id that has BOTH arms clean+eligible (a gateable pair).
  gateable_pairs AS (
    SELECT pair_id
    FROM eligible
    GROUP BY pair_id
    HAVING has(groupUniqArray(arm), 'head') AND has(groupUniqArray(arm), 'pinned')
  ),
  counted AS (
    SELECT count() AS clean_pair_count FROM gateable_pairs
  )
SELECT
  clean_pair_count                                                   AS clean_pair_count,
  {{ calibration_target | default(20) }}                             AS target,
  -- provisional state string per contract §3.
  if(
    clean_pair_count >= {{ calibration_target | default(20) }},
    'calibrated',
    concat('calibrating, n=', toString(clean_pair_count),
           '/', toString({{ calibration_target | default(20) }}))
  )                                                                  AS state,
  -- completion fraction in [0,1], capped at 1.0 once calibrated (display only).
  least(clean_pair_count / nullIf({{ calibration_target | default(20) }}, 0), 1.0) AS pct
FROM counted
