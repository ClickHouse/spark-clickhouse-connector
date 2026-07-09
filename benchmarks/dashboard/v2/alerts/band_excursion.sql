-- =============================================================================
-- ALERT: ratio band-excursion  ==>  REGRESSION   +   parts TRIPWIRE
-- =============================================================================
-- Shares the SQL semantics of the Tab 1 verdict tiles + Excursion-log table
-- (charts "v2: Tier 0 verdict" 5694, "v2: Tier 1 verdict" 5696,
--  "v2: Excursion log (ratio band-exits)" 5701), all on dataset v2_pair_ratios.
--
-- Contract: docs/benchmark-v2-contract.md §3 (Amendment 2026-07-09b —
--   CALIBRATED per-metric bands at 2x the measured noise floor; merge_amplification
--   is WATCH-ONLY, not gated; parts_per_insert is a binary TRIPWIRE). "Ratio
--   band-exit => a REGRESSION alert ... Band-exit tracking is on the RATIO, not
--   absolutes." Plan §6.2, §7 (Tab 1 excursion rules).
--
-- Trips when EITHER:
--   (a) a BANDED gated metric's H/P ratio leaves its CALIBRATED band (below) in
--       the BAD direction => REGRESSION; OR
--   (b) the parts_per_insert TRIPWIRE fires: head arm's ABSOLUTE value != 1.0
--       (parts is a constant-1.0 structural invariant, NOT a ratio) => TRIPWIRE.
--   One output row per (pair, tier, metric) excursion/trip.
--   FLAGGED pairs MUST NOT fire (contract §3: flagged => FLAGGED verdict, excluded
--   from bands/alerts BY DEFAULT — a flagged run is non-comparable, not a
--   regression). This alert reads the BASE tables directly (NOT v2_pair_ratios)
--   and enforces the exclusion in its OWN `eligible` CTE via `flagged = 0` (a run
--   enters only if NEITHER arm is flagged; a pair-level flag on either arm removes
--   that arm and the INNER head<->pinned join then drops the pair). This is a
--   CALIBRATION/GATE consumer, so per the CONSUMER OBLIGATION documented in
--   v_pair_ratios.sql it filters flagged=0. (NOTE 2026-07-09: v2_pair_ratios now
--   CARRIES flagged pairs — flagged=1 + flag_reason — so the FLAGGED verdict can
--   render on DISPLAY surfaces. This alert must therefore NOT be reworked to read
--   v2_pair_ratios without ALSO adding a `flagged = 0` filter; keeping the
--   independent base-table CTE with its own flagged=0 filter is the safe form.)
--   A row here is thus a genuine regression/tripwire — never a flagged run.
--
--   merge_amplification is WATCH-ONLY (contract §3): it is NOT gated and is
--   EXCLUDED from this alert — single-pair excursions <25% are indistinguishable
--   from merge-timing noise (12.7% within-arm floor, no pairing dividend), so
--   alerting on it would manufacture false regressions. It remains a covariate.
--
-- Channel wiring is a FLAGGED follow-up (open decision 3) — see README.md.
-- Non-empty result set == alert should fire; attach `pair_link` per row.
--
-- CALIBRATED per-metric band map (SINGLE SOURCE OF TRUTH — contract §3 Amendment
--   2026-07-09b; each = 2x the measured noise floor, SAME on both tiers, keyed on
--   the metric name, NOT the tier. Recalibrates from trailing-20 stats at ~12
--   pairs (~2026-07-21), then median±2*MAD per plan §6.2):
--     throughput_rows_per_sec                         = 0.09   (±9%)
--     null_rows_per_sec / null_drain / drain_rows/s   = 0.085  (±8.5%)
--     cpu_seconds_per_Mrows / ch_insert_cpu_..._Mrows = 0.06   (±6%)
--     serialize_seconds_per_Mrows                     = 0.085  (±8.5%)
--     parts_per_insert                                = TRIPWIRE (== 1.0 exactly)
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- Empty today (no pairs). MUST NOT error on the empty dataset.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    -- contract §3: belt for the fixture rows on a metrics-only join (metrics has
    -- no connector col; the fixture identity is run_id 'FIXTURE-*'). The
    -- connector != 'verdict_fixture' filter in runs_scoped is the braces; this is
    -- the belt so a mirrored fixture metric can never leak into a ratio.
    WHERE NOT startsWith(run_id, 'FIXTURE-')
    GROUP BY run_id, metric_name
  ),
  runs_scoped AS (
    SELECT
      r.run_id                                          AS run_id,
      r.runtime['pair_id']                              AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''), 'head')    AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')      AS tier,
      (r.runtime['flagged'] = '1')                      AS flagged,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome,
      p.integrity_ok_metric,
      p.rows_delivered, p.rows_expected, p.unique_delivered, p.unique_expected
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN (
      SELECT
        run_id,
        max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
        max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
        max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
        max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
        max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected
      FROM m GROUP BY run_id
    ) AS p ON r.run_id = p.run_id
    -- contract §3: exclude the reserved verdict-fixture connector (a CI truth
    -- table, never a real run) — matches the consumer views' predicate so a
    -- mirrored fixture row can never raise a REGRESSION/TRIPWIRE alert.
    WHERE r.connector != 'verdict_fixture'
  ),
  eligible AS (
    SELECT run_id, pair_id, arm, tier
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
  metric_long AS (
    SELECT e.pair_id AS pair_id, e.tier AS tier, e.arm AS arm,
           mm.metric_name AS metric, mm.value AS value
    FROM eligible AS e
    INNER JOIN (
      SELECT
        run_id,
        multiIf(
          metric_name = 'ch_parts_per_insert',         'parts_per_insert',
          metric_name = 'ch_merge_amplification',      'merge_amplification',
          metric_name = 'ch_inserts_delayed_fraction', 'inserts_delayed_fraction',
          metric_name = 'ch_merge_pool_peak_pct',      'merge_pool_peak_pct',
          metric_name = 'ch_settle_seconds',           'settle_seconds',
          metric_name
        ) AS metric_name,
        value
      FROM m
      WHERE metric_name IN (
        'throughput_rows_per_sec','null_rows_per_sec',
        'parts_per_insert','merge_amplification','inserts_delayed_fraction',
        'merge_pool_peak_pct','settle_seconds',
        'cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
        'ch_insert_cpu_seconds_per_Mrows','bytes_on_wire_per_row',
        'ch_parts_per_insert','ch_merge_amplification',
        'ch_inserts_delayed_fraction','ch_merge_pool_peak_pct','ch_settle_seconds'
      )
    ) AS mm ON e.run_id = mm.run_id
  ),
  ratios AS (
    SELECT h.pair_id AS pair_id, h.tier AS tier, h.metric AS metric,
           h.value AS head_value, pn.value AS pinned_value,
           h.value / nullIf(pn.value, 0) AS ratio
    FROM (SELECT * FROM metric_long WHERE arm = 'head')   AS h
    INNER JOIN (SELECT * FROM metric_long WHERE arm = 'pinned') AS pn
      ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
  ),
  -- Direction + CALIBRATED per-metric band + tripwire flag (contract §3
  -- Amendment 2026-07-09b). merge_amplification is dropped here (WATCH-ONLY).
  classified AS (
    SELECT
      pair_id, tier, metric, head_value, pinned_value, ratio,
      multiIf(
        metric IN ('throughput_rows_per_sec','null_rows_per_sec',
                   'null_drain_rows_per_sec','drain_rows_per_sec'), 'higher_better',
        metric IN ('cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
                   'ch_insert_cpu_seconds_per_Mrows'), 'lower_better',
        'tripwire'                                    -- parts_per_insert
      ) AS direction,
      (metric = 'parts_per_insert') AS is_tripwire,
      -- CALIBRATED band (2x measured noise floor; SAME on both tiers; keyed on
      -- metric name, NOT tier). Non-banded metrics get 0 (unused for them).
      multiIf(
        metric = 'throughput_rows_per_sec',                                    0.09,
        metric IN ('null_rows_per_sec','null_drain_rows_per_sec',
                   'drain_rows_per_sec'),                                       0.085,
        metric IN ('cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows'),  0.06,
        metric = 'serialize_seconds_per_Mrows',                                0.085,
        0.0
      ) AS band
    FROM ratios
    WHERE metric != 'merge_amplification'            -- WATCH-ONLY, not gated
  )
SELECT
  pair_id,
  tier,
  metric,
  round(head_value, 4)          AS head_value,
  round(pinned_value, 4)        AS pinned_value,
  -- delta_pct: for banded metrics the ratio excursion; for the tripwire, the
  -- head arm's deviation from the 1.0 invariant.
  if(is_tripwire, round((head_value - 1) * 100, 2), round((ratio - 1) * 100, 2)) AS delta_pct,
  if(is_tripwire, 'TRIPWIRE(==1.0)', concat('+-', toString(band * 100), '%')) AS band_label,
  if(is_tripwire, 'TRIPWIRE', 'REGRESSION')          AS verdict,
  -- pair link for the alert body (Superset dashboard 427, Tab 1):
  concat('https://superset.clickhouse-dev.com/superset/dashboard/427/?pair_id=', pair_id) AS pair_link
FROM classified
WHERE
  -- (b) parts TRIPWIRE: head arm's absolute value deviates from the 1.0 invariant.
  (is_tripwire AND head_value != 1.0)
  OR
  -- (a) banded REGRESSION: ratio leaves the calibrated band in the BAD direction
  --     ONLY (a GOOD-direction excursion is an IMPROVEMENT, reported not alarmed).
  (NOT is_tripwire AND ratio IS NOT NULL AND band > 0 AND (
      (direction = 'higher_better' AND ratio < 1 - band) OR   -- worse: slower/lower
      (direction = 'lower_better'  AND ratio > 1 + band)      -- worse: more cost
  ))
ORDER BY pair_id DESC, tier, metric
