-- =============================================================================
-- ALERT: ratio band-excursion  ==>  REGRESSION
-- =============================================================================
-- Shares the SQL semantics of the Tab 1 verdict tiles + Excursion-log table
-- (charts "v2: Tier 0 verdict" 5694, "v2: Tier 1 verdict" 5696,
--  "v2: Excursion log (ratio band-exits)" 5701), all on dataset v2_pair_ratios.
--
-- Contract: docs/benchmark-v2-contract.md §3.3 — "Ratio band-exit => a REGRESSION
--   alert ... files an alert with the pair link. Band-exit tracking is on the
--   RATIO, not absolutes." Plan §6.2, §7 (Tab 1 excursion rules).
--
-- Trips when: a gated metric's H/P ratio leaves its tier band
--   (Tier 0 ±3%, Tier 1 ±5%). One output row per (pair, tier, metric) excursion.
--   FLAGGED runs are ALREADY excluded upstream (v2_pair_ratios only emits pairs
--   whose BOTH arms are un-flagged, outcome!='failed', not integrity-failed), so
--   a row here is a genuine regression — never a flagged/non-comparable run.
--
-- Channel wiring is a FLAGGED follow-up (open decision 3) — see README.md.
-- Non-empty result set == alert should fire; attach `pair_link` per row.
--
-- Band constants (SINGLE SOURCE OF TRUTH — recalibrate here after ~20 pairs):
--     Tier 0 = 0.03  (±3%)
--     Tier 1 = 0.05  (±5%)
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- Empty today (no pairs). MUST NOT error on the empty dataset.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
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
  )
SELECT
  pair_id,
  tier,
  metric,
  round(head_value, 4)          AS head_value,
  round(pinned_value, 4)        AS pinned_value,
  round((ratio - 1) * 100, 2)   AS delta_pct,
  if(tier = '0', '+-3%', '+-5%') AS band,
  'REGRESSION'                  AS verdict,
  -- pair link for the alert body (Superset dashboard 427, Tab 1):
  concat('https://superset.clickhouse-dev.com/superset/dashboard/427/?pair_id=', pair_id) AS pair_link
FROM ratios
WHERE ratio IS NOT NULL
  AND (
    (tier = '0' AND abs(ratio - 1) > 0.03) OR   -- Tier 0 band ±3%
    (tier = '1' AND abs(ratio - 1) > 0.05)      -- Tier 1 band ±5%
  )
ORDER BY pair_id DESC, tier, metric
