-- =============================================================================
-- v2_pairs_in_band  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   Tab-1 "PAIRS IN BAND (20)" roll-up. For the trailing-20 CLEAN pairs, count —
--   PER GATED METRIC and PER TIER — how many pairs' HEAD/pinned ratio sits inside
--   the CALIBRATED band, plus an overall "pairs fully in band" roll-up. Contract
--   §3 pins the roll-up obligation verbatim: "Pair-level roll-ups MUST still
--   expose per-metric in-band counts so a single noisy metric cannot zero the
--   headline." This panel is that exposure — the per-metric rows sit next to the
--   overall row so a single noisy metric that drops one pair out of band is read
--   as one metric's miss, not a collapsed headline.
--
-- WHAT IT COUNTS (gate composition — contract §3 Amendment 2026-07-09b, as scoped
--   for the concurrent-change gate):
--     BANDED gated metrics (ratio ∈ [1−band, 1+band] ⇒ in band; band = 2× the
--     measured noise floor, keyed on the METRIC not the tier — same SSOT constants
--     as alerts/band_excursion.sql, replicated below):
--       ch_insert_cpu_seconds_per_Mrows / cpu_seconds_per_Mrows  = ±6%   (Tier-1)
--       null_rows_per_sec (+ null_drain/drain aliases)           = ±8.5% (Tier-0)
--       serialize_seconds_per_Mrows                              = ±8.5% (Tier-0)
--     TRIPWIRE gated metric (NOT banded — a binary structural invariant):
--       parts_per_insert  ⇒ in band iff the head arm's ABSOLUTE value == 1.0.
--     throughput_rows_per_sec and merge_amplification are WATCH-ONLY (NOT gated)
--       and are EXCLUDED from this roll-up — a watch-only excursion must never
--       move the in-band headline (band_excursion.sql likewise drops
--       merge_amplification as WATCH-ONLY).
--
--   "pairs fully in band" (the __all_gated__ row per tier): a pair is fully in
--   band iff EVERY gated metric present for it is in band (min over its gated
--   rows = 1). Pairs with no gated metric present contribute nothing (NO_DATA,
--   not a miss). Because the count is per-metric AND overall, one metric going
--   out of band lowers only that metric's row and the overall row — never zeroes
--   a per-metric headline for a metric that is fine.
--
-- BAND / CLEAN-WINDOW PROVENANCE (read before trusting the numbers):
--   * Band constants: replicated from alerts/band_excursion.sql's SINGLE SOURCE
--     OF TRUTH multiIf (contract §3 Amendment 2026-07-09b). NOT the trailing-20
--     median±2·MAD absolute bands in v_trailing_windows — those band ABSOLUTES;
--     the verdict gate (band_excursion) bands the RATIO against these fixed
--     calibrated constants until the ~12-pair recalibration, so this roll-up
--     matches the verdict by replicating the SAME ratio-band constants.
--   * Clean window: the "trailing-20 CLEAN" convention of v_pair_ratios /
--     v_trailing_windows. clean_seq there = dense_rank over the flagged=0 rows
--     only. Here flagged pairs are dropped pre-join in `eligible` (flagged = 0,
--     the CALIBRATION/GATE CONSUMER OBLIGATION documented in v_pair_ratios.sql),
--     so every surviving pair is clean and a dense_rank by pair_ts DESC within
--     tier reproduces clean_seq directly; we keep clean_seq <= {{ trailing_n }}.
--     pair_ts is derived from the pair_id timestamp prefix EXACTLY as
--     v_pair_ratios derives it (anchored regex, *OrNull parse, NULL-safe).
--
-- CONNECTOR SCOPING (2026-07-10): kafka rows share these SAME DWH tables (runs
--   carries a first-class `connector` column; our rows are connector='spark').
--   Scoped to 'spark' so kafka pairs never enter the roll-up; cross-connector
--   lives on kafka's Tab 5 per contract §6. The verdict-fixture connector (a CI
--   truth table, never a real run) is excluded — connector='spark' already moots
--   it; a FIXTURE- belt is added on the metrics-only join as in band_excursion.
--
-- Contract reference:  docs/benchmark-v2-contract.md §3 (calibrated per-metric
--   bands, gate composition, "pair-level roll-ups MUST expose per-metric in-band
--   counts"). Plan reference:  docs/benchmark-v2-plan.md §6.2, §7 (Tab 1).
--
-- STANDALONE: v_pair_ratios is a Superset VIRTUAL dataset, not a queryable
--   ClickHouse object, so this panel reads the base DWH mirror tables
--   (raw_connectors_load_testing.{runs,metrics}) and inlines the ratio + band
--   pipeline (same CTE shape as band_excursion.sql), so it is copy-paste runnable.
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- Empty today (no two-arm pairs). MUST return zero rows without erroring.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    -- belt for the fixture rows on this metrics-only join (metrics has no
    -- connector col; fixture identity is run_id 'FIXTURE-*'). connector='spark'
    -- in runs_scoped is the braces; this is the belt.
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
      -- max(if(...NULL)) NOT maxIf (which returns 0.0 on absence and would make
      -- an integrity-uncaptured run look integrity-FAILED): absence stays NULL.
      SELECT
        run_id,
        max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
        max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
        max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
        max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
        max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected
      FROM m GROUP BY run_id
    ) AS p ON r.run_id = p.run_id
    -- kafka rows share these tables since 2026-07-10; connector-scope to spark.
    WHERE r.connector = 'spark'
    -- belt-and-braces: connector='spark' already excludes the fixture connector.
      AND r.connector != 'verdict_fixture'
  ),
  -- CALIBRATION/GATE CONSUMER: exclude flagged pairs (flagged = 0) AND the
  -- FAIL class (outcome='failed', integrity-FAILED). Integrity-UNKNOWN (NULL)
  -- passes via coalesce(...,1) — legacy rows are first-class. Dropping flagged
  -- pairs here is what makes the recency rank below a CLEAN rank == clean_seq.
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
  -- Long-form gated metric values per eligible run, legacy ch_ names coalesced to
  -- the pinned contract name (contract §7) — identical remap to band_excursion.
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
  -- head/pinned ratio per (pair, tier, metric). INNER join: a pair with a missing
  -- arm produces no ratio (it cannot be gated), exactly like v_pair_ratios.
  ratios AS (
    SELECT h.pair_id AS pair_id, h.tier AS tier, h.metric AS metric,
           h.value AS head_value, pn.value AS pinned_value,
           h.value / nullIf(pn.value, 0) AS ratio
    FROM (SELECT * FROM metric_long WHERE arm = 'head')   AS h
    INNER JOIN (SELECT * FROM metric_long WHERE arm = 'pinned') AS pn
      ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
  ),
  -- Derive pair_ts (v_pair_ratios convention) and clean_seq = dense_rank by
  -- recency within tier. Flagged pairs are already gone, so this rank IS the
  -- clean rank. Every metric row of one pair shares one clean_seq (dense_rank +
  -- shared pair_ts). Malformed / prefix-less ids -> NULL pair_ts (sort last).
  ranked AS (
    SELECT
      pair_id, tier, metric, head_value, pinned_value, ratio,
      dense_rank() OVER (PARTITION BY tier ORDER BY pair_ts DESC) AS clean_seq
    FROM (
      SELECT
        pair_id, tier, metric, head_value, pinned_value, ratio,
        if(
          extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
          NULL,
          parseDateTimeBestEffortOrNull(
            replaceRegexpOne(
              extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
              'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
            )
          )
        ) AS pair_ts
      FROM ratios
    )
  ),
  -- Keep the trailing-N CLEAN pairs (default 20, the calibration window).
  window20 AS (
    SELECT * FROM ranked
    WHERE clean_seq <= {{ trailing_n | default(20) }}
  ),
  -- Classify each gated (pair, tier, metric): band constant + tripwire flag.
  -- WATCH-ONLY metrics (throughput, merge_amplification) are NOT in the IN-list,
  -- so they never enter the roll-up. Band constants = band_excursion.sql SSOT.
  classified AS (
    SELECT
      pair_id, tier, metric, head_value, ratio,
      (metric = 'parts_per_insert') AS is_tripwire,
      multiIf(
        metric IN ('cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows'), 0.06,
        metric IN ('null_rows_per_sec','null_drain_rows_per_sec',
                   'drain_rows_per_sec'),                                       0.085,
        metric = 'serialize_seconds_per_Mrows',                                0.085,
        0.0
      ) AS band
    FROM window20
    WHERE metric IN (
      'ch_insert_cpu_seconds_per_Mrows','cpu_seconds_per_Mrows',
      'null_rows_per_sec','null_drain_rows_per_sec','drain_rows_per_sec',
      'serialize_seconds_per_Mrows',
      'parts_per_insert'
    )
  ),
  -- in_band per (pair, tier, metric): tripwire ⇒ head abs == 1.0 exactly; banded
  -- ⇒ ratio ∈ [1−band, 1+band] (SYMMETRIC — a GOOD-direction excursion is still
  -- OUT of band, an IMPROVEMENT, correctly NOT counted as in band, contract §3).
  -- NULL ratio / absent metric ⇒ NULL in_band (NO_DATA — neither in nor out; it
  -- is dropped from every count so a missing metric can't be read as a miss).
  scored AS (
    SELECT
      pair_id, tier, metric,
      multiIf(
        is_tripwire, if(head_value IS NULL, NULL, toUInt8(head_value = 1.0)),
        band > 0 AND ratio IS NOT NULL, toUInt8(ratio BETWEEN 1 - band AND 1 + band),
        NULL
      ) AS in_band
    FROM classified
  ),
  -- Per-metric, per-tier: pairs evaluated (metric present, in_band not NULL) and
  -- pairs in band. One row per (pair,tier,metric) upstream, so a plain count is
  -- a per-pair count.
  per_metric AS (
    SELECT
      tier,
      metric,
      count()               AS pairs_evaluated,
      countIf(in_band = 1)  AS pairs_in_band
    FROM scored
    WHERE in_band IS NOT NULL
    GROUP BY tier, metric
  ),
  -- Per-pair fully-in-band roll-up: a pair is fully in band iff EVERY gated
  -- metric present for it is in band (min over its gated rows = 1).
  pair_roll AS (
    SELECT tier, pair_id, min(in_band) AS all_in_band
    FROM scored
    WHERE in_band IS NOT NULL
    GROUP BY tier, pair_id
  ),
  overall AS (
    SELECT
      tier,
      '__all_gated__'          AS metric,
      count()                  AS pairs_evaluated,   -- pairs with >=1 gated metric
      countIf(all_in_band = 1) AS pairs_in_band      -- pairs fully in band
    FROM pair_roll
    GROUP BY tier
  ),
  -- Clean pairs present in the trailing window per tier (context for the tile;
  -- counts distinct pair_id regardless of which metrics they carry).
  window_pairs AS (
    SELECT tier, uniqExact(pair_id) AS window_clean_pairs
    FROM window20
    GROUP BY tier
  ),
  combined AS (
    SELECT tier, metric, pairs_evaluated, pairs_in_band FROM per_metric
    UNION ALL
    SELECT tier, metric, pairs_evaluated, pairs_in_band FROM overall
  )
SELECT
  c.tier                                                       AS tier,
  c.metric                                                     AS metric,
  c.pairs_in_band                                              AS pairs_in_band,
  c.pairs_evaluated                                            AS pairs_evaluated,
  -- fraction of evaluated pairs in band, in [0,1]; NULL when nothing evaluated.
  c.pairs_in_band / nullIf(c.pairs_evaluated, 0)               AS in_band_pct,
  coalesce(w.window_clean_pairs, 0)                            AS window_clean_pairs,
  {{ trailing_n | default(20) }}                               AS trailing_n,
  -- sort the overall roll-up first within each tier, then metrics alphabetically.
  (c.metric = '__all_gated__')                                 AS is_overall
FROM combined AS c
LEFT JOIN window_pairs AS w ON c.tier = w.tier
ORDER BY c.tier, is_overall DESC, c.metric
