-- =============================================================================
-- v2_trailing_windows  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   PRE-COMPUTED trailing-window statistics that in-chart Superset SQL cannot
--   express. Superset virtual datasets cannot self-reference, and its chart SQL
--   cannot compute a rolling median / MAD / CoV over a per-series trailing frame;
--   this dataset materialises exactly those, one row per eligible run per gated
--   ABSOLUTE metric, so the charts only have to filter and plot.
--
--   For each (arm, tier, metric) series, in run_started_at order, over the
--   TRAILING 20 headline-eligible rows (current row + 19 preceding), it emits:
--       roll_median   — rolling median            (plan §6 deviation-band centre)
--       roll_mad      — rolling MAD (median absolute deviation about the median)
--       band_lo/hi    — roll_median ± 2*roll_mad   (plan §6 deviation bands)
--       roll_cov      — stddevPop / roll_median    (Tab-3 noise gauge, proper form)
--       window_n      — # rows actually in the frame (< 20 while warming up)
--       seq           — 1-based series position (row_number within arm/tier/metric)
--   'seq = max(seq) over the series' -> the LATEST point, so charts can filter to
--   the newest value per series without a second dataset.
--
-- Plan reference:  docs/benchmark-v2-plan.md
--   §6.2 "Deviation bands" — band excursions; the band is median ± k*spread over a
--         trailing window (here k=2, spread=MAD, robust to the step changes the
--         historical batch sweep showed).
--   §7 Tab 2 (absolute head trends, band overlay) + Tab 3 ("NOISE GAUGE: CoV
--         (stddev/median, trailing 20) per tier"). Band-width recalibration "from
--         the first ~20 pairs' measured CoV" reads this dataset's roll_cov.
-- Contract reference:  docs/benchmark-v2-contract.md §1 (runtime keys + coalesce
--   defaults), §2.1 (metric names), §3 (integrity/flag semantics), §7 (legacy ->
--   contract metric renames coalesced so history is one series).
--
-- HARD REQUIREMENTS met:
--   * ABSOLUTE metric set (6 series-kinds) — the deviation-band + noise-gauge
--     surface. Cardinality is (eligible rows) × 6, kept sane by never cross-joining:
--       Tier 0: null_rows_per_sec, cpu_seconds_per_Mrows, serialize_seconds_per_Mrows
--       Tier 1: throughput_rows_per_sec, parts_per_insert, merge_amplification
--     null_rows_per_sec is Tier-0-only and throughput_rows_per_sec Tier-1-only by
--     construction; the two connector-cost ratios are emitted at whatever tier they
--     were captured. A metric simply does not appear for a run that never emitted it.
--     NOTE (contract §3, Amendment 2026-07-09b — gate composition): this dataset
--     still materialises ALL SIX series, but they are no longer a homogeneous
--     "gated set". As of the calibrated-band amendment the PAIR-LEVEL verdict gate
--     is: Tier-1 = throughput (banded ±9%) + parts_per_insert (TRIPWIRE, ==1.0);
--     Tier-0 = null_rows/cpu/serialize (banded). merge_amplification is WATCH-ONLY
--     (NOT gated) and parts_per_insert is a TRIPWIRE (not banded). They are kept
--     HERE deliberately: their trailing median/MAD/CoV are the covariate + noise
--     surface AND the basis for the ~2026-07-21 (~12 pairs) band recalibration to
--     the median±2*MAD design — so their statistics are computed exactly as
--     before. This is a PROSE clarification only; the windows/statistics are
--     UNCHANGED.
--   * PER-ARM separation (design note): every window is partitioned by
--     (arm, tier, metric) — head and pinned are SEPARATE series and never share a
--     frame. This is deliberate and required:
--       - Tab 2 plots the arm='head' absolute trend with its own ±2*MAD band;
--       - Tab 3's noise gauge reads the arm='pinned' series (connector held
--         constant, so its CoV is pure instrument noise) — mixing a head run into
--         that window would inject connector-change signal and corrupt the gauge;
--       - the two arms run at different cadences/values, so a shared median/MAD
--         would be meaningless. Charts pick the arm they need via the `arm` column.
--     (This dataset covers BOTH arms; it is not head-only. Absolute-trend charts
--      that want head-only just filter arm='head'.)
--   * Eligibility == the headline_ok policy of v_runs_enriched: a row enters ANY
--     window only if outcome != 'failed' AND not flagged AND integrity is not
--     FAILED (integrity_ok = 0). Integrity-UNKNOWN (NULL, legacy rows with no
--     integrity capture) is eligible — legacy rows are FIRST-CLASS and participate
--     in the windows, so months of history are not orphaned. Ineligible rows are
--     dropped BEFORE the window functions run, so they never sit inside a frame and
--     never shift a median/MAD/CoV.
--   * Legacy -> contract metric-name coalesce (contract §7, cutover 2026-07-07):
--     parts_per_insert <- ch_parts_per_insert, merge_amplification <-
--     ch_merge_amplification. The pinned name is preferred, the ch_ legacy name is
--     the fallback, so a series straddling the cutover is one continuous window.
--     (The other four §7 renames are not in the gated ABSOLUTE set and are omitted.)
--
-- Notes:
--   * DWH mirror schema: perf.runs -> raw_connectors_load_testing.runs,
--     perf.metrics -> raw_connectors_load_testing.metrics (ClickPipe).
--   * Source-table + coalesce/rename conventions mirror v_runs_enriched: latest
--     capture per (run_id, metric_name) via argMax; max(if(...NULL)) pivot (NOT
--     maxIf, which returns 0.0 on absence and would be indistinguishable from a
--     genuine 0 and defeat the coalesce fallback); runtime map unnested with the
--     contract coalesce defaults (arm->'head', tier->'1', outcome->'success').
--   * MAD is a two-pass median (median of |x - median|), which a single windowed
--     aggregate cannot express. We therefore materialise the trailing frame as an
--     array with `groupArray(...) OVER (...)` and compute median / MAD / stddev
--     from that array with arrayReduce in the projection. One window pass, no
--     self-join. roll_cov uses stddevPop over the same frame (population form,
--     matching the plan's "stddev/median" noise gauge).
--   * Empty-today safe: if a metric/arm/tier has no eligible rows the series simply
--     does not appear; the dataset MUST return without erroring on empty input.
--
-- Created: 2026-07-08.  Deploy: create Superset virtual dataset `v2_trailing_windows`
--   on the DWH connection (dc93cd97), db 1, schema raw_connectors_load_testing.
-- =============================================================================
WITH
  -- One value per (run_id, metric_name): latest capture wins (rollback/retry
  -- re-captures the metric; argMax by recorded_at takes the final value).
  m AS (
    SELECT
      run_id,
      metric_name,
      argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  -- Pivot integrity inputs + the gated ABSOLUTE metrics to one column per metric.
  -- max(if(cond,value,NULL)) (NOT maxIf) so absence is NULL, not 0.0 — see header.
  -- Renamed metrics coalesce pinned-name over legacy ch_ name (contract §7).
  pivot AS (
    SELECT
      run_id,
      -- integrity inputs (contract §2.1 / §3)
      max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
      max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
      max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
      max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
      max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected,
      -- gated ABSOLUTE metrics — Tier 0
      max(if(metric_name = 'null_rows_per_sec',            value, NULL)) AS null_rows_per_sec,
      max(if(metric_name = 'cpu_seconds_per_Mrows',        value, NULL)) AS cpu_seconds_per_Mrows,
      max(if(metric_name = 'serialize_seconds_per_Mrows',  value, NULL)) AS serialize_seconds_per_Mrows,
      -- gated ABSOLUTE metrics — Tier 1 (parts/merge coalesce legacy ch_ names)
      max(if(metric_name = 'throughput_rows_per_sec',      value, NULL)) AS throughput_rows_per_sec,
      coalesce(
        max(if(metric_name = 'parts_per_insert',    value, NULL)),
        max(if(metric_name = 'ch_parts_per_insert', value, NULL))
      )                                                                  AS parts_per_insert,
      coalesce(
        max(if(metric_name = 'merge_amplification',    value, NULL)),
        max(if(metric_name = 'ch_merge_amplification', value, NULL))
      )                                                                  AS merge_amplification
    FROM m
    GROUP BY run_id
  ),
  -- Runs with unnested scope + the pivoted metrics + the headline eligibility flag.
  -- Coalesce defaults per contract §1 keep legacy rows first-class (head/tier1/
  -- success). eligible mirrors v_runs_enriched.headline_ok exactly.
  runs_scoped AS (
    SELECT
      r.run_id                                              AS run_id,
      r.run_started_at                                      AS run_started_at,
      r.git_sha                                             AS git_sha,
      r.runtime['pair_id']                                  AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''),  'head')       AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')          AS tier,
      (r.runtime['flagged'] = '1')                          AS flagged,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome,
      p.null_rows_per_sec,
      p.cpu_seconds_per_Mrows,
      p.serialize_seconds_per_Mrows,
      p.throughput_rows_per_sec,
      p.parts_per_insert,
      p.merge_amplification,
      -- integrity_ok derived exactly as v_runs_enriched: direct metric preferred,
      -- else delivered/expected comparison, else NULL (unknown, NOT failed).
      multiIf(
        p.integrity_ok_metric IS NOT NULL, p.integrity_ok_metric = 1,
        p.rows_delivered IS NOT NULL AND p.rows_expected IS NOT NULL
          AND p.unique_delivered IS NOT NULL AND p.unique_expected IS NOT NULL,
          (p.rows_delivered = p.rows_expected AND p.unique_delivered = p.unique_expected),
        NULL
      )                                                     AS integrity_ok
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN pivot AS p ON r.run_id = p.run_id
    -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
    -- from all real trends (fixture is a CI truth-table, never a real run).
    WHERE r.connector != 'verdict_fixture'
  ),
  -- Headline-eligible rows only. A row is eligible iff not flagged, outcome !=
  -- 'failed', and NOT integrity-failed. coalesce(integrity_ok, 1) lets unknown
  -- (NULL) pass — a bare `integrity_ok != 0` would be NULL for unknown and the
  -- WHERE would drop the row (three-valued logic), orphaning legacy history.
  eligible AS (
    SELECT *
    FROM runs_scoped
    WHERE flagged = 0
      AND outcome != 'failed'
      AND coalesce(integrity_ok, 1) != 0
  ),
  -- Tall form: one row per (run, arm, tier, metric) for the gated ABSOLUTE set.
  -- Only rows where the metric is present (value IS NOT NULL) enter — an absent
  -- metric contributes nothing to its series' window. arrayJoin over a tuple array
  -- keeps cardinality at exactly (eligible rows) × (present metrics ≤ 6).
  metric_long AS (
    SELECT
      run_id, run_started_at, git_sha, pair_id, arm, tier,
      mv.1 AS metric,
      mv.2 AS value
    FROM eligible
    ARRAY JOIN [
      ('null_rows_per_sec',           null_rows_per_sec),
      ('cpu_seconds_per_Mrows',       cpu_seconds_per_Mrows),
      ('serialize_seconds_per_Mrows', serialize_seconds_per_Mrows),
      ('throughput_rows_per_sec',     throughput_rows_per_sec),
      ('parts_per_insert',            parts_per_insert),
      ('merge_amplification',         merge_amplification)
    ] AS mv
    WHERE mv.2 IS NOT NULL
  ),
  -- Trailing frame per (arm, tier, metric) in time order, as an array. One window
  -- pass; median / MAD / stddev come from the array in the projection because MAD
  -- (median of |x - median|) is a two-pass median no single windowed aggregate can
  -- express. seq is the 1-based series position for 'latest' filtering.
  windowed AS (
    SELECT
      run_id, run_started_at, git_sha, pair_id, arm, tier, metric, value,
      groupArray(value) OVER w AS frame,
      row_number()      OVER w AS seq
    FROM metric_long
    WINDOW w AS (
      PARTITION BY arm, tier, metric
      ORDER BY run_started_at, run_id
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
  )
SELECT
  run_id,
  run_started_at,
  git_sha,
  pair_id,
  arm,
  tier,
  metric,
  value,
  seq,
  length(frame)                                                   AS window_n,
  arrayReduce('median', frame)                                    AS roll_median,
  arrayReduce('median',
    arrayMap(v -> abs(v - arrayReduce('median', frame)), frame))  AS roll_mad,
  arrayReduce('median', frame)
    - 2 * arrayReduce('median',
        arrayMap(v -> abs(v - arrayReduce('median', frame)), frame)) AS band_lo,
  arrayReduce('median', frame)
    + 2 * arrayReduce('median',
        arrayMap(v -> abs(v - arrayReduce('median', frame)), frame)) AS band_hi,
  -- Noise gauge (Tab 3): population stddev / median over the same trailing frame.
  -- NULL (not inf) when the median is 0, so an all-zero window does not error.
  arrayReduce('stddevPop', frame)
    / nullIf(arrayReduce('median', frame), 0)                     AS roll_cov
FROM windowed
