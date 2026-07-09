-- =============================================================================
-- v_verdict_fixture_check  —  CONTRACT §3 acceptance INCLUSION view (CI artifact)
-- =============================================================================
-- Purpose:
--   The companion INVERSE of the fixture EXCLUSION added to every consumer view.
--   Where the consumer views exclude connector='verdict_fixture', this view
--   INCLUDES ONLY that reserved fixture connector and computes, per fixture cell
--   (pair_id × metric), the PINNED ratio→verdict map from contract §3 — then
--   emits expected_verdict, actual_verdict and pass so the acceptance runner
--   (benchmarks/scripts/check_verdict_fixture.py) can assert the truth table.
--
--   NOT DEPLOYED TO SUPERSET. This is a repo / CI artifact only: it proves the
--   REAL dataset SQL (v_pair_ratios' ratio computation, copied verbatim below)
--   maps ratios to verdicts exactly as the contract pins. It never feeds a chart.
--
-- Contract reference: docs/benchmark-v2-contract.md §3 (Amendment 2026-07-09b —
--   CALIBRATED per-metric bands at 2x the measured noise floor; the flat
--   ±3%/±5% rule is superseded; merge_amplification is WATCH-ONLY, not gated;
--   parts_per_insert is a binary TRIPWIRE) —
--   * BANDED metrics (PINNED calibrated bands, SAME on both tiers — a property of
--     the metric's noise, not the tier):
--         throughput_rows_per_sec (verified)               ±9%   => [0.91, 1.09]
--         null_rows_per_sec / null_drain / drain_rows/s    ±8.5% => [0.915,1.085]
--         ch_insert_cpu_seconds_per_Mrows / cpu_..._Mrows  ±6%   => [0.94, 1.06]
--         serialize_seconds_per_Mrows                      ±8.5% => [0.915,1.085]
--   * DIRECTION (PINNED): throughput_rows_per_sec / null_* / drain = higher_better;
--     cpu_seconds_per_Mrows / ch_insert_cpu_..._Mrows / serialize_..._Mrows =
--     lower_better.
--   * TRIPWIRE (PINNED): parts_per_insert is NOT banded and NOT ratio-compared —
--     a BINARY tripwire on the HEAD arm's ABSOLUTE value: head == 1.0 => OK,
--     head != 1.0 => TRIPWIRE. (merge_amplification is WATCH-ONLY and is NOT
--     asserted here — it is not gated.)
--   * RATIO→VERDICT (PINNED), precedence FAIL > FLAG > {NO_DATA / TRIPWIRE /
--     IMPROVEMENT / REGRESSION / OK}:
--       pair flagged                     => FLAGGED  (overrides EVERYTHING below,
--                                                     incl. an armed TRIPWIRE)
--       (banded) ratio NULL/0-denominator=> NO_DATA
--       (banded) outside band, GOOD dir  => IMPROVEMENT
--       (banded) outside band, BAD dir   => REGRESSION
--       (banded) else                    => OK
--       (tripwire) head == 1.0           => OK
--       (tripwire) head != 1.0           => TRIPWIRE
--   (Integrity-FAIL precedence is not exercised here — all fixture rows pass
--    integrity by construction; the fixture targets the ratio→verdict map + the
--    tripwire, the layers this acceptance rule was written to protect.)
--
-- KEEP-IN-SYNC CONTRACT (read before editing):
--   The `runs_scoped`/`metric_long`/ratio CTEs below are COPIED from
--   benchmarks/dashboard/v2/v_pair_ratios.sql so the fixture ratio flows through
--   the EXACT same computation the production Tab-1 view uses (head.value /
--   nullIf(pinned.value, 0), same §7 rename remap, same argMax latest-capture).
--   Three DELIBERATE differences, all required by the acceptance rule:
--     1. INVERTED SCOPE: this view keeps ONLY connector='verdict_fixture'
--        (WHERE r.connector = 'verdict_fixture'); v_pair_ratios EXCLUDES it.
--     2. flagged rows are NOT filtered out — v_pair_ratios drops flagged pairs
--        (they never reach the ratio), but the verdict map's FLAGGED branch must
--        be exercised, so this view CARRIES the flagged bit through to the map.
--     3. metric_long carries the flag: because of (2) the projection adds
--        `any(e.flagged)` and therefore a `GROUP BY e.pair_id, e.tier, e.arm,
--        mm.metric_name, mm.value` that v_pair_ratios' metric_long does not have
--        (its rows are already flag-free). any() is safe — the flag is a per-run
--        constant across that run's metrics — and grouping ALSO by mm.value keeps
--        the row grain identical to v_pair_ratios' (one row per run/metric after
--        the inner argMax de-dup; the GROUP BY collapses nothing, it only
--        satisfies the aggregate).
--   A parameterized single-source approach was rejected: ClickHouse virtual-
--   dataset SQL cannot self-reference (a view cannot `SELECT ... FROM
--   v_pair_ratios WHERE ...`), and templating one body with an inverted predicate
--   would need a build step this repo does not have for the .sql files. Copying
--   the CTE skeleton is the faithful, self-contained choice. IF v_pair_ratios'
--   ratio/rename/argMax logic changes, MIRROR it here (the acceptance runner will
--   catch a drift only if the fixture cell it breaks is covered — keep both in
--   step by hand).
--
-- Reads perf.* DIRECTLY (NOT the DWH mirror): the fixture seed
--   (benchmarks/sql/perf/90_verdict_fixture_seed.sql) lands in perf.{runs,metrics}
--   on the metrics service; the acceptance runner runs seed + this view together
--   against the SAME database (clickhouse-local in CI, or perf.* on the metrics
--   host). The production consumer views read raw_connectors_load_testing.* (the
--   ClickPipe DWH mirror) — this view intentionally does NOT, so acceptance needs
--   no mirroring round-trip. See check_verdict_fixture.py header for the
--   DWH-vs-perf naming rationale.
-- =============================================================================
WITH
  -- Latest value per (run_id, metric_name) — v_pair_ratios convention.
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM perf.metrics
    WHERE startsWith(run_id, 'FIXTURE-')          -- fixture scope (perf.metrics has no connector col)
    GROUP BY run_id, metric_name
  ),
  -- Runs with unnested scope. COPIED from v_pair_ratios.runs_scoped, EXCEPT:
  --   * source is perf.runs filtered to connector='verdict_fixture' (INVERTED
  --     scope — the production view excludes this connector);
  --   * `flagged` is CARRIED THROUGH (production view filters flagged rows out
  --     here; the verdict map needs the flag to emit FLAGGED).
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
    FROM perf.runs AS r
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
    WHERE r.connector = 'verdict_fixture'             -- INVERTED SCOPE (contract §3)
  ),
  -- Eligibility mirrors v_pair_ratios BUT DOES NOT DROP flagged (see header): a
  -- pair still needs pair_id, non-failed outcome and non-FAILED integrity to be a
  -- valid comparable; the flag is preserved into `flagged` for the verdict map.
  eligible AS (
    SELECT run_id, pair_id, arm, tier, flagged
    FROM runs_scoped
    WHERE pair_id != ''
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
  -- Long-form gated metric values per eligible run, §7 rename coalesced —
  -- COPIED from v_pair_ratios.metric_long (same argMax de-dup / remap).
  metric_long AS (
    SELECT
      e.pair_id AS pair_id,
      e.tier    AS tier,
      e.arm     AS arm,
      any(e.flagged) AS flagged,          -- flag is a per-run constant across metrics
      mm.metric_name AS metric,
      mm.value       AS value
    FROM eligible AS e
    INNER JOIN (
      SELECT
        run_id,
        metric_name,
        argMax(value, prefer) AS value
      FROM (
        SELECT
          m.run_id AS run_id,
          multiIf(
            m.metric_name = 'ch_parts_per_insert',         'parts_per_insert',
            m.metric_name = 'ch_merge_amplification',      'merge_amplification',
            m.metric_name = 'ch_inserts_delayed_fraction', 'inserts_delayed_fraction',
            m.metric_name = 'ch_merge_pool_peak_pct',      'merge_pool_peak_pct',
            m.metric_name = 'ch_settle_seconds',           'settle_seconds',
            m.metric_name
          ) AS metric_name,
          if(startsWith(m.metric_name, 'ch_'), 0, 1) AS prefer,
          m.value AS value
        FROM m
        WHERE m.metric_name IN (
          'throughput_rows_per_sec','null_rows_per_sec',
          'parts_per_insert','merge_amplification','inserts_delayed_fraction',
          'merge_pool_peak_pct','settle_seconds',
          'cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
          'ch_insert_cpu_seconds_per_Mrows','bytes_on_wire_per_row',
          'ch_parts_per_insert','ch_merge_amplification',
          'ch_inserts_delayed_fraction','ch_merge_pool_peak_pct','ch_settle_seconds'
        )
      )
      GROUP BY run_id, metric_name
    ) AS mm ON e.run_id = mm.run_id
    GROUP BY e.pair_id, e.tier, e.arm, mm.metric_name, mm.value
  ),
  -- Ratio per (pair, tier, metric) — head LEFT JOIN pinned so a MISSING pinned
  -- gated metric (the NULL cell, P04/P11) yields a NULL pinned_value and hence a
  -- NULL ratio, and a pinned value of 0 (the 0-denominator cell, P05/P12) yields
  -- ratio NULL via nullIf(pinned,0). v_pair_ratios uses INNER (it gates complete
  -- pairs); acceptance NEEDS the missing-metric arm to surface as NO_DATA, so we
  -- LEFT JOIN here and toNullable() the pinned side (join_use_nulls=0 would else
  -- fake pinned=0). The ratio expression itself is v_pair_ratios' verbatim
  -- head.value / nullIf(pinned.value, 0).
  --
  -- head_value is carried THROUGH for the TRIPWIRE verdict (parts_per_insert),
  -- which reads the head ABSOLUTE value and IGNORES pinned/ratio entirely.
  ratios AS (
    SELECT
      h.pair_id                        AS pair_id,
      h.tier                           AS tier,
      h.metric                         AS metric,
      h.flagged                        AS flagged,
      h.value                          AS head_value,
      pn.value                         AS pinned_value,
      h.value / nullIf(pn.value, 0)    AS ratio
    FROM (SELECT * FROM metric_long WHERE arm = 'head') AS h
    LEFT JOIN (
      SELECT pair_id, tier, metric, toNullable(value) AS value
      FROM metric_long WHERE arm = 'pinned'
    ) AS pn
      ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
  ),
  -- Attach the PINNED direction per metric (contract §3 direction table), the
  -- CALIBRATED per-metric band (contract §3 Amendment 2026-07-09b — 2x the
  -- measured noise floor, SAME on both tiers), and the tripwire flag.
  classified AS (
    SELECT
      pair_id, tier, metric, flagged, head_value, pinned_value, ratio,
      multiIf(
        metric IN ('throughput_rows_per_sec','null_rows_per_sec',
                   'null_drain_rows_per_sec','drain_rows_per_sec'), 'higher_better',
        metric IN ('cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
                   'ch_insert_cpu_seconds_per_Mrows'), 'lower_better',
        'watch_or_tripwire'          -- parts_per_insert (tripwire), merge_amplification (watch-only)
      ) AS direction,
      -- TRIPWIRE metric flag: parts_per_insert is a binary tripwire, not banded.
      (metric = 'parts_per_insert') AS is_tripwire,
      -- CALIBRATED per-metric band (contract §3 Amendment 2026-07-09b). The band
      -- is a property of the metric's noise floor (2x measured), NOT the tier, so
      -- it is keyed on the metric name and is identical on Tier 0 and Tier 1.
      -- Non-banded metrics (tripwire / watch-only) get band 0 — unused, since the
      -- verdict for them never consults band_lo/band_hi.
      multiIf(
        metric = 'throughput_rows_per_sec',                                 0.09,
        metric IN ('null_rows_per_sec','null_drain_rows_per_sec',
                   'drain_rows_per_sec'),                                    0.085,
        metric IN ('cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows'), 0.06,
        metric = 'serialize_seconds_per_Mrows',                             0.085,
        0.0
      ) AS band,
      1 - multiIf(
        metric = 'throughput_rows_per_sec',                                 0.09,
        metric IN ('null_rows_per_sec','null_drain_rows_per_sec',
                   'drain_rows_per_sec'),                                    0.085,
        metric IN ('cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows'), 0.06,
        metric = 'serialize_seconds_per_Mrows',                             0.085,
        0.0
      ) AS band_lo,
      1 + multiIf(
        metric = 'throughput_rows_per_sec',                                 0.09,
        metric IN ('null_rows_per_sec','null_drain_rows_per_sec',
                   'drain_rows_per_sec'),                                    0.085,
        metric IN ('cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows'), 0.06,
        metric = 'serialize_seconds_per_Mrows',                             0.085,
        0.0
      ) AS band_hi
    FROM ratios
    -- merge_amplification is WATCH-ONLY (contract §3, not gated) — never asserted.
    WHERE metric != 'merge_amplification'
  )
SELECT
  pair_id,
  tier,
  metric,
  direction,
  ratio,
  flagged,
  -- ACTUAL verdict via the PINNED ratio→verdict map, in precedence order:
  --   FLAG > NO_DATA > (banded excursion by direction | tripwire) > OK.
  -- (FLAG is placed ABOVE everything to honour §3 precedence FLAG > all — a
  --  flagged pair is FLAGGED even when its ratio is NULL/0-denominator OR its
  --  tripwire is ARMED, so the flagged branch must dominate both.)
  multiIf(
    flagged,                                              'FLAGGED',
    -- TRIPWIRE branch (parts_per_insert): head absolute value, NO ratio/band.
    is_tripwire AND head_value = 1.0,                     'OK',
    is_tripwire,                                          'TRIPWIRE',
    -- BANDED branch: NO_DATA before band excursions.
    ratio IS NULL,                                        'NO_DATA',
    direction = 'higher_better' AND ratio > band_hi,      'IMPROVEMENT',
    direction = 'higher_better' AND ratio < band_lo,      'REGRESSION',
    direction = 'lower_better'  AND ratio < band_lo,      'IMPROVEMENT',
    direction = 'lower_better'  AND ratio > band_hi,      'REGRESSION',
    'OK'
  )                                              AS actual_verdict,
  -- EXPECTED verdict hard-coded from the fixture matrix (the truth table the seed
  -- was built to realise). Keyed on (pair_id, metric); documented in the seed
  -- header. This is the INDEPENDENT oracle — actual must equal it.
  multiIf(
    -- flagged pairs: always FLAGGED regardless of metric/ratio/tripwire — 10
    -- below-band, 11 NULL, 12 0-denom, 13 in-band, 14 above-band, 15 tripwire
    -- ARMED. 13/14/15 close the precedence product AND catch a bug that hoists an
    -- in-band=>OK, good-excursion=>IMPROVEMENT, or armed-TRIPWIRE arm above the
    -- flag check.
    pair_id IN ('FIXTURE-PAIR-10','FIXTURE-PAIR-11','FIXTURE-PAIR-12',
                'FIXTURE-PAIR-13','FIXTURE-PAIR-14','FIXTURE-PAIR-15'), 'FLAGGED',
    -- TRIPWIRE metric (parts_per_insert), unflagged pairs:
    metric = 'parts_per_insert' AND pair_id IN
      ('FIXTURE-PAIR-08','FIXTURE-PAIR-09'),                            'TRIPWIRE',
    metric = 'parts_per_insert',                                       'OK',  -- ==1.0 on 01-07
    -- BANDED metrics, unflagged pairs:
    -- NULL / 0-denominator => NO_DATA for both banded metrics (P04, P05)
    pair_id IN ('FIXTURE-PAIR-04','FIXTURE-PAIR-05'),                  'NO_DATA',
    -- below-band (P01): thr 0.85 REGRESSION (HB) / cpu 0.90 IMPROVEMENT (LB)
    pair_id = 'FIXTURE-PAIR-01' AND metric = 'throughput_rows_per_sec', 'REGRESSION',
    pair_id = 'FIXTURE-PAIR-01' AND metric = 'cpu_seconds_per_Mrows',   'IMPROVEMENT',
    -- in-band (P02) + tripwire-fired pairs' banded arms in-band (P08, P09)
    pair_id IN ('FIXTURE-PAIR-02','FIXTURE-PAIR-08','FIXTURE-PAIR-09'), 'OK',
    -- above-band (P03): thr 1.15 IMPROVEMENT (HB) / cpu 1.10 REGRESSION (LB)
    pair_id = 'FIXTURE-PAIR-03' AND metric = 'throughput_rows_per_sec', 'IMPROVEMENT',
    pair_id = 'FIXTURE-PAIR-03' AND metric = 'cpu_seconds_per_Mrows',   'REGRESSION',
    -- near-edge INSIDE (P06): thr 1.08 (<1.09) OK / cpu 1.05 (<1.06) OK
    pair_id = 'FIXTURE-PAIR-06',                                        'OK',
    -- near-edge OUTSIDE (P07): thr 1.10 (>1.09) IMPROVEMENT (HB) /
    --                          cpu 0.93 (<0.94) IMPROVEMENT (LB, good dir)
    pair_id = 'FIXTURE-PAIR-07',                                        'IMPROVEMENT',
    'UNEXPECTED-CELL'
  )                                              AS expected_verdict,
  (actual_verdict = expected_verdict)            AS pass
FROM classified
ORDER BY pair_id, metric
