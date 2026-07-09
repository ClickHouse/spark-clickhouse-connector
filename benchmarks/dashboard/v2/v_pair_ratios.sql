-- =============================================================================
-- v2_pair_ratios  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   The controlled-experiment view. For each two-arm pair (runtime['pair_id']),
--   compute the HEAD/pinned ratio of each gated metric at each tier:
--       ratio = head.value / pinned.value   per (pair_id, tier, metric)
--   Environment noise hits both arms of a pair and cancels in the ratio; only a
--   connector change moves it (plan §2, §7 v_pair_ratios sketch). Feeds Tab 1.
--
--   JOIN-strictness asymmetry (deliberate): this view keeps an INNER join —
--   incomplete pairs must not produce ratios (it is the GATE). The Tab-4 drill
--   view v2_pair_metrics_long uses a LEFT join and shows head-only pairs with
--   NULL pinned/delta (it is the MICROSCOPE, for inspection not gating).
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 ("v_pair_ratios ... self-join on
--   runtime['pair_id']: head.value / pinned.value AS ratio per (pair, tier,
--   metric)"), §3 (gated metric set).
-- Contract reference:  docs/benchmark-v2-contract.md §1 (pair_id/arm), §3
--   (flagged & integrity semantics), §7 (renamed metrics coalesced).
--
-- HARD REQUIREMENTS met:
--   * Self-join on runtime pair_id, ratio = head.value / pinned.value per
--     (pair_id, tier, metric).
--   * EXCLUDE BY DEFAULT, on BOTH arms: flagged runs (runtime['flagged']='1'),
--     integrity-FAILED runs (integrity_ok = 0), and outcome='failed' runs. A pair
--     is only emitted when BOTH its arms survive these filters.
--   * Pairs missing an arm simply don't appear (INNER JOIN head<->pinned).
--   * Renamed metrics are coalesced to the pinned name (contract §7) inside the
--     long-form CTE, so a pair straddling the 2026-07-07 cutover still joins as
--     one series.
--
-- Notes:
--   * DWH mirror: raw_connectors_load_testing.{runs,metrics}.
--   * Expected to be EMPTY today: no two-arm pairs have run yet (arm=pinned does
--     not exist in history). It MUST return zero rows without erroring — a valid
--     empty result, not a failure. First rows appear once the nightly two-arm
--     workflow lands (plan §9 step 2).
--   * pinned value = 0 would divide-by-zero; we guard with nullIf(pinned,0) so
--     the ratio is NULL rather than inf/error for that metric only.
-- =============================================================================
WITH
  -- Latest value per (run_id, metric_name).
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  -- Runs with unnested scope + a survivability filter applied per arm.
  -- integrity_ok derived exactly as in v_runs_enriched (direct metric preferred,
  -- else delivered/expected comparison, else unknown).
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
      -- max(if(cond, value, NULL)) NOT maxIf(value, cond): maxIf returns 0.0
      -- when no row matches, which would make a run with NO integrity capture
      -- look integrity-FAILED (0) instead of unknown (NULL) and wrongly exclude
      -- it. The if(...NULL) form yields NULL on absence.
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
  -- Apply the default exclusions. A run is eligible iff: not flagged, not
  -- outcome='failed', and NOT integrity-failed (integrity_ok=0). Integrity
  -- unknown (NULL) is allowed through (legacy rows) — same policy as headline_ok.
  eligible AS (
    SELECT run_id, pair_id, arm, tier
    FROM runs_scoped
    WHERE pair_id != ''
      AND flagged = 0
      AND outcome != 'failed'
      -- Exclude ONLY integrity-FAILED (= 0). The multiIf is NULL for
      -- integrity-unknown runs; coalesce(x, 1) lets unknown pass. A bare
      -- `NOT (x = 0)` would be NULL for unknown and WHERE would drop the row
      -- (three-valued logic) — the opposite of the intended policy.
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
  -- Long-form gated metric values per eligible run, renamed metrics coalesced.
  metric_long AS (
    SELECT
      e.pair_id AS pair_id,
      e.tier    AS tier,
      e.arm     AS arm,
      mm.metric_name AS metric,
      mm.value       AS value
    FROM eligible AS e
    INNER JOIN (
      -- Re-map legacy ch_-prefixed names to the pinned contract name so both
      -- sides of the cutover share one `metric` key (contract §7). Non-renamed
      -- gated metrics pass through unchanged.
      --
      -- REGRESSION GUARD (review nit, 2026-07-08): a run straddling the rename
      -- cutover could transiently emit BOTH spellings of the same metric (e.g.
      -- `parts_per_insert` AND `ch_parts_per_insert`). Without de-dup the remap
      -- would yield TWO (run_id, 'parts_per_insert') rows, and the head<->pinned
      -- self-join below would fan out (up to 4 ratio rows for that pair/tier/
      -- metric cell instead of 1). We collapse to one row per (run_id, remapped
      -- name) here, PREFERRING the new spelling: `prefer` = 1 for a row whose
      -- ORIGINAL name already was the pinned spelling (not remapped), 0 for a
      -- row remapped from a legacy ch_ alias; argMax(value, prefer) then takes
      -- the new-spelling value when both exist. When only one spelling is
      -- present (the normal case) argMax returns that sole value unchanged, so
      -- results are byte-identical to the pre-guard query.
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
          -- 1 = original name was already the pinned spelling (no remap applied),
          -- 0 = remapped from a legacy ch_ alias -> new spelling wins the argMax.
          -- MUST read the RAW source name (m.metric_name); the projected
          -- `metric_name` alias above is the POST-remap value, so a legacy row
          -- would look pinned (prefer=1) and win ties wrongly.
          if(startsWith(m.metric_name, 'ch_'), 0, 1) AS prefer,
          m.value AS value
        FROM m
        WHERE m.metric_name IN (
          -- gated / comparable metrics (plan §3): pinned names ...
          'throughput_rows_per_sec','null_rows_per_sec',
          'parts_per_insert','merge_amplification','inserts_delayed_fraction',
          'merge_pool_peak_pct','settle_seconds',
          'cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
          'ch_insert_cpu_seconds_per_Mrows','bytes_on_wire_per_row',
          -- ... and their legacy aliases (coalesced above)
          'ch_parts_per_insert','ch_merge_amplification',
          'ch_inserts_delayed_fraction','ch_merge_pool_peak_pct','ch_settle_seconds'
        )
      )
      GROUP BY run_id, metric_name
    ) AS mm ON e.run_id = mm.run_id
  )
SELECT
  h.pair_id                        AS pair_id,
  h.tier                           AS tier,
  h.metric                         AS metric,
  h.value                          AS head_value,
  pn.value                         AS pinned_value,
  h.value / nullIf(pn.value, 0)    AS ratio
FROM (SELECT * FROM metric_long WHERE arm = 'head')   AS h
INNER JOIN (SELECT * FROM metric_long WHERE arm = 'pinned') AS pn
  ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
