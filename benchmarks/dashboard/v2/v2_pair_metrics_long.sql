-- =============================================================================
-- v2_pair_metrics_long  —  Benchmark v2 Superset virtual dataset (dataset B)
-- =============================================================================
-- Purpose:
--   Tab-4 "arm comparison" table for a single drilled pair. One row per gated
--   metric, exposing head_value, pinned_value and delta_pct side-by-side so the
--   run-drill table can read like an experiment report. This is the raw long
--   form the chart consumes directly (columns: pair_id, tier, metric,
--   head_value, pinned_value, delta_pct); the native pair_id filter (Tab-4
--   scope) narrows it to one pair.
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 (data foundation).
-- Spec reference:  merged-dashboard-spec.md §3 (Tab-4 arm-comparison table on
--   NEW dataset v2_pair_metrics_long: raw metric/head_value/pinned_value/
--   delta_pct) and §6 (dataset B).
-- Contract reference:  docs/benchmark-v2-contract.md §1 (pair_id/arm/tier), §7
--   (renamed metrics coalesced via multiIf remap).
--
-- CONNECTOR SCOPING (2026-07-10): kafka pairs now share these SAME DWH tables (runs
--   carries a first-class `connector` column; our rows are connector='spark').
--   Scoped to 'spark' so kafka pairs never contaminate the Tab-4 pair drill;
--   cross-connector lives on kafka's Tab 5 per contract §6. FLAGGED NOTE: kafka's
--   first ingested pair carries a pre-correction flagged='true' spelling our flagged
--   predicate (runtime['flagged']='1') would misread as unflagged — connector
--   scoping moots that here; the contract pins '1'.
--
-- Relationship to v_pair_ratios:
--   v_pair_ratios feeds Tab-1 (all pairs, ratio = head/pinned). This view is the
--   per-pair DRILL companion: same head<->pinned self-join skeleton and the same
--   §7 rename remap, but emits an absolute head/pinned/delta_pct row set for the
--   Tab-4 table rather than a ratio trend. It reads the raw mirror tables
--   directly (spec §4: no self-referencing virtual datasets).
--
-- HARD REQUIREMENTS met:
--   * §7 rename coalesce: legacy ch_-prefixed names are remapped to the pinned
--     name via multiIf BEFORE the arm self-join, so a pair straddling the
--     2026-07-07 cutover compares like-for-like on one `metric` key.
--   * DUP-EDGE HARDENING (documented per spec §6): the multiIf remap collapses
--     e.g. `ch_parts_per_insert` and `parts_per_insert` to the same key. If a
--     single run ever emitted BOTH spellings, the remap would yield TWO rows for
--     that (run_id, metric) key and the arm self-join would fan out into
--     duplicate table rows. We de-dup with
--         argMax(value, (recorded_at, raw_metric_name = metric))
--     GROUP BY (run_id, remapped metric): latest capture wins, and on an EQUAL
--     recorded_at the PINNED spelling (raw name == remapped key) beats the
--     legacy ch_ alias. The tie-break matters because a same-run dup would most
--     plausibly land in one insert batch — same second-granularity recorded_at —
--     where a bare argMax(value, recorded_at) is undefined on the tie. Mirrors
--     the de-dup hardening in v_pair_ratios. Absent both-spellings this is a
--     no-op (one row in, one row out).
--   * Default exclusions on BOTH arms (same policy as v_pair_ratios / headline):
--     drop flagged runs, outcome='failed' runs, and integrity-FAILED (=0) runs;
--     integrity-unknown (NULL, legacy) is allowed through.
--   * MISSING-ARM POLICY — DELIBERATE ASYMMETRY vs v_pair_ratios (manager
--     decision, 2026-07-08): head LEFT JOIN pinned. A head-only pair (pinned arm
--     missing or excluded by the filters above) still appears, with NULL
--     pinned_value and NULL delta_pct — a drill view is for INSPECTION, so an
--     incomplete pair should be visible, not vanish into an empty table.
--     v_pair_ratios keeps its strict INNER JOIN: that view is the regression
--     GATE and must only ever compare complete pairs. Gate = INNER (strict),
--     microscope = LEFT (permissive). Pinned-only rows (head arm excluded) do
--     NOT appear — head is the subject of the drill.
--
-- Notes:
--   * DWH mirror: raw_connectors_load_testing.{runs,metrics}.
--   * Expected EMPTY until two-arm pairs exist (arm=pinned has no history yet).
--     MUST return zero rows without erroring.
--   * delta_pct = (head - pinned) / |pinned| * 100, guarded by nullIf(pinned,0)
--     so a zero pinned value yields NULL rather than inf/error for that metric.
-- =============================================================================
WITH
  -- Latest value per (run_id, metric_name) before any remap.
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  -- Runs with unnested scope + integrity inputs (derived exactly as in
  -- v_runs_enriched / v_pair_ratios: direct metric preferred, else
  -- delivered/expected comparison, else unknown).
  runs_scoped AS (
    SELECT
      r.run_id                                              AS run_id,
      r.runtime['pair_id']                                  AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''),  'head')       AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')          AS tier,
      (r.runtime['flagged'] = '1')                          AS flagged,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome,
      p.integrity_ok_metric,
      p.rows_delivered, p.rows_expected, p.unique_delivered, p.unique_expected
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN (
      -- max(if(cond,value,NULL)) NOT maxIf: absent integrity capture must read
      -- NULL (unknown), not 0.0 (failed). Matches v_runs_enriched.
      SELECT
        run_id,
        max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
        max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
        max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
        max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
        max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected
      FROM m GROUP BY run_id
    ) AS p ON r.run_id = p.run_id
    -- kafka rows share these tables since 2026-07-10; Spark dashboard datasets are
    -- connector-scoped; cross-connector lives on kafka's Tab 5 per contract §6.
    WHERE r.connector = 'spark'
    -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
    -- from all real trends (fixture is a CI truth-table, never a real run).
    -- (connector='spark' already excludes it — kept as belt-and-braces.)
      AND r.connector != 'verdict_fixture'
  ),
  -- Default exclusions. Eligible iff: not flagged, not outcome='failed', and
  -- NOT integrity-failed (=0). Integrity-unknown (NULL) passes via coalesce(x,1).
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
  -- Gated metric values per eligible run, with the §7 legacy->pinned remap.
  -- We remap FIRST, then de-dup, so both-spellings collapse to one value.
  remapped AS (
    SELECT
      mt.run_id AS run_id,
      multiIf(
        mt.metric_name = 'ch_parts_per_insert',         'parts_per_insert',
        mt.metric_name = 'ch_merge_amplification',      'merge_amplification',
        mt.metric_name = 'ch_inserts_delayed_fraction', 'inserts_delayed_fraction',
        mt.metric_name = 'ch_merge_pool_peak_pct',      'merge_pool_peak_pct',
        mt.metric_name = 'ch_settle_seconds',           'settle_seconds',
        mt.metric_name = 'ch_connections_per_insert',   'connections_per_insert',
        mt.metric_name
      ) AS metric,
      mt.metric_name AS raw_metric_name,
      mt.value       AS value,
      mt.recorded_at AS recorded_at
    FROM (
      -- Raw capture rows (NO de-dup here — that is the outer argMax's job).
      -- recorded_at and the raw metric_name are carried through so the
      -- post-remap de-dup can pick latest-then-pinned across both spellings.
      SELECT run_id, metric_name, value, recorded_at
      FROM raw_connectors_load_testing.metrics
      WHERE metric_name IN (
        -- pinned / comparable gated metrics (plan §3) ...
        'throughput_rows_per_sec','null_rows_per_sec',
        'parts_per_insert','merge_amplification','inserts_delayed_fraction',
        'merge_pool_peak_pct','settle_seconds','connections_per_insert',
        'cpu_seconds_per_Mrows','serialize_seconds_per_Mrows',
        'ch_insert_cpu_seconds_per_Mrows','bytes_on_wire_per_row',
        -- ... and their legacy aliases (remapped above)
        'ch_parts_per_insert','ch_merge_amplification',
        'ch_inserts_delayed_fraction','ch_merge_pool_peak_pct',
        'ch_settle_seconds','ch_connections_per_insert'
      )
    ) AS mt
  ),
  -- DUP-EDGE de-dup: collapse to one value per (run_id, remapped metric).
  -- argMax over the tuple (recorded_at, is-pinned-spelling): latest capture
  -- wins; on an equal recorded_at (the likely dup shape — one insert batch,
  -- second-granularity timestamps) the pinned spelling beats the legacy ch_
  -- alias. `raw_metric_name = metric` is exact: it is 1 only for the spelling
  -- that survived the remap unchanged. No-op when only one spelling is present.
  metric_dedup AS (
    SELECT
      run_id,
      metric,
      argMax(value, (recorded_at, raw_metric_name = metric)) AS value
    FROM remapped
    GROUP BY run_id, metric
  ),
  -- Attach scope; one long row per (pair, tier, arm, metric).
  metric_long AS (
    SELECT
      e.pair_id AS pair_id,
      e.tier    AS tier,
      e.arm     AS arm,
      d.metric  AS metric,
      d.value   AS value
    FROM eligible AS e
    INNER JOIN metric_dedup AS d ON e.run_id = d.run_id
  )
SELECT
  h.pair_id                                                      AS pair_id,
  h.tier                                                         AS tier,
  h.metric                                                       AS metric,
  h.value                                                        AS head_value,
  pn.value                                                       AS pinned_value,
  (h.value - pn.value) / nullIf(abs(pn.value), 0) * 100          AS delta_pct
FROM (SELECT * FROM metric_long WHERE arm = 'head') AS h
-- LEFT JOIN (head preserved), NOT INNER — see MISSING-ARM POLICY in the header.
-- The pinned side's value is toNullable()'d because with the default
-- join_use_nulls = 0 an unmatched right side yields the Float64 default (0.0),
-- which would fake a pinned_value of 0 and an infinite-looking delta; Nullable's
-- default is NULL, so head-only rows read NULL pinned_value / NULL delta_pct.
LEFT JOIN (
  SELECT pair_id, tier, metric, toNullable(value) AS value
  FROM metric_long WHERE arm = 'pinned'
) AS pn
  ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
