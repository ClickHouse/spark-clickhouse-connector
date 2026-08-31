-- =============================================================================
-- v2_instrument_annotations  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   Change-detection over the measurement HARNESS (the load generator), emitting
--   one row per detected instrument step-change so Superset annotation layers can
--   draw markers and so downstream gate/band consumers can EXCLUDE the affected
--   pair. It surfaces the `instrument_resize` flag reason (contract §1.3):
--
--     * instrument_resize — the harness's compute resources changed vs the
--                           PREVIOUS pair in time order. On Spark the instrument
--                           truth is the EMR config (emr_instance_type +
--                           emr_core_count + emr_release), carried on every
--                           perf.runs row via the runtime map (see run-arm
--                           action.yml, contract §1.3 "instrument-truth runtime
--                           keys"). A change is the one environment step-change
--                           the H/P ratio CANNOT cancel (both arms of the new
--                           pair run on the new instrument), so band calibration
--                           MUST restart at the first pair on the new instrument.
--
-- WHY A SEPARATE, PAIR-LEVEL VIEW (additive, deliberately):
--   * This is emitted ADDITIVELY as its own view — it does NOT alter a column of
--     v2_runs_enriched or v2_pair_ratios (those back the live charts; touching
--     their columns risks breaking bindings). Deploy it as a new Superset dataset
--     + annotation layer; nothing existing changes.
--   * The instrument is a PAIR property, not a run property: both arms of a pair
--     run on the same cluster, so the emr_* keys are identical across a pair's
--     rows. We therefore collapse to ONE instrument signature per pair (any() over
--     the pair's runs) and lag over the PREVIOUS PAIR in time order — exactly the
--     "compare a pair's emr config to the previous pair" shape the task specifies,
--     and the same lag-over-previous shape as v_env_annotations.
--   * run-arm does NOT emit `instrument_resize` into runtime['flag_reason'] (it
--     cannot see the previous pair). The flag is DERIVED here from the raw
--     instrument-truth keys that already exist on every row — "detectable from
--     data, not memory" (contract §1.3). Consumers that gate/calibrate can join
--     this view's pair_id back to v2_pair_ratios and drop matching pairs, and the
--     v2_flag_rate panel counts it alongside the emitted tokens.
--
-- Plan reference:  docs/benchmark-v2-plan.md §6.10 (validity guards),
--   docs/benchmark-v2-contract.md §1.3 (instrument_resize + instrument-truth keys).
-- Contract reference:  §4 scoping — instrument events, like env events, MUST carry
--   the (connector, target_service, environment_class) scope tuple; an unscoped
--   view would paint one benchmark's harness change onto the other's charts.
--
-- HARD REQUIREMENTS met:
--   * ADDITIVE: new file / new dataset, no change to existing view columns.
--   * Scoped by (connector, target_service, environment_class) — the lag window
--     partitions BY this tuple and every output row carries it (contract §4).
--   * Pair-level: one instrument signature per pair, lag over the previous pair
--     in time order; exactly one event per genuine transition.
--   * NULL-safe on absence: legacy rows predate the emr_* keys; the '' guards
--     suppress a backfill first-appearance ('' -> value) so no false event fires.
--
-- Notes:
--   * DWH mirror: raw_connectors_load_testing.{runs}. (emr_* live on the runtime
--     Map of perf.runs, so no metrics join is needed.)
--   * Ordering for lag() is by the pair's start time. pair_ts is derived from the
--     pair_id prefix exactly as v2_pair_ratios / charts 5703/5704 parse it, so
--     ordering is consistent across the dashboard; a malformed id yields NULL and
--     sorts last, never erroring.
--   * Expected EMPTY today for Spark until the EMR instrument actually changes
--     between two consecutive pairs — a valid empty result, not a failure.
-- =============================================================================
WITH
  -- One row per (connector, scope, pair) with the instrument signature. Both arms
  -- of a pair carry identical emr_* keys (same cluster), so any() collapses the
  -- pair's rows to one signature. Scope tuple derived exactly as v_env_annotations
  -- (target_service = explicit key if present, else region:class proxy).
  pair_instrument AS (
    SELECT
      r.connector                                       AS connector,
      coalesce(
        nullIf(r.runtime['target_service'], ''),
        concat(
          coalesce(nullIf(r.runtime['target_region'], ''), 'unknown_region'),
          ':',
          coalesce(nullIf(r.runtime['environment_class'], ''), 'unknown_class')
        )
      )                                                 AS target_service,
      coalesce(nullIf(r.runtime['environment_class'], ''), 'unknown_class') AS environment_class,
      r.runtime['pair_id']                              AS pair_id,
      -- Instrument-truth signature (contract §1.3). Spark: the emr_* keys. Any
      -- change to instance type, core count, or release label is an instrument
      -- step-change. any() is safe: identical across a pair's arms by construction.
      any(nullIf(r.runtime['emr_instance_type'], ''))   AS emr_instance_type,
      any(nullIf(r.runtime['emr_core_count'], ''))      AS emr_core_count,
      any(nullIf(r.runtime['emr_release'], ''))         AS emr_release,
      -- Pair start time, derived from the pair_id prefix (same parse as
      -- v2_pair_ratios). NULL-safe: a malformed/fixture id yields '' -> NULL.
      if(
        extract(r.runtime['pair_id'], '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
        NULL,
        parseDateTimeBestEffortOrNull(
          replaceRegexpOne(
            extract(r.runtime['pair_id'], '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
            'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
          )
        )
      )                                                 AS pair_ts
    FROM raw_connectors_load_testing.runs AS r
    -- Real Spark pairs only. Legacy rows with no pair_id are not pairs; the
    -- verdict-fixture connector is a CI truth-table, never a real run (contract §3).
    WHERE r.connector = 'spark'
      AND r.connector != 'verdict_fixture'
      AND r.runtime['pair_id'] != ''
    GROUP BY connector, target_service, environment_class, pair_id
  ),
  -- Lag the instrument signature over the PREVIOUS PAIR in the SAME scope tuple,
  -- ordered by pair start time. Same lag-over-previous shape as v_env_annotations'
  -- seq CTE, but the unit is the PAIR (not the run) because the instrument is a
  -- pair property.
  seq AS (
    SELECT
      connector, target_service, environment_class, pair_id, pair_ts,
      emr_instance_type, emr_core_count, emr_release,
      concat(emr_instance_type, ' / ', emr_core_count, ' cores / ', emr_release) AS instrument_sig,
      lagInFrame(concat(emr_instance_type, ' / ', emr_core_count, ' cores / ', emr_release)) OVER w AS prev_instrument_sig,
      lagInFrame(emr_instance_type) OVER w AS prev_emr_instance_type,
      lagInFrame(emr_core_count)    OVER w AS prev_emr_core_count,
      lagInFrame(emr_release)       OVER w AS prev_emr_release
    FROM pair_instrument
    WINDOW w AS (
      PARTITION BY connector, target_service, environment_class
      ORDER BY pair_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  )
-- ---- instrument_resize events ----
-- One row per genuine transition: the FIRST pair on a new instrument differs from
-- its predecessor and fires exactly one event. The '' / NULL guards suppress the
-- legacy first-appearance ('' -> value backfill) so a schema roll-forward does not
-- manufacture a false resize. emitted columns mirror v2_env_annotations' schema
-- (event_time, event_type, scope tuple, detail, pair_id / run_id analogue) so the
-- two annotation datasets can share one Superset annotation layer shape.
SELECT
  pair_ts                                               AS event_time,
  'instrument_resize'                                    AS event_type,
  connector, target_service, environment_class,
  concat(prev_instrument_sig, ' -> ', instrument_sig)   AS detail,
  pair_id
FROM seq
WHERE prev_instrument_sig IS NOT NULL
  AND prev_instrument_sig != ''
  AND instrument_sig != ''
  AND instrument_sig != prev_instrument_sig
  -- Do not fire on a pure '' -> value first-appearance of any single component
  -- (legacy backfill of an emr_* key): require that BOTH the previous and current
  -- signatures name a real instrument. (prev != '' above already covers the whole
  -- signature; this keeps parity with the per-component intent for clarity.)
  AND prev_emr_instance_type != ''
  AND emr_instance_type != ''
ORDER BY event_time
