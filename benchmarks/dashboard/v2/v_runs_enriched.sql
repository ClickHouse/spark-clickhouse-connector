-- =============================================================================
-- v2_runs_enriched  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   The base fact table for every v2 chart. One row per perf.runs row (i.e. per
--   (arm, tier) run), with the runtime Map unnested into typed columns and the
--   tall perf.metrics pivoted into one wide column per metric.
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 ("Data foundation", the
--   v_runs_enriched sketch) — "runs + unnested runtime map (arm, tier, pair_id,
--   config...) joined to pivoted metrics; missing arm -> 'head', missing tier
--   -> '1' (legacy rows stay first-class)".
-- Contract reference:  docs/benchmark-v2-contract.md §1 (runtime keys + coalesce
--   defaults), §2 (metric names), §3 (integrity semantics), §7 (legacy->contract
--   metric renames, cutover 2026-07-07).
--
-- CONNECTOR SCOPING (2026-07-10): kafka benchmark pairs now land in these SAME
--   DWH tables (raw_connectors_load_testing.runs carries a first-class `connector`
--   column; our rows are connector='spark'). This dataset is connector-scoped to
--   'spark' so kafka rows never contaminate Spark tiles; cross-connector lives on
--   kafka's Tab 5 per contract §6. FLAGGED NOTE: kafka's first ingested pair carries
--   a pre-correction flagged='true' spelling that our flagged predicate
--   (runtime['flagged']='1') would misread as unflagged — connector scoping moots
--   that here; the contract pins '1'. (Context for future Tab-5 work.)
--
-- HARD REQUIREMENTS met:
--   * runtime map unnested with contract coalesce defaults: arm -> 'head',
--     tier -> '1', outcome -> 'success' (contract §1.1 "Absent =>" column).
--     Legacy rows (no runtime keys at all) therefore appear as head / tier 1 /
--     success and remain FIRST-CLASS — the absolute trend line is never broken.
--   * integrity_ok computed from the integrity metrics (contract §3): prefers the
--     directly-emitted `integrity_ok` metric; falls back to
--     rows_delivered == rows_expected AND unique_delivered == unique_expected for
--     rows that predate the direct metric. Rows with NO integrity capture at all
--     are integrity_ok = NULL (unknown) — they are NOT treated as failed.
--   * integrity_failed runs are DEFAULT-EXCLUDED from headline use via the
--     `headline_ok` flag (1 = safe for bands/ratios/trends). Charts filter
--     headline_ok = 1; drill-downs can drop the filter to see the raw rows
--     (integrity_ok = 0 rows are still present in this dataset).
--   * legacy -> contract metric-name coalesce (rename cutover 2026-07-07,
--     contract §7): the new name is preferred, the ch_-prefixed legacy name is
--     the fallback, so history on both sides of the cutover is one series:
--       parts_per_insert         <- ch_parts_per_insert
--       merge_amplification      <- ch_merge_amplification
--       inserts_delayed_fraction <- ch_inserts_delayed_fraction
--       merge_pool_peak_pct      <- ch_merge_pool_peak_pct
--       settle_seconds           <- ch_settle_seconds
--       connections_per_insert   <- ch_connections_per_insert
--         (6th pair, contract amendment commit 73c8969d; emitted by
--          benchmarks/sql/perf/12_insert_from_metric_log.sql. Rename lands with
--          the Tier-0 work: post-cutover rows may not exist yet — until they do,
--          only the legacy ch_ name returns values, which the coalesce handles.)
--
-- Notes:
--   * DWH mirror schema: perf.runs -> raw_connectors_load_testing.runs,
--     perf.metrics -> raw_connectors_load_testing.metrics (ClickPipe).
--   * A metric_name may be re-captured within a run (rollback/retry); we take the
--     latest by recorded_at via argMax (the same convention used across the
--     capture SQL when a metric_name is re-captured within a run).
--   * Map value access on a missing key yields '' in ClickHouse, so nullIf(...,'')
--     turns "key absent" into NULL before COALESCE applies the contract default.
--
-- PATCH A (merged-dashboard build, 2026-07-08 — merged-dashboard-spec.md §2):
--   Adds the Tier-0 connector-cost pivot columns and SELECT-level derived
--   task-time decomposition shares needed by the new Tab-2 charts
--   (v2_t0_cpu_serialize_per_Mrows, v2_t0_time_decomposition).
--   New pivot columns (all `max(if(...,value,NULL))`, NEVER maxIf — see the
--   pivot note below; absent metric must stay NULL, not collapse to 0.0):
--       cpu_seconds_per_Mrows         (emitted by 10_insert_from_event_log.sql)
--       serialize_seconds_per_Mrows   (emitted by 10_insert_from_event_log.sql)
--       serialize_time_total          (emitted by 10_insert_from_event_log.sql)
--       executor_cpu_time_total       (emitted by 10_insert_from_event_log.sql)
--       write_time_total              (emitted by 10_insert_from_event_log.sql)
--       write_wait_share              (the EMITTED metric, 10_insert_from_event_log.sql:
--                                      write_time_total / (write + serialize + cpu))
--       ch_insert_cpu_share_tier0     (contract §2.1 Tier-0 instrument-health
--                                      watch: Null-target insert CPU / wall-clock
--                                      ×100. WATCH-ONLY — live verification saw
--                                      it swing 20-44% between t0 arms; surfaced
--                                      so dashboards can watch for the parse-
--                                      bound-instrument failure mode, NOT gated.)
--   New SELECT-level derived shares (normalized task-time decomposition; the
--   three sum to 1 on any row where the three *_total metrics are present):
--       serialize_share          = serialize_time_total  / denom
--       other_cpu_share          = executor_cpu_time_total / denom
--       write_wait_share_derived = write_time_total       / denom
--     where denom = nullIf(write_time_total + serialize_time_total
--                          + executor_cpu_time_total, 0)  (NULL denom -> NULL share).
--
--   NAMING DECISION (write_wait_share collision) — documented per spec §2:
--     `write_wait_share` ALREADY exists as a first-class emitted metric
--     (10_insert_from_event_log.sql line ~189), so it is pivoted UNCHANGED under
--     that exact name (preserves the raw series for any chart binding it).
--     The SELECT-level RE-derivation of the same quantity is emitted under the
--     DISTINCT name `write_wait_share_derived` to avoid a column-name collision
--     with the pivoted emitted metric. Algebraically the derived value equals the
--     emitted `write_wait_share` (identical formula); the decomposition chart uses
--     the *_derived trio so all three shares are computed from one consistent
--     denominator on the same row.
-- =============================================================================
WITH
  -- One value per (run_id, metric_name): latest capture wins.
  m AS (
    SELECT
      run_id,
      metric_name,
      argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  -- Pivot the tall metrics to one column per metric. We use
  -- `max(if(cond, value, NULL))` (NOT maxIf(value, cond)) deliberately: maxIf
  -- returns the numeric-type DEFAULT (0.0) when NO row matches, which would make
  -- an absent metric indistinguishable from a genuine 0 and would defeat the
  -- coalesce fallback below. The if(...NULL) branch makes the aggregated column
  -- Nullable, so max() ignores NULLs and yields NULL when the metric is absent.
  -- Renamed metrics then COALESCE the pinned name over the legacy ch_ name; the
  -- NULL-on-absence semantics make that fallback fire for pre-cutover rows.
  pivot AS (
    SELECT
      run_id,
      -- integrity (contract §2.1 / §3)
      max(if(metric_name = 'integrity_ok',        value, NULL)) AS integrity_ok_metric,
      max(if(metric_name = 'rows_delivered',      value, NULL)) AS rows_delivered,
      max(if(metric_name = 'rows_expected',       value, NULL)) AS rows_expected,
      max(if(metric_name = 'unique_delivered',    value, NULL)) AS unique_delivered,
      max(if(metric_name = 'unique_expected',     value, NULL)) AS unique_expected,
      max(if(metric_name = 'duplicate_rows',      value, NULL)) AS duplicate_rows,
      max(if(metric_name = 'ch_dedup_dropped_blocks', value, NULL)) AS ch_dedup_dropped_blocks,
      -- Tier 1 headline + cost-per-row
      max(if(metric_name = 'throughput_rows_per_sec', value, NULL)) AS throughput_rows_per_sec,
      max(if(metric_name = 'throughput_mb_per_sec',   value, NULL)) AS throughput_mb_per_sec,
      max(if(metric_name = 'e2e_duration',            value, NULL)) AS e2e_duration,
      -- Renamed metrics: pinned name preferred, legacy ch_ name as fallback
      -- (contract §7, cutover 2026-07-07).
      coalesce(
        max(if(metric_name = 'parts_per_insert',    value, NULL)),
        max(if(metric_name = 'ch_parts_per_insert', value, NULL))
      )                                                   AS parts_per_insert,
      coalesce(
        max(if(metric_name = 'merge_amplification',    value, NULL)),
        max(if(metric_name = 'ch_merge_amplification', value, NULL))
      )                                                   AS merge_amplification,
      coalesce(
        max(if(metric_name = 'inserts_delayed_fraction',    value, NULL)),
        max(if(metric_name = 'ch_inserts_delayed_fraction', value, NULL))
      )                                                   AS inserts_delayed_fraction,
      coalesce(
        max(if(metric_name = 'merge_pool_peak_pct',    value, NULL)),
        max(if(metric_name = 'ch_merge_pool_peak_pct', value, NULL))
      )                                                   AS merge_pool_peak_pct,
      coalesce(
        max(if(metric_name = 'settle_seconds',    value, NULL)),
        max(if(metric_name = 'ch_settle_seconds', value, NULL))
      )                                                   AS settle_seconds,
      coalesce(
        max(if(metric_name = 'connections_per_insert',    value, NULL)),
        max(if(metric_name = 'ch_connections_per_insert', value, NULL))
      )                                                   AS connections_per_insert,
      max(if(metric_name = 'settle_timed_out',     value, NULL)) AS settle_timed_out,
      max(if(metric_name = 'ch_parts_active_peak', value, NULL)) AS ch_parts_active_peak,
      max(if(metric_name = 'ch_memory_limit_errors', value, NULL)) AS ch_memory_limit_errors,
      max(if(metric_name = 'ch_avg_rows_per_insert', value, NULL)) AS ch_avg_rows_per_insert,
      max(if(metric_name = 'bytes_on_wire_per_row', value, NULL)) AS bytes_on_wire_per_row,
      -- covariates (contract §2.1)
      max(if(metric_name = 'ch_uptime',            value, NULL)) AS ch_uptime,
      max(if(metric_name = 'pre_run_rss',          value, NULL)) AS pre_run_rss,
      max(if(metric_name = 'pre_run_active_parts', value, NULL)) AS pre_run_active_parts,
      -- Tier 0 (may not exist yet)
      max(if(metric_name = 'null_rows_per_sec',    value, NULL)) AS null_rows_per_sec,
      max(if(metric_name = 'run_cost_usd',         value, NULL)) AS run_cost_usd,
      -- PATCH A (spec §2): Tier-0 connector-cost signals + task-time decomposition
      -- inputs. All max(if(...,value,NULL)) — an absent metric MUST be NULL, not
      -- 0.0 (see pivot note above); the derived shares below rely on NULL-on-absence.
      max(if(metric_name = 'cpu_seconds_per_Mrows',       value, NULL)) AS cpu_seconds_per_Mrows,
      max(if(metric_name = 'serialize_seconds_per_Mrows', value, NULL)) AS serialize_seconds_per_Mrows,
      max(if(metric_name = 'serialize_time_total',        value, NULL)) AS serialize_time_total,
      max(if(metric_name = 'executor_cpu_time_total',     value, NULL)) AS executor_cpu_time_total,
      max(if(metric_name = 'write_time_total',            value, NULL)) AS write_time_total,
      -- The EMITTED write_wait_share metric, pivoted under its exact name (the
      -- SELECT-level re-derivation is `write_wait_share_derived` — see header).
      max(if(metric_name = 'write_wait_share',            value, NULL)) AS write_wait_share,
      -- Tier-0 instrument-health watch (contract §2.1) — watch-only, see header.
      max(if(metric_name = 'ch_insert_cpu_share_tier0',   value, NULL)) AS ch_insert_cpu_share_tier0
    FROM m
    GROUP BY run_id
  )
SELECT
  r.run_id                                               AS run_id,
  r.run_started_at                                       AS run_started_at,
  r.run_ended_at                                         AS run_ended_at,
  r.git_sha                                              AS git_sha,
  r.connector                                            AS connector,
  r.run_profile                                          AS run_profile,
  r.connector_version                                    AS connector_version,
  r.clickhouse_version                                   AS clickhouse_version,
  r.notes                                                AS notes,

  -- ---- runtime map unnested with contract coalesce defaults (§1) ----
  coalesce(nullIf(r.runtime['arm'], ''),  'head')        AS arm,
  coalesce(nullIf(r.runtime['tier'], ''), '1')           AS tier,
  r.runtime['pair_id']                                   AS pair_id,
  coalesce(nullIf(r.runtime['outcome'], ''), 'success')  AS outcome,
  (r.runtime['flagged'] = '1')                           AS flagged,
  r.runtime['flag_reason']                               AS flag_reason,
  -- config keys (§1.4)
  r.runtime['batch_size']                                AS batch_size,
  r.runtime['write_parallelism']                         AS write_parallelism,
  r.runtime['async_insert']                              AS async_insert,
  r.runtime['partition_scheme']                          AS partition_scheme,
  r.runtime['dataset']                                   AS dataset,
  -- scope keys (§1.1)
  r.runtime['environment_class']                         AS environment_class,
  r.runtime['target_region']                             AS target_region,
  r.runtime['compute_region']                            AS compute_region,
  r.runtime['warm_up']                                   AS warm_up,

  -- ---- pivoted metrics ----
  p.throughput_rows_per_sec,
  p.throughput_mb_per_sec,
  p.e2e_duration,
  p.parts_per_insert,
  p.merge_amplification,
  p.inserts_delayed_fraction,
  p.merge_pool_peak_pct,
  p.settle_seconds,
  p.connections_per_insert,
  p.settle_timed_out,
  p.ch_parts_active_peak,
  p.ch_memory_limit_errors,
  p.ch_avg_rows_per_insert,
  p.bytes_on_wire_per_row,
  p.rows_delivered,
  p.rows_expected,
  p.unique_delivered,
  p.unique_expected,
  p.duplicate_rows,
  p.ch_dedup_dropped_blocks,
  p.ch_uptime,
  p.pre_run_rss,
  p.pre_run_active_parts,
  p.null_rows_per_sec,
  p.run_cost_usd,

  -- ---- PATCH A pivoted connector-cost metrics (spec §2) ----
  p.cpu_seconds_per_Mrows,
  p.serialize_seconds_per_Mrows,
  p.serialize_time_total,
  p.executor_cpu_time_total,
  p.write_time_total,
  p.write_wait_share,             -- emitted metric, verbatim
  p.ch_insert_cpu_share_tier0,    -- t0 instrument-health watch (watch-only)

  -- ---- PATCH A derived task-time decomposition shares (spec §2) ----
  -- Normalized share of client task-time: serialize vs other CPU vs wire-wait.
  -- The three sum to 1 whenever all three *_total inputs are present; any NULL
  -- input (or a zero total -> nullIf denom NULL) yields NULL shares, so absent
  -- rows stay blank rather than reading as 0. `write_wait_share_derived` is the
  -- deliberately-distinct twin of the emitted `write_wait_share` (header note).
  p.serialize_time_total
    / nullIf(p.write_time_total + p.serialize_time_total + p.executor_cpu_time_total, 0)
                                                         AS serialize_share,
  p.executor_cpu_time_total
    / nullIf(p.write_time_total + p.serialize_time_total + p.executor_cpu_time_total, 0)
                                                         AS other_cpu_share,
  p.write_time_total
    / nullIf(p.write_time_total + p.serialize_time_total + p.executor_cpu_time_total, 0)
                                                         AS write_wait_share_derived,

  -- ---- integrity (contract §3) ----
  -- Prefer the directly-emitted integrity_ok metric; else derive from the
  -- delivered/expected comparison; else NULL (integrity was never captured for
  -- this legacy row — unknown, NOT failed).
  multiIf(
    p.integrity_ok_metric IS NOT NULL, p.integrity_ok_metric = 1,
    p.rows_delivered IS NOT NULL AND p.rows_expected IS NOT NULL
      AND p.unique_delivered IS NOT NULL AND p.unique_expected IS NOT NULL,
      (p.rows_delivered = p.rows_expected AND p.unique_delivered = p.unique_expected),
    NULL
  )                                                      AS integrity_ok,

  -- Headline-safe flag: default-EXCLUDES integrity-FAILED runs from anything
  -- headline-facing. integrity_ok = 0 => not headline-safe. Unknown (NULL, legacy
  -- rows with no integrity capture) is treated as headline-safe so months of
  -- legacy history stay on the trend. Charts filter `headline_ok = 1`;
  -- drill-downs drop the filter to see failed rows (still present in this view).
  (integrity_ok IS NULL OR integrity_ok = 1)             AS headline_ok

FROM raw_connectors_load_testing.runs AS r
LEFT JOIN pivot AS p ON r.run_id = p.run_id
-- kafka rows share these tables since 2026-07-10; Spark dashboard datasets are
-- connector-scoped; cross-connector lives on kafka's Tab 5 per contract §6.
WHERE r.connector = 'spark'
-- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
-- from all real trends (fixture rows are a CI truth-table, never a real run).
-- (connector='spark' already excludes it — kept as belt-and-braces.)
  AND r.connector != 'verdict_fixture'
