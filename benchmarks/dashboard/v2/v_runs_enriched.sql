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
--     latest by recorded_at via argMax, matching the drill_metrics_for_run
--     convention in the legacy build_superset.py.
--   * Map value access on a missing key yields '' in ClickHouse, so nullIf(...,'')
--     turns "key absent" into NULL before COALESCE applies the contract default.
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
      max(if(metric_name = 'run_cost_usd',         value, NULL)) AS run_cost_usd
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
