-- =============================================================================
-- v2_env_annotations  —  Benchmark v2 Superset virtual dataset
-- =============================================================================
-- Purpose:
--   Change-detection over the target environment, emitting one row per detected
--   event so Superset annotation layers can draw vertical markers on the trend
--   charts (Tabs 1-3). Four event types:
--     * upgrade       — clickhouse_version changed vs the previous run in scope.
--     * restart       — ch_uptime dropped vs the previous run in scope (service
--                       restarted between the two runs).
--     * pin_bump      — the pinned arm's connector_version changed vs the
--                       previous pinned run in scope.
--     * config_change — a version-controlled config-under-test key changed vs the
--                       previous run in the same scope AND TIER. Currently
--                       detects the runtime['partition_scheme'] switch (contract
--                       §1.4), added 2026-07-07 for the toYYYYMM partition change
--                       (plan §9 step 4): the switch shifts Tier-1 absolutes, so
--                       it MUST be annotated on every absolute chart. Unlike the
--                       environment branches, this lag ALSO partitions by tier
--                       (cfg CTE below): config keys are per-tier facts (tier-0
--                       records partition_scheme='none' for the Null table every
--                       night, on the SAME scope tuple), so a scope-only lag
--                       would interleave none/toYYYYMM nightly and fire false
--                       events forever. The H/P ratio view is immune to the
--                       underlying config change itself.
--
-- Plan reference:  docs/benchmark-v2-plan.md §7 ("v_env_annotations ... change
--   detection over clickhouse_version + ch_uptime < prev -> 'upgrade'/'restart'
--   events; pin bump = change in pinned arm's connector_version").
-- Contract reference:  docs/benchmark-v2-contract.md §4 — annotations MUST be
--   scoped by (connector, target_service, environment_class). An UNSCOPED shared
--   annotation view is PROHIBITED (it would paint one benchmark's target events
--   onto the other's charts).
--
-- HARD REQUIREMENTS met:
--   * Scoped by (connector, target_service, environment_class) — the change
--     detection windows (lag over previous run) partition BY this scope tuple,
--     never unscoped, and every output row carries the tuple.
--   * upgrade / restart / pin_bump detection as specified.
--
-- Scope-tuple derivation note (target_service):
--   The runtime Map has no dedicated `target_service` key today (contract §1.1
--   lists target_region + environment_class as the mandatory scope keys; §4 calls
--   target_service "the concrete service identity"). We therefore take
--   target_service = COALESCE(runtime['target_service'], <region:class>) so that:
--     - if/when the pipeline adds an explicit target_service key it is used
--       verbatim, and
--     - until then, (target_region, environment_class) is the stable per-pipeline
--       service proxy — the Spark prod us-east-2 service is one scope, a Kafka
--       staging us-east-2 service is a different scope.
--   This keeps the view SCOPED (never unscoped) exactly as §4 requires. When the
--   explicit key lands, no consumer change is needed.
--
-- Notes:
--   * DWH mirror: raw_connectors_load_testing.{runs,metrics}.
--   * Known-answer check: a target service restart occurred 2026-06-29 ~11:16;
--     it MUST surface as a `restart` event on/after that timestamp.
--   * clickhouse_version lives on perf.runs; ch_uptime + the pinned
--     connector_version come from metrics/runs respectively.
--   * Ordering for lag() is by run_started_at within scope.
-- =============================================================================
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  -- One row per run with the scope tuple + the signals we detect changes on.
  base AS (
    SELECT
      r.run_id                                          AS run_id,
      r.run_started_at                                  AS run_started_at,
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
      coalesce(nullIf(r.runtime['arm'], ''), 'head')    AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')      AS tier,
      r.clickhouse_version                              AS clickhouse_version,
      r.connector_version                               AS connector_version,
      -- config-under-test key (contract §1.4). Empty when a legacy row predates
      -- the key; the change predicate skips '' -> non-'' first-appearance so a
      -- backfill does not manufacture a config_change event.
      r.runtime['partition_scheme']                     AS partition_scheme,
      up.ch_uptime                                      AS ch_uptime
    FROM raw_connectors_load_testing.runs AS r
    -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
    -- from all real trends (fixture is a CI truth-table, never a real run).
    LEFT JOIN (
      -- max(if(cond, value, NULL)) NOT maxIf(value, cond): maxIf returns 0.0
      -- when the metric is absent, so a run missing ch_uptime that follows a
      -- run with uptime (same scope) would read as "uptime dropped to 0" — a
      -- FALSE restart event. NULL-on-absence makes the restart predicate skip
      -- runs without the covariate instead.
      SELECT run_id, max(if(metric_name = 'ch_uptime', value, NULL)) AS ch_uptime
      FROM m GROUP BY run_id
    ) AS up ON r.run_id = up.run_id
    WHERE r.connector != 'verdict_fixture'
  ),
  -- lag() the version + uptime over the PREVIOUS run in the SAME scope tuple.
  -- Scope = (connector, target_service, environment_class) per contract §4.
  seq AS (
    SELECT
      run_id, run_started_at, connector, target_service, environment_class, arm,
      clickhouse_version, connector_version, ch_uptime,
      lagInFrame(clickhouse_version) OVER w AS prev_clickhouse_version,
      lagInFrame(ch_uptime)          OVER w AS prev_ch_uptime
    FROM base
    WINDOW w AS (
      PARTITION BY connector, target_service, environment_class
      ORDER BY run_started_at
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  ),
  -- Config-under-test change detection (config_change events). SEPARATE window
  -- from seq, partitioned by the scope tuple PLUS tier: config keys are
  -- PER-TIER facts. Concretely, tier-0 rows record partition_scheme='none'
  -- (the Cloud-hosted ENGINE=Null table has no partitioning — run-arm sets
  -- T0_PARTITION_SCHEME='none') while tier-1 rows record the hits DDL scheme,
  -- and post-#41 both tiers share the same (connector, target_service,
  -- environment_class) scope. A scope-only lag (like seq's) would therefore
  -- interleave none -> toYYYYMM -> none every night and fire 2-3 FALSE
  -- config_change events per night, forever. Partitioning the lag by tier keeps
  -- each tier's config series independent — and still detects a genuine tier-0
  -- config change if one ever happens. Do NOT instead filter out tier '0' rows
  -- or the 'none' value: 'none' is a legitimate §1.4 partition_scheme value
  -- (other connectors/tables may run unpartitioned by design), and a value
  -- filter would blind the detector to real none<->partitioned transitions
  -- within a tier. tier coalesces absent -> '1' (contract §1.1), so legacy rows
  -- stay one unbroken tier-1 series.
  --
  -- NO arm filter, deliberately (do not "dedup" on arm): both arms share the
  -- target DDL, so within a (scope, tier) series the time-ordered lag sees
  -- ... old, old, NEW, new ... — only the FIRST row after the change differs
  -- from its predecessor, so exactly ONE event fires per transition regardless
  -- of which arm ran first that night. An arm = 'head' output filter on top of
  -- this interleaved lag is a BUG, not a dedup: on pinned-first nights the
  -- pinned row is the detecting row and gets filtered away, while the head
  -- row's prev already carries the new value — the real transition would be
  -- silently lost on ~half the nights (arm order alternates by day, plan §2).
  cfg AS (
    SELECT
      run_id, run_started_at, connector, target_service, environment_class,
      tier, partition_scheme,
      lagInFrame(partition_scheme) OVER wc AS prev_partition_scheme
    FROM base
    WINDOW wc AS (
      PARTITION BY connector, target_service, environment_class, tier
      ORDER BY run_started_at
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  ),
  -- Pinned-arm connector_version change, detected within the pinned arm only,
  -- scoped identically. Kept separate because the pin bump is defined on the
  -- pinned arm's own version, not the pair's head sha (contract §1.2 rationale).
  pin AS (
    SELECT
      run_id, run_started_at, connector, target_service, environment_class,
      connector_version,
      lagInFrame(connector_version) OVER wp AS prev_pinned_version
    FROM base
    WHERE arm = 'pinned'
    WINDOW wp AS (
      PARTITION BY connector, target_service, environment_class
      ORDER BY run_started_at
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  )
-- ---- upgrade events ----
SELECT
  run_started_at                                        AS event_time,
  'upgrade'                                              AS event_type,
  connector, target_service, environment_class,
  concat(prev_clickhouse_version, ' -> ', clickhouse_version) AS detail,
  run_id
FROM seq
WHERE prev_clickhouse_version != ''
  AND clickhouse_version != ''
  AND clickhouse_version != prev_clickhouse_version

UNION ALL
-- ---- restart events (uptime dropped vs previous run in scope) ----
SELECT
  run_started_at                                        AS event_time,
  'restart'                                              AS event_type,
  connector, target_service, environment_class,
  concat('uptime ', toString(round(prev_ch_uptime)), 's -> ', toString(round(ch_uptime)), 's') AS detail,
  run_id
FROM seq
WHERE prev_ch_uptime IS NOT NULL
  AND ch_uptime IS NOT NULL
  AND ch_uptime < prev_ch_uptime

UNION ALL
-- ---- pin bump events (pinned-arm connector_version changed) ----
SELECT
  run_started_at                                        AS event_time,
  'pin_bump'                                             AS event_type,
  connector, target_service, environment_class,
  concat(prev_pinned_version, ' -> ', connector_version) AS detail,
  run_id
FROM pin
WHERE prev_pinned_version != ''
  AND connector_version != ''
  AND connector_version != prev_pinned_version

UNION ALL
-- ---- config_change events (version-controlled config-under-test changed) ----
-- Currently: runtime['partition_scheme'] (contract §1.4). Detection reads the
-- cfg CTE — lag over the previous run in the SAME (scope, tier) series; see the
-- cfg comment for why the window partitions by tier and why there is NO arm
-- filter (exactly one event fires per transition as-is). The '' guards suppress
-- the legacy first-appearance ('' -> value) when the key was backfilled.
-- The tier is carried in detail so a per-tier change is attributable at a
-- glance (the output schema stays identical across all four branches).
SELECT
  run_started_at                                        AS event_time,
  'config_change'                                        AS event_type,
  connector, target_service, environment_class,
  concat('partition_scheme ', prev_partition_scheme, ' -> ', partition_scheme, ' (tier ', tier, ')') AS detail,
  run_id
FROM cfg
WHERE prev_partition_scheme != ''
  AND partition_scheme != ''
  AND partition_scheme != prev_partition_scheme
