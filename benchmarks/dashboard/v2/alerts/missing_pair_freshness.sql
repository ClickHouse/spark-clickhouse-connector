-- =============================================================================
-- ALERT (PRECURSOR for board task #39): missing-pair / staleness freshness watch
-- =============================================================================
-- No Tab 1 tile mirrors this yet — it is the freshness PRECURSOR the task brief
-- asks for, staged here so #39 (missing-pair alerting) has a ready condition that
-- shares the v2_runs_enriched semantics. Same dataset + arm/tier coalesce as the
-- tiles; contract §1.1 (arm/tier defaults), §1.2 (a pair is the two arms of one
-- nightly invocation sharing pair_id).
--
-- Two freshness conditions (either non-empty => alert):
--   (A) STALE: the newest run in scope is older than the freshness SLA
--       (default 36h — a nightly cadence with slack). The two-arm pipeline has
--       not produced a run recently.
--   (B) HALF_PAIR: within the freshness window there exists a pair_id that has a
--       'head' arm but NO 'pinned' arm (or vice-versa) for a given tier — the
--       two-arm run half-completed, so v2_pair_ratios will silently emit no ratio
--       for it (INNER JOIN drops half-pairs). This is the exact blind spot #39
--       must cover: a missing arm looks identical to "no regression" on Tab 1.
--
-- RUNNER (board task #39, implemented): the scheduled watchdog is
-- benchmarks/scripts/check_freshness.py + .github/workflows/benchmark-freshness.yml,
-- which run the SAME two conditions against perf.runs on the metrics service (CI
-- has those creds; the DWH connection below is per-user Superset auth CI can't
-- use) and file a GitHub issue on a miss. THIS file stays the Superset/DWH
-- variant for a dashboard-native alert; keep the two conditions in step.
-- Channel wiring beyond the GitHub issue is a follow-up (open decision 3) — see README.md.
--
-- Run against: DWH connection dc93cd97, db 1, schema raw_connectors_load_testing.
-- TODAY: condition (A) will fire if the newest run is >36h old; condition (B) is
-- empty because no pair_id exists yet (all history is arm=head with no pair_id).
-- Neither errors on the current data. Tune INTERVAL 36 HOUR to the real cadence.
-- =============================================================================
WITH
  scoped AS (
    SELECT
      r.run_id                                          AS run_id,
      r.run_started_at                                  AS run_started_at,
      r.connector                                       AS connector,
      r.runtime['pair_id']                              AS pair_id,
      coalesce(nullIf(r.runtime['arm'], ''),  'head')   AS arm,
      coalesce(nullIf(r.runtime['tier'], ''), '1')      AS tier,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome
    FROM raw_connectors_load_testing.runs AS r
    -- contract §3: the reserved verdict-fixture connector is ALREADY excluded
    -- here — this alert positively scopes to connector = 'spark', so a mirrored
    -- 'verdict_fixture' row (and its 'FIXTURE-*' run_ids) can never enter. No
    -- extra != 'verdict_fixture' / FIXTURE- belt is needed (no metrics-only join).
    WHERE r.connector = 'spark'
  )

-- (A) STALE: newest run older than the SLA.
SELECT
  'STALE'                                                       AS condition,
  ''                                                            AS pair_id,
  ''                                                            AS tier,
  max(run_started_at)                                           AS last_run_at,
  toString(round(dateDiff('hour', max(run_started_at), now()))) AS hours_since_last_run,
  'No spark run within the freshness SLA (36h) — two-arm pipeline may be down' AS detail
FROM scoped
HAVING max(run_started_at) < now() - INTERVAL 36 HOUR

UNION ALL

-- (B) HALF_PAIR: a pair_id present in the window that is missing one arm for a tier.
SELECT
  'HALF_PAIR'                                                   AS condition,
  pair_id                                                       AS pair_id,
  tier                                                          AS tier,
  max(run_started_at)                                           AS last_run_at,
  toString(groupUniqArray(arm))                                 AS hours_since_last_run,  -- reuse col: shows which arm(s) present
  concat('pair ', pair_id, ' tier ', tier,
         ' has arms [', arrayStringConcat(groupUniqArray(arm), ','),
         '] — expected both head+pinned; v2_pair_ratios will emit no ratio for it') AS detail
FROM scoped
WHERE pair_id != ''
  AND run_started_at >= now() - INTERVAL 36 HOUR
  AND outcome != 'failed'
GROUP BY pair_id, tier
HAVING uniqExact(arm) < 2      -- fewer than both arms => half-completed pair
