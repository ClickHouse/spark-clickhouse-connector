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
-- ============================================================================
-- CONSUMER OBLIGATION (READ THIS — flagged pairs are now CARRIED, not dropped):
--   Contract §3 pins a FLAGGED verdict state (pair flagged => FLAGGED, excluded
--   from bands/calibration BY DEFAULT). This view historically DROPPED flagged
--   pairs pre-join, so the FLAGGED state never rendered in prod (only the CI
--   fixture ever exercised it). Kafka renders FLAGGED rows per the map. To ALIGN
--   (manager-concurred design change, 2026-07-09), this view now CARRIES flagged
--   pairs through and emits two extra columns:
--       flagged      UInt8   — 1 if EITHER arm of the (pair,tier) is flagged.
--       flag_reason  String  — the flag token(s), '' when not flagged.
--   Ratio is still computed where possible (head.value / nullIf(pinned,0)).
--
--   THEREFORE, per contract §3 ("flagged pairs excluded from bands/calibration BY
--   DEFAULT"), the OBLIGATION MOVES TO THE CONSUMER:
--     * CALIBRATION / GATE / BAND consumers  MUST filter  flagged = 0.
--       (band-excursion alert, calibration stats, deviation-band inputs — a
--        flagged pair must NEVER raise a REGRESSION/TRIPWIRE or shift a band.)
--     * DISPLAY consumers  MAY render FLAGGED rows (Tab-1 verdict tiles /
--       excursion log surface them as the FLAGGED verdict state per the map).
--   A consumer that neither calibrates nor is a pure display surface should
--   default to flagged = 0 (the safe, band-excluding default).
--
--   SEQUENCE-COLUMN OBLIGATION (READ — the pair_seq/flagged TRAP):
--     This view emits TWO recency ranks. They are NOT interchangeable:
--       pair_seq   — dense_rank over ALL pairs (FLAG-INCLUSIVE). A flagged pair
--                    occupies a pair_seq slot. So `flagged = 0 AND pair_seq = 1`
--                    renders EMPTY whenever the NEWEST pair is flagged (the
--                    flagged pair holds seq 1, and the flagged=0 predicate then
--                    removes it, leaving no seq-1 row) — it does NOT fall back to
--                    the previous clean pair. pair_seq MUST NOT be combined with
--                    flagged=0 for latest / trailing-window scoping.
--       clean_seq  — dense_rank over the flagged=0 rows ONLY (NULL on flagged
--                    rows). Clean pairs are numbered 1..N by recency, SKIPPING
--                    flagged pairs entirely. clean_seq = 1 always lands on the
--                    newest CLEAN pair even when a newer pair is flagged.
--     * GATE / trailing-window consumers  MUST scope on clean_seq
--       (clean_seq = 1 = latest gateable pair; clean_seq <= 20 = trailing-20
--        clean window). They do NOT need a separate flagged=0 filter for the
--        scope — clean_seq is already NULL on flagged rows.
--     * DISPLAY consumers  use pair_seq (they render flagged pairs too, so the
--       flag-inclusive rank is the correct display order).
--
-- PAIR-LEVEL FLAG SEMANTICS (documented): a (pair, tier, metric) row is flagged
--   iff EITHER arm of that (pair, tier) carries runtime['flagged']='1'. The flag
--   is a per-RUN property; a pair inherits it from EITHER arm (contract §3: "a
--   pair is flagged, so both arms exclude from bands"). flag_reason is the OR-arm
--   union of the two arms' reason tokens (deduped, '|'-joined).
-- ============================================================================
--
-- HARD REQUIREMENTS met:
--   * Self-join on runtime pair_id, ratio = head.value / pinned.value per
--     (pair_id, tier, metric).
--   * FAIL-class exclusions kept AS-IS, on BOTH arms (these are FAIL, distinct
--     from FLAG): integrity-FAILED runs (integrity_ok = 0) and outcome='failed'
--     runs are DROPPED (they produce no headline number, contract §3). A pair is
--     only emitted when BOTH its arms survive these FAIL filters.
--   * FLAGGED pairs are NO LONGER dropped — they are carried with flagged=1 +
--     flag_reason (see CONSUMER OBLIGATION above).
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
      r.runtime['flag_reason']                          AS flag_reason,
      coalesce(nullIf(r.runtime['outcome'], ''), 'success') AS outcome,
      p.integrity_ok_metric,
      p.rows_delivered, p.rows_expected, p.unique_delivered, p.unique_expected
    FROM raw_connectors_load_testing.runs AS r
    LEFT JOIN (
      -- max(if(cond, value, NULL)) NOT maxIf(value, cond): maxIf returns 0.0
      -- when no row matches, which would make a run with NO integrity capture
      -- look integrity-FAILED (0) instead of unknown (NULL) and wrongly exclude
      -- it. The if(...NULL) form yields NULL on absence.
      -- (fixture exclusion is applied in the WHERE below.)
      SELECT
        run_id,
        max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
        max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
        max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
        max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
        max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected
      FROM m GROUP BY run_id
    ) AS p ON r.run_id = p.run_id
    -- contract §3 acceptance rule: exclude the reserved verdict-fixture connector
    -- from all real trends (fixture is a CI truth-table, never a real run).
    WHERE r.connector != 'verdict_fixture'
  ),
  -- Apply the FAIL-class exclusions ONLY (contract §3: FAIL is distinct from
  -- FLAG). A run is eligible iff: NOT outcome='failed' and NOT integrity-failed
  -- (integrity_ok=0). Integrity unknown (NULL) is allowed through (legacy rows) —
  -- same policy as headline_ok.
  --   FLAG IS NO LONGER A FILTER HERE (design change 2026-07-09): flagged runs are
  --   carried through with flagged + flag_reason so the FLAGGED verdict state can
  --   render (contract §3). Band/calibration consumers filter flagged=0 downstream
  --   (see CONSUMER OBLIGATION in the header). FAIL still drops the run entirely —
  --   a failed / integrity-failed run yields NO headline number at all.
  eligible AS (
    SELECT run_id, pair_id, arm, tier, flagged, flag_reason
    FROM runs_scoped
    WHERE pair_id != ''
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
      e.flagged      AS flagged,       -- per-arm flag, OR-ed across arms at the join
      e.flag_reason  AS flag_reason,   -- per-arm reason token(s)
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
-- TIME COLUMNS (added for time-windowed Tab-1 verdict tiles):
--   pair_ts   — the pair's start time, DERIVED from pair_id exactly as charts
--     5703/5704 parse it. Real pair_ids carry a 'YYYY-MM-DDTHH-MM-SSZ' prefix
--     (e.g. 2026-07-09T07-08-54Z-718f00e). We extract that prefix, turn the
--     time-part hyphens back into colons ('T07-08-54' -> 'T07:08:54') and parse.
--     NULL-SAFE by construction: the anchored regex only matches a real
--     timestamp prefix, so any malformed id (or the reserved FIXTURE-PAIR-NN
--     ids, though those are already excluded upstream) yields '' -> NULL pair_ts
--     without erroring. parseDateTimeBestEffortOrNull is the *OrNull variant so
--     a surprise value can never throw.
--   pair_seq  — dense rank of the pair WITHIN its tier by pair_ts DESC (1 =
--     newest pair), FLAG-INCLUSIVE (flagged pairs occupy a slot). dense_rank
--     (not row_number) so every metric row of the same pair shares one sequence
--     number. This is the DISPLAY rank: it preserves true chronology including
--     flagged pairs. Pairs with a NULL pair_ts sort last under DESC ordering.
--     *** DO NOT combine pair_seq with flagged=0 for latest/trailing scoping ***
--     — see the SEQUENCE-COLUMN OBLIGATION in the header: if the newest pair is
--     flagged, `flagged=0 AND pair_seq=1` is EMPTY, not the previous clean pair.
--   clean_seq — dense rank of the pair WITHIN its tier over the flagged=0 rows
--     ONLY (NULL on flagged rows). Computed by partitioning the dense_rank on
--     (tier, flagged=0): the clean rows land in their own partition and are
--     numbered 1..N by pair_ts DESC independently of the flagged rows (whose
--     rank in the other partition is then discarded by the if(flagged,NULL,...)).
--     This is the GATE rank: clean_seq = 1 = latest GATEABLE pair (skips a
--     flagged-newest pair), clean_seq <= 20 = trailing-20 CLEAN window. Every
--     metric row of one clean pair shares one clean_seq (dense_rank + shared
--     pair_ts). Requires no extra flagged=0 filter — it is already NULL on
--     flagged rows.
SELECT
  pr.pair_id                       AS pair_id,
  pr.tier                          AS tier,
  pr.metric                        AS metric,
  pr.head_value                    AS head_value,
  pr.pinned_value                  AS pinned_value,
  pr.ratio                         AS ratio,
  -- FLAGGED-carry (contract §3, design change 2026-07-09). See CONSUMER
  -- OBLIGATION in the header: band/calibration/gate consumers MUST filter
  -- flagged = 0; display consumers MAY render these rows as the FLAGGED verdict.
  pr.flagged                       AS flagged,
  pr.flag_reason                   AS flag_reason,
  pr.pair_ts                       AS pair_ts,
  dense_rank() OVER (
    PARTITION BY pr.tier ORDER BY pr.pair_ts DESC
  )                                AS pair_seq,
  -- clean_seq: dense_rank over the flagged=0 rows ONLY, NULL on flagged rows
  -- (see the pair_seq/clean_seq doc block above + SEQUENCE-COLUMN OBLIGATION in
  -- the header). Partitioning ALSO by (pr.flagged = 0) puts clean rows in a
  -- separate window partition from flagged rows, so the rank over clean rows is
  -- 1..N by recency SKIPPING flagged pairs; the if(flagged, NULL, ...) discards
  -- the flagged partition's rank. clean_seq = 1 lands on the newest CLEAN pair
  -- even when a NEWER pair is flagged (pair_seq would put the flagged pair at 1).
  if(
    pr.flagged = 1,
    NULL,
    dense_rank() OVER (
      PARTITION BY pr.tier, (pr.flagged = 0) ORDER BY pr.pair_ts DESC
    )
  )                                AS clean_seq
FROM (
  SELECT
    h.pair_id                        AS pair_id,
    h.tier                           AS tier,
    h.metric                         AS metric,
    h.value                          AS head_value,
    pn.value                         AS pinned_value,
    h.value / nullIf(pn.value, 0)    AS ratio,
    -- Pair-level flag: EITHER arm flagged => the pair is flagged (contract §3).
    toUInt8(h.flagged OR pn.flagged) AS flagged,
    -- flag_reason: union the two arms' reason tokens, dedup + '|'-join, '' when
    -- neither arm carries a reason. arms usually share the same reason, but a
    -- one-sided flag (only head OR only pinned) still surfaces its token.
    arrayStringConcat(
      arrayDistinct(
        arrayFilter(x -> x != '',
          arrayConcat(
            splitByChar('|', h.flag_reason),
            splitByChar('|', pn.flag_reason)
          )
        )
      ), '|'
    )                                AS flag_reason,
    if(
      extract(h.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
      NULL,
      parseDateTimeBestEffortOrNull(
        replaceRegexpOne(
          extract(h.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
          'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
        )
      )
    )                                AS pair_ts
  FROM (SELECT * FROM metric_long WHERE arm = 'head')   AS h
  INNER JOIN (SELECT * FROM metric_long WHERE arm = 'pinned') AS pn
    ON h.pair_id = pn.pair_id AND h.tier = pn.tier AND h.metric = pn.metric
) AS pr
