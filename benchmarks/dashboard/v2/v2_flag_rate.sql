--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- Tab-1 panel: "Head-arm flag rate".
--
-- WHY THIS EXISTS (closes a real bias): flagged pairs are EXCLUDED from bands,
-- ratios, and calibration by default (contract §3; v_pair_ratios carries them but
-- gate/band consumers filter flagged = 0). That exclusion is correct for the
-- clean-comparison gate, but it opens a blind spot: a HEAD build that regresses
-- *into instability* — rising task_retries / settle_timeout — would produce MORE
-- flagged pairs, and each flagged pair is then silently dropped from the bands. The
-- regression hides behind flag-and-drop: the band never sees it, no REGRESSION
-- verdict fires, and the only symptom (a climbing flag rate) is invisible unless
-- something charts it. This panel charts it: the fraction/count of trailing head-arm
-- pairs flagged, split by reason. A rising line here is the safety-net signal that
-- an instability regression is being excluded rather than reported.
--
-- WHAT IT COUNTS:
--   * Unit = the HEAD arm of a pair at TIER 1 (the verified-throughput gate arm).
--     One head run per pair/tier; we count over the trailing N head-arm pairs by
--     recency. (Restrict to tier 1 so t0/t1 rows of the same pair aren't double
--     counted; tier 1 is the gate the exclusion bias matters most for.)
--   * flagged = runtime['flagged'] = '1' (contract §1.3 pins the value '1').
--   * by reason = the flag_reason tokens the Spark pipeline actually EMITS today:
--     task_retries and settle_timeout (contract §1.3 implemented-reality). A pair
--     may trip both (flag_reason = 'task_retries|settle_timeout'); we count each
--     token independently via has(splitByChar('|', ...)), so the per-reason counts
--     may sum to MORE than the flagged-pair count — that is intended (a pair can
--     contribute to two reasons). The headline `flag_rate` uses the DISTINCT
--     flagged-pair count so it is a true fraction in [0,1].
--   * instrument_resize is NOT emitted into flag_reason by run-arm (it is derived
--     in v2_instrument_annotations from the emr_* keys); it is intentionally NOT
--     summed here. This panel tracks the RUN-EMITTED instability tokens — the ones
--     that flag-and-drop can hide a HEAD regression behind. Instrument changes are
--     surfaced by the instrument-annotation layer, not by this rate.
--
-- TRAILING WINDOW: parameterised as a Superset template/jinja value, default 20 to
--   match the calibration trailing-20 window (contract §3 recalibration rule). We
--   rank head-arm tier-1 pairs by recency and keep the newest N.
--
-- FAIL-CLASS handling: outcome='failed' and integrity-failed runs are the FAIL
--   class (contract §3), distinct from FLAGGED. They are their own panel
--   (v2_failed_runs). This panel is about the FLAGGED (non-comparable-but-valid)
--   class, so it counts over runs that are NOT fail-class — a failed run has no
--   throughput to be "excluded from bands", so folding it in would muddy the rate.
--
-- DEPLOYMENT: rendered as a Tab-1 panel on BOTH connectors' dashboards (the
--   projection is connector-agnostic; only the source dataset differs):
--     * Spark  dashboard #427 — over v_runs_enriched   (deployed v2_runs_enriched)
--     * Kafka  dashboard #432 — over v_kc_runs
--   Suggested viz: a "Big Number with trendline" on flag_rate for the headline,
--   plus a bar/time-series broken out by the per-reason counts. A rising flag_rate
--   is the alarm that an instability regression is being flag-and-dropped.
--
-- SOURCE: raw_connectors_load_testing.* (ClickPipe DWH mirror of perf.*), read via
--   the enriched view so arm/tier/flagged/flag_reason/integrity are already
--   unnested with the contract coalesce defaults.

WITH
  -- Head-arm, tier-1, non-fail-class runs, ranked by recency. headline_ok = 1
  -- drops integrity-failed runs; outcome != 'failed' drops the hard-fail class.
  -- DEPLOYED over the base `runs` table: the v_runs_enriched "view" is a Superset
  -- VIRTUAL dataset, not a queryable ClickHouse object, so a standalone dataset must
  -- read base runs and inline the arm/tier/flagged/outcome coalesce (contract §1.1
  -- defaults) — same fields the enriched view would expose. connector = 'spark'
  -- here; the Kafka deploy uses 'kafka-connect'. The integrity-failed exclusion
  -- (headline_ok=1) is approximated by outcome != 'failed' — an integrity-FAILED
  -- head run is FAIL-class (its own panel) and negligible in this denominator.
  head_pairs AS (
    SELECT
      runtime['pair_id']                                       AS pair_id,
      run_started_at,
      (runtime['flagged'] = '1')                               AS flagged,
      runtime['flag_reason']                                   AS flag_reason,
      row_number() OVER (ORDER BY run_started_at DESC)         AS recency_rank
    FROM raw_connectors_load_testing.runs
    WHERE connector = 'spark'                                  -- Kafka deploy: 'kafka-connect'
      AND coalesce(nullIf(runtime['arm'], ''),  'head') = 'head'
      AND coalesce(nullIf(runtime['tier'], ''), '1')    = '1'
      AND coalesce(nullIf(runtime['outcome'], ''), 'success') != 'failed'
      AND runtime['pair_id'] != ''
  ),
  -- Keep only the trailing-N most recent head-arm pairs (default 20).
  windowed AS (
    SELECT pair_id, run_started_at, flagged, flag_reason
    FROM head_pairs
    WHERE recency_rank <= {{ trailing_n | default(20) }}
  )
SELECT
  count()                                                    AS head_pairs_in_window,
  countIf(flagged)                                           AS flagged_pairs,
  -- True fraction in [0,1]: distinct flagged pairs / window size.
  countIf(flagged) / nullIf(count(), 0)                      AS flag_rate,
  -- Per-reason counts (a pair with 'task_retries|settle_timeout' counts in BOTH;
  -- these may sum to more than flagged_pairs — intended, see header).
  countIf(has(splitByChar('|', flag_reason), 'task_retries'))   AS n_task_retries,
  countIf(has(splitByChar('|', flag_reason), 'settle_timeout')) AS n_settle_timeout,
  -- Per-reason rates over the window (each in [0,1]).
  countIf(has(splitByChar('|', flag_reason), 'task_retries'))   / nullIf(count(), 0) AS rate_task_retries,
  countIf(has(splitByChar('|', flag_reason), 'settle_timeout')) / nullIf(count(), 0) AS rate_settle_timeout,
  min(run_started_at)                                        AS window_start,
  max(run_started_at)                                        AS window_end
FROM windowed
