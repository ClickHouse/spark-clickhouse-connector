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
-- =============================================================================
-- 90_verdict_fixture_seed.sql — PERMANENT verdict truth-table fixture
-- =============================================================================
-- Contract reference: docs/benchmark-v2-contract.md §3, "Acceptance rule
--   (PINNED)" (Amendment 2026-07-09b): any artifact that emits a VERDICT (not a
--   number) requires fixture-based acceptance — synthetic PAIR rows under a
--   RESERVED fixture connector, asserted THROUGH THE REAL dataset SQL, covering
--   at least {NULL, 0-denominator, below-band, in-band, above-band} ×
--   {higher_better, lower_better} × {flagged, unflagged}, PLUS the
--   parts_per_insert TRIPWIRE cells (==1.0 => OK, !=1.0 => TRIPWIRE). Consumer
--   views MUST exclude the fixture connector from real trends.
--
-- RESERVED FIXTURE IDENTITY (schema-checked against 02_create_runs.sql /
--   03_create_metrics.sql):
--     * perf.runs has a first-class `connector String` column, so the reserved
--       identity is  runs.connector = 'verdict_fixture'.
--     * ALL fixture run_ids are ALSO prefixed 'FIXTURE-' (belt and braces): the
--       consumer-view exclusion predicate keys on `connector != 'verdict_fixture'`
--       where the connector column is in scope, and on
--       NOT startsWith(run_id,'FIXTURE-') in the ch_inserts drill views (C1-C4)
--       whose base row is a ch_inserts row that carries only run_id. Seeding both
--       lets every consumer exclude the fixture with whichever key it has.
--     * pair_id lives in runtime['pair_id'] (Map(String,String)); each pair uses
--       a distinct id FIXTURE-PAIR-01 .. FIXTURE-PAIR-16 (tier-1 banded/tripwire),
--       FIXTURE-PAIR-20 .. FIXTURE-PAIR-23 (tier-0), FIXTURE-PAIR-30 .. -32
--       (integrity-FAIL / failed-run), arm in runtime['arm'] ('head'|'pinned'),
--       tier in runtime['tier'] ('0' for the tier-0 pairs, else '1').
--
-- BANDS / DIRECTION / TRIPWIRE (contract §3, PINNED — Amendment 2026-07-09b:
--   CALIBRATED per-metric bands at 2x the measured noise floor; the flat
--   ±3%/±5% rule is superseded. D4 Amendment 2026-08-31 "full demote + re-base":
--   the Tier-1 gate is re-based onto ch_insert_cpu_seconds_per_Mrows and
--   throughput_rows_per_sec is DEMOTED to WATCH-ONLY). The fixture exercises:
--
--   (a) BANDED lower_better cpu family, band ±6% => in-band ratio ∈ [0.94, 1.06]:
--         ch_insert_cpu_seconds_per_Mrows — the VERIFIED Tier-1 GATE (D4). Server
--           insert CPU per Mrows (emitted by 11_insert_from_query_log.sql:90-103).
--         cpu_seconds_per_Mrows           — the Tier-0 client-cpu gate.
--       BOTH are seeded with IDENTICAL values per pair => IDENTICAL verdicts (the
--       oracle asserts both; adding ch_insert_cpu is the D4 gate-coverage add).
--         ratio < 0.94 (GOOD) => IMPROVEMENT ; ratio > 1.06 (BAD) => REGRESSION.
--   (b) BANDED tier-0 pair (tier='0'): null_rows_per_sec (higher_better, ±8.5%)
--       + serialize_seconds_per_Mrows (lower_better, ±8.5%) => in-band ∈
--       [0.915, 1.085]. Exercises the ±8.5% band AND the tier-0 metric identities
--       (bands are tier-independent, but no other cell proves a tier-0 flow).
--   (c) TRIPWIRE — parts_per_insert. NOT banded, NO ratio: a BINARY tripwire on
--       the HEAD arm's ABSOLUTE value. head value == 1.0 => OK ; ANY deviation
--       (head value != 1.0) => TRIPWIRE (investigate). head metric absent =>
--       NO_DATA. The pinned arm is IRRELEVANT to this metric.
--   (d) INTEGRITY FAIL — a pair with integrity_ok=0 on EITHER arm => FAIL on EVERY
--       asserted metric (contract §3 precedence FAIL > FLAG). outcome='failed'
--       runs are EXCLUDED entirely (they are not comparables — no cell).
--
--   throughput_rows_per_sec is WATCH-ONLY as of D4 (NOT gated, NOT asserted). It is
--   NOT seeded here, mirroring merge_amplification (the other watch-only metric —
--   neither is seeded, and v_verdict_fixture_check EXCLUDES both from the asserted
--   set). v_pair_ratios still DISPLAYS throughput as a covariate on the real data.
--
--   ratio = head.value / nullIf(pinned.value, 0)  (v_pair_ratios verbatim).
--
-- Ratio→verdict map (contract §3, PINNED), precedence
--   FAIL > FLAG > {NO_DATA / TRIPWIRE / IMPROVEMENT / REGRESSION / OK}:
--     integrity FAILED (either arm)    => FAIL      (overrides EVERYTHING, incl. FLAG)
--     pair flagged                     => FLAGGED   (overrides everything below,
--                                                    incl. an armed TRIPWIRE)
--     (banded) ratio NULL/0-denominator=> NO_DATA   (absent EITHER arm, or /0)
--     (banded) outside band, GOOD dir  => IMPROVEMENT
--     (banded) outside band, BAD dir   => REGRESSION
--     (banded) else                    => OK
--     (tripwire) head absent (NULL)    => NO_DATA   (P16 — absent-HEAD cell)
--     (tripwire) head == 1.0           => OK
--     (tripwire) head != 1.0           => TRIPWIRE
--
-- FIXTURE MATRIX. Asserted cells (62 total — see check_verdict_fixture.py):
--   * TIER-1 pairs P01-P16: cpu family (cpu_seconds_per_Mrows AND
--     ch_insert_cpu_seconds_per_Mrows — IDENTICAL values, LB banded ±6%) + parts
--     (TRIPWIRE) = 3 cells/pair = 48.
--   * TIER-0 pairs P20-P23: null_rows_per_sec (HB ±8.5%) + serialize_seconds_per_Mrows
--     (LB ±8.5%) = 2 cells/pair = 8.
--   * INTEGRITY-FAIL pairs P30-P31: cpu family + parts = 3 cells/pair = 6, ALL FAIL.
--   * P32 (outcome='failed'): EXCLUDED upstream => 0 cells (proves the exclusion).
--   Banded cells fix pinned = 10 (cpu) / 100 (null) / 10 (serialize), head = pinned*ratio;
--   NULL cells omit the PINNED arm's banded metric; 0-denom cells set pinned = 0.
--   Tripwire cells set the HEAD arm's parts value directly (pinned = don't-care).
--
--   TIER-1 (tier='1'):  cpu family (LB±6%)  +  parts (TRIP)
--   pair_id          flagged  integ  bucket                cpu-family   parts(TRIP)
--   ---------------  -------  -----  --------------------  -----------  -----------
--   FIXTURE-PAIR-01  no       ok     below-band            IMPROVEMENT  OK (1.0)
--   FIXTURE-PAIR-02  no       ok     in-band               OK           OK (1.0)
--   FIXTURE-PAIR-03  no       ok     above-band            REGRESSION   OK (1.0)
--   FIXTURE-PAIR-04  no       ok     NULL (pinned absent)  NO_DATA      OK (1.0)
--   FIXTURE-PAIR-05  no       ok     0-denominator (pin=0) NO_DATA      OK (1.0)
--   FIXTURE-PAIR-06  no       ok     near-edge INSIDE      OK           OK (1.0)
--   FIXTURE-PAIR-07  no       ok     near-edge OUTSIDE     IMPROVEMENT  OK (1.0)
--   FIXTURE-PAIR-08  no       ok     tripwire fired hi     OK           TRIPWIRE (1.05)
--   FIXTURE-PAIR-09  no       ok     tripwire fired lo     OK           TRIPWIRE (0.95)
--   FIXTURE-PAIR-10  YES      ok     below-band            FLAGGED      FLAGGED
--   FIXTURE-PAIR-11  YES      ok     NULL (pinned absent)  FLAGGED      FLAGGED
--   FIXTURE-PAIR-12  YES      ok     0-denominator (pin=0) FLAGGED      FLAGGED
--   FIXTURE-PAIR-13  YES      ok     in-band               FLAGGED      FLAGGED
--   FIXTURE-PAIR-14  YES      ok     above-band            FLAGGED      FLAGGED
--   FIXTURE-PAIR-15  YES      ok     tripwire ARMED        FLAGGED      FLAGGED (armed 1.05)
--   FIXTURE-PAIR-16  no       ok     HEAD arm absent       NO_DATA      NO_DATA
--                            (pinned present; head emits NO gated metric)
--
--   TIER-0 (tier='0'):  null_rows_per_sec (HB±8.5%)  +  serialize_seconds_per_Mrows (LB±8.5%)
--   pair_id          flagged  integ  bucket                null(HB)     serialize(LB)
--   ---------------  -------  -----  --------------------  -----------  -------------
--   FIXTURE-PAIR-20  no       ok     below-band            REGRESSION   IMPROVEMENT
--   FIXTURE-PAIR-21  no       ok     in-band               OK           OK
--   FIXTURE-PAIR-22  no       ok     above-band            IMPROVEMENT  REGRESSION
--   FIXTURE-PAIR-23  no       ok     NULL (pinned absent)  NO_DATA      NO_DATA
--
--   INTEGRITY (tier='1'):  cpu family + parts seeded in-band/OK; verdict driven by integrity
--   pair_id          flagged  integ       bucket                       => verdict
--   ---------------  -------  ----------  ---------------------------  ------------------------
--   FIXTURE-PAIR-30  no       FAIL (0)    integrity mismatch           FAIL (every metric)
--   FIXTURE-PAIR-31  YES      FAIL (0)    integrity mismatch+flagged   FAIL (proves FAIL>FLAG)
--   FIXTURE-PAIR-32  no       ok, failed  outcome='failed' run         (excluded — NO cell)
--
--   Coverage of the PINNED acceptance grid:
--     BANDED {below,in,above,NULL(pinned-absent),NULL(head-absent),0-denom} ×
--       {HB,LB} × {flagged,unflagged}:
--       LB cpu family unflagged: 01(below) 02(in) 03(above) 04(pinned-NULL)
--         05(0-denom) 16(head-NULL) + near-edge 06(inside)/07(outside).
--       HB/LB tier-0 unflagged: 20(below) 21(in) 22(above) 23(pinned-NULL) —
--         null_rows is the HB exemplar (throughput was HB before D4 but is now
--         watch-only), serialize is a second LB exemplar at the ±8.5% band.
--       flagged: 10(below) 11(NULL) 12(0-denom) 13(in) 14(above) — verdict is
--         CONSTANT FLAGGED, but each guards a DISTINCT precedence bug (an impl that
--         hoists in-band=>OK or good-excursion=>IMPROVEMENT above the flag check is
--         caught only by 13/14).
--     TRIPWIRE {OK(==1.0), fired(!=1.0), head-absent} × {flagged,unflagged}:
--       unflagged: 08(fired hi 1.05) 09(fired lo 0.95) prove ANY deviation trips
--         (both directions); 01-07 (==1.0) prove OK; 16(head parts ABSENT) proves
--         an absent HEAD tripwire metric => NO_DATA (kafka cross-check gap: the
--         head-side join drop meant this cell could never render — the contract
--         map's "NULL/absent parts_per_insert => NO_DATA" was unreachable in the
--         Spark artifact. NOTE the asymmetry: P04/P11 exercise absent-PINNED only;
--         P16 is the NEW absent-HEAD cell, which the head-driven join used to drop
--         for BOTH banded metrics AND the tripwire — hence P16 asserts NO_DATA on
--         all three).
--       flagged:   15 ARMS the tripwire (head parts=1.05) yet expects FLAGGED —
--         proves FLAG > TRIPWIRE precedence (10-14 carry parts=1.0, so 15 is the
--         cell that catches a tripwire hoisted above the flag check).
--     INTEGRITY-FAIL (D4 re-base): 30 integrity mismatch (unflagged) => FAIL on all
--       3 metrics; 31 integrity mismatch AND flagged => FAIL (proves FAIL > FLAG —
--       the cell that catches a flag hoisted above the integrity check); 32
--       outcome='failed' => EXCLUDED upstream, emits NO cell (proves failed-run
--       exclusion via the exact 62-cell count — a leaked cell would be UNEXPECTED).
--
-- ELIGIBILITY THROUGH v_verdict_fixture_check: MOST fixture runs are COMPLETE,
--   comparable rows — outcome success, integrity PASSING (integrity_ok=1 on both
--   arms), both arms present per pair (except the NULL cells, whose pinned arm run
--   EXISTS and is eligible but deliberately lacks the banded metric so the ratio is
--   NULL). The 0-denominator cells emit the banded metric on the pinned arm with
--   value 0. EXCEPTIONS added by the D4 re-base:
--     * P30/P31 emit integrity_ok=0 (+ a delivered!=expected mismatch) on both arms:
--       they are CARRIED (not dropped) so the FAIL verdict renders (the check view's
--       eligible CTE no longer drops integrity-failed rows — see its header #5).
--     * P32 runs carry outcome='failed': they ARE dropped by the check view's
--       `outcome != 'failed'` filter, so P32 emits NO cell (the exclusion proof).
--
-- IDEMPOTENCY (perf.runs / perf.metrics / perf.ch_inserts are plain MergeTree —
--   see 02/03/04 DDL; no ReplacingMergeTree, so a re-run would DUPLICATE rows):
--   we DELETE-then-INSERT, scoped to the reserved fixture identity, so a re-run
--   is a clean replace and NOTHING outside the fixture is touched.
--     * runs / ch_inserts: DELETE keyed on connector / run_id prefix.
--     * metrics has NO connector column (only run_id) — DELETE keys on the
--       'FIXTURE-' run_id prefix (the belt-and-braces reason that prefix exists).
--   Lightweight DELETE (mutation) is synchronous under the default
--   mutations_sync; on clickhouse-local it is immediate. Safe to run repeatedly.
-- =============================================================================

-- ---- idempotency: clear any prior fixture rows (scoped to the reserved id) ----
DELETE FROM perf.runs    WHERE connector = 'verdict_fixture' OR startsWith(run_id, 'FIXTURE-');
DELETE FROM perf.metrics WHERE startsWith(run_id, 'FIXTURE-');
-- ch_inserts: PROPHYLACTIC — this seed inserts NO ch_inserts rows today; the
-- DELETE reserves the FIXTURE- prefix there so a future per-insert fixture (or a
-- stray manual insert) is swept by the same idempotent replace.
DELETE FROM perf.ch_inserts WHERE startsWith(run_id, 'FIXTURE-');

-- ---- perf.runs : two arms per pair (head + pinned) --------------------------
-- flagged pairs carry runtime['flagged']='1' on BOTH arms (contract §3: a pair is
-- flagged, so both arms exclude from bands). All rows: connector='verdict_fixture',
-- tier '1', outcome 'success', environment_class/target_region set so the row is
-- fully-formed and scoped.
INSERT INTO perf.runs
  (run_id, run_started_at, run_ended_at, git_sha, connector, run_profile,
   connector_version, clickhouse_version, runtime, notes)
SELECT
  run_id,
  toDateTime('2026-07-09 00:00:00') + toIntervalSecond(rn) AS run_started_at,
  toDateTime('2026-07-09 00:10:00') + toIntervalSecond(rn) AS run_ended_at,
  'FIXTUREsha' AS git_sha,
  'verdict_fixture' AS connector,
  'fixture' AS run_profile,
  'fixture-conn-v1' AS connector_version,
  '99.9.9-fixture' AS clickhouse_version,
  runtime,
  'verdict truth-table fixture (contract §3, Amendment 2026-07-09b)' AS notes
FROM
(
  SELECT
    arrayJoin([
      -- (run_id, pair_id, arm, flagged)
      ('FIXTURE-P01-head',   'FIXTURE-PAIR-01', 'head',   '0'),
      ('FIXTURE-P01-pinned', 'FIXTURE-PAIR-01', 'pinned', '0'),
      ('FIXTURE-P02-head',   'FIXTURE-PAIR-02', 'head',   '0'),
      ('FIXTURE-P02-pinned', 'FIXTURE-PAIR-02', 'pinned', '0'),
      ('FIXTURE-P03-head',   'FIXTURE-PAIR-03', 'head',   '0'),
      ('FIXTURE-P03-pinned', 'FIXTURE-PAIR-03', 'pinned', '0'),
      ('FIXTURE-P04-head',   'FIXTURE-PAIR-04', 'head',   '0'),
      ('FIXTURE-P04-pinned', 'FIXTURE-PAIR-04', 'pinned', '0'),
      ('FIXTURE-P05-head',   'FIXTURE-PAIR-05', 'head',   '0'),
      ('FIXTURE-P05-pinned', 'FIXTURE-PAIR-05', 'pinned', '0'),
      ('FIXTURE-P06-head',   'FIXTURE-PAIR-06', 'head',   '0'),
      ('FIXTURE-P06-pinned', 'FIXTURE-PAIR-06', 'pinned', '0'),
      ('FIXTURE-P07-head',   'FIXTURE-PAIR-07', 'head',   '0'),
      ('FIXTURE-P07-pinned', 'FIXTURE-PAIR-07', 'pinned', '0'),
      ('FIXTURE-P08-head',   'FIXTURE-PAIR-08', 'head',   '0'),
      ('FIXTURE-P08-pinned', 'FIXTURE-PAIR-08', 'pinned', '0'),
      ('FIXTURE-P09-head',   'FIXTURE-PAIR-09', 'head',   '0'),
      ('FIXTURE-P09-pinned', 'FIXTURE-PAIR-09', 'pinned', '0'),
      ('FIXTURE-P10-head',   'FIXTURE-PAIR-10', 'head',   '1'),
      ('FIXTURE-P10-pinned', 'FIXTURE-PAIR-10', 'pinned', '1'),
      ('FIXTURE-P11-head',   'FIXTURE-PAIR-11', 'head',   '1'),
      ('FIXTURE-P11-pinned', 'FIXTURE-PAIR-11', 'pinned', '1'),
      ('FIXTURE-P12-head',   'FIXTURE-PAIR-12', 'head',   '1'),
      ('FIXTURE-P12-pinned', 'FIXTURE-PAIR-12', 'pinned', '1'),
      ('FIXTURE-P13-head',   'FIXTURE-PAIR-13', 'head',   '1'),
      ('FIXTURE-P13-pinned', 'FIXTURE-PAIR-13', 'pinned', '1'),
      ('FIXTURE-P14-head',   'FIXTURE-PAIR-14', 'head',   '1'),
      ('FIXTURE-P14-pinned', 'FIXTURE-PAIR-14', 'pinned', '1'),
      ('FIXTURE-P15-head',   'FIXTURE-PAIR-15', 'head',   '1'),
      ('FIXTURE-P15-pinned', 'FIXTURE-PAIR-15', 'pinned', '1'),
      -- P16: absent-HEAD cell (kafka cross-check gap). Both arm RUNS exist and are
      -- eligible; the HEAD arm deliberately emits NO gated metric (throughput/cpu/
      -- parts) while the PINNED arm emits all three. => every metric NO_DATA.
      ('FIXTURE-P16-head',   'FIXTURE-PAIR-16', 'head',   '0'),
      ('FIXTURE-P16-pinned', 'FIXTURE-PAIR-16', 'pinned', '0')
    ]) AS t,
    t.1 AS run_id,
    t.2 AS pair_id,
    t.3 AS arm,
    t.4 AS flagged,
    rowNumberInAllBlocks() AS rn,
    map(
      'pair_id',           pair_id,
      'arm',               arm,
      'tier',              '1',
      'outcome',           'success',
      'flagged',           flagged,
      'flag_reason',       if(flagged = '1', 'task_retries', ''),
      'environment_class', 'fixture',
      'target_region',     'fixture-region',
      'compute_region',    'fixture-region',
      'target_service',    'fixture-service',
      'partition_scheme',  'none'
    ) AS runtime
);

-- ---- perf.runs : D4 re-base pairs (tier-0 + integrity) ----------------------
-- These carry a per-pair tier AND outcome in the arrayJoin tuple (the tier-1 block
-- above hardcodes tier '1' / outcome 'success'):
--   * P20-P23 are tier='0' (the ±8.5% band + tier-0 metric-identity coverage);
--   * P30/P31 are integrity-FAIL pairs (integrity_ok=0 seeded below) expecting FAIL;
--     P31 is ALSO flagged (flagged='1') to prove FAIL > FLAG;
--   * P32 carries outcome='failed' on both arms — the check view's `outcome !=
--     'failed'` filter drops it, so it emits NO cell (the exclusion proof).
INSERT INTO perf.runs
  (run_id, run_started_at, run_ended_at, git_sha, connector, run_profile,
   connector_version, clickhouse_version, runtime, notes)
SELECT
  run_id,
  toDateTime('2026-07-09 01:00:00') + toIntervalSecond(rn) AS run_started_at,
  toDateTime('2026-07-09 01:10:00') + toIntervalSecond(rn) AS run_ended_at,
  'FIXTUREsha' AS git_sha,
  'verdict_fixture' AS connector,
  'fixture' AS run_profile,
  'fixture-conn-v1' AS connector_version,
  '99.9.9-fixture' AS clickhouse_version,
  runtime,
  'verdict truth-table fixture — D4 re-base (contract §3, Amendment 2026-08-31)' AS notes
FROM
(
  SELECT
    arrayJoin([
      -- (run_id, pair_id, arm, flagged, tier, outcome)
      ('FIXTURE-P20-head',   'FIXTURE-PAIR-20', 'head',   '0', '0', 'success'),
      ('FIXTURE-P20-pinned', 'FIXTURE-PAIR-20', 'pinned', '0', '0', 'success'),
      ('FIXTURE-P21-head',   'FIXTURE-PAIR-21', 'head',   '0', '0', 'success'),
      ('FIXTURE-P21-pinned', 'FIXTURE-PAIR-21', 'pinned', '0', '0', 'success'),
      ('FIXTURE-P22-head',   'FIXTURE-PAIR-22', 'head',   '0', '0', 'success'),
      ('FIXTURE-P22-pinned', 'FIXTURE-PAIR-22', 'pinned', '0', '0', 'success'),
      ('FIXTURE-P23-head',   'FIXTURE-PAIR-23', 'head',   '0', '0', 'success'),
      ('FIXTURE-P23-pinned', 'FIXTURE-PAIR-23', 'pinned', '0', '0', 'success'),
      -- integrity-FAIL (integrity_ok=0 seeded below). P31 ALSO flagged => FAIL>FLAG.
      ('FIXTURE-P30-head',   'FIXTURE-PAIR-30', 'head',   '0', '1', 'success'),
      ('FIXTURE-P30-pinned', 'FIXTURE-PAIR-30', 'pinned', '0', '1', 'success'),
      ('FIXTURE-P31-head',   'FIXTURE-PAIR-31', 'head',   '1', '1', 'success'),
      ('FIXTURE-P31-pinned', 'FIXTURE-PAIR-31', 'pinned', '1', '1', 'success'),
      -- outcome='failed' run — EXCLUDED upstream (no cell).
      ('FIXTURE-P32-head',   'FIXTURE-PAIR-32', 'head',   '0', '1', 'failed'),
      ('FIXTURE-P32-pinned', 'FIXTURE-PAIR-32', 'pinned', '0', '1', 'failed')
    ]) AS t,
    t.1 AS run_id,
    t.2 AS pair_id,
    t.3 AS arm,
    t.4 AS flagged,
    t.5 AS tier,
    t.6 AS outcome,
    rowNumberInAllBlocks() AS rn,
    map(
      'pair_id',           pair_id,
      'arm',               arm,
      'tier',              tier,
      'outcome',           outcome,
      'flagged',           flagged,
      'flag_reason',       if(flagged = '1', 'task_retries', ''),
      'environment_class', 'fixture',
      'target_region',     'fixture-region',
      'compute_region',    'fixture-region',
      'target_service',    'fixture-service',
      'partition_scheme',  'none'
    ) AS runtime
);

-- ---- perf.metrics : integrity (both arms, PASSING) --------------------------
-- Integrity: these fixture runs emit integrity_ok=1 (+ delivered==expected /
-- unique==expected) so headline_ok = 1 and the pair is eligible. Covers ALL pairs
-- EXCEPT the integrity-FAIL pairs P30/P31 (seeded 0 in the next block). P32 is
-- seeded PASSING here — it is excluded by outcome='failed', not by integrity, so a
-- broken outcome filter would leak it as OK (=> UNEXPECTED-CELL), the exclusion trap.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, metric_name, unit, value, toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT
    arrayJoin([
      'FIXTURE-P01-head','FIXTURE-P01-pinned','FIXTURE-P02-head','FIXTURE-P02-pinned',
      'FIXTURE-P03-head','FIXTURE-P03-pinned','FIXTURE-P04-head','FIXTURE-P04-pinned',
      'FIXTURE-P05-head','FIXTURE-P05-pinned','FIXTURE-P06-head','FIXTURE-P06-pinned',
      'FIXTURE-P07-head','FIXTURE-P07-pinned','FIXTURE-P08-head','FIXTURE-P08-pinned',
      'FIXTURE-P09-head','FIXTURE-P09-pinned','FIXTURE-P10-head','FIXTURE-P10-pinned',
      'FIXTURE-P11-head','FIXTURE-P11-pinned','FIXTURE-P12-head','FIXTURE-P12-pinned',
      'FIXTURE-P13-head','FIXTURE-P13-pinned','FIXTURE-P14-head','FIXTURE-P14-pinned',
      'FIXTURE-P15-head','FIXTURE-P15-pinned','FIXTURE-P16-head','FIXTURE-P16-pinned',
      -- D4 tier-0 pairs (PASSING) + the outcome='failed' pair (PASSING integrity).
      'FIXTURE-P20-head','FIXTURE-P20-pinned','FIXTURE-P21-head','FIXTURE-P21-pinned',
      'FIXTURE-P22-head','FIXTURE-P22-pinned','FIXTURE-P23-head','FIXTURE-P23-pinned',
      'FIXTURE-P32-head','FIXTURE-P32-pinned'
    ]) AS run_id,
    arrayJoin([
      ('integrity_ok',     'bool',  1.0),
      ('rows_delivered',   'rows',  1000000.0),
      ('rows_expected',    'rows',  1000000.0),
      ('unique_delivered', 'rows',  1000000.0),
      ('unique_expected',  'rows',  1000000.0)
    ]) AS mt,
    mt.1 AS metric_name,
    mt.2 AS unit,
    mt.3 AS value
);

-- ---- perf.metrics : integrity FAILED (P30/P31, both arms) -------------------
-- D4 re-base: integrity_ok=0 on both arms + a delivered!=expected mismatch. Both
-- signals agree the run is integrity-FAILED. The check view CARRIES these rows
-- (does NOT drop them) so the FAIL verdict renders on every asserted metric —
-- contract §3 precedence FAIL > FLAG (P31 is also flagged, so it proves FAIL>FLAG).
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, metric_name, unit, value, toDateTime('2026-07-09 01:20:00')
FROM
(
  SELECT
    arrayJoin([
      'FIXTURE-P30-head','FIXTURE-P30-pinned',
      'FIXTURE-P31-head','FIXTURE-P31-pinned'
    ]) AS run_id,
    arrayJoin([
      ('integrity_ok',     'bool',  0.0),        -- explicit FAIL
      ('rows_delivered',   'rows',   999000.0),  -- mismatch: delivered < expected
      ('rows_expected',    'rows',  1000000.0),
      ('unique_delivered', 'rows',   999000.0),
      ('unique_expected',  'rows',  1000000.0)
    ]) AS mt,
    mt.1 AS metric_name,
    mt.2 AS unit,
    mt.3 AS value
);

-- ---- WATCH-ONLY (NOT seeded): throughput_rows_per_sec ----------------------
-- D4 "full demote + re-base" (Amendment 2026-08-31): throughput_rows_per_sec is
-- DEMOTED to WATCH-ONLY and is NO LONGER seeded or asserted here — mirroring
-- merge_amplification, the other watch-only metric that is likewise not seeded and
-- is EXCLUDED by v_verdict_fixture_check's classified WHERE. (Its former HB banded
-- coverage is replaced by the tier-0 null_rows_per_sec HB block below.) The
-- verified Tier-1 gate is now ch_insert_cpu_seconds_per_Mrows (banded ±6%), seeded
-- alongside cpu_seconds_per_Mrows in the two cpu-family blocks that follow.

-- ---- Gated BANDED cpu family (lower_better, ±6%) ----------------------------
-- cpu_seconds_per_Mrows (Tier-0 client-cpu gate) AND ch_insert_cpu_seconds_per_Mrows
-- (the D4 Tier-1 server-cpu gate) share the SAME band, direction AND — here — the
-- SAME seeded values, so both map to the SAME verdict per pair. We seed the value
-- set ONCE via arrayJoin over BOTH metric names.
--   pinned baseline = 10; head = 10 * ratio. In-band ∈ [0.94, 1.06].
--   Below-ratio = 0.90 (<0.94 => IMPROVEMENT for LB); above-ratio = 1.10
--   (>1.06 => REGRESSION for LB). near-edge INSIDE = 1.05 (<1.06 => OK);
--   near-edge OUTSIDE = 0.93 (<0.94 => IMPROVEMENT, GOOD direction for LB).
--   P30/P31 (integrity-FAIL) + P32 (outcome='failed') carry an in-band 1.00 pair so
--   that, absent the FAIL/exclusion layers, they would read OK — proving the FAIL
--   verdict and the failed-run exclusion actually override an otherwise-OK cell.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, metric_name, 's/Mrows' AS unit, value, toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT
    arrayJoin(['cpu_seconds_per_Mrows','ch_insert_cpu_seconds_per_Mrows']) AS metric_name,
    t.1 AS run_id, t.2 AS value
  FROM (
    SELECT arrayJoin([
      -- (run_id, value)  head = pinned(10) * ratio
      ('FIXTURE-P01-head',     9.0), ('FIXTURE-P01-pinned', 10.0),  -- ratio 0.90 below => IMPROVEMENT (LB)
      ('FIXTURE-P02-head',    10.0), ('FIXTURE-P02-pinned', 10.0),  -- ratio 1.00 in => OK
      ('FIXTURE-P03-head',    11.0), ('FIXTURE-P03-pinned', 10.0),  -- ratio 1.10 above => REGRESSION (LB)
      ('FIXTURE-P04-head',    10.0),                                -- P04 pinned absent => NULL
      ('FIXTURE-P05-head',    10.0), ('FIXTURE-P05-pinned',  0.0),  -- 0-denom => NULL
      ('FIXTURE-P06-head',    10.5), ('FIXTURE-P06-pinned', 10.0),  -- ratio 1.05 near-edge INSIDE => OK
      ('FIXTURE-P07-head',     9.3), ('FIXTURE-P07-pinned', 10.0),  -- ratio 0.93 near-edge OUTSIDE => IMPROVEMENT
      ('FIXTURE-P08-head',    10.0), ('FIXTURE-P08-pinned', 10.0),  -- ratio 1.00 in (tripwire-fired pair)
      ('FIXTURE-P09-head',    10.0), ('FIXTURE-P09-pinned', 10.0),  -- ratio 1.00 in (tripwire-fired pair)
      ('FIXTURE-P10-head',     9.0), ('FIXTURE-P10-pinned', 10.0),  -- 0.90 but FLAGGED
      ('FIXTURE-P11-head',    10.0),                                -- P11 pinned absent => NULL but FLAGGED
      ('FIXTURE-P12-head',    10.0), ('FIXTURE-P12-pinned',  0.0),  -- 0-denom but FLAGGED
      ('FIXTURE-P13-head',    10.0), ('FIXTURE-P13-pinned', 10.0),  -- 1.00 in-band but FLAGGED
      ('FIXTURE-P14-head',    11.0), ('FIXTURE-P14-pinned', 10.0),  -- 1.10 above but FLAGGED
      ('FIXTURE-P15-head',    10.0), ('FIXTURE-P15-pinned', 10.0),  -- 1.00 in (tripwire ARMED) but FLAGGED
      -- P16: HEAD arm OMITS cpu; only PINNED present => NO_DATA (absent-head banded).
      ('FIXTURE-P16-pinned',  10.0),
      -- D4: integrity-FAIL (P30/P31) + failed-run (P32) — in-band 1.00 => would be OK
      -- absent FAIL/exclusion; the FAIL verdict / outcome filter must override.
      ('FIXTURE-P30-head',    10.0), ('FIXTURE-P30-pinned', 10.0),
      ('FIXTURE-P31-head',    10.0), ('FIXTURE-P31-pinned', 10.0),
      ('FIXTURE-P32-head',    10.0), ('FIXTURE-P32-pinned', 10.0)
    ]) AS t
  )
);

-- ---- Gated TRIPWIRE metric: parts_per_insert (binary, HEAD absolute) --------
-- NOT banded, NO ratio. The verdict keys on the HEAD arm's ABSOLUTE value:
--   head == 1.0 => OK ; head != 1.0 => TRIPWIRE ; head ABSENT => NO_DATA.
-- The pinned arm is a don't-care for this metric; where a pinned value is seeded
-- it mirrors the head value purely for tidiness (the verdict never reads it).
--   P04 / P11 (NULL cells): parts ABSENT on BOTH arms => NO_DATA.
--   P08 (fired hi): head 1.05 => TRIPWIRE ; P09 (fired lo): head 0.95 => TRIPWIRE.
--   P15 (flagged): head 1.05 ARMS the tripwire, but FLAGGED overrides => FLAGGED.
--   all other pairs: head 1.0 => OK (or FLAGGED for 10/12/13/14).
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'parts_per_insert' AS metric_name, 'ratio' AS unit, value,
       toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT arrayJoin([
    ('FIXTURE-P01-head',   1.00), ('FIXTURE-P01-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P02-head',   1.00), ('FIXTURE-P02-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P03-head',   1.00), ('FIXTURE-P03-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P04-head',   1.00), ('FIXTURE-P04-pinned', 1.00),  -- ==1.0 => OK (parts ignores pinned)
    ('FIXTURE-P05-head',   1.00), ('FIXTURE-P05-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P06-head',   1.00), ('FIXTURE-P06-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P07-head',   1.00), ('FIXTURE-P07-pinned', 1.00),  -- ==1.0 => OK
    ('FIXTURE-P08-head',   1.05), ('FIXTURE-P08-pinned', 1.05),  -- !=1.0 => TRIPWIRE (fired hi)
    ('FIXTURE-P09-head',   0.95), ('FIXTURE-P09-pinned', 0.95),  -- !=1.0 => TRIPWIRE (fired lo)
    ('FIXTURE-P10-head',   1.00), ('FIXTURE-P10-pinned', 1.00),  -- ==1.0 but FLAGGED
    ('FIXTURE-P11-head',   1.00), ('FIXTURE-P11-pinned', 1.00),  -- ==1.0 but FLAGGED
    ('FIXTURE-P12-head',   1.00), ('FIXTURE-P12-pinned', 1.00),  -- ==1.0 but FLAGGED
    ('FIXTURE-P13-head',   1.00), ('FIXTURE-P13-pinned', 1.00),  -- ==1.0 but FLAGGED
    ('FIXTURE-P14-head',   1.00), ('FIXTURE-P14-pinned', 1.00),  -- ==1.0 but FLAGGED
    ('FIXTURE-P15-head',   1.05), ('FIXTURE-P15-pinned', 1.05),  -- !=1.0 ARMED but FLAGGED => FLAGGED
    -- P16: HEAD arm OMITS parts entirely (only PINNED present). head_value NULL =>
    -- NO_DATA (absent-head TRIPWIRE metric — NOT an armed tripwire, NOT OK). This is
    -- the kafka cross-check gap cell: the head-driven join used to DROP this row.
    ('FIXTURE-P16-pinned', 1.00),
    -- D4: integrity-FAIL (P30/P31) + failed-run (P32) — head parts=1.0 => would be OK
    -- absent FAIL/exclusion; the FAIL verdict / outcome filter must override.
    ('FIXTURE-P30-head',   1.00), ('FIXTURE-P30-pinned', 1.00),
    ('FIXTURE-P31-head',   1.00), ('FIXTURE-P31-pinned', 1.00),
    ('FIXTURE-P32-head',   1.00), ('FIXTURE-P32-pinned', 1.00)
  ]) AS t, t.1 AS run_id, t.2 AS value
);

-- ---- Gated BANDED metric: null_rows_per_sec (higher_better, ±8.5%) ----------
-- TIER-0 coverage (D4 re-base). pinned baseline = 100; head = 100 * ratio.
-- In-band ∈ [0.915, 1.085]. HB direction: ratio < 0.915 (BAD) => REGRESSION;
-- ratio > 1.085 (GOOD) => IMPROVEMENT.
--   P20 below = 0.85 (<0.915 => REGRESSION); P21 in = 1.00 (=> OK);
--   P22 above = 1.15 (>1.085 => IMPROVEMENT); P23 pinned ABSENT => ratio NULL => NO_DATA.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'null_rows_per_sec' AS metric_name, 'rows/s' AS unit, value,
       toDateTime('2026-07-09 01:20:00')
FROM
(
  SELECT arrayJoin([
    -- (run_id, value)  head = pinned(100) * ratio
    ('FIXTURE-P20-head',    85.0), ('FIXTURE-P20-pinned', 100.0),  -- ratio 0.85 below => REGRESSION (HB)
    ('FIXTURE-P21-head',   100.0), ('FIXTURE-P21-pinned', 100.0),  -- ratio 1.00 in => OK
    ('FIXTURE-P22-head',   115.0), ('FIXTURE-P22-pinned', 100.0),  -- ratio 1.15 above => IMPROVEMENT (HB)
    ('FIXTURE-P23-head',   100.0)                                  -- P23 pinned ABSENT => ratio NULL => NO_DATA
  ]) AS t, t.1 AS run_id, t.2 AS value
);

-- ---- Gated BANDED metric: serialize_seconds_per_Mrows (lower_better, ±8.5%) --
-- TIER-0 coverage (D4 re-base). pinned baseline = 10; head = 10 * ratio.
-- In-band ∈ [0.915, 1.085]. LB direction: ratio < 0.915 (GOOD) => IMPROVEMENT;
-- ratio > 1.085 (BAD) => REGRESSION.
--   P20 below = 0.85 (<0.915 => IMPROVEMENT); P21 in = 1.00 (=> OK);
--   P22 above = 1.15 (>1.085 => REGRESSION); P23 pinned ABSENT => ratio NULL => NO_DATA.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'serialize_seconds_per_Mrows' AS metric_name, 's/Mrows' AS unit, value,
       toDateTime('2026-07-09 01:20:00')
FROM
(
  SELECT arrayJoin([
    -- (run_id, value)  head = pinned(10) * ratio
    ('FIXTURE-P20-head',     8.5), ('FIXTURE-P20-pinned', 10.0),  -- ratio 0.85 below => IMPROVEMENT (LB)
    ('FIXTURE-P21-head',    10.0), ('FIXTURE-P21-pinned', 10.0),  -- ratio 1.00 in => OK
    ('FIXTURE-P22-head',    11.5), ('FIXTURE-P22-pinned', 10.0),  -- ratio 1.15 above => REGRESSION (LB)
    ('FIXTURE-P23-head',    10.0)                                 -- P23 pinned ABSENT => ratio NULL => NO_DATA
  ]) AS t, t.1 AS run_id, t.2 AS value
);
