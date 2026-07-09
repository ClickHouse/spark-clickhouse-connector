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
--       a distinct id FIXTURE-PAIR-01 .. FIXTURE-PAIR-15, arm in runtime['arm']
--       ('head'|'pinned'), tier in runtime['tier'] ('1').
--
-- BANDS / DIRECTION / TRIPWIRE (contract §3, PINNED — Amendment 2026-07-09b:
--   CALIBRATED per-metric bands at 2x the measured noise floor; the flat
--   ±3%/±5% rule is superseded). The fixture exercises three gated behaviours:
--
--   (a) BANDED higher_better — throughput_rows_per_sec, band ±9% =>
--       in-band ratio ∈ [0.91, 1.09].
--         ratio > 1.09 (GOOD) => IMPROVEMENT ; ratio < 0.91 (BAD) => REGRESSION.
--   (b) BANDED lower_better — cpu_seconds_per_Mrows, band ±6% =>
--       in-band ratio ∈ [0.94, 1.06]. (Replaces parts_per_insert as the LB
--       banded exemplar — parts is now a TRIPWIRE, not a banded ratio.)
--         ratio < 0.94 (GOOD) => IMPROVEMENT ; ratio > 1.06 (BAD) => REGRESSION.
--   (c) TRIPWIRE — parts_per_insert. NOT banded, NO ratio: a BINARY tripwire on
--       the HEAD arm's ABSOLUTE value. head value == 1.0 => OK ; ANY deviation
--       (head value != 1.0) => TRIPWIRE (investigate). head metric absent =>
--       NO_DATA. The pinned arm is IRRELEVANT to this metric.
--
--   ratio = head.value / nullIf(pinned.value, 0)  (v_pair_ratios verbatim).
--
-- Ratio→verdict map (contract §3, PINNED), precedence
--   FAIL > FLAG > {NO_DATA / TRIPWIRE / IMPROVEMENT / REGRESSION / OK}:
--     pair flagged                     => FLAGGED   (overrides EVERYTHING below,
--                                                    incl. an armed TRIPWIRE)
--     (banded) ratio NULL/0-denominator=> NO_DATA   (absent EITHER arm, or /0)
--     (banded) outside band, GOOD dir  => IMPROVEMENT
--     (banded) outside band, BAD dir   => REGRESSION
--     (banded) else                    => OK
--     (tripwire) head absent (NULL)    => NO_DATA   (P16 — absent-HEAD cell)
--     (tripwire) head == 1.0           => OK
--     (tripwire) head != 1.0           => TRIPWIRE
--
-- FIXTURE MATRIX (16 pairs × {throughput_rows_per_sec (HB banded ±9%),
--   cpu_seconds_per_Mrows (LB banded ±6%), parts_per_insert (TRIPWIRE)}
--   = 48 asserted cells). Banded-ratio cells are engineered by fixing
--   pinned = 100 (throughput) / 10 (cpu) and setting head = pinned*ratio; NULL
--   cells omit the PINNED arm's banded metric; 0-denom cells set pinned = 0.
--   Tripwire cells set the HEAD arm's parts value directly (pinned parts is a
--   don't-care; seeded = head for tidiness where present).
--
--   pair_id          flagged  bucket              thr(HB±9%)  cpu(LB±6%)  parts(TRIP)
--   ---------------  -------  ------------------   ----------  ----------  -----------
--   FIXTURE-PAIR-01  no       below-band          REGRESSION  IMPROVEMENT OK (1.0)
--   FIXTURE-PAIR-02  no       in-band             OK          OK          OK (1.0)
--   FIXTURE-PAIR-03  no       above-band          IMPROVEMENT REGRESSION  OK (1.0)
--   FIXTURE-PAIR-04  no       NULL (pinned absent) NO_DATA     NO_DATA     OK (1.0)
--   FIXTURE-PAIR-05  no       0-denominator (pin=0)NO_DATA     NO_DATA     OK (1.0)
--   FIXTURE-PAIR-06  no       near-edge INSIDE     OK          OK          OK (1.0)
--   FIXTURE-PAIR-07  no       near-edge OUTSIDE    IMPROVEMENT IMPROVEMENT OK (1.0)
--   FIXTURE-PAIR-08  no       tripwire fired hi    OK          OK          TRIPWIRE (1.05)
--   FIXTURE-PAIR-09  no       tripwire fired lo    OK          OK          TRIPWIRE (0.95)
--   FIXTURE-PAIR-10  YES      below-band          FLAGGED     FLAGGED     FLAGGED
--   FIXTURE-PAIR-11  YES      NULL (pinned absent) FLAGGED     FLAGGED     FLAGGED
--   FIXTURE-PAIR-12  YES      0-denominator (pin=0)FLAGGED     FLAGGED     FLAGGED
--   FIXTURE-PAIR-13  YES      in-band             FLAGGED     FLAGGED     FLAGGED
--   FIXTURE-PAIR-14  YES      above-band          FLAGGED     FLAGGED     FLAGGED
--   FIXTURE-PAIR-15  YES      tripwire ARMED      FLAGGED     FLAGGED     FLAGGED (armed 1.05)
--   FIXTURE-PAIR-16  no       HEAD arm absent      NO_DATA     NO_DATA     NO_DATA
--                            (pinned present; head emits NO gated metric)
--
--   Coverage of the PINNED acceptance grid:
--     BANDED {below,in,above,NULL(pinned-absent),NULL(head-absent),0-denom} ×
--       {HB,LB} × {flagged,unflagged}:
--       unflagged HB/LB: 01(below) 02(in) 03(above) 04(pinned-NULL) 05(0-denom)
--         16(head-NULL) + near-edge 06(inside)/07(outside) for band-edge precision.
--       flagged  HB/LB: 10(below) 11(NULL) 12(0-denom) 13(in) 14(above) — verdict
--         is CONSTANT FLAGGED, but each guards a DISTINCT precedence bug (an impl
--         that hoists in-band=>OK or good-excursion=>IMPROVEMENT above the flag
--         check is caught only by 13/14).
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
--
-- ELIGIBILITY THROUGH v_runs_enriched / v_pair_ratios: every fixture run is a
--   COMPLETE, comparable row — outcome success, integrity PASSING (integrity_ok=1
--   emitted on both arms), both arms present per pair (except the NULL cells,
--   whose pinned arm run EXISTS and is eligible but deliberately lacks the banded
--   metric so the ratio is NULL — that is the point of that cell). The
--   0-denominator cells emit the banded metric on the pinned arm with value 0.
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

-- ---- perf.metrics : integrity (both arms, PASSING) --------------------------
-- Integrity: every fixture run emits integrity_ok=1 (+ delivered==expected /
-- unique==expected) so v_runs_enriched.headline_ok = 1 and the pair is eligible.
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
      'FIXTURE-P15-head','FIXTURE-P15-pinned','FIXTURE-P16-head','FIXTURE-P16-pinned'
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

-- ---- Gated BANDED metric: throughput_rows_per_sec (higher_better, ±9%) ------
-- pinned baseline = 100; head = 100 * ratio. In-band ∈ [0.91, 1.09].
--   NULL cells (P04, P11) omit ONLY the PINNED arm's metric (head present) =>
--   ratio NULL via the missing denominator; 0-denom cells (P05, P12) set
--   pinned = 0. Below-band = 0.85 (<0.91); above-band = 1.15 (>1.09).
--   near-edge INSIDE = 1.08 (< 1.09 => OK); near-edge OUTSIDE = 1.10 (> 1.09).
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'throughput_rows_per_sec' AS metric_name, 'rows/s' AS unit, value,
       toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT arrayJoin([
    -- (run_id, value)  head = pinned(100) * ratio
    ('FIXTURE-P01-head',    85.0), ('FIXTURE-P01-pinned', 100.0),  -- ratio 0.85 below
    ('FIXTURE-P02-head',   100.0), ('FIXTURE-P02-pinned', 100.0),  -- ratio 1.00 in
    ('FIXTURE-P03-head',   115.0), ('FIXTURE-P03-pinned', 100.0),  -- ratio 1.15 above
    ('FIXTURE-P04-head',   100.0),                                 -- P04 pinned ABSENT => ratio NULL
    ('FIXTURE-P05-head',   100.0), ('FIXTURE-P05-pinned',   0.0),  -- /0 => NULL (NO_DATA)
    ('FIXTURE-P06-head',   108.0), ('FIXTURE-P06-pinned', 100.0),  -- ratio 1.08 near-edge INSIDE
    ('FIXTURE-P07-head',   110.0), ('FIXTURE-P07-pinned', 100.0),  -- ratio 1.10 near-edge OUTSIDE
    ('FIXTURE-P08-head',   100.0), ('FIXTURE-P08-pinned', 100.0),  -- ratio 1.00 in (tripwire-fired pair)
    ('FIXTURE-P09-head',   100.0), ('FIXTURE-P09-pinned', 100.0),  -- ratio 1.00 in (tripwire-fired pair)
    ('FIXTURE-P10-head',    85.0), ('FIXTURE-P10-pinned', 100.0),  -- 0.85 below but FLAGGED
    ('FIXTURE-P11-head',   100.0),                                 -- P11 pinned absent => NULL but FLAGGED
    ('FIXTURE-P12-head',   100.0), ('FIXTURE-P12-pinned',   0.0),  -- 0-denom but FLAGGED
    ('FIXTURE-P13-head',   100.0), ('FIXTURE-P13-pinned', 100.0),  -- 1.00 in-band but FLAGGED
    ('FIXTURE-P14-head',   115.0), ('FIXTURE-P14-pinned', 100.0),  -- 1.15 above but FLAGGED
    ('FIXTURE-P15-head',   100.0), ('FIXTURE-P15-pinned', 100.0),  -- 1.00 in (tripwire ARMED) but FLAGGED
    -- P16: HEAD arm OMITS throughput; only PINNED present => head_value NULL =>
    -- ratio NULL => NO_DATA (absent-head banded cell).
    ('FIXTURE-P16-pinned', 100.0)
  ]) AS t, t.1 AS run_id, t.2 AS value
);

-- ---- Gated BANDED metric: cpu_seconds_per_Mrows (lower_better, ±6%) ----------
-- pinned baseline = 10; head = 10 * ratio. In-band ∈ [0.94, 1.06].
--   Replaces parts_per_insert as the LB banded exemplar. Same NULL / 0-denom
--   construction. Below-ratio = 0.90 (<0.94 => IMPROVEMENT for LB);
--   above-ratio = 1.10 (>1.06 => REGRESSION for LB).
--   near-edge INSIDE = 1.05 (<1.06 => OK); near-edge OUTSIDE = 0.93 (<0.94 =>
--   IMPROVEMENT, GOOD direction for LB).
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'cpu_seconds_per_Mrows' AS metric_name, 's/Mrows' AS unit, value,
       toDateTime('2026-07-09 00:20:00')
FROM
(
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
    ('FIXTURE-P16-pinned',  10.0)
  ]) AS t, t.1 AS run_id, t.2 AS value
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
    ('FIXTURE-P16-pinned', 1.00)
  ]) AS t, t.1 AS run_id, t.2 AS value
);
