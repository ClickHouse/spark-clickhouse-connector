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
--   (PINNED)" (amendment a58c95d7): any artifact that emits a VERDICT (not a
--   number) requires fixture-based acceptance — synthetic PAIR rows under a
--   RESERVED fixture connector, asserted THROUGH THE REAL dataset SQL, covering
--   at least {NULL, 0-denominator, below-band, in-band, above-band} ×
--   {higher_better, lower_better} × {flagged, unflagged}. Consumer views MUST
--   exclude the fixture connector from real trends.
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
--       a distinct id FIXTURE-PAIR-01 .. FIXTURE-PAIR-10, arm in runtime['arm']
--       ('head'|'pinned'), tier in runtime['tier'] ('1').
--
-- BAND / DIRECTION (contract §3, PINNED): Tier-1 band is ±5% => in-band
--   ratio ∈ [0.95, 1.05]. Directions:
--     throughput_rows_per_sec = higher_better  (ratio>1.05 GOOD => IMPROVEMENT,
--                                                ratio<0.95 BAD  => REGRESSION)
--     parts_per_insert        = lower_better   (ratio<0.95 GOOD => IMPROVEMENT,
--                                                ratio>1.05 BAD  => REGRESSION)
--   ratio = head.value / pinned.value  (computed EXACTLY as v_pair_ratios:
--   head.value / nullIf(pinned.value, 0)).
--
-- Ratio→verdict map (contract §3, PINNED), precedence
--   FAIL > FLAG > NO_DATA/IMPROVEMENT/REGRESSION/OK:
--     ratio NULL or 0-denominator   => NO_DATA   (never REGRESSION)
--     pair flagged                  => FLAGGED   (overrides band verdicts AND NO_DATA)
--     ratio outside band, GOOD dir  => IMPROVEMENT
--     ratio outside band, BAD dir   => REGRESSION
--     else                          => OK
--
-- FIXTURE MATRIX (10 pairs × {throughput_rows_per_sec (HB), parts_per_insert (LB)}
--   = 20 asserted cells — the FULL literal contract product
--   {NULL, 0-denominator, below-band, in-band, above-band} × {higher_better,
--   lower_better} × {flagged, unflagged} = 5 × 2 × 2 = 20). Ratios are engineered
--   by fixing pinned=100 (throughput) / pinned=10 (parts) and setting
--   head = pinned*ratio; the NULL cell omits the PINNED arm's metric; the
--   0-denominator cell pins pinned value = 0.
--
--   pair_id          flagged  ratio-bucket        throughput(HB)  parts(LB)
--   ---------------  -------  ------------------   --------------  ------------
--   FIXTURE-PAIR-01  no       below-band 0.90      REGRESSION      IMPROVEMENT
--   FIXTURE-PAIR-02  no       in-band    1.00      OK              OK
--   FIXTURE-PAIR-03  no       above-band 1.10      IMPROVEMENT      REGRESSION
--   FIXTURE-PAIR-04  no       NULL (pinned absent) NO_DATA         NO_DATA
--   FIXTURE-PAIR-05  no       0-denominator (pin=0)NO_DATA         NO_DATA
--   FIXTURE-PAIR-06  YES      below-band 0.90      FLAGGED         FLAGGED
--   FIXTURE-PAIR-07  YES      NULL (pinned absent) FLAGGED         FLAGGED
--   FIXTURE-PAIR-08  YES      0-denominator (pin=0)FLAGGED         FLAGGED
--   FIXTURE-PAIR-09  YES      in-band    1.00      FLAGGED         FLAGGED
--   FIXTURE-PAIR-10  YES      above-band 1.10      FLAGGED         FLAGGED
--
--   Coverage of the PINNED acceptance grid
--   {NULL,0-denom,below,in,above} × {HB,LB} × {flagged,unflagged}:
--     unflagged: 01(below) 02(in) 03(above) 04(NULL) 05(0-denom) — all 5 × both
--       metrics = 10 cells.
--     flagged:   06(below) 07(NULL) 08(0-denom) 09(in) 10(above) — all 5 × both
--       metrics = 10 cells. The expected verdict is CONSTANT FLAGGED across the
--       flagged row (the flag overrides every ratio class), but each cell guards
--       a DISTINCT precedence bug: an implementation that hoists an explicit
--       in-band=>OK arm ABOVE the flag check passes 06-08 undetected and is
--       caught ONLY by PAIR-09; a good-direction-excursion=>IMPROVEMENT arm
--       hoisted above the flag is caught only by PAIR-10 (throughput cell).
--     => 20 cells, every (ratio-class × direction × flag-state) combination.
--
-- ELIGIBILITY THROUGH v_runs_enriched / v_pair_ratios: every fixture run is a
--   COMPLETE, comparable row — outcome success, integrity PASSING (integrity_ok=1
--   emitted on both arms), both arms present per pair (except the NULL cells,
--   whose pinned arm run EXISTS and is eligible but deliberately lacks the gated
--   metric so the ratio is NULL — that is the point of that cell). The
--   0-denominator cells emit the gated metric on the pinned arm with value 0.
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
  'verdict truth-table fixture (contract §3)' AS notes
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
      ('FIXTURE-P06-head',   'FIXTURE-PAIR-06', 'head',   '1'),
      ('FIXTURE-P06-pinned', 'FIXTURE-PAIR-06', 'pinned', '1'),
      ('FIXTURE-P07-head',   'FIXTURE-PAIR-07', 'head',   '1'),
      ('FIXTURE-P07-pinned', 'FIXTURE-PAIR-07', 'pinned', '1'),
      ('FIXTURE-P08-head',   'FIXTURE-PAIR-08', 'head',   '1'),
      ('FIXTURE-P08-pinned', 'FIXTURE-PAIR-08', 'pinned', '1'),
      ('FIXTURE-P09-head',   'FIXTURE-PAIR-09', 'head',   '1'),
      ('FIXTURE-P09-pinned', 'FIXTURE-PAIR-09', 'pinned', '1'),
      ('FIXTURE-P10-head',   'FIXTURE-PAIR-10', 'head',   '1'),
      ('FIXTURE-P10-pinned', 'FIXTURE-PAIR-10', 'pinned', '1')
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

-- ---- perf.metrics : integrity (both arms, PASSING) + gated metrics ----------
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
      'FIXTURE-P09-head','FIXTURE-P09-pinned','FIXTURE-P10-head','FIXTURE-P10-pinned'
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

-- Gated metric: throughput_rows_per_sec (higher_better). pinned baseline = 100.
--   head = 100 * ratio. The NULL cells (P04, P07) omit ONLY the PINNED arm's
--   metric (the head arm's value is present) => ratio NULL via the missing
--   denominator; the 0-denominator cells (P05, P08) set pinned = 0.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'throughput_rows_per_sec' AS metric_name, 'rows/s' AS unit, value,
       toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT arrayJoin([
    -- (run_id, value)  head = pinned(100) * ratio
    ('FIXTURE-P01-head',    90.0), ('FIXTURE-P01-pinned', 100.0),  -- ratio 0.90
    ('FIXTURE-P02-head',   100.0), ('FIXTURE-P02-pinned', 100.0),  -- ratio 1.00
    ('FIXTURE-P03-head',   110.0), ('FIXTURE-P03-pinned', 100.0),  -- ratio 1.10
    ('FIXTURE-P04-head',   100.0),                                 -- P04 pinned: metric ABSENT => ratio NULL
    ('FIXTURE-P05-head',   100.0), ('FIXTURE-P05-pinned',   0.0),  -- ratio: /0 => NULL (NO_DATA)
    ('FIXTURE-P06-head',    90.0), ('FIXTURE-P06-pinned', 100.0),  -- ratio 0.90 but FLAGGED
    ('FIXTURE-P07-head',   100.0),                                 -- P07 pinned absent => NULL but FLAGGED
    ('FIXTURE-P08-head',   100.0), ('FIXTURE-P08-pinned',   0.0),  -- 0-denom but FLAGGED
    ('FIXTURE-P09-head',   100.0), ('FIXTURE-P09-pinned', 100.0),  -- ratio 1.00 (in-band) but FLAGGED
    ('FIXTURE-P10-head',   110.0), ('FIXTURE-P10-pinned', 100.0)   -- ratio 1.10 (above) but FLAGGED
  ]) AS t, t.1 AS run_id, t.2 AS value
);

-- Gated metric: parts_per_insert (lower_better). pinned baseline = 10.
--   head = 10 * ratio. Same NULL / 0-denominator construction as throughput.
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at)
SELECT run_id, 'parts_per_insert' AS metric_name, 'ratio' AS unit, value,
       toDateTime('2026-07-09 00:20:00')
FROM
(
  SELECT arrayJoin([
    ('FIXTURE-P01-head',     9.0), ('FIXTURE-P01-pinned', 10.0),  -- ratio 0.90
    ('FIXTURE-P02-head',    10.0), ('FIXTURE-P02-pinned', 10.0),  -- ratio 1.00
    ('FIXTURE-P03-head',    11.0), ('FIXTURE-P03-pinned', 10.0),  -- ratio 1.10
    ('FIXTURE-P04-head',    10.0),                                -- P04 pinned absent => NULL
    ('FIXTURE-P05-head',    10.0), ('FIXTURE-P05-pinned',  0.0),  -- 0-denom => NULL
    ('FIXTURE-P06-head',     9.0), ('FIXTURE-P06-pinned', 10.0),  -- ratio 0.90 but FLAGGED
    ('FIXTURE-P07-head',    10.0),                                -- P07 pinned absent => NULL but FLAGGED
    ('FIXTURE-P08-head',    10.0), ('FIXTURE-P08-pinned',  0.0),  -- 0-denom but FLAGGED
    ('FIXTURE-P09-head',    10.0), ('FIXTURE-P09-pinned', 10.0),  -- ratio 1.00 (in-band) but FLAGGED
    ('FIXTURE-P10-head',    11.0), ('FIXTURE-P10-pinned', 10.0)   -- ratio 1.10 (above) but FLAGGED
  ]) AS t, t.1 AS run_id, t.2 AS value
);
