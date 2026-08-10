# ClickBench Connector Benchmark v2 — Top-Level Plan

Status: DRAFT for review. Top-level plan; to be split into concrete tasks later.
Scope: Spark connector benchmark only (the Kafka Connect plan is a separate document).
Tiers 2 (overload/robustness) and 3 (right-sized ceiling) are **deferred** — captured in
Appendix A so the design stays coherent, but they are not part of the initial build.

---

## 1. What are we benchmarking?

The current benchmark answers one blended question. V2 splits it into distinct
questions, each with its own instrument, because they have different bottlenecks
and a single number cannot answer both:

| Q | Question | Bottleneck | Instrument |
|---|----------|-----------|------------|
| A | Is the **connector client** getting slower or hungrier? (CPU/row, serialization, memory, protocol efficiency) | client | Tier 0 — Null-engine run |
| B | Is the connector sending the server **more expensive work per row**? (batching, part counts, sortedness, merge cost) | server | Tier 1 — saturated Cloud run |

Explicitly NOT a goal: benchmarking ClickHouse Cloud itself. Server capacity numbers
fall out of Tier 1 as context, but the product under test is always the connector.

Design principle for everything below: **a regression signal must move when the
connector changes and stay flat when the environment changes.**

Nuances agreed during review:
- Connector efficiency legitimately shows up server-side (better batching -> fewer
  parts -> cheaper merges -> more rows/s on the same target). The saturated Tier 1
  detects regressions in *server cost per row*. What it is blind to is regressions in
  *client cost per row* (serialization CPU, client memory) — while the server is the
  bottleneck the client has slack. Tier 0 exists to close that blindness.
- The metrics window being CI wall-clock is NOT a problem for the current metric set:
  all windowed metrics are peaks (max) or counter deltas (max-min of cumulative
  ProfileEvents), so idle EMR-provisioning time does not dilute them. Residual note:
  counter deltas (network bytes, connections) also swallow non-job activity in the
  window (truncate/warmup traffic). Acceptable; documented.

---

## 2. The two-arm protocol (applies to every tier)

Every scheduled run executes the same ingest **twice on the same EMR cluster**:

- **Arm H** — connector built from HEAD (as today).
- **Arm P** — pinned reference build (last tagged release jar, fetched from artifact
  storage; bumped only deliberately, and every bump is an annotated dashboard event).

Same cluster, same hour, same server, same Cloud version, same memory-pressure state.
Arm order alternates by day (even/odd) so first-run advantage (cleaner server state)
cannot systematically favor one arm.

Why: we cannot pin the environment (ClickHouse Cloud auto-upgrades, restarts, state
drift), so we pin a *reference point inside it*. Environment noise hits both arms and
cancels in the ratio H/P; only connector changes move the ratio. This converts the
benchmark from a noisy time series into a controlled experiment.

Cost: one extra ingest pass (~6-8 min) on an already-provisioned cluster (EMR
provisioning dominates run wall-clock, so the marginal cost is small).

Data model: both arms are ordinary rows in `perf.runs` sharing
`runtime['pair_id'] = <RUN_ID>`, distinguished by `runtime['arm'] = 'head' | 'pinned'`.
All historical runs are implicitly `arm=head` — **the absolute trend line is never
broken**.

---

## 3. Tiers

### Tier 0 — Connector ceiling (`ENGINE=Null`) — nightly; the regression gate for Q-A

> **REDESIGNED 2026-07-07 (contract Amendment 2026-07-07c).** Tier 0 originally
> targeted a self-hosted ClickHouse in Docker on the EMR master. That instrument was
> dropped in favour of an `ENGINE=Null` table on the **same Cloud target as Tier 1**
> (`clickbench.hits_null`): zero extra infra, and the H/P ratio still cancels Cloud
> drift because both arms hit the same table in the same hour. The paragraphs below
> describe the shipped design; the former Docker-teardown capture-ordering problem is
> moot (see the resolved open decision #4).

- Target: an `ENGINE=Null` table (`clickbench.hits_null`, identical schema to `hits`)
  on the **same 3 vCPU / 12 GiB Cloud service as Tier 1**. The server parses each
  insert and discards it — no parts, no merges, no storage, no memory pressure,
  near-zero variance.
- What it DOES exercise (answering the "is this a correct client test?" question):
  the full client and network path — serialization, compression, HTTPS connection
  setup, keep-alive/pooling, block send, server parse, ack wait (`wait_end_of_query`
  applies). What it removes is only server-side *storage* work, which is exactly the
  noise source. What it does NOT test: client behavior under server pushback
  (throttling, delayed inserts, slow acks) — that remains Tier 1's job (and the
  deferred Tier 2's).
- Caveats: (a) parse still costs server CPU, and the shared 3 vCPU target has less
  headroom than a dedicated box, so `ch_insert_cpu_share_tier0` is captured every run
  and charted with a threshold (Tab 3) — if it camps near 100%, Tier 0 has become a
  server benchmark and the decision is revisited (fallback: a pinned CH pod);
  (b) Tier 0 complements — does not replace — the per-row CPU metrics, which are
  gated as well since they are free.
- Because Tier 0 runs on the Cloud target (not a Docker CH that dies at EMR teardown),
  its server-side capture reuses Tier 1's post-ingest capture path — there is no
  capture-before-teardown ordering constraint.

### Tier 1 — Realistic ingest (current run, kept) — nightly; the gate for Q-B

- Target: the same 3 vCPU / 12 GiB Cloud service. Pipeline unchanged, reframed
  honestly: the headline is a *server-bound ingest rate*; its job is the long-term
  trend and cross-connector comparison (the Kafka test will share the target).
- Gating shifts from raw rows/s alone to **server-cost-per-row**:
  `parts_per_insert`, `merge_amplification`, `inserts_delayed_fraction` — these stay
  meaningful even when the capacity ceiling moves.
- Changes folded in:
  - `PARTITION BY toYYYYMM(EventDate)` (~12 real partitions from the same dataset) —
    finally exercises the partition-aware write path (`repartitionByPartition` +
    `localSortByPartition`) non-degenerately. NOTE: changes Tier 1 absolutes; must be
    annotated, and lands only after the two-arm ratio view exists (ratio is immune).
  - Row-count + duplicate verification after settle (see section 6).
  - Pre-run server state captured as covariates (uptime, RSS, active parts).

---

## 4. Workload configuration and sweeps

Informed by the "Supercharge your ClickHouse data loads" posts (parts 1-3):

- **Operating config** (Tier 1): 100k batch, async OFF, baseline EMR profile — but
  **moved into the repo** (workflow env / ingest script) and recorded per run in the
  `runtime` map. The config under test must be version-controlled; the current state
  (batchSize only in a diverged S3 orchestrator script) ends.
- **The next sweep axis is parallelism, not batch.** Blog-derived guideline: insert
  parallelism ~= half the *server's* cores -> the 3 vCPU target wants ~1-2 concurrent
  inserts; we currently hit it with 32 (baseline) / 128 (stress). This single number
  explains merge-pool 100%, the OOM regime, and the stress failures.
  One-time campaign: grid `write parallelism {2, 4, 8, 16, 32} x batch {50k, 100k}`,
  interleaved order (not sequential — sequential order confounds with server-pressure
  drift), >=5 runs per cell.
- **Memory formula used up front**: peak_memory ~= 3 x threads x block_bytes
  (from part 2). Plug in concurrent inserts x batch rows x bytes/row against the
  12.88 GB cap to predict the OOM boundary and pick grid points that bracket it.
- **Batch re-verification**: the "100k optimum" claim is re-established inside that
  grid (interleaved, n>=5). The original sweep (2 runs/point, the day after a server
  restart, sequential) cannot support it.
- **Datasets**: `hits` stays forever as the continuity anchor (nightly). Add one
  alternate shape weekly — PyPI sample (~200M rows; narrow, string-heavy) — and later
  the synthetic JSON/Variant set from the backlog. Shape variety at low cadence; the
  nightly trend is never touched. Larger sizes buy little at Tier 1 (100M rows
  already saturates the target).

---

## 5. Metrics catalog

Convention: [E] = exists in today's capture, [N] = new. All land in the same
tall/narrow `perf.metrics`, tagged via `runtime['tier']` and `runtime['arm']`.

### Tier 0 — every metric is a statement about the connector

| Metric | Unit | Source | What it tells us |
|---|---|---|---|
| `null_rows_per_sec` / `null_mb_per_sec` [N] | rows/s, MB/s | event log | The connector's raw ceiling — headline for Q-A |
| `cpu_seconds_per_Mrows` [N, derived from E] | s/M rows | executor_cpu_time_total / write_rows | Compute cost per million rows; purest client-regression signal |
| `serialize_seconds_per_Mrows` [N, derived from E] | s/M rows | serialize_time_total | Cost of format conversion; isolates serializer changes |
| `write_wait_share` [N, derived] | % | write_time / (write+serialize+cpu) | Waiting-on-wire vs computing; separates protocol/network from CPU changes |
| `bytes_on_wire_per_row` [N] | bytes | server network_receive_bytes / rows | Wire efficiency; compression/encoding regressions |
| `peak_jvm_heap_bytes` / `offheap` [E] | bytes | StageExecutorMetrics | Client memory footprint; buffer-management regressions |
| `connections_per_insert` [E] | ratio | server metric_log | Pool/keep-alive health with zero server confounders |
| `jvm_gc_time_share` [N, derived from E] | % | jvm_gc_time_total / cpu | Allocation-pressure regressions (classic serializer failure mode) |
| `ch_insert_cpu_share_tier0` [N] | % | Null-target query_log CPU / wall | Instrument-health watch: if the Null target ever becomes parse-bound (CPU share camping near 100%), Tier 0 is measuring the server, not the connector — resize or investigate |

Statistics / learnings:
- **H/P ratio on `cpu_seconds_per_Mrows`** — the most sensitive regression detector
  in the system; expected noise is tiny (pinned server, no merges), so a +-3% band is
  defensible.
- **CoV (stddev/median, trailing 20 runs)** — measures the *instrument's* stability;
  Tier 0 CoV must be well under Tier 1's, else the setup is broken.
- **Time decomposition** serialize : other-CPU : wire-wait as a stacked share — over
  months, shows where the connector spends its life and where optimization pays.

### Tier 1 — cost-per-row and interaction quality, not raw speed

| Metric | Unit | Source | What it tells us |
|---|---|---|---|
| `throughput_rows_per_sec` [E], now **verified** | rows/s | event log + count() check | Trend/comparability headline — trusted only after integrity passes |
| `rows_delivered` vs `rows_expected`, `duplicate_rows` [N] | count | target count(), uniqExact(WatchID) | Correctness under retries; converts throughput into a verified number |
| `parts_per_insert` [E] | ratio | part_log | Batching quality as the server experiences it — #1 server-cost signal |
| `merge_amplification` [E] | x | part_log | Downstream work our write pattern creates (today ~1.05x — guard it) |
| `ch_insert_cpu_seconds_per_Mrows` [N, derived from E] | s/M rows | query_log | Server-side compute cost of our inserts; moves when we send worse-batched/sorted data |
| `inserts_delayed_fraction` [E], `ch_memory_limit_errors` [E] | %, count | query_log/part_log | Pushback pressure induced at the fixed operating point |
| `merge_pool_peak_pct` [E], `ch_parts_active_peak` [E] | %, count | metric_log | Distance from the throttling cliff (parts_to_delay_insert = 150) |
| `settle_seconds` [E] + `settle_timed_out` [N] | s, flag | workflow | Merge debt left behind — the hidden half of "fast" ingest |
| `ch_uptime`, `pre_run_rss`, `pre_run_active_parts` [N] | s, bytes, count | asynchronous_metrics, pre-run | Covariates: turns restart-class step-changes into filterable facts |
| `run_cost_usd` [N] | $ | instance-hours x price | Makes cadence/tier decisions data-driven |
| per-partition `parts_per_insert` [N] | ratio | part_log by partition | First visibility into partition-aware write distribution (post-toYYYYMM) |
| `ingest_rate_stability` [N] | CoV | per-minute insert rate from perf.ch_inserts (bucketed sum of written_rows) | Plateau vs sawtooth — throttling-stall patterns invisible in the run average. Zero new capture; derived from existing per-insert rows |
| `ch_dedup_dropped_blocks` [N] | count | ProfileEvents (deduplicated inserted blocks) | Whether the server absorbed retried batches as duplicates-dropped — direct context for the integrity check when Spark task retries re-send committed work |

Statistics / learnings:
- **H/P ratio** on parts_per_insert, merge_amplification, verified rows/s — the Q-B gate.
- **Pinned-arm absolute trend** = environment monitor (Cloud upgrades/restarts/state,
  quantified for free).
- **Throughput vs ch_uptime regression** — quantifies the restart effect and its decay
  half-life once, so future step-changes are corrected for, not investigated.
- **Deviation bands** (median +- 2*MAD, trailing 20 runs) per gated metric — "a
  regression" stops being eyeball judgment.

---

## 6. Trust & hygiene (what makes the numbers believable)

1. **Integrity verification every run**: post-settle `count()` + `uniqExact(WatchID)`
   vs source; recorded as metrics; mismatch fails the run. No unverified throughput.
   (Motivation: the historical batch sweep showed write_rows of 89.6M-96.9M for a
   fixed ~100M-row dataset — either loss or event-log miscounting; both unacceptable
   to leave unmeasured.) A third check — an order-independent **content checksum**
   (`sum(cityHash64(...))` over high-entropy columns, target vs source) — closes the
   blind spot where an equal number of rows lost and duplicated on non-unique
   WatchIDs leaves both totals unchanged, and also catches a mangled non-key column.
   It is captured every run and folds into `integrity_ok` once the source constant
   (`SOURCE_CONTENT_CHECKSUM`) is measured on a clean load and pinned (staged so a
   representation mismatch can't fail runs before it is validated).
   **Implemented-reality note (2026-07-22):** the checksum is computed today but the
   gate is **DORMANT** — `SOURCE_CONTENT_CHECKSUM` is unset, so
   `content_checksum_expected <= 0` and the checksum clause is a no-op. Only the
   `count()` + `uniqExact(WatchID)` equalities gate today; the checksum stays
   captured-but-non-gating until the constant is measured and pinned (contract §3).
2. **Deviation bands + alerting**: band excursions and integrity failures file an
   alert (GitHub issue or Slack) with the pair link. The benchmark stops relying on a
   human looking at charts.
3. **Covariates captured pre-run**: server uptime, RSS, active parts,
   clickhouse_version — every known noise source becomes filterable.
4. **Event-log accounting fixed**: `write_rows` summed over successful task attempts
   only (failed/retried attempts currently pollute the sums); `settle_timed_out` flag
   recorded (settle currently right-censors silently at the 1800s timeout).
5. **Failed-run telemetry**: capture server-side metrics even when the Spark step
   fails (today capture gates on submit success — survivorship bias; the most
   informative runs are the least observed).
6. **Cost per run** recorded.
7. **Warm-up**: implement it (small priming insert before truncate) or delete the
   claim from the report. No documented procedures that don't exist in code.
8. **Region co-location** confirmed once, recorded.
9. **Config in repo**: batchSize / async / parallelism / partition scheme all set in
   version control and echoed into `runtime`.
10. **Validity guards (flagged-not-failed semantics)**: some conditions do not fail a
    run and are not regressions — they make the run non-comparable, and it must be
    FLAGGED so it is excluded from bands/ratios by default: `task_failed_count > 0`
    (retried work pollutes throughput), `settle_timed_out`, integrity mismatch
    already fails outright. Flagged runs still capture and export fully.
    **Implemented-reality note (2026-07-22):** of the shared `flag_reason`
    vocabulary (contract §1.3), the Spark pipeline (run-arm) EMITS only
    `task_retries` (`task_failed_count > 0`) and `settle_timeout`
    (`settle_timed_out`). `rebalance`/`drain_incomplete` are Kafka-only;
    `task_restart` and `instrument_shift` are unimplemented on Spark;
    `integrity_unverified` is a STATE handled via `outcome='failed'`, not an
    emitted token; `instrument_resize` is DERIVED in the SQL layer from the
    `emr_*` instrument-truth keys (contract §1.3, `v_instrument_annotations.sql`);
    `duplicate_rows` is a diagnostic readout equal to the volume check, not a gate.

---

## 7. Dashboards — exact layout

One Superset dashboard, 4 tabs. Tab 1 is the daily answer; today's 26 charts survive
as Tab 2.

### Data foundation (3 virtual datasets, no DDL)

| Dataset | SQL sketch | Feeds |
|---|---|---|
| `v_runs_enriched` | runs + unnested runtime map (arm, tier, pair_id, config...) joined to pivoted metrics; missing arm -> 'head', missing tier -> '1' (legacy rows stay first-class) | everything |
| `v_pair_ratios` | self-join on runtime['pair_id']: head.value / pinned.value AS ratio per (pair, tier, metric) | Tab 1 |
| `v_env_annotations` | change detection over clickhouse_version + ch_uptime < prev -> "upgrade" / "restart" events; pin bump = change in pinned arm's connector_version | annotation layers, Tabs 1-3 |

Global filter bar (all tabs): Date range, Run profile, Config (runtime key/value,
multi-select), Tier. `Arm` is scoped per-tab (Tab 2 fixed to head, Tab 3 to pinned).

### Tab 1 — REGRESSION (default tab: "did we break it last night?")

```
+------------+------------+------------+------------+--------------+
| TIER 0     | TIER 1     | INTEGRITY  | PAIRS IN   | LAST PIN     |
| verdict    | verdict    | rows OK    | BAND (20)  | BUMP         |
|  OK +0.8%  |  OK -1.2%  | OK 0 dup   |  19 / 20   | v0.9.0 06-02 |
+------------+------------+------------+------------+--------------+
| LATEST PAIR - gated metrics table                                |
| tier metric                 HEAD    pinned   d%     band verdict |
| T0   null_rows_per_sec      412K    409K    +0.8%   +-3%   OK    |
| T0   cpu_s_per_Mrows        41.2    41.5    -0.7%   +-3%   OK    |
| T0   serialize_s_per_Mrows  12.1    12.0    +0.9%   +-3%   OK    |
| T1   rows_per_sec(verified) 236K    239K    -1.2%   +-5%   OK    |
| T1   parts_per_insert       1.04    1.03    +1.1%   +-5%   OK    |
| T1   merge_amplification    1.05    1.05     0.0%   +-5%   OK    |
+--------------------------------+---------------------------------+
| RATIO TREND - Tier 0 (line)    | RATIO TREND - Tier 1 (line)     |
| y: H/P, 3 series, ref line 1.0 | y: H/P, 3 series, ref line 1.0  |
| shaded band +-3%, annotations: | shaded band +-5%, annotations:  |
| pin bumps (vertical lines)     | pin bumps + config changes      |
+--------------------------------+---------------------------------+
| EXCURSION LOG (table): date, tier, metric, d%, linked pair       |
+------------------------------------------------------------------+
```

7 charts: 5 big-number-with-trendline tiles, latest-pair table, 2 mixed-series line
charts (band as annotation interval, pin bumps as event annotations), excursion table
(empty = good). Gated set: T0 -> null_rows_per_sec, cpu_per_Mrows, serialize_per_Mrows;
T1 -> parts_per_insert, merge_amplification, verified rows/s.
Validity guards (hygiene item 10): runs with task retries or settle timeout appear in
the excursion log as FLAGGED (non-comparable), not as regressions, and are excluded
from bands/ratios by default.

### Tab 2 — PERFORMANCE (absolute history, arm=head; today's dashboard reorganized)

```
+-- HEADLINES ----------------------------------------------------+
| [BigNum] med rows/s (30d)  [BigNum] med e2e  [BigNum] cost/run  |
+-- TIER 0 . CONNECTOR (new row) ---------------------------------+
| null_rows_per_sec trend     | cpu & serialize s/Mrows trend     |
| (line, per run)             | (line, 2 series)                  |
| TIME DECOMPOSITION (stacked area, %): serialize / other-CPU /   |
| wire-wait share per run  <- "where does the connector spend life"|
+-- TIER 1 . THROUGHPUT & COST-PER-ROW ---------------------------+
| verified rows/s trend       | parts_per_insert + merge_amp      |
| (line + integrity markers)  | (line, dual axis)                 |
+-- TIER 1 . SERVER INTERACTION (existing charts, kept) ----------+
| throttling/delayed, parts-vs-threshold, merge-pool %, memory vs |
| 12.88 cap, CH cpu/network, batch distributions   (~12 charts)   |
+-- SPARK-SIDE (existing, kept) ----------------------------------+
| executor CPU, GC, peak JVM, task counts, time split (~5 charts) |
+-----------------------------------------------------------------+
```

New: 3 Tier-0 charts + decomposition area + integrity markers on the throughput trend
(points colored by rows_verified). Everything else is the existing 26 regrouped under
labeled rows. X-axes switch from time-of-day to dates (existing backlog item).

### Tab 3 — ENVIRONMENT (instrument health, arm=pinned)

```
+-----------------------------------------------------------------+
| PINNED-ARM rows/s trend (line) - connector constant, so every   |
| move = environment. Annotations: CH upgrades, restarts          |
+-----------------------------+-----------------------------------+
| CH VERSION TIMELINE         | THROUGHPUT vs UPTIME (scatter +   |
| (bar per version epoch)     | trend) - quantifies restart       |
|                             | effect and its decay              |
+-----------------------------+-----------------------------------+
| PRE-RUN STATE trends: RSS + | NOISE GAUGE: CoV (stddev/median,  |
| active parts at run start   | trailing 20) per tier - T0 must   |
| (line, 2 series)            | be << T1 or instrument is broken  |
+-----------------------------+-----------------------------------+
```

5 charts. A restart-class investigation becomes: pinned line moved + restart
annotation on the same date = case closed.

### Tab 4 — RUN DRILL (one pair under the microscope)

```
+ [Filter: pair_id] ----------------------------------------------+
| [BigNum] rows verified  [BigNum] e2e  [BigNum] dup=0 [BigNum] $ |
+-----------------------------------------------------------------+
| ARM COMPARISON (table): metric | HEAD | pinned | d% - all ~58   |
+-----------------------------+-----------------------------------+
| throughput over time,       | per-insert latency sequence,      |
| 2 series (H vs P overlaid)  | 2 series (H vs P)                 |
+-----------------------------+-----------------------------------+
| batch-size & latency        | covariates panel (table): uptime, |
| distributions (existing)    | RSS, parts, CH ver, config        |
+-----------------------------+-----------------------------------+
```

Existing drill charts made dual-series on arm, plus arm-comparison table and
covariates panel.

Totals: ~24 kept + ~17 new ~= 41 charts, 3 virtual datasets, 2 annotation layers.
Tab 1's tiles share their SQL with the alert queries, so dashboard and alerting
cannot drift apart.

Cross-connector comparison (Spark vs Kafka Connect on the shared target/dataset)
lives on the **Kafka dashboard's Tab 5** (sibling plan), reading both connectors'
rows from the shared tables — this dashboard does not duplicate it.

Band widths: start at +-3% (Tier 0) / +-5% (Tier 1); recalibrate from the first ~20
pairs' measured CoV. Excursion log tracks **ratio** band-exits only — absolutes are
context, the ratio is the contract.

---

## 8. Schema impact

**None required — by design.** The tall/narrow `perf.metrics` + `runs.runtime` map
absorbs everything:

- `arm` / `tier` / `pair_id` and all config -> new `runtime` map keys.
- ~15-20 new metric names -> rows in `perf.metrics`.
- Pairs -> two ordinary `runs` rows sharing `pair_id`.
- Integrity verdicts -> metrics with value 1/0.
- Ratios and annotations -> Superset virtual datasets, computed at query time.
- DWH: ClickPipe mirrors the tables; new map keys and metric names flow with zero
  pipeline changes.
- Legacy: dashboards coalesce missing arm -> 'head', missing tier -> '1'; no old row
  is touched.
- Escape hatch if map-key extraction ever gets slow (it won't at this volume):
  `ALTER TABLE runs ADD COLUMN arm String MATERIALIZED runtime['arm']` — additive,
  non-breaking.
- The only forced change is operational: Tier 0 server-side capture ordering vs EMR
  teardown (section 3, Tier 0 note).

---

## 9. Migration path (nothing breaks)

| Step | What | Exit criterion | Risk to history |
|------|------|----------------|-----------------|
| 1 | Hygiene items (integrity check, covariates, config into repo, event-log fix, settle flag) | integrity + covariates present on a green nightly run | none — additive |
| 2 | Add arm P to the nightly workflow; build v_pair_ratios + Tab 1 + Tab 3 | first pair visible on Tab 1 with a computed ratio | none — H arm identical to today |
| 3 | Add Tier 0 (`ENGINE=Null` table on the same Cloud target as Tier 1) | Tier 0 CoV (trailing runs) measurably below Tier 1 CoV — the instrument is quieter than the thing it de-noises | none — separate metrics |
| 4 | toYYYYMM partition switch | per-partition parts_per_insert captured; annotation on all absolute charts | annotated: changes Tier 1 absolutes; ratio view (step 2) is immune — which is why step 2 comes first |
| 5 | Parallelism x batch grid campaign (one-time) -> possibly new operating config | sweep report with >=5 interleaved runs per cell; operating config re-frozen | annotated config change |
| 6 | Weekly alternate dataset (PyPI sample) | first weekly run lands with shape tag in runtime map | additive |
| — | Deferred: Tier 2 (overload/robustness), Tier 3 (right-sized ceiling) | — | see Appendix A |

---

## 10. Open decisions

1. **Pinned reference**: last tagged release vs a frozen "golden" sha. (Lean: release;
   every bump is an annotated event.)
2. **Gate placement**: dashboard/alert only, or additionally a CI check on connector
   PRs (Tier 0 is cheap enough to run per-PR eventually).
3. **Alert channel**: GitHub issue vs Slack.
4. ~~**Tier 0 capture mode**~~ — RESOLVED by the 2026-07-07 redesign: Tier 0 runs on
   the Cloud target, so it reuses Tier 1's post-ingest capture path (there is no
   Docker-on-master teardown to race).
5. **PyPI sample size** for the weekly shape run (suggest ~200M rows to keep runtime
   comparable to hits).
6. **Band recalibration policy**: who/when adjusts +-3%/+-5% after the first ~20 pairs.

---

## Appendix A — Deferred tiers (design retained for later)

### Tier 2 — Overload / robustness (weekly; pass/fail, not throughput)

Deliberate overload (the current 128-core stress profile, on purpose). Pass/fail
semantics: the run must complete via retries/backpressure with zero lost and zero
duplicated rows (count + uniqExact). Metrics: run_verdict, rows_lost/duplicated,
task_retries + retry_success_rate, failure-mode split (241 / 252 / connection-reset /
socket-timeout), time_to_first_failure, longest_stall_seconds (hang detection),
recovery_slope after throttling episodes, throughput_under_pressure (context only).
Learnings: a reliability SLO (pass-rate over time), failure-mode drift across
versions, retroactive error bar on Tier 1 rows/s from duplicate findings, and a
permanent guard for the socket-timeout/retry fix. Requires capture-on-failure
(hygiene item 5) as a prerequisite.

### Tier 3 — Right-sized ceiling (~30 vCPU Cloud target, monthly, torn down after)

Real MergeTree storage with server headroom — the connector's true ceiling against a
real engine. Metrics: rightsized_rows_per_sec, ceiling_gap_null (T0/T3),
ceiling_gap_saturated (T3/T1), parts_per_insert and merge_amplification at headroom,
scaling_efficiency vs write cores, server_cpu_per_Mrows at headroom (cleanest
cross-connector comparison number for the Kafka test).
Learnings: the three-point profile (Null -> right-sized -> saturated) settles any
ambiguous regression (moved at all three = client change; only at Tier 1 =
server-interaction change; only at Tier 3 = storage-path interaction). The
parallelism-sweep knee at headroom calibrates the "half the server's cores"
recommendation. Deferred until cost is justified or the first ambiguous regression
forces the question.

---

## Appendix B — References

- Sibling plan: `clickhouse-kafka-connect/docs/benchmark-v2-plan.md` — the Kafka
  Connect sink benchmark on the same v2 architecture (two-arm, Tier 0/1), sharing
  the perf.* landing and the DWH pipeline, running against its own dedicated Cloud
  target (same spec), and hosting the cross-connector dashboard tab.
- ClickHouse blog, "Supercharge your ClickHouse data loads", parts 1-3:
  - part 1: insert mechanics (blocks, parts, merges), tuning knobs
    (max_insert_threads, min_insert_block_size_rows/bytes), hardware factors.
  - part 2: PyPI 65B-row load on 3x (59 vCPU / 236 GiB) Cloud nodes; joint sweep of
    insert threads x block size (optimum 32 threads x 10M rows = 3x over defaults);
    trade-off triangle speed/parts/memory; memory formula
    peak ~= 3 x threads x block_bytes; guideline: insert parallelism ~= half the
    server's cores (rest for merges).
  - part 3: large-scale resilience — staging-table pattern, atomic part moves,
    at-least-once + retry safety, linear scaling with workers/servers.
- Current benchmark: `.github/workflows/clickbench-load-test.yml`,
  `benchmarks/scripts/*`, `benchmarks/sql/perf/*.sql`.
- Load-test report (Spark Part I + Kafka Part II): Notion, "ClickHouse Connector
  Load-Test / Benchmark Report".
