# Benchmark v2 — Naming & Semantics Contract

**Status:** NORMATIVE (v1.0). This document pins the shared vocabulary and
semantics of the Benchmark v2 data model. It uses RFC-2119 keywords: **MUST**,
**MUST NOT**, **SHOULD**, **MAY** carry their usual weight.

**Owner / source of truth:** `spark-clickhouse-connector`.

**Consuming repositories (identical copy):**
- `spark-clickhouse-connector` — the owner. Changes land in
  `spark-clickhouse-connector` first.
- `clickhouse-kafka-connect` — the consumer. Carries a byte-identical copy of
  this document and of the `perf.*` DDL (see §5).

**Change-control rule:**
1. Any change to this contract or to the `perf.*` DDL **MUST** land in the Spark
   repo first (PR against `spark-clickhouse-connector`).
2. The Kafka side (kafka-manager) **MUST** ack the change before it is copied.
3. The identical document / DDL is then synced into `clickhouse-kafka-connect`.
   A CI sync check (§5) diffs the vendored copy against the canonical files in
   `spark-clickhouse-connector` and fails on drift.

The two benchmarks are the **Spark connector** ingest benchmark
(`spark-clickhouse-connector`) and
the **Kafka Connect sink** benchmark (`clickhouse-kafka-connect`). They share the
`perf.*` landing tables and the DWH pipeline, but run against **different**
dedicated ClickHouse Cloud targets. That difference is the reason several rules
below (§1 scope keys, §4 annotation scoping) exist: an unscoped shared artifact
would paint one benchmark's environment events onto the other's charts.

> **Note on `pre_run_active_parts`:** this metric is a *cleanliness verifier*,
> expected to be ~0 by design (each run ends with a `TRUNCATE`, so the next run
> starts empty). A non-zero value means the previous run's data or merge debt
> survived (e.g. `SKIP_TRUNCATE` or a failed end-of-run truncate) and the run is
> suspect. It is **not** a variance explainer like `ch_uptime` / `pre_run_rss`;
> do not use it to "explain" throughput drift.

---

## 1. `perf.runs.runtime` map keys

`perf.runs.runtime` is `Map(String, String)`. All v2 tagging rides in this map;
**no schema change** is required or permitted for tagging (plan §8). The keys
below are the pinned contract. Both pipelines **MUST** spell them identically.

### 1.1 Scope & identity keys (both pipelines, mandatory unless noted)

| Key | Values | Absent ⇒ | Meaning |
|-----|--------|----------|---------|
| `arm` | `'head'` \| `'pinned'` | `'head'` | Which build produced the run: connector built from HEAD, or the pinned reference build. Legacy rows have no `arm` and are therefore `head` — the absolute trend line is never broken. |
| `tier` | `'0'` \| `'1'` | `'1'` | Benchmark tier, **as a string**. `'0'` = Null-engine connector ceiling; `'1'` = saturated Cloud ingest. Legacy rows have no `tier` and are therefore Tier 1. |
| `pair_id` | see §1.2 | — | Shared by the two runs of one nightly two-arm pair. |
| `target_region` | e.g. `'us-east-1'` | — (MANDATORY) | Cloud region of the run's target service. **MUST** be set from repo config, never hardcoded per run. |
| `compute_region` | e.g. `'us-east-1'` | — (MANDATORY) | Cloud region of the load generator (EMR / EKS). When it differs from `target_region` the connector→server path is cross-region — a structural bias that cross-connector comparisons **MUST** be able to compute. (Amendment 2026-07-07.) |
| `environment_class` | `'staging'` \| `'production'` \| `'self_hosted'` | — (MANDATORY) | Deployment class of the target service. Tier-0 rows whose Null target is **Cloud-hosted** keep the target service's own class and region — this is now the case for **BOTH** pipelines: Spark's Tier 0 runs `clickbench.hits_null` (`ENGINE=Null`) on its production Cloud target (`'production'` / `us-east-2`; Amendment 2026-07-07c — was Docker-on-master), and Kafka's Null table on its staging target (`'staging'`, per that plan's decision 9). `'self_hosted'` (Amendment 2026-07-07, scoped 2026-07-07b) is the value for Tier-0 rows whose instrument is **load-generator-local** (a pinned ClickHouse riding the load generator, `target_region` = compute region); it remains a **defined-but-currently-unused** branch (no pipeline emits it as of 2026-07-07c) — see the `tier0_ch_version` scoping note. |

`target_region`, `compute_region`, and `environment_class` are **MANDATORY on
every `perf.runs` row in both pipelines**, from day one.

- **Spark pipeline:** `environment_class` = `'production'` (its dedicated target
  is a production service). `target_region` = `'us-east-2'` (confirmed 2026-07-07),
  `compute_region` = `'us-east-1'` (EMR) — the Spark connector→server path is
  **cross-region**; both values are set from repo config — **MUST NOT** be
  hardcoded per run. **Both tiers** carry these same values (Amendment 2026-07-07c):
  Spark's Tier 0 now runs a Cloud-hosted `ENGINE=Null` table (`clickbench.hits_null`)
  on the SAME production Cloud target, so its t0 rows **inherit** `environment_class`
  = `'production'` and `target_region` = `'us-east-2'`, carry **no**
  `tier0_ch_version`, and MUST emit the mandatory `ch_insert_cpu_share_tier0`
  parse-watch (see §1.4 and §2.1).
- **Kafka pipeline:** its dedicated target is a **us-east-2 staging** service, so
  `environment_class` = `'staging'`, `target_region` = `'us-east-2'`, and
  `compute_region` per its own infrastructure (in-region when compute is also
  us-east-2).

Why this matters: the H/P ratio gates are unaffected by the class/region split
(both arms of a pair share the same target, so the difference cancels), but
**absolute cross-connector comparisons** (Spark vs Kafka rows/s, server cost per
row) are comparing across a production/staging boundary. That boundary MUST be a
recorded, filterable fact from the first run, not reconstructed later — hence
these keys are mandatory (see also §4 and §6).

### 1.2 `pair_id` format (PINNED)

`pair_id` **MUST** equal the `RUN_ID` of the pair, where `RUN_ID` is generated by
`benchmarks/scripts/lib_runid.sh` as:

```
RUN_ID = <UTC timestamp>-<short git sha>
       = $(date -u +%Y-%m-%dT%H-%M-%SZ)-$(git rev-parse --short HEAD)
example: 2026-07-07T04-15-32Z-91ac2dd
```

`lib_runid.sh` falls back to the literal `nogit` when `git rev-parse` fails.
Pipelines **MUST** fail the run rather than emit a `…-nogit` `pair_id`/`run_id`:
an identifier that does not pin the commit under test is useless as a pair key.

**Rationale for reusing `RUN_ID` as `pair_id`:**
- It is already the canonical run identifier in the pipeline (`lib_runid.sh`), so
  no new generator, no new collision domain, no new failure mode.
- It is unique per nightly invocation (UTC second + HEAD sha), which is exactly
  the granularity of a two-arm pair (both arms run in the same invocation, same
  hour, same cluster).
- The embedded date makes pairs human-sortable and greppable on the dashboard.
- The embedded sha ties the pair to the HEAD commit under test — the arm=head
  build. (The arm=pinned build's own connector version is recorded separately in
  `connector_version`; see §4 pin-bump detection.)

**`run_id` per (arm, tier) row (PINNED):** each `perf.runs` row is ONE
(arm, tier) combination. There is exactly **one `perf.runs` row per (arm, tier)**.
Once both tiers exist that is **4 rows per night** — `(head,0)`, `(pinned,0)`,
`(head,1)`, `(pinned,1)` — **all four sharing the same `pair_id`**. Each row's
`run_id` **MUST** be distinct; the recommended form is the shared `pair_id`
suffixed with the arm and tier, e.g.:

```
run_id = <pair_id>-<arm>-t<tier>
       = 2026-07-07T04-15-32Z-91ac2dd-head-t1
```

`run_id` is the primary key of a `perf.runs` row and the join key into
`perf.metrics.run_id`; `pair_id` is the *grouping* key across the pair. They are
different keys and **MUST NOT** be conflated.

> **Dataset-integrity rule (MUST):** `perf.metrics` rows carry **no** runtime map
> — they inherit their (arm, tier) tagging solely through their `run_id`'s join
> to the owning `perf.runs` row. Therefore every metric row **MUST** be attached
> to the `run_id` of the correct (arm, tier) run. Tagging a Tier 0 metric onto a
> Tier 1 run row (or a head metric onto a pinned row) is **PROHIBITED**: it
> collapses the tier/arm separation that the entire two-arm, two-tier design
> rests on, silently mixing a client-ceiling number into a server-cost series.
> One (arm, tier) run row owns exactly its own tier's, its own arm's metrics.

### 1.3 Verdict / flag keys (both pipelines)

| Key | Values | Meaning |
|-----|--------|---------|
| `outcome` | `'success'` \| `'failed'` | Outcome class of the run (Amendment 2026-07-07). `'failed'` = the ingest step failed; the run is still **fully captured and exported, marked** (plan §6.5 — no survivorship bias). Absent ⇒ `'success'` (legacy rows only ever landed on the success path). Dashboards **MUST** exclude failed-class runs from headlines/bands/ratios **by outcome, not by absence**. Orthogonal to `flagged` (a failed run is not merely non-comparable; a flagged run is not failed). Integrity on a failed-class run is `integrity_unverified` by definition. |
| `flagged` | `'1'` | Present with value `'1'` iff the run tripped a validity guard (plan §6.10). Absent ⇒ not flagged. |
| `flag_reason` | fixed vocabulary (below) | Token — or `\|`-joined tokens when several guards trip — naming the guard(s). Present iff `flagged='1'`. |

**`flag_reason` FIXED token vocabulary (PINNED — MUST NOT extend without a
contract change):**

| Token | Trips when |
|-------|-----------|
| `task_retries` | `task_failed_count > 0` — retried work pollutes throughput/volume accounting (plan §6.4, §6.10). |
| `settle_timeout` | `settle_timed_out = 1` — settle right-censored at the 1800s cap; `settle_seconds` is a floor, not the true value. |
| `rebalance` | (Kafka) a consumer-group rebalance occurred mid-run, making the drain non-comparable. |
| `task_restart` | a worker/task process restarted mid-run (Spark executor loss / Kafka worker restart). |
| `drain_incomplete` | (Kafka) the sink did not fully drain the topic within the run window. |
| `integrity_unverified` | integrity verification could not be computed (capture failed / source glob unavailable) — distinct from an integrity **mismatch**, which FAILS the run (§3). |
| `instrument_resize` | (Amendment 2026-07-09c) the measurement harness's **compute resources changed** (node/instance type, worker pod CPU/memory requests or limits). Runs so flagged are **excluded from band calibration and trend baselines**; band calibration **restarts at the first pair on the new instrument**. |
| `instrument_shift` | (Amendment 2026-07-09d) an **instrument regime shift observed without a config change**: the harness-saturation guard dropped SYMMETRICALLY on both arms (e.g. Kafka `kafka_worker_cpu_share` < ~0.85 on both) — the pair is non-comparable and investigated. Distinct from `instrument_resize` (deliberate resource change) and from an ASYMMETRIC departure, which is a real connector signal and MUST NOT be flagged. |

A run MAY trip more than one guard; when it does, `flag_reason` **MUST** list the
tokens separated by a single `|` (pipe), e.g. `task_retries|settle_timeout`. No
other separator is permitted (commas would collide with map-value parsing).

**Instrument-truth runtime keys (Amendment 2026-07-09c):** each pipeline MUST
record the deployed compute truth of its harness as connector-namespaced runtime
keys (§1.4 namespacing), so `instrument_resize` is detectable from data, not
memory — e.g. Kafka: `kafka_compute_instance_type`, `kafka_worker_cpu_limit`,
`kafka_worker_mem_limit`; Spark: the existing `emr_*` keys serve this role. A
change in any instrument-truth key between consecutive pairs of the same
connector is the trigger for the `instrument_resize` flag on the first affected
run and an environment annotation.

**Kafka instrument definition (NORMATIVE — principal accept-and-document ruling,
2026-07-09d):** the Connect worker is deliberately CPU-quota-bounded at 3.5
cores; `kafka_worker_cpu_share` expected ~0.9–1.0; instrument acceptance =
resources recorded + drain-rate stability good + throttling symmetric across
arms (2026-07-09 live evidence: pinned-t1 95%/94%, head-t0 92%/91%, head-t1
96%/94%, cores-of-limit/periods-throttled). Ruling conditions:

- **(a) Quota + node type are INSTRUMENT IDENTITY.** Any change to either is an
  annotated instrument event and restarts band calibration (see
  `instrument_resize`).
- **(b) The cpu_share guard separates signal from regime shift.** ASYMMETRIC
  departure (one arm wait-bound while the other saturates) is a **real connector
  signal** — the ratio gate speaks; it MUST NOT be flagged away. A SYMMETRIC
  drop below ~0.85 on BOTH arms is an **instrument regime shift**: the pair is
  flagged `instrument_shift` and investigated.
- **(c) Documented blind spot:** parallel-CPU-scaling changes are clipped by the
  quota and invisible to the nightly; they are assessed only by the parallelism
  sweep campaign (kafka #37 / spark #22). A deliberate quota raise is an
  instrument version bump, not a fix to this blind spot.

**Charting hold (same ruling):** `drain_rate_stability` MUST NOT be charted
until the commit-cadence aliasing refinement lands — dashboards exclude it until
this sentence is amended away.

### 1.4 Config keys (shared concepts share names)

Config under test is version-controlled and echoed into `runtime` (plan §4, §6.9).
Concepts common to both connectors **MUST** use the **same** key name so a single
dashboard filter spans both pipelines. Connector-specific keys are allowed and
**MUST** be prefixed with the connector name to avoid a false shared filter.

**Shared config keys (PINNED):**

| Key | Meaning | Spark source | Kafka analogue |
|-----|---------|--------------|----------------|
| `batch_size` | rows per insert batch (the "batch-size-like" knob) | connector `batchSize` | sink flush size |
| `write_parallelism` | concurrent insert streams to the target (the "parallelism-like" knob) | Spark write parallelism | Kafka `tasks.max` / consumer parallelism |
| `async_insert` | `'0'` \| `'1'` — server async_insert mode | ingest setting | ingest setting |
| `partition_scheme` | target table partitioning under test (e.g. `'toYYYYMM(EventDate)'` or `'none'`) | DDL | DDL |
| `dataset` | logical dataset shape (e.g. `'hits'`, `'pypi'`) | workload config | workload config |
| `warm_up` | OPTIONAL — present iff a priming insert ran before the arm's truncate; value identifies the warm-up workload (e.g. `'hits_0'`). Absent ⇒ no warm-up. (Amendment 2026-07-07.) | warm-up step | warm-up step |
| `tier0_ch_version` | MANDATORY on `tier='0'` rows **whose instrument is load-generator-local** (pinned version, e.g. `'25.8.28'`; a bump is an annotatable environment event), absent otherwise. Tier-0 rows on a **Cloud-hosted** Null target have no pinned instrument version by design (the Cloud version drifts): they omit this key, rely on the `clickhouse_version` covariate, and the `ch_insert_cpu_share_tier0` parse-watch metric is MANDATORY for them instead. As of Amendment 2026-07-07c **BOTH** pipelines use Cloud-hosted Null targets, so **neither emits `tier0_ch_version`** — it is reserved for the defined-but-currently-unused `self_hosted` branch. (Amendment 2026-07-07, scoped 2026-07-07b, both-Cloud 2026-07-07c.) | omitted (Cloud-hosted Null): clickhouse_version covariate + parse-watch | clickhouse_version covariate + parse-watch (Cloud-hosted Null) |

**Connector-specific keys (allowed, MUST be namespaced):** e.g.
`spark_version`, `scala_version`, `emr_release` (Spark); `kafka_connect_version`,
`consumer_max_poll_records` (Kafka). These do not need a cross-connector meaning
and **MUST NOT** reuse a shared key name for a connector-private concept.

---

## 2. `perf.metrics` — shared metric names + units

`perf.metrics` is tall/narrow: `(run_id, metric_name, unit, value, recorded_at)`.
`metric_name` is `LowCardinality(String)`; **spelling is the contract** — a typo
is a different series. The names below **MUST** be spelled identically in both
pipelines. Units are the string written to the `unit` column.

> **Naming discrepancy notice (READ THIS):** several names in this table differ
> from the plan §5 catalog *and* from what the Spark capture SQL emits on disk
> today (see §7). This contract picks ONE spelling per metric; where the current
> Spark SQL emits a different name, §7 lists the required rename. The pinned name
> is the one in this table.

### 2.1 Cross-connector shared metrics (identical spelling both sides)

| Metric name | Unit | Definition |
|-------------|------|------------|
| `rows_delivered` | `count` | Post-settle target `count()` of the ingested table. Ground-truth delivered rows. |
| `rows_expected` | `count` | `count()` of the source dataset. For the default full glob this MUST come from a constant pinned in the consuming repo; per-run re-derivation from the input glob is allowed only for non-default (smoke/override) globs. |
| `unique_delivered` | `count` | Post-settle target `uniqExact(<row id>)` of the ingested table. Spark uses `WatchID`. |
| `unique_expected` | `count` | `uniqExact(<row id>)` of the source dataset (same constant-vs-derived rule as `rows_expected`). |
| `duplicate_rows` | `count` | `rows_delivered − rows_expected` (target-vs-source delta). `>0` ⇒ the same logical row landed more than once (retry re-sent committed work, server did not dedup). Integrity additionally requires `unique_delivered == unique_expected`. MUST NOT be computed as `count() − uniqExact()` on the target alone: sources may legitimately contain duplicate row-ids (ClickBench `hits` has duplicate `WatchID`s), so that formula false-positives every run. |
| `parts_per_insert` | `ratio` | Parts created (`part_log` NewPart) ÷ insert count over the run window. `1.0` = one part per insert; `>1.0` = batches sprayed across partitions. #1 server-cost signal. |
| `merge_amplification` | `ratio` | Total bytes merges read ÷ total bytes inserted, over the settle window. Healthy ~1–2x; tiny batches push it to 10x+. (Erratum 2026-07-07: unit was `x`; pinned to `ratio` to match the emitting pipelines and the sibling ratio metrics.) |
| `ch_insert_cpu_seconds_per_Mrows` | `s/Mrows` | Server-side insert CPU (`query_log` OSCPUVirtualTime) ÷ (delivered rows / 1e6). The cross-connector **Tab 5** server-cost headline. **MUST** be spelled `ch_insert_cpu_seconds_per_Mrows` — **NOT** `server_cpu_per_Mrows`. |
| `ch_avg_rows_per_insert` | `rows` | Mean `written_rows` per real write (mode-agnostic: Insert when sync, AsyncInsertFlush when async). Batching quality as the server sees it. |
| `inserts_delayed_fraction` | `ratio` | Fraction of inserts that experienced any artificial delay (`ProfileEvents['DelayedInserts'] > 0`). Pushback pressure at the operating point. Range 0..1. |
| `merge_pool_peak_pct` | `percent` | Peak of the per-sample ratio background-merge-pool-tasks / pool-size, ×100. `100` = merger is the bottleneck. Peak-of-ratio (robust to mid-run pool resize). |
| `ch_parts_active_peak` | `count` | Peak active part count on the target during the window. Distance from the `parts_to_delay_insert` (150) throttling cliff. |
| `ch_memory_limit_errors` | `count` | Insert/flush queries that failed with MEMORY_LIMIT_EXCEEDED (241) or CANNOT_READ_ALL_DATA (33) over the run window, scoped to the target table. |
| `ch_dedup_dropped_blocks` | `count` | `DuplicatedInsertedBlocks` ProfileEvent delta over the window (max−min). Retried batches the server absorbed as duplicate-drops — the benign counterpart to `duplicate_rows`. |
| `bytes_on_wire_per_row` | `bytes/row` | Server `NetworkReceiveBytes` (post-compression ingress on Insert receipts) ÷ delivered rows. Wire efficiency; catches compression/encoding regressions. |
| `ch_insert_cpu_share_tier0` | `percent` | (Tier 0 only) Null-target `query_log` insert CPU ÷ wall-clock, ×100 (read against whatever hosts the Null table — a Cloud-hosted target for both pipelines as of 2026-07-07c). Instrument-health watch: if it camps near 100%, the Null target is parse-bound and Tier 0 is measuring the server, not the connector. **MANDATORY** on Cloud-hosted Null t0 rows (§1.4). |
| `connections_per_insert` | `ratio` | New server connections created ÷ insert count over the run window (`metric_log` connection counters). Pool/keep-alive health. Same spelling on ALL tiers (Amendment 2026-07-07); the Tier-1 rename from `ch_connections_per_insert` landed 2026-07-07 (see §7 for the legacy-name coalesce). |
| `settle_seconds` | `seconds` | Seconds from ingest-end until active parts stabilise (merge debt left behind). **MUST** be spelled `settle_seconds` (§7: Spark SQL currently emits `ch_settle_seconds`). |
| `settle_timed_out` | `bool` | `1` iff settle hit the timeout cap (default 1800s), right-censoring `settle_seconds`. Feeds the `settle_timeout` flag (§1.3). |
| `run_cost_usd` | `usd` | Instance-hours × price for the run. Makes cadence/tier decisions data-driven. Two-arm attribution (Amendment 2026-07-07): the shared infrastructure is provisioned once per pair, so the FULL pair cost is charged once, on the first-run arm's row; the other arm's row omits the metric. Per-pair sums stay correct; per-arm cost is undefined by design. Both pipelines MUST use this convention. |
| `ch_uptime` | `seconds` | Target server uptime at run start. A drop vs the previous run ⇒ the service restarted between runs (covariate). |
| `pre_run_rss` | `bytes` | Target `MemoryResident` at run start (pre-truncate). Inherited memory pressure (covariate). |
| `pre_run_active_parts` | `count` | Active parts on the target table at run start. Cleanliness verifier, expected ~0 (see preamble note). |

### 2.2 Connector-specific headline names stay distinct

The two connectors' raw throughput headlines are **deliberately not** merged at
the metric-name level — they measure different operations. They keep distinct
names in `perf.metrics`; the **cross-connector view aliases them to one
`rows_per_sec` series in SQL**, not by renaming the stored metric.

| Concept | Spark name | Kafka name | Cross-connector alias (view only) |
|---------|-----------|-----------|-----------------------------------|
| Tier 1 verified throughput | `throughput_rows_per_sec` | `drain_rows_per_sec` | `rows_per_sec` |
| Tier 0 null-target throughput | `null_rows_per_sec` | `null_drain_rows_per_sec` | (aliased in the Tier 0 view) |

The alias lives in the Tab-5 / cross-connector virtual dataset SQL
(`clickhouse-kafka-connect` side). Neither pipeline **MAY** store its headline
under the aliased name — that would erase which operation produced it.

---

## 3. Verdict semantics (identical both sides)

Three outcomes, applied identically by both pipelines:

1. **Integrity mismatch ⇒ the run FAILS.** If `rows_delivered != rows_expected`
   OR `duplicate_rows != 0`, the run **FAILS**: it produces **no headline
   number** and **MUST NOT** contribute a throughput/cost figure to any trend,
   band, or ratio. (Integrity that could not be *computed* is a different case:
   flagged `integrity_unverified`, §1.3 — the run is non-comparable, not failed.)

2. **Validity-guard trip ⇒ the run is FLAGGED.** A guard condition (§1.3
   vocabulary: task retries, settle timeout, rebalance, task restart, drain
   incomplete) does **not** fail the run and is **not** a regression. The run is
   **FLAGGED** (`flagged='1'`, `flag_reason=<token[|token…]>`) and is **excluded
   from bands and ratios by default**. Flagged runs are still **fully captured
   and exported** — the most informative runs are often the least clean.

3. **Ratio band-exit ⇒ a verdict via the PINNED ratio→verdict map** (Amendment
   2026-07-09; root cause on record: the contract pinned verdict classes but
   never the mapping or the direction of goodness, so "outside band =
   REGRESSION" was the natural wrong implementation). Tracking is on the
   **ratio**, not absolutes. The flat ±3%/±5% starter bands are **superseded**
   by the per-metric CALIBRATED bands below (Amendment 2026-07-09b).

   **Per-gated-metric `direction` (PINNED):**

   | Metric | direction |
   |--------|-----------|
   | `throughput_rows_per_sec` (verified), `null_rows_per_sec` / `null_drain_rows_per_sec`, `drain_rows_per_sec` | `higher_better` |
   | `parts_per_insert`, `merge_amplification`, `cpu_seconds_per_Mrows`, `serialize_seconds_per_Mrows`, `ch_insert_cpu_seconds_per_Mrows` | `lower_better` |

   **Calibrated per-metric bands (PINNED — Amendment 2026-07-09b, supersedes the
   flat ±3%/±5% rule).** Each band is **2× the measured noise floor** for its
   metric (basis in the provenance note below). The band is the same on both
   tiers — it is a property of the metric's noise, not the tier:

   | Metric (and its rename/alias siblings) | band (± of ratio) |
   |----------------------------------------|-------------------|
   | `throughput_rows_per_sec` (verified) | **±9%** |
   | `null_rows_per_sec`, `null_drain_rows_per_sec`, `drain_rows_per_sec` (Tier-0 null + Kafka drain analogues) | **±8.5%** |
   | `ch_insert_cpu_seconds_per_Mrows`, `cpu_seconds_per_Mrows` | **±6%** |
   | `serialize_seconds_per_Mrows` | **±8.5%** |

   in-band ⇒ ratio ∈ `[1 − band, 1 + band]` (e.g. throughput `[0.91, 1.09]`;
   cpu `[0.94, 1.06]`). A metric not in this table is **not banded** — it is
   either watch-only or a tripwire (see gate composition).

   **Ratio→verdict map (PINNED, both pipelines, any artifact emitting a
   verdict):** ratio NULL or 0-denominator ⇒ **NO_DATA** (never REGRESSION);
   pair flagged ⇒ **FLAGGED** (excluded from bands); ratio outside the band in
   the GOOD direction ⇒ **IMPROVEMENT** (reported, not alarmed); outside in the
   BAD direction ⇒ **REGRESSION** (alerts once calibration completes); else
   **OK**. During calibration (fewer than ~20 pairs) verdicts MUST display as
   provisional ("calibrating, n=X/20") and band-excursion alerts stay unwired;
   only integrity failures (and the parts **TRIPWIRE**, below) alert.

   **`TRIPWIRE` verdict (PINNED — Amendment 2026-07-09b, added to the verdict
   vocabulary).** `parts_per_insert` is a **constant 1.0 by design** (one part
   per insert). It is **not banded** and gets **no ratio comparison**: the
   check is a **binary tripwire on the head arm's absolute value** — exactly
   `1.0` ⇒ **OK**; **any** deviation from `1.0` ⇒ **TRIPWIRE** (investigate;
   batches are spraying across partitions). A TRIPWIRE alerts regardless of
   calibration state (it is not a noise-band excursion; it is a structural
   invariant break). NULL/absent `parts_per_insert` ⇒ **NO_DATA** as usual.

   **Gate composition (PINNED — Amendment 2026-07-09b, replaces the prior
   flat-band gated set):**
   - **Tier 1 gate** = verified throughput (`throughput_rows_per_sec` /
     `drain_rows_per_sec`, banded) **+** `parts_per_insert` (tripwire).
   - **Tier 0 gate** = `null_rows_per_sec` (banded), the cpu-per-Mrows metric
     (`ch_insert_cpu_seconds_per_Mrows` / `cpu_seconds_per_Mrows`, banded) and
     `serialize_seconds_per_Mrows` (banded).
   - **`merge_amplification` is DEMOTED to WATCH-ONLY** — it is **not gated** and
     does **not** raise a REGRESSION. Single-pair excursions **<25%** are
     indistinguishable from merge-timing noise (measured within-arm floor 12.7%,
     and it gets **no pairing dividend** — the noise is per-run, not shared
     across the pair), so gating it would manufacture false regressions. It stays
     a reported covariate.
   - `settle_seconds` / `settle_timed_out` and `merge_pool_peak_pct` remain
     **covariates** (already non-gate).

   Pair-level roll-ups MUST still expose per-metric in-band counts so a single
   noisy metric cannot zero the headline.

   **Calibration provenance (PINNED — Amendment 2026-07-09b).** The bands above
   were calibrated on **2026-07-09** from `n=4` two-arm pairs' between-pair
   ratio CoV (chi-square `df=3` — a small-sample caveat: the point estimate is
   wide), **anchored** by `n=180` legacy calm-segment (2026-07-01..06)
   within-arm CoVs. Measured noise floors: throughput ratio **4.24%**,
   `merge_amplification` **12.7%**, Tier-0 cpu **2.83%**, Tier-0 serialize
   **4.24%**, `null_rows` **4.24%**. `merge_amplification` and throughput get
   **no pairing dividend** (per-run noise, not shared across arms).
   `parts_per_insert` is a **constant 1.0** (not a noisy series → tripwire, not a
   band). **Recalibration rule:** at ~12 pairs (~**2026-07-21**) the bands
   recalibrate from **trailing-20** window statistics, and thereafter follow the
   plan's **median ± 2·MAD** deviation-band design (`v2_trailing_windows`).

   **Acceptance rule (PINNED):** any artifact that emits a VERDICT (not a
   number) requires fixture-based acceptance: synthetic pair rows under a
   reserved fixture connector, asserted through the REAL dataset SQL, covering
   at least {NULL, 0-denominator, below-band, in-band, above-band} ×
   {higher_better, lower_better} × {flagged, unflagged}, **plus** the
   `parts_per_insert` tripwire cells (`==1.0` ⇒ OK, `!=1.0` ⇒ TRIPWIRE).
   Consumer views MUST exclude the fixture connector from real trends.

Precedence: FAIL (integrity mismatch) overrides FLAG overrides
NO_DATA/TRIPWIRE/IMPROVEMENT/REGRESSION/OK — a failed run yields no headline, so
it cannot also be a regression or a tripwire.

---

## 4. `v_env_annotations` scoping (MUST)

`v_env_annotations` is the shared virtual dataset that detects environment events
(CH upgrades = `clickhouse_version` change; restarts = `ch_uptime` drop; pin
bumps = pinned-arm `connector_version` change).

Because the two benchmarks run against **different** Cloud targets in **different
regions and environment classes**, every annotation row **MUST** carry the scope
tuple:

```
(connector, target_service, environment_class)
```

An **unscoped** shared annotation view is **PROHIBITED**: it would paint a
Spark-target restart (production, its region) onto Kafka charts (staging,
us-east-2), and vice versa, manufacturing false correlations. Annotation layers,
alert queries, and every consumer of this view **MUST** filter on the scope
tuple. (`target_service` here is the concrete service identity; `connector` and
`environment_class` come from `perf.runs` per §1.)

---

## 5. `perf.*` DDL ownership & sync (MUST)

The canonical `perf.*` DDL lives in **`spark-clickhouse-connector`** at
`benchmarks/sql/perf/0*_create_*.sql`:

- `01_create_database.sql` — `CREATE DATABASE IF NOT EXISTS perf`
- `02_create_runs.sql` — `perf.runs` (incl. the `runtime Map(String,String)`)
- `03_create_metrics.sql` — `perf.metrics`
- `04_create_ch_inserts.sql` — `perf.ch_inserts`

**Ownership:** `spark-clickhouse-connector` **owns** this DDL. Structural changes
land in `spark-clickhouse-connector` first (see the change-control rule in the
preamble).

**Consumption by the Kafka repo:** `clickhouse-kafka-connect` **consumes** the
same tables. A vendored copy of the DDL files in the Kafka repo is permitted
**ONLY** if accompanied by a **CI sync check** that diffs the vendored copy,
byte-for-byte, against the canonical files in `spark-clickhouse-connector` and
**fails the build on any difference**.

**Why the CI check is mandatory:** every DDL file uses `CREATE … IF NOT EXISTS`.
That means a *drifted* vendored copy — say the Kafka repo adds a column or changes
a type — will **silently no-op** against the already-created shared tables and
hide the drift indefinitely; the two repos would believe they share a schema they
do not. `IF NOT EXISTS` cannot detect drift, so a diff-based CI check is the only
guard. Without the CI check, a vendored copy is **PROHIBITED** (reference the
canonical files instead).

---

## 6. Cross-connector comparison surface (Tab 5, Kafka dashboard)

The cross-connector comparison (Spark vs Kafka on the aliased `rows_per_sec` and
`ch_insert_cpu_seconds_per_Mrows` series) lives on the **Kafka dashboard's Tab 5**
(sibling plan); the Spark dashboard (`spark-clickhouse-connector`) does not
duplicate it.

Tab 5 **MUST** surface `environment_class` beside `clickhouse_version` for each
connector's rows, and **MUST** show a visible caveat when the classes differ
across the connectors being compared (they do today: Spark = production, Kafka =
staging). H/P ratio gates are unaffected by the class difference (both arms share
the target), but absolute cross-connector numbers straddle a production/staging
boundary and the reader **MUST** be told.

**Matched-dataset rule (Amendment 2026-07-09d; enforcement mechanism concurred
2026-07-09e):** cross-connector comparisons are valid **only on a MATCHED
dataset**, where **matched = equal `(dataset, rows_expected)`** — the `dataset`
runtime key (§1.4, logical shape, correctly `'hits'` on both pipelines today)
joined with the `rows_expected` integrity metric (§2.1, volume truth).
Rationale: `dataset` alone cannot discriminate volume variants (`'hits'`@10M
would "match" `'hits'`@100M — today's actual state: Kafka 10,000,000 vs Spark
99,997,497); `rows_expected` already exists on every integrity-bearing run,
requires zero new keys and zero row mutations, and self-maintains (a future
volume change un-matches automatically). Rows missing `rows_expected` are
UNMATCHED by default (safe failure mode). The Tab-5 cross-connector view MUST
implement this join, and a mismatched row **MUST NEVER render as a comparable
efficiency number** — it may appear only in clearly-separated per-connector
context, never on a shared comparison series.

---

## 7. Required Spark-side renames (pinned name vs currently emitted name)

The Spark capture SQL (`spark-clickhouse-connector`,
`benchmarks/sql/perf/1x_*.sql`, verified against the files on disk at contract
time) emits several metrics under names that differ from the pinned spellings in
§2.1. The table below is the authoritative rename list; the Spark pipeline
**MUST** converge on the pinned name. The Spark-side conformance work is tracked
as board task **#40**.

| Pinned name (§2.1) | Currently emitted as | Emitting file (canonical, in `spark-clickhouse-connector`) |
|--------------------|----------------------|------------------------------------------------------------|
| `connections_per_insert` | `ch_connections_per_insert` | `benchmarks/sql/perf/12_insert_from_metric_log.sql` (Amendment 2026-07-07; rename lands with the Tier-0 work) |
| `parts_per_insert` | `ch_parts_per_insert` | `benchmarks/sql/perf/13_insert_from_part_log.sql` |
| `merge_amplification` | `ch_merge_amplification` | `benchmarks/sql/perf/13_insert_from_part_log.sql` |
| `inserts_delayed_fraction` | `ch_inserts_delayed_fraction` | `benchmarks/sql/perf/16_insert_throttling.sql` |
| `merge_pool_peak_pct` | `ch_merge_pool_peak_pct` | `benchmarks/sql/perf/16_insert_throttling.sql` |
| `settle_seconds` | `ch_settle_seconds` | `benchmarks/sql/perf/14_insert_settle_seconds.sql` |
| `ch_insert_cpu_seconds_per_Mrows` | **not emitted** — only the raw `ch_insert_cpu_seconds` exists; the per-Mrows derivation is missing | `benchmarks/sql/perf/11_insert_from_query_log.sql` (raw source) |

Also not yet emitted anywhere (planned, pinned in §2.1 for when they land):
`run_cost_usd`; the Tier 0 set (`null_rows_per_sec`, `ch_insert_cpu_share_tier0`)
pending the Tier 0 build.

Already conformant on disk (no action): `rows_delivered`, `rows_expected`,
`duplicate_rows`, `ch_dedup_dropped_blocks`, `bytes_on_wire_per_row`,
`ch_parts_active_peak`, `ch_memory_limit_errors`, `ch_avg_rows_per_insert`,
`settle_timed_out`, `ch_uptime`, `pre_run_rss`, `pre_run_active_parts`.

Rename mechanics: because `metric_name` spelling IS the series identity (§2), a
rename **MUST** be treated as a series cut-over — dashboards/views coalesce the
old and new name (`coalesce`-style union in the virtual dataset) so history is
not orphaned; the capture SQL emits only the pinned name from the cut-over
commit onward.

---

## Appendix — pinned-value quick reference

| Thing | Pinned value |
|-------|--------------|
| `arm` | `head` \| `pinned` (absent ⇒ `head`) |
| `tier` | `0` \| `1` as strings (absent ⇒ `1`) |
| `pair_id` | `= RUN_ID` = `YYYY-MM-DDTHH-MM-SSZ-<shortsha>` |
| `run_id` | `<pair_id>-<arm>-t<tier>` (recommended form; MUST be distinct per (arm, tier) row) |
| rows per night (both tiers) | 4 `perf.runs` rows, one `pair_id` |
| `flag_reason` tokens | `task_retries`, `settle_timeout`, `rebalance`, `task_restart`, `drain_incomplete`, `integrity_unverified`, `instrument_resize`, `instrument_shift` (join multiples with `|`) |
| mandatory scope keys | `target_region`, `environment_class` (`staging`\|`production`) |
| shared config keys | `batch_size`, `write_parallelism`, `async_insert`, `partition_scheme`, `dataset` |
| Tab-5 server-cost name | `ch_insert_cpu_seconds_per_Mrows` (NOT `server_cpu_per_Mrows`) |
| annotation scope | `(connector, target_service, environment_class)` |
| DDL owner | `spark-clickhouse-connector`; vendored copy needs CI diff check |
