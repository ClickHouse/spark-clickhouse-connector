# Benchmark v2 — Tab 1 alert queries

These `.sql` files are the **alert conditions** behind the Tab 1 "Regression"
dashboard tiles. They deliberately **share the SQL semantics of the tiles** (plan
§7 closing line: "Tab 1's tiles share their SQL with the alert queries, so
dashboard and alerting cannot drift apart"). If a query returns a non-empty
result (or a `should_alert = 1` row), the corresponding condition has tripped.

Semantics are pinned by `docs/benchmark-v2-contract.md` §3 (verdicts) and
`docs/benchmark-v2-plan.md` §6.2 (deviation bands + alerting) / §7 (Tab 1).

## FLAGGED follow-up — channel wiring is NOT configured

Plan **open decision 3 (alert channel: GitHub issue vs Slack) is undecided**, and
configuring Superset's own Alerts & Reports delivery needs admin-level channel
config this pipeline does not own. Therefore **these files define the alert
QUERIES only** — no delivery is wired. Wiring the channel (Superset A&R against
the "DWH" connection, or an external scheduler firing these against DWH and
posting to the chosen channel with the pair link) is a **flagged follow-up**,
tracked with board task **#39** (missing-pair freshness alerting) for the
freshness precursor below.

## How these map to the tiles (same dataset, same filters)

| Alert file | Tile it mirrors | Dataset (DWH mirror) |
|---|---|---|
| `band_excursion.sql` | Tier 0 / Tier 1 verdict + Excursion log | `v2_pair_ratios` |
| `integrity_failure.sql` | Integrity rows-OK tile | `v2_runs_enriched` |
| `missing_pair_freshness.sql` | (precursor for #39; no tile yet) | `v2_runs_enriched` |

Run them against the **DWH** ClickHouse connection
(uuid `dc93cd97-0d1f-4ed9-be6b-10b40e13e74c`, db 1), schema
`raw_connectors_load_testing` (the ClickPipe mirror of `perf.*`). The Superset
virtual datasets `v2_pair_ratios` / `v2_runs_enriched` are inlined here as CTEs so
each file is a standalone runnable query (Superset virtual datasets cannot be
self-referenced by name from a query — see the connector notes).

Band widths are **CALIBRATED per-metric** (contract §3 Amendment 2026-07-09b/f —
the flat Tier 0 ±3% / Tier 1 ±5% rule is SUPERSEDED). Each band is **2× the
measured noise floor** and is **keyed on the metric, NOT the tier** (the band is a
property of the metric's noise, so it is identical on Tier 0 and Tier 1):

| Metric | Band |
|---|---|
| `throughput_rows_per_sec` | ±9% |
| `null_rows_per_sec` / `null_drain_rows_per_sec` / `drain_rows_per_sec` | ±8.5% |
| `cpu_seconds_per_Mrows` / `ch_insert_cpu_seconds_per_Mrows` | ±6% |
| `serialize_seconds_per_Mrows` | ±8.5% |
| `merge_amplification` | **WATCH-ONLY** — not gated, never alerts |
| `parts_per_insert` | **TRIPWIRE** — head arm `== 1.0` exactly; any deviation trips |

**Recalibration rule:** at **~12 pairs (~2026-07-21)** the bands recalibrate from
**trailing-20** window statistics and thereafter follow the plan's **median ±
2·MAD** deviation-band design (`v2_trailing_windows`). The band constants live in
`band_excursion.sql` — change them in ONE place.

## Empty-state / no-pairs behavior

All three return **zero rows today** (no two-arm pairs exist yet; all history is
`arm=head`, `tier=1`). Zero rows = nothing to alert on = correct. They MUST NOT
error on the empty dataset (verified against DWH at authoring time).
