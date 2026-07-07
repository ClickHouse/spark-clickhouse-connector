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

Band widths follow the plan: **Tier 0 ±3%, Tier 1 ±5%**, recalibrated from the
first ~20 pairs' measured CoV (plan §7 band-width rule / open decision 6). The
band constants live in `band_excursion.sql` — change them in ONE place.

## Empty-state / no-pairs behavior

All three return **zero rows today** (no two-arm pairs exist yet; all history is
`arm=head`, `tier=1`). Zero rows = nothing to alert on = correct. They MUST NOT
error on the empty dataset (verified against DWH at authoring time).
