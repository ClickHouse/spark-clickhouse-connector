# `grafana_dashboard.json` — DEPRECATED

**Status: deprecated / unused (per user, 2026-07-07).**

`benchmarks/dashboard/grafana_dashboard.json` is **no longer used**. The live
benchmark dashboarding runs on the DWH **Superset** deployment
(`superset.clickhouse-dev.com`), where **dashboard 427** ("Spark Benchmark v2")
supersedes this Grafana artifact. The Superset datasets live in
`benchmarks/dashboard/v2/*.sql` (bound to `raw_connectors_load_testing.*`).

## Do not update its SQL

The Grafana JSON is retained only as a historical reference. **Do not edit its
`rawSql` to track schema or metric-name changes** — it is not deployed anywhere
and updating it is wasted effort. All active dashboard work happens in Superset
427 via the `v2/*.sql` virtual datasets.

## If it is ever revived

Its panel queries read `perf.*` directly and reference **pre-rename** metric
names from before the 2026-07-07 rename cutover (see
`docs/benchmark-v2-contract.md` §7). Reviving the dashboard would require
applying the §7 legacy→pinned `coalesce` to at least these panels:

| Line (approx.) | Panel | Pre-rename metric referenced | Pinned name (§7) |
|----------------|-------|------------------------------|------------------|
| ~57 | "Latest runs" overview table | `ch_connections_per_insert` (as `conns_per_insert` column) | `connections_per_insert` |
| ~249 | connections-per-insert trend | `ch_connections_per_insert` | `connections_per_insert` |
| ~288 | parts-per-insert trend | `ch_parts_per_insert` | `parts_per_insert` |
| ~457 | merge-amplification trend | `ch_merge_amplification` | `merge_amplification` |

(`ch_connections_per_insert` appears twice in total: the ~57 overview table and
the ~249 trend panel.)

(The other §7 renames — `inserts_delayed_fraction`, `merge_pool_peak_pct`,
`settle_seconds` — are not currently referenced by name in the JSON but would
need the same treatment if such panels were added.)

Without that coalesce, any panel above would go blind to post-cutover rows
(the pinned name returns no rows for pre-cutover history and vice-versa), so the
series would split at the cutover date. The v2 Superset datasets already handle
this coalesce once, centrally, in `v2/v_runs_enriched.sql` and
`v2/v_pair_metrics_long.sql`.
