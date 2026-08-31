# Tier 0 — connector ceiling (`ENGINE=Null`) target

> **DEPRECATION NOTE (2026-07-07).** The Docker-on-master Tier-0 instrument
> described by the files that used to live here has been **removed**. Tier 0 now
> runs against an `ENGINE=Null` table **on the Cloud target** (the same dedicated
> service Tier 1 ingests into), mirroring the Kafka pipeline's Cloud-hosted Null
> design. This is a deliberate user-directed simplification: it drops the whole
> Docker instrument (container bring-up, readiness sidecars, master-IP plumbing,
> on-master capture, the metrics-secret / instance-profile IAM follow-up) in
> exchange for symmetry with Kafka and one fewer moving part.

## What Tier 0 is now

Tier 0 answers **Q-A: "is the connector *client* getting slower or hungrier?"**
(CPU/row, serialization, memory, protocol efficiency). It runs the exact same
ingest as the Tier 1 run, over the **same host / HTTPS / port / credential**, but
into `clickbench.hits_null` — a table with the `hits` schema and `ENGINE = Null`.
The Null engine parses each inserted block (exercising the full client + network
+ HTTPS/keep-alive + server-parse + ack path) and then **discards** it: no parts,
no merges, no storage, no memory pressure.

## Where the pieces live now

| Concern | Where |
|---------|-------|
| Null table DDL | `benchmarks/sql/clickbench/03_create_hits_null.sql` — `clickbench.hits_null`, `ENGINE=Null`, bootstrapped by `benchmarks/scripts/bootstrap_schema.py` alongside `02_create_hits.sql` (idempotent `CREATE IF NOT EXISTS`). |
| Runner-side event-log capture | `benchmarks/sql/tier0/10_event_log_tier0.sql` (unchanged) — `null_rows_per_sec` / `null_mb_per_sec` renames; all other client-cost metrics keep their Tier-1 spellings. |
| Server-side capture | Normal `remoteSecure()` reads of the Cloud target's `system.*_log`, scoped to `clickbench.hits_null`, folded into the run-arm "Capture metrics" step: `perf/11`, `perf/12` (`connections_per_insert`), `perf/15`+`perf/19`, `perf/18` (`bytes_on_wire_per_row`), and `perf/23_insert_tier0_parse_watch.sql` (`ch_insert_cpu_share_tier0`, MANDATORY). |
| Ingest target selection | `.github/actions/run-arm/action.yml` submit step: `tier='0'` sets `CH_TABLE=hits_null`; host/port/protocol/user/secret are identical to Tier 1. |

## Contract & design references

- `docs/benchmark-v2-contract.md` §1.1 — the Cloud-hosted Null branch: t0 rows
  inherit the target's `environment_class` (`production`) and `target_region`
  (`us-east-2`), carry **no** `tier0_ch_version`, and the
  `ch_insert_cpu_share_tier0` parse-watch is **MANDATORY** instead.
- `docs/benchmark-v2-plan.md` §3 — the trade this design makes: Cloud drift now
  enters Tier 0, guarded by the parse-watch (user-accepted for symmetry/
  simplicity).

## Recovering the Docker instrument

The removed Docker instrument (pinned-version ClickHouse in Docker on the EMR
master, `self_hosted`/load-generator-local semantics) is preserved in git
history. If a future Tier-3-style, fully version-controlled instrument is ever
wanted, recover `bootstrap_tier0_ch.sh`, `capture_tier0_onmaster.sh`, and
`sql/tier0/12_server_side_tier0.sql` from history rather than rewriting them.

## IAM follow-up — CANCELLED

The prior "instance profile needs `secretsmanager:GetSecretValue` on the metrics
secret" one-time human setup is **cancelled**: there is no on-master capture and
no `METRICS_SECRET_ID` anymore, so the EMR instance profile does not need the
metrics-secret grant. The metrics password is used only by the GitHub-hosted
capture steps (as before), never on the master.
