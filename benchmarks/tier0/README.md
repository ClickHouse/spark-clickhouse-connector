# Tier 0 — connector ceiling (`ENGINE=Null`) target

Assets for **Tier 0** of the ClickBench connector benchmark v2 (see
`docs/benchmark-v2-plan.md`, section 3 and section 9 step 3).

## What this is

Tier 0 answers **Q-A: "is the connector *client* getting slower or hungrier?"**
(CPU/row, serialization, memory, protocol efficiency). It runs the exact same
ingest as the normal (Tier 1) run, but against a **self-hosted ClickHouse in
Docker on the EMR master node**, into a table with the `hits` schema and
`ENGINE = Null`.

The Null engine parses each inserted block — exercising the full client +
network + HTTPS/keep-alive + server-parse + ack path — and then **discards** it.
No parts, no merges, no storage, no memory pressure. That removes the dominant
server-side variance source, so a regression signal here moves only when the
*connector* changes. It is a fully version-controlled instrument (pinned CH
version, zero Cloud drift, ~zero extra cost — the master has idle cores).

## Files

| File | Purpose |
|------|---------|
| `bootstrap_tier0_ch.sh` | Master-only: install/start Docker, run pinned ClickHouse (with an explicit config.d override enabling `query_log`/`part_log`/`metric_log` — not left to image defaults), wait for readiness, create `tier0.hits` (ENGINE=Null) from the DDL, verify `system.metric_log` is live. Idempotent. |
| `../sql/tier0/01_create_hits_null.sql` | The `hits` schema with `ENGINE = Null` (DB name templated as `${TIER0_DB}`). |

## Pinned ClickHouse version

`TIER0_CH_VERSION=25.8.28` (top of `bootstrap_tier0_ch.sh`). `25.8` is the
current ClickHouse **LTS** line (the yearly `.8` release); pinned to a full
patch, not the floating `25.8` tag, so the instrument is reproducible. Bump
deliberately and annotate it as a dashboard event (plan §2). Override for local
testing with `TIER0_CH_VERSION=...`.

## Bootstrap action vs EMR step (design choice)

`bootstrap_tier0_ch.sh` is written to work **either way**, so task 18 can pick
without a rewrite:

- **As a bootstrap action** — runs on *every* node before Hadoop/Spark start.
  The script self-guards with the standard `isMaster` check from
  `/mnt/var/lib/info/instance.json` and no-ops on core/task nodes. Pro: CH is up
  before any Spark step. Con: bootstrap failures abort the whole cluster.
- **As an EMR step** — runs only on the master, after the cluster is up (the
  `isMaster` guard then always passes). Pro: sequenced with the other steps,
  clearer failure isolation, easy `ActionOnFailure`. Con: adds a step, and the
  ingest step must be ordered after it.

**Recommendation for task 18:** an **EMR step** placed before the ingest step —
it matches how the pipeline already adds work (`emr_submit.sh` uses
`aws emr add-steps`), gives clean failure semantics, and keeps provisioning
(`emr_provision.sh`) unchanged except for staging the script.

## How task 18 must wire this in

Not done here (task 17 = new files only). Task 18 needs to:

1. **Stage the assets on the master.** Either add a `--bootstrap-actions` entry
   to the `aws emr create-cluster` call in `benchmarks/scripts/emr_provision.sh`
   (path must be an S3 URI — upload `bootstrap_tier0_ch.sh` and the DDL first),
   or add an EMR step that `aws s3 cp`s them down and runs the script. The DDL
   path is overridable via `TIER0_DDL_FILE`.
2. **Thread the Tier 0 target to Spark.** Point the ingest at the master's
   **private IP** over HTTP: `CH_PROTOCOL=http`, `CH_PORT=8123`, ssl=false,
   `CH_DATABASE=tier0`, `CH_TABLE=hits`. The master private IP is available on
   the master itself (`hostname -i`) or via
   `aws emr describe-cluster ... MasterPublicDnsName`/instance metadata. The
   connector config already flows from `emr_submit.sh` env
   (`CH_HOST/CH_PORT/CH_PROTOCOL`), so this is a per-arm override, not new code.
3. **Verify — not assume — the security group.** Core/task nodes must be able
   to reach the master on 8123 (and 9000 if native is used) over the private
   subnet. EMR's managed intra-cluster security groups *usually* allow all
   master↔slave traffic, but that must be **checked on the actual cluster**
   (the acceptance smoke test below does exactly this from a core node), and
   an explicit ingress rule added if it does not hold.
4. **Tag the run** `runtime['tier']='0'` and capture the Tier 0 metric set
   (plan §5). Confirm the Null target never becomes parse-bound
   (`ch_insert_cpu_share_tier0`).
5. **Implement capture-before-teardown** (see next section — the mode is
   decided; task 18 implements it, it does not re-decide).

## Capture mode — DECIDED: capture BEFORE teardown (plan decision 4, resolved)

Plan §3 (Tier 0 operational note) / §10 decision 4 is **resolved**: the Tier 0
server-side capture runs **before EMR teardown** — the Tier 0 metric queries
execute against the Docker ClickHouse while the cluster is still up, and only
then is the cluster torn down. This is the only mode that preserves the full
Tier 0 server-side set, in particular `connections_per_insert` (metric_log) and
the parse-watch metric `ch_insert_cpu_share_tier0` (query_log). Note this
reorders today's workflow, where "Teardown EMR" precedes "Capture metrics" in
`.github/workflows/clickbench-load-test.yml`.

Rejected alternatives (for the record):

  - **Ship logs to S3** (export query_log/metric_log extracts to S3 before
    teardown; capture reads S3) — rejected: extra moving part and an
    intermediate format for no gain over querying the live server first.
  - **Event-log-only** (Spark-event-log-derived metrics only) — rejected: loses
    the Docker-CH server-side rows (`bytes_on_wire_per_row`,
    `connections_per_insert`, `ch_insert_cpu_share_tier0`), including the
    instrument-health parse watch.

The bootstrap script already enables `query_log`/`part_log`/`metric_log`
explicitly (config.d override) so the capture-before-teardown queries have
their source tables guaranteed.

## Manual smoke test on a running cluster

SSH to the EMR master, then:

```bash
# Optional: pin a version / db explicitly
export TIER0_CH_VERSION=25.8.28 TIER0_DB=tier0
sudo bash bootstrap_tier0_ch.sh          # installs docker if needed, starts CH, creates tier0.hits

# From the master (loopback) — schema present, engine is Null:
sudo docker exec tier0-clickhouse clickhouse-client --query \
  "SELECT engine FROM system.tables WHERE database='tier0' AND name='hits'"   # -> Null
sudo docker exec tier0-clickhouse clickhouse-client --query \
  "SELECT count() FROM system.columns WHERE database='tier0' AND table='hits'" # -> 105
sudo docker exec tier0-clickhouse clickhouse-client --query \
  "SELECT count() > 0 FROM system.metric_log"                                  # -> 1 (log override active)

# Reachability from a CORE node over the private IP (acceptance criterion):
MASTER_IP=$(hostname -i)                 # run on the master to learn its private IP
# then, from a core node:
curl -s "http://${MASTER_IP}:8123/" --data-binary "SELECT 1"                   # -> 1
```

Re-running `bootstrap_tier0_ch.sh` is safe: Docker install no-ops, the running
container is left as-is, and the DDL is `CREATE ... IF NOT EXISTS`.
