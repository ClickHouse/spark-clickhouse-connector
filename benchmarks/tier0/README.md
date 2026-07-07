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
| `bootstrap_tier0_ch.sh` | Master-only: install/start Docker, run pinned ClickHouse (with an explicit config.d override enabling `query_log`/`part_log`/`metric_log` — not left to image defaults), wait for readiness, create `tier0.hits` (ENGINE=Null) from the DDL, verify `system.metric_log` is live AND the config override file is mounted inside the container, then publish the `master_ip` + `tier0_ready` S3 sidecars ONLY after confirming the container answers on the master's PRIVATE IP. Idempotent (recreates the container on an image-tag or config-mount mismatch). |
| `capture_tier0_onmaster.sh` | On-master (task 18): before teardown, query the Docker CH's `system.query_log`/`metric_log` (windowed) via `../sql/tier0/12_server_side_tier0.sql` and ship the resulting `perf.metrics` rows to the Cloud metrics service. Metrics password read from Secrets Manager via the instance profile (never a plaintext step arg). |
| `../sql/tier0/01_create_hits_null.sql` | The `hits` schema with `ENGINE = Null` (DB name templated as `${TIER0_DB}`). |
| `../sql/tier0/10_event_log_tier0.sql` | Runner-side event-log capture (Null-engine variant of `perf/10`): `null_rows_per_sec`/`null_mb_per_sec` renames, all other client-cost metrics keep their Tier-1 spellings. |
| `../sql/tier0/12_server_side_tier0.sql` | On-master server-side capture: `connections_per_insert`, `bytes_on_wire_per_row`, `ch_insert_cpu_share_tier0` from the Docker CH's own system tables. |

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

## How task 18 wires this in (IMPLEMENTED)

Task 18 landed the wiring in `.github/workflows/clickbench-load-test.yml` and
`.github/actions/run-arm/action.yml`. Reality as shipped:

1. **Assets staged once per pair** to `s3://<bucket>/tier0/<pair_id>/` in the
   workflow's "Upload jar and script to S3" step: `bootstrap_tier0_ch.sh`,
   `01_create_hits_null.sql`, `capture_tier0_onmaster.sh`,
   `12_server_side_tier0.sql`.
2. **Tier-0 bootstrap = an EMR step** (`command-runner.jar`,
   `ActionOnFailure=CONTINUE`), after "Provision EMR": s3-cp the assets down and
   run `bootstrap_tier0_ch.sh` with `TIER0_CH_VERSION`, `TIER0_DDL_FILE`, and the
   two sidecar URIs. `emr_provision.sh` is unchanged (no bootstrap-action). The
   step is `continue-on-error`; the next workflow step reads the sidecars back
   and sets `TIER0_AVAILABLE` (1 iff `tier0_ready==ready` and a non-empty
   `master_ip`). All t0 arm calls gate on `TIER0_AVAILABLE=='1'`; the t1 calls
   are unconditional. A Tier-0 bring-up failure therefore skips t0 and never
   blocks t1.
3. **The run-arm action gained a `tier` input** (`'0'|'1'`, default `'1'`); the
   workflow calls it 4× per pair — `(arm1,t0)(arm1,t1)(arm2,t0)(arm2,t1)`. On
   t0 the submit step overrides the target to the master private IP over HTTP
   (`CH_PROTOCOL=http`, `CH_PORT=8123`, `CH_DATABASE=tier0`, empty password /
   no `CH_SECRET_ID`); the workload (jar, glob, batch_size, async_insert) is
   identical to t1. t0 skips warm-up, truncate, pre-run covariates, settle,
   integrity, and the perf/1x server-side capture; it runs the tier0 event-log
   SQL, the on-master server-side capture, flags, run-record (`tier='0'`,
   `environment_class='self_hosted'`, `target_region`=compute region,
   `tier0_ch_version`, `partition_scheme='none'`, no `warm_up`), rollback
   (t0-run_id-scoped), and export.
4. **Capture-before-teardown** is implemented: the on-master server-side capture
   (`capture_tier0_onmaster.sh`) runs as an EMR step INSIDE the t0 run-arm call,
   before the workflow's "Teardown EMR". A skipped t0 (`TIER0_AVAILABLE=0`)
   emits NO t0 rows — the missing t0 row per pair is the dashboard signal.

### Security-group reachability — verified by the executor, remediated offline

The t0 ingest driver's own connection attempt to `http://<master-ip>:8123` IS
the true core→master probe: if the managed EMR security groups do not allow it,
the t0 submit fails, that arm is a marked failed-class t0, and t1 is untouched.
The pipeline does **not** auto-mutate managed EMR security groups. If t0 submits
fail with a connection error, remediate offline and re-run: authorize an ingress
rule for **TCP 8123** (and 9000 if native is used) from the core-node security
group to the master security group, e.g.

```bash
aws ec2 authorize-security-group-ingress \
  --group-id <master-sg> --protocol tcp --port 8123 --source-group <core-sg>
```

### On-master metrics secret — instance-profile policy is a ONE-TIME HUMAN SETUP

The on-master capture INSERTs into the **Cloud metrics service** and reads that
service's password from Secrets Manager on the master via the EMR instance
profile (`METRICS_SECRET_ID`, default `clickbench-load-test/metrics-ch-password`).

The secret itself is created **automatically** by the workflow's "Ensure metrics
CH password in Secrets Manager" step (mirrors the target-password step: pushes
the GH secret into Secrets Manager, create-if-absent; rotate by editing the SM
secret). Two IAM caveats remain human territory:

1. **REQUIRED:** the instance profile (`EMR_EC2_DefaultRole`) is scoped to read
   ONLY the target-password secret — a human MUST add
   `secretsmanager:GetSecretValue` on the metrics secret's ARN to its policy.
2. The CI OIDC role's Secrets Manager permissions are scoped to the
   target-password secret name; if `Describe/CreateSecret` on the metrics secret
   name is denied, the ensure step fails (it is `continue-on-error`) and the
   secret must be created manually once.

Until both hold, the on-master capture step fails cleanly and the t0 run_id is
rolled back (t1 unaffected).

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
