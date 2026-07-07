#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# =============================================================================
# TIER 0 on-master server-side capture (benchmark-v2-plan §3 decision 4:
# capture BEFORE teardown). Runs as an EMR STEP on the master (via
# command-runner.jar), while the cluster — and the Docker ClickHouse — is still
# up. It:
#   1. queries the LOCAL Docker ClickHouse (loopback, `docker exec`) with the
#      Tier-0 server-side SQL (benchmarks/sql/tier0/12_server_side_tier0.sql),
#      windowed to this t0 pass, producing perf.metrics rows;
#   2. streams those rows OUT of the Docker CH and INTO the Cloud METRICS service
#      (the same perf.* landing tables the Tier-1 capture writes to).
#
# WHY ON THE MASTER: the Docker CH dies at EMR teardown, so its query_log /
# metric_log MUST be read before teardown. The master is the only host that can
# reach the container over loopback. See benchmarks/tier0/README.md.
#
# INJECTION SAFETY: run_id / run_start / run_end / table_qualified are passed to
# clickhouse-client as --param_<name> bindings, never interpolated into the SQL
# text. The SQL is delivered on stdin. No user value reaches the shell-expanded
# SQL string.
#
# SECRETS: the METRICS service password is read from AWS Secrets Manager on the
# master via the EMR instance profile (mirrors the target-password pattern in
# clickbench_ingest.py). It is NEVER a plaintext EMR step argument (step args are
# visible in the EMR console). See METRICS_SECRET_ID below and the README note on
# the one-time human secret/permission setup this requires.
#
# FAILURE DISCIPLINE (#6-consistent, scoped to the t0 run_id): this script exits
# non-zero on ANY capture/insert failure. The workflow treats a non-zero t0
# capture as an incomplete t0 artifact and rolls back the t0 run_id (t1 rows are
# a different run_id and are untouched). t0 NEVER blocks t1.
# =============================================================================
set -euo pipefail

# --- Inputs (env-driven; the EMR step sets these) --------------------------
# Identity + window for THIS t0 pass (contract §1.2: run_id = <pair>-<arm>-t0).
RUN_ID="${RUN_ID:?RUN_ID (…-<arm>-t0) is required}"
RUN_START="${RUN_START:?RUN_START (t0 submit window start, UTC) is required}"
RUN_END="${RUN_END:?RUN_END (t0 submit window end, UTC) is required}"

# Tier-0 database/table on the local Docker CH (single source of truth: the
# bootstrap script's TIER0_DB; default matches it).
TIER0_DB="${TIER0_DB:-tier0}"
TIER0_TABLE="${TIER0_TABLE:-hits}"
TABLE_QUALIFIED="${TIER0_DB}.${TIER0_TABLE}"
CONTAINER_NAME="${TIER0_CONTAINER_NAME:-tier0-clickhouse}"

# The Tier-0 server-side SQL, staged next to this script on the master.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_SQL="${TIER0_SERVER_SQL:-${SCRIPT_DIR}/../sql/tier0/12_server_side_tier0.sql}"

# Cloud METRICS service connection. HOST/USER are non-secret (passed as env by
# the EMR step); the PASSWORD is resolved from Secrets Manager (never a step arg).
METRICS_CH_HOST="${METRICS_CH_HOST:?METRICS_CH_HOST is required}"
METRICS_CH_USER="${METRICS_CH_USER:-default}"
METRICS_CH_PORT="${METRICS_CH_PORT:-9440}"   # native-secure
# Secrets Manager id holding the METRICS service password. REQUIRES a one-time
# human setup: create this secret AND extend the EMR instance-profile policy to
# GetSecretValue on it (today the profile can read only the target-password
# secret). See benchmarks/tier0/README.md "on-master metrics secret".
METRICS_SECRET_ID="${METRICS_SECRET_ID:?METRICS_SECRET_ID is required (Secrets Manager id for the metrics-service password)}"
AWS_REGION="${AWS_REGION:-us-east-1}"

log() { echo "[tier0-capture] $*"; }

# --- Resolve the metrics password from Secrets Manager (instance profile) ----
# Mirrors clickbench_ingest.py resolve_password: accept either a raw string
# secret or a JSON object with a "password" key.
resolve_metrics_password() {
  local raw
  raw="$(aws secretsmanager get-secret-value \
           --region "$AWS_REGION" --secret-id "$METRICS_SECRET_ID" \
           --query SecretString --output text)"
  # If it parses as JSON with a .password field, use that; else the raw string.
  if printf '%s' "$raw" | jq -e 'has("password")' >/dev/null 2>&1; then
    printf '%s' "$raw" | jq -r '.password'
  else
    printf '%s' "$raw"
  fi
}

main() {
  log "run_id=$RUN_ID window=[$RUN_START .. $RUN_END] table=$TABLE_QUALIFIED"

  # 1) Ensure the perf.* landing schema exists inside the Docker CH (the SQL
  #    INSERTs into perf.metrics locally, then we ship the rows out). CREATE IF
  #    NOT EXISTS — idempotent, and independent of the Cloud metrics schema.
  log "ensuring local perf.metrics exists in the Docker CH"
  sudo docker exec -i "$CONTAINER_NAME" clickhouse-client --multiquery <<'DDL'
CREATE DATABASE IF NOT EXISTS perf;
CREATE TABLE IF NOT EXISTS perf.metrics
(
    run_id String,
    metric_name LowCardinality(String),
    unit String,
    value Float64,
    recorded_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (run_id, metric_name);
DDL

  # 2) Run the Tier-0 server-side SQL against the local Docker CH. Params are
  #    bound (injection-safe); SQL delivered on stdin.
  log "running $SERVER_SQL against the local Docker CH (windowed)"
  sudo docker exec -i "$CONTAINER_NAME" clickhouse-client --multiquery \
    --param_run_id="$RUN_ID" \
    --param_run_start="$RUN_START" \
    --param_run_end="$RUN_END" \
    --param_table_qualified="$TABLE_QUALIFIED" \
    < "$SERVER_SQL"

  # 3) Extract the just-written rows for THIS run_id as TSV and stream them into
  #    the Cloud METRICS service. Both legs are set -e / pipefail guarded, so any
  #    failure exits non-zero and the workflow rolls the t0 run_id back.
  local metrics_password
  metrics_password="$(resolve_metrics_password)"
  log "shipping perf.metrics rows for $RUN_ID to the Cloud metrics service ($METRICS_CH_HOST:$METRICS_CH_PORT)"

  # SECRET HANDLING (review nit 3): the resolved password must NOT appear in the
  # docker exec argv (`-e VAR=value` is visible in `ps` on the master for the
  # process lifetime). Write it to a 0600 root-owned env-file consumed via
  # --env-file, and delete it on exit (the trap covers the failure paths too).
  local envfile
  envfile="$(sudo mktemp /root/.tier0-metrics-env.XXXXXX)"
  # shellcheck disable=SC2064  # expand $envfile NOW (local var, fixed value)
  trap "sudo rm -f '$envfile'" EXIT
  sudo chmod 600 "$envfile"
  {
    printf 'METRICS_CH_HOST=%s\n' "$METRICS_CH_HOST"
    printf 'METRICS_CH_PORT=%s\n' "$METRICS_CH_PORT"
    printf 'METRICS_CH_USER=%s\n' "$METRICS_CH_USER"
    printf 'METRICS_CH_PASSWORD=%s\n' "$metrics_password"
  } | sudo tee "$envfile" >/dev/null

  # docker exec (loopback read) | clickhouse-client (TLS insert into Cloud).
  # A non-installed host clickhouse-client is unlikely on EMR, but we run the
  # OUTBOUND client inside the SAME container (it has clickhouse-client) so we do
  # not depend on the host having the binary. --secure + native-secure port.
  sudo docker exec -i "$CONTAINER_NAME" clickhouse-client --multiquery \
    --param_run_id="$RUN_ID" \
    --query "SELECT run_id, metric_name, unit, value FROM perf.metrics WHERE run_id = {run_id:String} FORMAT TabSeparated" \
  | sudo docker exec -i --env-file "$envfile" "$CONTAINER_NAME" \
      sh -c 'clickhouse-client --host "$METRICS_CH_HOST" --port "$METRICS_CH_PORT" \
               --user "$METRICS_CH_USER" --password "$METRICS_CH_PASSWORD" --secure \
               --query "INSERT INTO perf.metrics (run_id, metric_name, unit, value) FORMAT TabSeparated"'

  sudo rm -f "$envfile"
  trap - EXIT
  log "done: Tier-0 server-side metrics for $RUN_ID shipped to the metrics service"
}

main "$@"
