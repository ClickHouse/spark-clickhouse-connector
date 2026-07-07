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
# Tier 0 (benchmark v2 plan, section 3 / section 9 step 3):
# start a pinned-version ClickHouse in Docker on the EMR MASTER node and create
# the hits-schema table with ENGINE=Null. This becomes the connector's
# self-hosted, fully version-controlled target for the Q-A (client-ceiling) run.
#
# DESIGN: bootstrap-action compatible, master-only.
#   EMR bootstrap actions run on EVERY node (master + all core/task) before
#   Hadoop/Spark start, so this script self-guards with the standard isMaster
#   check from /mnt/var/lib/info/instance.json and no-ops on non-master nodes.
#   It is equally usable as an EMR *step* (steps run only on the master, after
#   the cluster is up): the isMaster guard simply always passes there. Task 18
#   picks the wiring; see benchmarks/tier0/README.md for the trade-off. Written
#   to be safe under either invocation.
#
# The container binds 0.0.0.0:8123 (HTTP) and 0.0.0.0:9000 (native) on the host
# network namespace via -p publish, so Spark executors on the core nodes can
# reach it at the master's PRIVATE IP. The connector's ingest path uses HTTP
# (CH_PROTOCOL=http, CH_PORT=8123, ssl=false) -> no TLS to terminate locally.
#
# IDEMPOTENT / re-runnable: installing Docker is a no-op if present; the
# container is (re)created only if not already running; the DDL uses
# CREATE ... IF NOT EXISTS. Safe to run twice on the same node.
#
# NOTE ON SCOPE (task 17): this script and its DDL are delivered as standalone
# assets. Wiring them into benchmarks/scripts/emr_provision.sh (as a
# --bootstrap-actions arg or an EMR step) and threading the Tier 0 CH_HOST to
# the Spark submit is TASK 18. Nothing here modifies the existing pipeline.
# =============================================================================
set -euo pipefail

# --- Pinned ClickHouse version --------------------------------------------
# 25.8 is the current ClickHouse LTS line (the yearly ".8" release; supported
# with security/critical fixes for ~1 year, matching the "pin a stable point"
# intent of the plan). Pinned to a full patch (not the floating "25.8" tag) so
# the instrument is byte-for-byte reproducible across runs; every deliberate
# bump is a dashboard-annotatable event (plan section 2). Override for local
# smoke tests via TIER0_CH_VERSION=... in the environment.
TIER0_CH_VERSION="${TIER0_CH_VERSION:-25.8.28}"
TIER0_CH_IMAGE="clickhouse/clickhouse-server:${TIER0_CH_VERSION}"

# Tier 0 database name — single source of truth; substituted into the DDL.
TIER0_DB="${TIER0_DB:-tier0}"

# Ports published on the host (the master's private IP). HTTP is what the
# connector ingest path uses; native is published for manual smoke tests.
TIER0_HTTP_PORT="${TIER0_HTTP_PORT:-8123}"
TIER0_NATIVE_PORT="${TIER0_NATIVE_PORT:-9000}"

CONTAINER_NAME="${TIER0_CONTAINER_NAME:-tier0-clickhouse}"
READY_TIMEOUT_SECONDS="${TIER0_READY_TIMEOUT_SECONDS:-120}"

# Task 18: S3 sidecars the workflow reads back to learn (a) the master private IP
# to point the t0 ingest at, and (b) whether Tier 0 came up (TIER0_AVAILABLE).
# BOTH are written ONLY after the container is confirmed answering on the
# master's PRIVATE IP (not just loopback) — that is the real core->master reach
# the executors need. Empty ⇒ the workflow skips Tier 0 (t1 unaffected).
TIER0_MASTER_IP_S3_URI="${TIER0_MASTER_IP_S3_URI:-}"
TIER0_READY_S3_URI="${TIER0_READY_S3_URI:-}"

# Host directory for config.d overrides bind-mounted into the container.
TIER0_CONF_DIR="${TIER0_CONF_DIR:-/etc/tier0-clickhouse/config.d}"

# Directory this script lives in, so we can find the DDL relative to it whether
# invoked from a checkout or from a copy staged on the master (e.g. via S3).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# DDL default: sibling sql/tier0 tree in the repo. Overridable so task 18 can
# point at wherever it stages the file on the master.
DDL_FILE="${TIER0_DDL_FILE:-${SCRIPT_DIR}/../sql/tier0/01_create_hits_null.sql}"

log() { echo "[tier0-bootstrap] $*"; }

# --- Master-node guard -----------------------------------------------------
# EMR bootstrap actions execute on all nodes. Only the master hosts the Tier 0
# ClickHouse. /mnt/var/lib/info/instance.json is written by EMR on every node
# and carries "isMaster": true|false. If the file is absent (e.g. a step
# invocation, or a manual run off-EMR) we assume master and proceed.
is_master() {
  local info=/mnt/var/lib/info/instance.json
  if [[ -f "$info" ]]; then
    grep -q '"isMaster"[[:space:]]*:[[:space:]]*true' "$info"
  else
    log "no $info found; assuming master (step/manual invocation)"
    return 0
  fi
}

if ! is_master; then
  log "not the master node — nothing to do on this instance"
  exit 0
fi

# --- Install / start Docker (Amazon Linux) ---------------------------------
ensure_docker() {
  if command -v docker >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
    log "docker already installed and running"
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1; then
    log "installing docker via yum (Amazon Linux)"
    # Amazon Linux 2 and 2023 both provide the 'docker' package in the default
    # repos. -y for non-interactive; retry-free (bootstrap failure aborts the
    # cluster, which is the correct signal).
    sudo yum install -y docker
  fi
  log "starting docker daemon"
  sudo systemctl enable --now docker 2>/dev/null || sudo service docker start
  # Give dockerd a moment to accept connections.
  local i
  for i in $(seq 1 30); do
    if sudo docker info >/dev/null 2>&1; then
      log "docker daemon is up"
      return 0
    fi
    sleep 2
  done
  log "ERROR: docker daemon did not become ready"
  return 1
}

# --- System-log tables config override --------------------------------------
# Tier 0's server-side capture set (connections_per_insert from metric_log,
# ch_insert_cpu_share_tier0 from query_log, plus part_log for symmetry with the
# Tier 1 capture SQL) requires these tables. EXPLICITLY enable them via a
# config.d override bind-mounted into the container — do not rely on the
# image's default config keeping them on across versions.
write_log_config() {
  local conf="${TIER0_CONF_DIR}/tier0_system_logs.xml"
  log "writing system-log config override ${conf}"
  sudo mkdir -p "${TIER0_CONF_DIR}"
  # Overwritten unconditionally (idempotent: content is deterministic).
  sudo tee "${conf}" >/dev/null <<'EOF'
<clickhouse>
    <!-- Tier 0 benchmark: system log tables the server-side capture reads.
         Explicitly enabled; do not rely on image defaults. -->
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
    <part_log>
        <database>system</database>
        <table>part_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </part_log>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </metric_log>
</clickhouse>
EOF
}

# --- (Re)start the ClickHouse container ------------------------------------
# Task 18 nit (c): does the already-running container match what THIS bootstrap
# would create — the pinned image tag AND the config.d mount? A version bump (or
# a config change) must recreate, not silently leave a stale container serving
# the old pin. Returns 0 = matches (leave it), 1 = mismatch/absent (recreate).
running_container_matches() {
  sudo docker ps --filter "name=^/${CONTAINER_NAME}$" --filter "status=running" \
      --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$" || return 1
  local got_image
  got_image="$(sudo docker inspect --format '{{.Config.Image}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
  if [[ "$got_image" != "${TIER0_CH_IMAGE}" ]]; then
    log "running container image '$got_image' != pinned '${TIER0_CH_IMAGE}' — will recreate"
    return 1
  fi
  # Config.d mount present? A running container without the system-log override
  # would silently lack the capture source tables (nit (a) mount re-verify).
  if ! sudo docker exec "${CONTAINER_NAME}" \
        test -f /etc/clickhouse-server/config.d/tier0_system_logs.xml 2>/dev/null; then
    log "running container is missing the tier0_system_logs.xml mount — will recreate"
    return 1
  fi
  return 0
}

start_container() {
  # Already running the RIGHT thing (image + config mount)? Leave it (idempotent).
  if running_container_matches; then
    log "container ${CONTAINER_NAME} already running with the pinned image + config; leaving it"
    return 0
  fi
  # Remove any running/stopped container of the same name so we can recreate it
  # cleanly (mismatched image/config, or a stale exited container).
  if sudo docker ps -a --filter "name=^/${CONTAINER_NAME}$" \
        --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log "removing existing container ${CONTAINER_NAME} (recreating with pinned image + config)"
    sudo docker rm -f "${CONTAINER_NAME}" >/dev/null
  fi

  log "pulling ${TIER0_CH_IMAGE}"
  sudo docker pull "${TIER0_CH_IMAGE}"

  log "starting ${CONTAINER_NAME} (HTTP :${TIER0_HTTP_PORT}, native :${TIER0_NATIVE_PORT}, bind 0.0.0.0)"
  # --ulimit nofile: CH wants a high fd limit.
  # -p 0.0.0.0:host:container publishes on all host interfaces incl. the
  #   private IP the executors reach.
  # --restart unless-stopped: survive a transient dockerd hiccup during the run.
  # -v config.d mount: explicit system-log config (see write_log_config).
  sudo docker run -d \
    --name "${CONTAINER_NAME}" \
    --restart unless-stopped \
    --ulimit nofile=262144:262144 \
    -p "0.0.0.0:${TIER0_HTTP_PORT}:8123" \
    -p "0.0.0.0:${TIER0_NATIVE_PORT}:9000" \
    -v "${TIER0_CONF_DIR}/tier0_system_logs.xml:/etc/clickhouse-server/config.d/tier0_system_logs.xml:ro" \
    "${TIER0_CH_IMAGE}" >/dev/null
}

# --- Wait for readiness -----------------------------------------------------
wait_ready() {
  log "waiting up to ${READY_TIMEOUT_SECONDS}s for ClickHouse HTTP readiness"
  local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
  while (( $(date +%s) < deadline )); do
    # Query inside the container over the loopback so we don't depend on the
    # host having curl; 'SELECT 1' proves the server accepts queries.
    if sudo docker exec "${CONTAINER_NAME}" \
         clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
      log "ClickHouse is ready"
      return 0
    fi
    sleep 2
  done
  log "ERROR: ClickHouse did not become ready within ${READY_TIMEOUT_SECONDS}s"
  sudo docker logs --tail 50 "${CONTAINER_NAME}" || true
  return 1
}

# --- Apply the Tier 0 DDL ---------------------------------------------------
apply_ddl() {
  if [[ ! -f "$DDL_FILE" ]]; then
    log "ERROR: DDL file not found: $DDL_FILE"
    return 1
  fi
  log "creating database ${TIER0_DB} and applying $(basename "$DDL_FILE")"
  sudo docker exec "${CONTAINER_NAME}" \
    clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${TIER0_DB}"
  # Substitute ${TIER0_DB} in the DDL and pipe it to clickhouse-client's stdin.
  # Only TIER0_DB is expanded so nothing else in the SQL is touched. Task 18 nit:
  # envsubst ships in the 'gettext' package, which is NOT guaranteed present on a
  # minimal Amazon Linux; install it, and fall back to a targeted sed if the
  # install is unavailable (the DDL uses exactly one placeholder, ${TIER0_DB}).
  if command -v envsubst >/dev/null 2>&1 || sudo yum install -y gettext >/dev/null 2>&1 && command -v envsubst >/dev/null 2>&1; then
    TIER0_DB="${TIER0_DB}" envsubst '${TIER0_DB}' < "$DDL_FILE" \
      | sudo docker exec -i "${CONTAINER_NAME}" clickhouse-client --multiquery
  else
    log "envsubst unavailable; substituting \${TIER0_DB} via sed fallback"
    sed "s/\${TIER0_DB}/${TIER0_DB}/g" "$DDL_FILE" \
      | sudo docker exec -i "${CONTAINER_NAME}" clickhouse-client --multiquery
  fi
  log "verifying ${TIER0_DB}.hits exists"
  sudo docker exec "${CONTAINER_NAME}" \
    clickhouse-client --query "EXISTS TABLE ${TIER0_DB}.hits" | grep -q '^1$'
  log "Tier 0 target ready: ${TIER0_DB}.hits (ENGINE=Null), CH ${TIER0_CH_VERSION}"
}

# --- Verify system-log tables are live --------------------------------------
# metric_log materializes after its first collect+flush cycle (~10s with the
# override above); poll briefly so a broken/ignored config override fails the
# bootstrap loudly instead of surfacing as missing capture data at run end.
verify_system_logs() {
  # Task 18 nit (a): first assert the config override file is actually mounted
  # INSIDE the container — a missing mount is the root cause of empty capture
  # tables, and asserting it here fails the bootstrap loudly at the source rather
  # than as absent data at run end.
  log "asserting config override present inside container"
  if ! sudo docker exec "${CONTAINER_NAME}" \
        test -f /etc/clickhouse-server/config.d/tier0_system_logs.xml; then
    log "ERROR: /etc/clickhouse-server/config.d/tier0_system_logs.xml not present in container — mount failed"
    return 1
  fi
  log "verifying system.metric_log is being populated (config override active)"
  local deadline=$(( $(date +%s) + 60 ))
  while (( $(date +%s) < deadline )); do
    if sudo docker exec "${CONTAINER_NAME}" clickhouse-client \
         --query "SELECT count() FROM system.metric_log" >/dev/null 2>&1; then
      log "system.metric_log live; query_log/part_log enabled by the same override"
      return 0
    fi
    sleep 5
  done
  log "ERROR: system.metric_log not present after 60s — config override not applied?"
  return 1
}

# --- Confirm private-IP reach + publish sidecars (task 18) ------------------
# The executors on the CORE nodes reach the Docker CH at the master's PRIVATE IP
# over the published 0.0.0.0:8123 bind. Prove the server answers on that IP (not
# just loopback) FROM THE MASTER, then publish two S3 sidecars the workflow reads
# back: the master IP (so the t0 submit can set CH_HOST) and a tier0_ready flag
# (so the workflow gates the t0 arm calls on TIER0_AVAILABLE). If either sidecar
# URI is unset (e.g. a manual/off-EMR run) we still verify reachability and log,
# but skip the S3 publish.
publish_sidecars() {
  local master_ip
  master_ip="$(hostname -i | awk '{print $1}')"
  log "confirming ClickHouse answers on the master private IP ${master_ip}:${TIER0_HTTP_PORT}"
  # curl may be absent; install is cheap and idempotent. Prove a real query.
  command -v curl >/dev/null 2>&1 || sudo yum install -y curl >/dev/null 2>&1 || true
  if ! curl -sf "http://${master_ip}:${TIER0_HTTP_PORT}/" --data-binary "SELECT 1" | grep -q '^1$'; then
    log "ERROR: ClickHouse did not answer on ${master_ip}:${TIER0_HTTP_PORT} — Tier 0 not reachable by executors"
    return 1
  fi
  log "reachable on ${master_ip}:${TIER0_HTTP_PORT}"
  if [[ -n "${TIER0_MASTER_IP_S3_URI}" ]]; then
    printf '%s\n' "${master_ip}" | aws s3 cp - "${TIER0_MASTER_IP_S3_URI}"
    log "published master IP sidecar ${TIER0_MASTER_IP_S3_URI}"
  else
    log "TIER0_MASTER_IP_S3_URI unset; skipping master-IP sidecar publish"
  fi
  # tier0_ready written LAST, only after everything above passed — its presence
  # is the workflow's TIER0_AVAILABLE=1 signal.
  if [[ -n "${TIER0_READY_S3_URI}" ]]; then
    printf 'ready\n' | aws s3 cp - "${TIER0_READY_S3_URI}"
    log "published tier0_ready sidecar ${TIER0_READY_S3_URI}"
  else
    log "TIER0_READY_S3_URI unset; skipping tier0_ready sidecar publish"
  fi
}

main() {
  ensure_docker
  write_log_config
  start_container
  wait_ready
  apply_ddl
  verify_system_logs
  publish_sidecars
  log "done"
}

main "$@"
