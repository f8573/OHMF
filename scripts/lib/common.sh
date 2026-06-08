#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GO_BIN="${REPO_ROOT}/ohmf/.tools/go/bin/go.exe"
DEFAULT_NAMESPACE="${DEFAULT_NAMESPACE:-ohmf}"
DEFAULT_LOCAL_PORT="${DEFAULT_LOCAL_PORT:-18080}"
DEFAULT_TARGET_PORT="${DEFAULT_TARGET_PORT:-8081}"
PF_PID=""

log() {
  printf '\n\033[1;34m==>\033[0m %s\n' "$*"
}

ok() {
  printf '\033[1;32m[ok]\033[0m %s\n' "$*"
}

fail() {
  printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 not found in PATH"
}

require_stage_a_tools() {
  require_cmd kubectl
  require_cmd python
  require_cmd curl
  [[ -x "${GO_BIN}" ]] || fail "Go toolchain not found at ${GO_BIN}"
}

ensure_cluster() {
  kubectl cluster-info --request-timeout=5s >/dev/null 2>&1 || fail "no healthy Kubernetes cluster is reachable"
}

cleanup_port_forward() {
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  PF_PID=""
}

trap cleanup_port_forward EXIT

start_gateway_port_forward() {
  local namespace="${1:-${DEFAULT_NAMESPACE}}"
  local local_port="${2:-${DEFAULT_LOCAL_PORT}}"
  local target_port="${3:-${DEFAULT_TARGET_PORT}}"

  cleanup_port_forward
  log "Port-forwarding svc/gateway ${local_port} -> ${target_port}"
  kubectl -n "${namespace}" port-forward svc/gateway "${local_port}:${target_port}" >/dev/null 2>&1 &
  PF_PID=$!

  local health_url="http://127.0.0.1:${local_port}/healthz"
  local attempt=0
  until curl -fsS --max-time 3 "${health_url}" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [[ "${attempt}" -ge 30 ]]; then
      fail "gateway /healthz did not respond after ${attempt} attempts"
    fi
    sleep 1
  done
  ok "gateway /healthz -> $(curl -fsS --max-time 3 "${health_url}")"
}

pg_count_for_conversation() {
  local conversation_id="$1"
  local namespace="${2:-${DEFAULT_NAMESPACE}}"
  kubectl -n "${namespace}" exec deploy/postgres -- sh -lc \
    "psql -U ohmf -d ohmf -t -A -c \"SELECT COUNT(*) FROM messages WHERE conversation_id = '${conversation_id}'::uuid;\""
}

cass_count_for_conversation() {
  local conversation_id="$1"
  local bucket="$2"
  local namespace="${3:-${DEFAULT_NAMESPACE}}"
  kubectl -n "${namespace}" exec deploy/cassandra -- sh -lc \
    "cqlsh -e \"CONSISTENCY ONE; SELECT COUNT(*) FROM ohmf_messages.messages_by_conversation WHERE conversation_id = ${conversation_id} AND bucket_yyyymmdd = '${bucket}';\""
}

kafka_group_lag() {
  local namespace="${1:-${DEFAULT_NAMESPACE}}"
  kubectl -n "${namespace}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1"
}

capture_env_snapshot() {
  local output_path="$1"
  python - "${output_path}" <<'PY'
import json
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone

output_path = sys.argv[1]

def run(*args):
    try:
        out = subprocess.check_output(args, stderr=subprocess.STDOUT, text=True).strip()
        return {"command": " ".join(args), "output": out}
    except Exception as exc:  # noqa: BLE001
        return {"command": " ".join(args), "error": str(exc)}

payload = {
    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "machine": {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "processor": platform.processor(),
    },
    "git_head": run("git", "rev-parse", "HEAD"),
    "kubectl": run("kubectl", "version", "--client"),
    "kubernetes": run("kubectl", "version"),
    "k3d": run("k3d", "version"),
}

with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

build_loadgen() {
  log "Building benchmarks/cmd/loadgen with ${GO_BIN}"
  "${GO_BIN}" build -o "${REPO_ROOT}/tmp/loadgen.exe" ./benchmarks/cmd/loadgen
}

run_loadgen() {
  local config_path="$1"
  shift || true
  "${REPO_ROOT}/tmp/loadgen.exe" -config "${config_path}" "$@"
}
