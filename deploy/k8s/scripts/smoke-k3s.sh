#!/usr/bin/env bash
#
# smoke-k3s.sh — apply the local-k3s overlay and prove the OHMF stage-1 profile
# comes up and serves a health endpoint on a single-node cluster.
#
# What it does:
#   1. applies deploy/k8s/overlays/local-k3s
#   2. waits for the key deployments to roll out (gateway, apps, postgres, redis)
#   3. port-forwards the gateway and curls /healthz
#   4. prints `kubectl get pods` and `kubectl get svc`
#   5. exits non-zero on any failure
#
# What it does NOT do:
#   - build or load images (do that first — see deploy/k8s/README.md)
#   - deploy Kafka/Cassandra or the message-processor worker (out of scope)
#   - delete anything you did not create with this script (teardown is opt-in)
#
# Portability: plain bash + kubectl + curl. On Windows run it from Git Bash or
# WSL. Requires a reachable single-node cluster (k3s/k3d/kind/Docker Desktop).
#
# Usage:
#   deploy/k8s/scripts/smoke-k3s.sh             # apply + smoke test
#   KEEP=0 deploy/k8s/scripts/smoke-k3s.sh      # also tear the namespace down at the end
#   deploy/k8s/scripts/smoke-k3s.sh --down      # only tear down, then exit

set -euo pipefail

NS="ohmf"
# Resolve the overlay path relative to this script so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERLAY="${SCRIPT_DIR}/../overlays/local-k3s"
LOCAL_PORT="${LOCAL_PORT:-18081}"
TARGET_PORT="8081"
PF_PID=""

log()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

teardown_cluster() {
  log "Tearing down namespace/${NS}"
  kubectl delete -k "${OVERLAY}" --ignore-not-found=true || true
}

cleanup() {
  # Always kill the port-forward we started.
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
  # Only delete cluster resources if the user explicitly opted in (KEEP=0).
  if [[ "${KEEP:-1}" == "0" ]]; then
    teardown_cluster
  fi
}
trap cleanup EXIT

# --down: explicit teardown only.
if [[ "${1:-}" == "--down" ]]; then
  teardown_cluster
  ok "Teardown complete."
  exit 0
fi

command -v kubectl >/dev/null 2>&1 || { fail "kubectl not found in PATH"; exit 1; }
command -v curl    >/dev/null 2>&1 || { fail "curl not found in PATH";    exit 1; }

if ! kubectl cluster-info --request-timeout=5s >/dev/null 2>&1; then
  fail "no reachable Kubernetes cluster. Start k3s/k3d/kind/Docker Desktop first."
  exit 1
fi

log "Applying overlay: ${OVERLAY}"
kubectl apply -k "${OVERLAY}"

log "Waiting for deployments to become available (timeout 180s each)"
# postgres/redis/apps/gateway should all reach Available. messages-processor is
# intentionally at replicas:0 in this profile, so it is NOT waited on.
rc=0
for dep in postgres redis apps gateway; do
  if kubectl -n "${NS}" rollout status "deploy/${dep}" --timeout=180s; then
    ok "deploy/${dep} rolled out"
  else
    fail "deploy/${dep} did not become ready"
    rc=1
  fi
done

log "kubectl get pods -n ${NS}"
kubectl -n "${NS}" get pods -o wide || true
log "kubectl get svc -n ${NS}"
kubectl -n "${NS}" get svc || true

if [[ "${rc}" -ne 0 ]]; then
  fail "one or more deployments failed to roll out; see events above"
  kubectl -n "${NS}" get events --sort-by=.lastTimestamp | tail -20 || true
  exit 1
fi

log "Port-forwarding svc/gateway ${LOCAL_PORT} -> ${TARGET_PORT}"
kubectl -n "${NS}" port-forward "svc/gateway" "${LOCAL_PORT}:${TARGET_PORT}" >/dev/null 2>&1 &
PF_PID=$!

# Give the port-forward a moment, then poll the health endpoint.
health_url="http://127.0.0.1:${LOCAL_PORT}/healthz"
log "Checking gateway health at ${health_url}"
attempt=0
until curl -fsS --max-time 3 "${health_url}" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [[ "${attempt}" -ge 15 ]]; then
    fail "gateway /healthz did not respond after ${attempt} attempts"
    exit 1
  fi
  sleep 1
done

body="$(curl -fsS --max-time 3 "${health_url}")"
ok "gateway /healthz -> ${body}"

log "Smoke check PASSED"
echo "Namespace '${NS}' left running. Tear down with:"
echo "  deploy/k8s/scripts/smoke-k3s.sh --down"
exit 0
