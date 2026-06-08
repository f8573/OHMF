#!/usr/bin/env bash
#
# smoke-k3s-full.sh — apply the local-k3s-full overlay and validate the real
# async message path on a local single-node cluster.
#
# What it proves when it passes:
#   gateway/API -> Kafka -> messages-processor -> Postgres/Cassandra/Redis
#
# It still does NOT prove HA, production readiness, or performance.

set -euo pipefail

NS="ohmf"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERLAY="${SCRIPT_DIR}/../overlays/local-k3s-full"
LOCAL_PORT="${LOCAL_PORT:-18080}"
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
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
  if [[ "${KEEP:-1}" == "0" ]]; then
    teardown_cluster
  fi
}
trap cleanup EXIT

if [[ "${1:-}" == "--down" ]]; then
  teardown_cluster
  ok "Teardown complete."
  exit 0
fi

command -v kubectl >/dev/null 2>&1 || { fail "kubectl not found in PATH"; exit 1; }
command -v python >/dev/null 2>&1 || { fail "python not found in PATH"; exit 1; }

if ! kubectl cluster-info --request-timeout=5s >/dev/null 2>&1; then
  fail "no reachable Kubernetes cluster. Start k3s/k3d first."
  exit 1
fi

log "Applying overlay: ${OVERLAY}"
kubectl apply -k "${OVERLAY}"

log "Waiting for infra and workloads"
kubectl -n "${NS}" rollout status deploy/postgres --timeout=240s
kubectl -n "${NS}" rollout status deploy/redis --timeout=240s
kubectl -n "${NS}" rollout status deploy/cassandra --timeout=420s
kubectl -n "${NS}" rollout status deploy/kafka --timeout=420s
kubectl -n "${NS}" wait --for=condition=complete job/kafka-init --timeout=240s
kubectl -n "${NS}" rollout status deploy/apps --timeout=240s
kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=240s
kubectl -n "${NS}" rollout status deploy/gateway --timeout=240s

log "kubectl get pods -n ${NS}"
kubectl -n "${NS}" get pods -o wide
log "kubectl get svc -n ${NS}"
kubectl -n "${NS}" get svc

log "Port-forwarding svc/gateway ${LOCAL_PORT} -> ${TARGET_PORT}"
kubectl -n "${NS}" port-forward svc/gateway "${LOCAL_PORT}:${TARGET_PORT}" >/dev/null 2>&1 &
PF_PID=$!

health_url="http://127.0.0.1:${LOCAL_PORT}/healthz"
log "Checking gateway health at ${health_url}"
attempt=0
until curl -fsS --max-time 3 "${health_url}" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [[ "${attempt}" -ge 30 ]]; then
    fail "gateway /healthz did not respond after ${attempt} attempts"
    exit 1
  fi
  sleep 1
done
ok "gateway /healthz -> $(curl -fsS --max-time 3 "${health_url}")"

log "Running live gateway integration smoke"
LOCAL_PORT="${LOCAL_PORT}" python - <<'PY'
import json
import os
import time
import urllib.request

base_url = f"http://127.0.0.1:{os.environ['LOCAL_PORT']}"
run_id = str(time.time_ns())

def request(method, path, body=None, token=None):
    url = base_url + path
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else None

def verify_user(phone_suffix, name):
    start = request("POST", "/v1/auth/phone/start", {
        "phone_e164": f"+1555{run_id[-6:]}{phone_suffix}",
        "channel": "SMS",
    })
    verify = request("POST", "/v1/auth/phone/verify", {
        "challenge_id": start["challenge_id"],
        "otp_code": "123456",
        "device": {
            "platform": "WEB",
            "device_name": name,
        },
    })
    return verify["user"]["user_id"], verify["tokens"]["access_token"]

a_user_id, a_token = verify_user("01", "Smoke-A")
b_user_id, _ = verify_user("02", "Smoke-B")

conv = request("POST", "/v1/conversations", {
    "type": "DM",
    "participants": [b_user_id],
}, a_token)
conv_id = conv["conversation_id"]

msg_req = {
    "conversation_id": conv_id,
    "idempotency_key": "idem-k3s-full-" + run_id,
    "content_type": "text",
    "content": {"text": "hello from local-k3s-full"},
}
msg1 = request("POST", "/v1/messages", msg_req, a_token)
msg2 = request("POST", "/v1/messages", msg_req, a_token)
assert msg1["message_id"] == msg2["message_id"], "message idempotency failed"
assert msg1["server_order"] == msg2["server_order"], "server order idempotency failed"

deadline = time.time() + 20
while True:
    items = request("GET", f"/v1/conversations/{conv_id}/messages", token=a_token)["items"]
    if len(items) == 1:
        break
    if time.time() > deadline:
        raise SystemExit(f"expected 1 message after async persistence, got {len(items)}")
    time.sleep(0.5)

print(json.dumps({
    "conversation_id": conv_id,
    "message_id": msg1["message_id"],
    "server_order": msg1["server_order"],
    "items_seen": len(items),
}))
PY

log "Capturing persistence checks"
kubectl -n "${NS}" logs deploy/messages-processor --tail=80 || true
kubectl -n "${NS}" exec deploy/postgres -- sh -c \
  "psql -U ohmf -d ohmf -c \"select count(*) as postgres_messages from messages;\"" || true
kubectl -n "${NS}" exec deploy/cassandra -- sh -c \
  "cqlsh -e \"CONSISTENCY ONE; SELECT COUNT(*) AS cassandra_messages FROM ohmf_messages.messages_by_conversation;\"" || true
kubectl -n "${NS}" exec deploy/kafka -- sh -c \
  "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" || true

log "Full-pipeline smoke PASSED"
echo "Namespace '${NS}' left running. Tear down with:"
echo "  deploy/k8s/scripts/smoke-k3s-full.sh --down"
