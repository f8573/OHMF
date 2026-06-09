#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
RUN_STAMP="${RUN_STAMP:-$(date -u +%Y%m%dt%H%M%S%6Nz)}"
RESULT_DIR="${REPO_ROOT}/benchmarks/results/${RUN_DATE}-processor-scaling-matrix"
DEPLOY_RESULT_MD="deploy/k8s/results/${RUN_DATE}-processor-scaling-matrix.md"
ROLL_OUT_JSON="${RESULT_DIR}/rollout-evidence.json"
NS="${NS:-ohmf}"
PRE_RUN_LAG_TIMEOUT_SECONDS="${PRE_RUN_LAG_TIMEOUT_SECONDS:-1800}"
PRE_RUN_LAG_POLL_SECONDS="${PRE_RUN_LAG_POLL_SECONDS:-10}"
KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS:-600}"
KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS:-4}"
WARMUP_DURATION_SECONDS="${WARMUP_DURATION_SECONDS:-15}"
IMAGE_TAG_SUFFIX="${IMAGE_TAG_SUFFIX:-procscale-${RUN_STAMP}}"
RESTORE_ORIGINAL_REPLICAS="${RESTORE_ORIGINAL_REPLICAS:-true}"

mkdir -p "${RESULT_DIR}"

K3D_CONTEXT="$(kubectl config current-context)"
K3D_CLUSTER="${K3D_CONTEXT#k3d-}"
K3D_SERVER_CONTAINER="k3d-${K3D_CLUSTER}-server-0"

gateway_image="ohmf-gateway:${IMAGE_TAG_SUFFIX}"
apps_image="ohmf-apps:${IMAGE_TAG_SUFFIX}"
processor_image="ohmf-messages-processor:${IMAGE_TAG_SUFFIX}"

replicas_matrix=(1 2 4)
rates=(105 120)
rung_dirs=()
ORIGINAL_REPLICAS="$(kubectl -n "${NS}" get deploy messages-processor -o jsonpath='{.spec.replicas}')"

current_kafka_lag() {
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" \
    | awk '$1=="messages-processor-v1" && $2=="msg.ingress.v1" && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}

wait_for_zero_lag() {
  local started now lag
  started="$(date +%s)"
  while true; do
    lag="$(current_kafka_lag)"
    if [[ "${lag}" == "0" ]]; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= PRE_RUN_LAG_TIMEOUT_SECONDS )); then
      echo "lag did not drain to zero within ${PRE_RUN_LAG_TIMEOUT_SECONDS}s; remaining lag=${lag}" >&2
      return 1
    fi
    echo "waiting for lag to drain: ${lag}"
    sleep "${PRE_RUN_LAG_POLL_SECONDS}"
  done
}

duration_for_rate() {
  case "$1" in
    105) echo "120" ;;
    120) echo "600" ;;
    *) return 1 ;;
  esac
}

shard_counts_for_rate() {
  case "$1" in
    105) echo "9,9,9,9,9,9,9,9,9,8,8,8" ;;
    120) echo "10,10,10,10,10,10,10,10,10,10,10,10" ;;
    *) return 1 ;;
  esac
}

per_pod_label_for_rate() {
  case "$1" in
    105) echo "mixed 8-9 msg/sec per pod" ;;
    120) echo "10 msg/sec per pod" ;;
    *) return 1 ;;
  esac
}

import_image() {
  local image_ref="$1"
  if command -v k3d >/dev/null 2>&1; then
    k3d image import "${image_ref}" -c "${K3D_CLUSTER}"
    return
  fi

  docker save "${image_ref}" | MSYS_NO_PATHCONV=1 docker exec -i "${K3D_SERVER_CONTAINER}" ctr -n k8s.io images import - >/dev/null
}

build_and_import_images() {
  echo "=== building uniquely tagged images ==="
  docker build -t "${gateway_image}" "${REPO_ROOT}/ohmf/services/gateway"
  docker build -t "${apps_image}" -f "${REPO_ROOT}/ohmf/services/apps/Dockerfile" "${REPO_ROOT}"
  docker build -t "${processor_image}" "${REPO_ROOT}/ohmf/services/messages-processor"

  import_image "${gateway_image}"
  import_image "${apps_image}"
  import_image "${processor_image}"
}

rollout_unique_images() {
  echo "=== forcing rollout onto uniquely tagged images ==="
  kubectl -n "${NS}" set image deploy/gateway gateway="${gateway_image}"
  kubectl -n "${NS}" set image deploy/apps apps="${apps_image}"
  kubectl -n "${NS}" set image deploy/messages-processor messages-processor="${processor_image}"

  kubectl -n "${NS}" rollout status deploy/gateway --timeout=300s
  kubectl -n "${NS}" rollout status deploy/apps --timeout=300s
  kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=300s
}

capture_rollout_evidence() {
  local stage_events_present="$1"
  python - "${ROLL_OUT_JSON}" "${NS}" "${K3D_CONTEXT}" "${gateway_image}" "${apps_image}" "${processor_image}" "${stage_events_present}" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone

output_path, namespace, cluster_context, gateway_image, apps_image, processor_image, stage_events_present = sys.argv[1:]

services = [
    ("gateway", "gateway", gateway_image),
    ("apps", "apps", apps_image),
    ("messages-processor", "messages-processor", processor_image),
]

def run(args):
    completed = subprocess.run(args, check=True, capture_output=True, text=True)
    return completed.stdout

def fetch_json(kind, name):
    return json.loads(run(["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]))

verified = True
service_entries = {}
for deployment_name, container_name, intended_image in services:
    selector = f"app.kubernetes.io/name={deployment_name}"
    pods = json.loads(run([
        "kubectl", "-n", namespace, "get", "pods",
        "-l", selector,
        "-o", "json",
    ]))
    matching_items = []
    for item in pods.get("items", []):
        if item.get("status", {}).get("phase") != "Running":
            continue
        for status in item.get("status", {}).get("containerStatuses", []):
            if status.get("name") != container_name:
                continue
            matching_items.append({
                "pod_name": item["metadata"]["name"],
                "pod_ip": item.get("status", {}).get("podIP"),
                "image": status.get("image"),
                "image_id": status.get("imageID"),
                "ready": status.get("ready"),
                "restart_count": status.get("restartCount", 0),
            })
    deployment_json = fetch_json("deploy", deployment_name)
    container_images = {
        container["name"]: container["image"]
        for container in deployment_json["spec"]["template"]["spec"]["containers"]
    }
    deployment_image = container_images.get(container_name, "")
    current_verified = bool(matching_items) and deployment_image == intended_image and all(
        item["image"] == intended_image and item["image_id"] for item in matching_items
    )
    verified = verified and current_verified
    service_entries[deployment_name] = {
        "intended_image": intended_image,
        "deployment_image": deployment_image,
        "verified": current_verified,
        "running_pods": matching_items,
    }

payload = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "namespace": namespace,
    "cluster_context": cluster_context,
    "stage_events_metric_present": stage_events_present.lower() == "true",
    "verified": verified,
    "services": service_entries,
}

with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

confirm_stage_events_metric() {
  local metrics_output
  metrics_output="$(kubectl -n "${NS}" exec deploy/messages-processor -- wget -qO- http://localhost:18088/metrics)"
  if ! grep -q "ohmf_messages_processor_stage_events_total" <<<"${metrics_output}"; then
    echo "messages-processor metrics do not expose ohmf_messages_processor_stage_events_total" >&2
    return 1
  fi
}

scale_processor_replicas() {
  local replicas="$1"
  echo "=== scaling messages-processor to ${replicas} replica(s) ==="
  kubectl -n "${NS}" scale deploy/messages-processor --replicas="${replicas}"
  kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=300s
}

capture_assignment_evidence() {
  local result_dir="$1"
  local stamp="$2"
  local obs_dir="${result_dir}/observations"
  mkdir -p "${obs_dir}"
  kubectl -n "${NS}" logs deploy/messages-processor --tail=200 > "${obs_dir}/messages-processor-logs-${stamp}.txt" || true
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" \
    > "${obs_dir}/messages-processor-consumer-group-${stamp}.txt" || true
  kubectl -n "${NS}" get pods -o wide > "${obs_dir}/pods-${stamp}.txt" || true
  kubectl -n "${NS}" get pods -o json > "${obs_dir}/pods-${stamp}.json" || true
}

restore_original_replicas() {
  if [[ "${RESTORE_ORIGINAL_REPLICAS}" != "true" ]]; then
    return
  fi
  if [[ -z "${ORIGINAL_REPLICAS}" ]]; then
    return
  fi
  kubectl -n "${NS}" scale deploy/messages-processor --replicas="${ORIGINAL_REPLICAS}" >/dev/null
  kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=300s >/dev/null
}

cleanup() {
  restore_original_replicas || true
}
trap cleanup EXIT

build_and_import_images
rollout_unique_images
confirm_stage_events_metric
capture_rollout_evidence "true"
wait_for_zero_lag

for replicas in "${replicas_matrix[@]}"; do
  scale_processor_replicas "${replicas}"
  wait_for_zero_lag

  for rate in "${rates[@]}"; do
    duration="$(duration_for_rate "${rate}")"
    scenario="processor-scaling-${replicas}replicas-${rate}msgsec-multisource"
    result_dir="${RESULT_DIR}/${replicas}replicas-${rate}msgsec"
    deploy_result_md="deploy/k8s/results/${RUN_DATE}-${scenario}.md"
    shard_user_counts="$(shard_counts_for_rate "${rate}")"
    per_pod_label="$(per_pod_label_for_rate "${rate}")"

    mkdir -p "${result_dir}/observations"
    capture_assignment_evidence "${result_dir}" "scale-ready"

    echo "=== processor-scaling rung replicas=${replicas} rate=${rate} msg/sec ==="
    SCENARIO_NAME="${scenario}" \
    RESULT_DIR="${result_dir}" \
    DEPLOY_RESULT_MD="${deploy_result_md}" \
    DEPLOY_TITLE="Local k3s processor-scaling rung - ${replicas} replicas - ${rate} msg/sec - ${RUN_DATE}" \
    BENCHMARK_LABEL="${scenario}" \
    JOB_NAME="loadgen-${scenario}" \
    SHARD_USER_COUNTS="${shard_user_counts}" \
    USERS_PER_SHARD=10 \
    WARMUP_USERS=5 \
    PER_USER_RATE=1 \
    PER_POD_RATE=1 \
    PER_POD_RATE_LABEL="${per_pod_label}" \
    LOADGEN_PODS=12 \
    AGGREGATE_TARGET_RATE="${rate}" \
    WARMUP_DURATION_SECONDS="${WARMUP_DURATION_SECONDS}" \
    MAIN_DURATION_SECONDS="${duration}" \
    KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS}" \
    KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS}" \
    CLEAN_RESULT_DIR=false \
    NS="${NS}" \
    bash "${REPO_ROOT}/deploy/k8s/scripts/run-throughput-multisource.sh"

    capture_assignment_evidence "${result_dir}" "post-run"
    rung_dirs+=("${result_dir}")

    wait_for_zero_lag
    capture_assignment_evidence "${result_dir}" "post-drain"
  done
done

python "${REPO_ROOT}/benchmarks/scripts/summarize_processor_scaling_matrix.py" \
  --output-dir "${RESULT_DIR}" \
  --deploy-result-md "${DEPLOY_RESULT_MD}" \
  --run-date "${RUN_DATE}" \
  --namespace "${NS}" \
  --rollout-evidence "${ROLL_OUT_JSON}" \
  "${rung_dirs[@]}"
