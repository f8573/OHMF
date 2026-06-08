#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
BASE_RESULT_DIR="${REPO_ROOT}/benchmarks/results/${RUN_DATE}-processor-stage-instrumentation"
DEPLOY_RESULT_MD="deploy/k8s/results/${RUN_DATE}-processor-stage-instrumentation.md"
WARMUP_DURATION_SECONDS="${WARMUP_DURATION_SECONDS:-15}"
MAIN_DURATION_SECONDS="${MAIN_DURATION_SECONDS:-120}"
KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS:-180}"
KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS:-4}"
NS="${NS:-ohmf}"
PRE_RUN_LAG_TIMEOUT_SECONDS="${PRE_RUN_LAG_TIMEOUT_SECONDS:-1800}"
PRE_RUN_LAG_POLL_SECONDS="${PRE_RUN_LAG_POLL_SECONDS:-10}"

mkdir -p "${BASE_RESULT_DIR}"

rates=(45 60)

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
      echo "pre-run lag did not drain to zero within ${PRE_RUN_LAG_TIMEOUT_SECONDS}s; remaining lag=${lag}" >&2
      return 1
    fi
    echo "waiting for pre-run lag to drain: ${lag}"
    sleep "${PRE_RUN_LAG_POLL_SECONDS}"
  done
}

shard_counts_for_rate() {
  case "$1" in
    45) echo "4,4,4,4,4,4,4,4,4,3,3,3" ;;
    60) echo "5,5,5,5,5,5,5,5,5,5,5,5" ;;
    *) return 1 ;;
  esac
}

per_pod_label_for_rate() {
  case "$1" in
    45) echo "mixed 3-4 msg/sec per pod" ;;
    60) echo "5 msg/sec per pod" ;;
    *) return 1 ;;
  esac
}

run_dirs=()
wait_for_zero_lag
for rate in "${rates[@]}"; do
  scenario="processor-stage-${rate}msgsec-multisource"
  result_dir="${BASE_RESULT_DIR}/${rate}msgsec"
  deploy_result_md="deploy/k8s/results/${RUN_DATE}-${scenario}.md"
  shard_user_counts="$(shard_counts_for_rate "${rate}")"
  per_pod_label="$(per_pod_label_for_rate "${rate}")"

  echo "=== processor stage diagnostic ${rate} msg/sec ==="
  SCENARIO_NAME="${scenario}" \
  RESULT_DIR="${result_dir}" \
  DEPLOY_RESULT_MD="${deploy_result_md}" \
  DEPLOY_TITLE="Local k3s processor stage diagnostic - ${rate} msg/sec multisource - ${RUN_DATE}" \
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
  MAIN_DURATION_SECONDS="${MAIN_DURATION_SECONDS}" \
  KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS}" \
  KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS}" \
  NS="${NS}" \
  bash "${REPO_ROOT}/deploy/k8s/scripts/run-throughput-multisource.sh"

  run_dirs+=("${result_dir}")
done

python "${REPO_ROOT}/benchmarks/scripts/summarize_processor_stage_instrumentation.py" \
  --output-dir "${BASE_RESULT_DIR}" \
  --deploy-result-md "${DEPLOY_RESULT_MD}" \
  --run-date "${RUN_DATE}" \
  --namespace "${NS}" \
  "${run_dirs[@]}"
