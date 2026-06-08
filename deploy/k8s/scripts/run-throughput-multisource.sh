#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${REPO_ROOT}/scripts/lib/common.sh"

NS="${NS:-ohmf}"
LOADGEN_PODS="${LOADGEN_PODS:-12}"
USERS_PER_SHARD="${USERS_PER_SHARD:-10}"
SHARD_USER_COUNTS="${SHARD_USER_COUNTS:-}"
WARMUP_USERS="${WARMUP_USERS:-5}"
PER_USER_RATE="${PER_USER_RATE:-1}"
PER_POD_RATE="${PER_POD_RATE:-10}"
PER_POD_RATE_LABEL="${PER_POD_RATE_LABEL:-${PER_POD_RATE} msg/sec per active shard}"
WARMUP_DURATION_SECONDS="${WARMUP_DURATION_SECONDS:-60}"
MAIN_DURATION_SECONDS="${MAIN_DURATION_SECONDS:-600}"
AGGREGATE_TARGET_RATE="${AGGREGATE_TARGET_RATE:-120}"
KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS:-180}"
KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS:-2}"
RUN_DATE="$(date -u +%F)"
RUN_STAMP="$(date -u +%Y%m%dt%H%M%sz)"
SCENARIO_NAME="${SCENARIO_NAME:-sustained-120msgsec-multisource}"
JOB_NAME="${JOB_NAME:-loadgen-${SCENARIO_NAME}}"
RUN_ID_PREFIX="${RUN_ID_PREFIX:-${RUN_STAMP}-multisource}"
RESULT_DIR="${RESULT_DIR:-${REPO_ROOT}/benchmarks/results/${RUN_DATE}-${SCENARIO_NAME}}"
DEPLOY_RESULT_MD="${DEPLOY_RESULT_MD:-deploy/k8s/results/${RUN_DATE}-${SCENARIO_NAME}.md}"
DEPLOY_TITLE="${DEPLOY_TITLE:-Local k3s throughput result - ${AGGREGATE_TARGET_RATE} msg/sec multisource - ${RUN_DATE}}"
BENCHMARK_LABEL="${BENCHMARK_LABEL:-${SCENARIO_NAME}}"
MESSAGE_TEXT="${MESSAGE_TEXT:-m4 sustained multisource local k3s validation}"
OBS_DIR="${RESULT_DIR}/observations"
SHARDS_DIR="${RESULT_DIR}/shards"
K3D_CONTEXT="$(kubectl config current-context)"
K3D_CLUSTER="${K3D_CONTEXT#k3d-}"
K3D_SERVER_CONTAINER="k3d-${K3D_CLUSTER}-server-0"
SAMPLER_PID=""

require_stage_a_tools
require_cmd docker
ensure_cluster

rm -rf "${RESULT_DIR}"
mkdir -p "${OBS_DIR}" "${SHARDS_DIR}"

capture_snapshot() {
  local stamp="$1"
  kubectl -n "${NS}" get pods -o wide > "${OBS_DIR}/pods-${stamp}.txt"
  kubectl -n "${NS}" get pods -o json > "${OBS_DIR}/pods-${stamp}.json"
  kubectl -n "${NS}" top pods > "${OBS_DIR}/top-pods-${stamp}.txt"
}

capture_hpa() {
  local stamp="$1"
  if kubectl -n "${NS}" get hpa >/dev/null 2>&1; then
    kubectl -n "${NS}" get hpa -o wide > "${OBS_DIR}/hpa-${stamp}.txt"
  else
    printf 'No resources found in %s namespace.\n' "${NS}" > "${OBS_DIR}/hpa-${stamp}.txt"
  fi
}

capture_kafka_lag() {
  local stamp="$1"
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" \
    > "${OBS_DIR}/kafka-lag-${stamp}.txt"
}

capture_processor_diagnostics() {
  local stamp="$1"
  kubectl -n "${NS}" logs deploy/messages-processor --tail=2000 > "${OBS_DIR}/messages-processor-logs-${stamp}.txt" || true
  kubectl -n "${NS}" exec deploy/messages-processor -- wget -qO- http://localhost:18088/metrics > "${OBS_DIR}/messages-processor-metrics-${stamp}.txt" || true
  kubectl -n "${NS}" exec deploy/messages-processor -- wget -qO- http://localhost:18088/readyz > "${OBS_DIR}/messages-processor-readyz-${stamp}.txt" || true
  kubectl -n "${NS}" get deploy messages-processor -o yaml > "${OBS_DIR}/messages-processor-deployment-${stamp}.yaml" || true
}

capture_gateway_diagnostics() {
  local stamp="$1"
  kubectl -n "${NS}" logs deploy/gateway --tail=2000 > "${OBS_DIR}/gateway-logs-${stamp}.txt" || true
}

capture_topic_diagnostics() {
  local stamp="$1"
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic msg.ingress.v1" \
    > "${OBS_DIR}/kafka-topic-msg.ingress.v1-${stamp}.txt" || true
}

capture_database_diagnostics() {
  local stamp="$1"
  kubectl -n "${NS}" exec deploy/postgres -- psql -U ohmf -d ohmf -At -c "select count(*) from messages;" > "${OBS_DIR}/postgres-messages-count-${stamp}.txt" || true
  kubectl -n "${NS}" exec deploy/postgres -- psql -U ohmf -d ohmf -At -c "select state, count(*) from pg_stat_activity group by state order by state;" > "${OBS_DIR}/postgres-activity-${stamp}.txt" || true
  kubectl -n "${NS}" exec deploy/cassandra -- sh -lc "/opt/cassandra/bin/cqlsh -e \"CONSISTENCY ONE; SELECT COUNT(*) FROM ohmf_messages.messages_by_conversation;\"" > "${OBS_DIR}/cassandra-count-${stamp}.txt" || true
}

cleanup_job() {
  kubectl -n "${NS}" delete job "${JOB_NAME}" --ignore-not-found=true >/dev/null 2>&1 || true
}

import_image() {
  if command -v k3d >/dev/null 2>&1; then
    k3d image import ohmf-loadgen:dev -c "${K3D_CLUSTER}"
    return
  fi

  docker save ohmf-loadgen:dev | MSYS_NO_PATHCONV=1 docker exec -i "${K3D_SERVER_CONTAINER}" ctr -n k8s.io images import - >/dev/null
}

cleanup_job

sampler() {
  while true; do
    local succeeded failed stamp
    succeeded="$(kubectl -n "${NS}" get job "${JOB_NAME}" -o jsonpath='{.status.succeeded}' 2>/dev/null || printf '0')"
    failed="$(kubectl -n "${NS}" get job "${JOB_NAME}" -o jsonpath='{.status.failed}' 2>/dev/null || printf '0')"
    stamp="$(date -u +%H%M%S)"
    capture_snapshot "${stamp}"
    if [[ "${succeeded:-0}" == "${LOADGEN_PODS}" ]] || [[ "${failed:-0}" != "0" ]]; then
      break
    fi
    sleep 120
  done
}

cleanup() {
  if [[ -n "${SAMPLER_PID}" ]] && kill -0 "${SAMPLER_PID}" 2>/dev/null; then
    kill "${SAMPLER_PID}" 2>/dev/null || true
    wait "${SAMPLER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log "Capturing pre-run cluster state"
kubectl version --client > "${OBS_DIR}/kubectl-version.txt"
kubectl version > "${OBS_DIR}/kubernetes-version.txt"
kubectl get nodes -o wide > "${OBS_DIR}/nodes.txt"
capture_snapshot "before"
capture_hpa "before"
capture_kafka_lag "before"
capture_processor_diagnostics "before"
capture_gateway_diagnostics "before"
capture_topic_diagnostics "before"
capture_database_diagnostics "before"

log "Building Linux loadgen image"
docker build -t ohmf-loadgen:dev -f "${REPO_ROOT}/benchmarks/loadgen.Dockerfile" "${REPO_ROOT}"

log "Importing loadgen image into k3d cluster ${K3D_CLUSTER}"
import_image

log "Applying indexed multisource loadgen job"
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NS}
spec:
  backoffLimit: 0
  completions: ${LOADGEN_PODS}
  parallelism: ${LOADGEN_PODS}
  completionMode: Indexed
  template:
    metadata:
      labels:
        app: ohmf-loadgen
        benchmark: ${BENCHMARK_LABEL}
    spec:
      restartPolicy: Never
      containers:
        - name: loadgen
          image: ohmf-loadgen:dev
          imagePullPolicy: IfNotPresent
          env:
            - name: JOB_COMPLETION_INDEX
              valueFrom:
                fieldRef:
                  fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
            - name: NAMESPACE
              value: "${NS}"
            - name: BASE_URL
              value: "http://gateway.${NS}.svc.cluster.local:8081"
            - name: POSTGRES_DSN
              value: "postgres://ohmf:ohmf@postgres:5432/ohmf?sslmode=disable"
            - name: JWT_SECRET
              value: "dev-only-change-me"
            - name: RUN_ID_PREFIX
              value: "${RUN_ID_PREFIX}"
            - name: USERS_PER_SHARD
              value: "${USERS_PER_SHARD}"
            - name: SHARD_USER_COUNTS
              value: "${SHARD_USER_COUNTS}"
            - name: WARMUP_USERS
              value: "${WARMUP_USERS}"
            - name: PER_USER_RATE
              value: "${PER_USER_RATE}"
            - name: WARMUP_DURATION_SECONDS
              value: "${WARMUP_DURATION_SECONDS}"
            - name: MAIN_DURATION_SECONDS
              value: "${MAIN_DURATION_SECONDS}"
            - name: MESSAGE_TEXT
              value: "${MESSAGE_TEXT}"
EOF

log "Sampling cluster state during the run"
sampler &
SAMPLER_PID=$!

log "Waiting for indexed loadgen job completion"
kubectl -n "${NS}" wait --for=condition=complete "job/${JOB_NAME}" --timeout=1800s
wait "${SAMPLER_PID}"
SAMPLER_PID=""

log "Capturing post-run cluster state"
capture_snapshot "after"
capture_hpa "after"
capture_kafka_lag "after"
capture_processor_diagnostics "after"
capture_gateway_diagnostics "after"
capture_topic_diagnostics "after"
capture_database_diagnostics "after"
kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o wide > "${OBS_DIR}/loadgen-pods.txt"
kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o json > "${OBS_DIR}/loadgen-pods.json"

log "Collecting shard logs"
while IFS= read -r pod_name; do
  kubectl -n "${NS}" logs "${pod_name}" > "${SHARDS_DIR}/${pod_name}.log"
done < <(kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

SYSTEM_UNDER_TEST_COMMIT="$(git rev-parse HEAD)"
ARTIFACT_HEAD="${SYSTEM_UNDER_TEST_COMMIT}"

log "Aggregating shard summaries and performing host-side reconciliation"
python "${REPO_ROOT}/benchmarks/scripts/aggregate_multisource_results.py" \
  --result-dir "${RESULT_DIR}" \
  --namespace "${NS}" \
  --job-name "${JOB_NAME}" \
  --loadgen-pods "${LOADGEN_PODS}" \
  --per-pod-rate "${PER_POD_RATE}" \
  --main-duration-seconds "${MAIN_DURATION_SECONDS}" \
  --aggregate-target-rate "${AGGREGATE_TARGET_RATE}" \
  --scenario-name "${SCENARIO_NAME}" \
  --deploy-result-md "${DEPLOY_RESULT_MD}" \
  --deploy-title "${DEPLOY_TITLE}" \
  --per-pod-rate-label "${PER_POD_RATE_LABEL}" \
  --overlay "deploy/k8s/overlays/local-k3s-full" \
  --cluster-name "${K3D_CLUSTER}" \
  --cluster-context "${K3D_CONTEXT}" \
  --system-under-test-commit "${SYSTEM_UNDER_TEST_COMMIT}" \
  --artifact-head "${ARTIFACT_HEAD}" \
  --run-id-prefix "${RUN_ID_PREFIX}" \
  --run-date "${RUN_DATE}" \
  --principal-provisioning-mode "seed_db" \
  --kafka-timeout-seconds "${KAFKA_TIMEOUT_SECONDS}" \
  --kafka-poll-seconds "${KAFKA_POLL_SECONDS}"

log "Capturing settled post-reconciliation state"
capture_snapshot "settled"
capture_hpa "settled"
capture_kafka_lag "settled"
capture_processor_diagnostics "settled"
capture_gateway_diagnostics "settled"
capture_topic_diagnostics "settled"
capture_database_diagnostics "settled"

ok "Multisource throughput artifacts written to ${RESULT_DIR}"
