#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${REPO_ROOT}/scripts/lib/common.sh"

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
RUN_STAMP="${RUN_STAMP:-$(date -u +%Y%m%dt%H%M%S%6Nz)}"
NS="${NS:-ohmf}"
SCENARIO_NAME="${SCENARIO_NAME:-processor-pod-deletion-120msgsec}"
RESULT_DIR="${RESULT_DIR:-${REPO_ROOT}/benchmarks/results/${RUN_DATE}-${SCENARIO_NAME}}"
DEPLOY_RESULT_MD="${DEPLOY_RESULT_MD:-deploy/k8s/results/${RUN_DATE}-${SCENARIO_NAME}.md}"
DEPLOY_TITLE="${DEPLOY_TITLE:-Local k3s processor pod deletion result - 120 msg/sec - ${RUN_DATE}}"
BENCHMARK_LABEL="${BENCHMARK_LABEL:-${SCENARIO_NAME}}"
JOB_NAME="${JOB_NAME:-loadgen-${SCENARIO_NAME}}"
RUN_ID_PREFIX="${RUN_ID_PREFIX:-${RUN_STAMP}-poddelete}"
LOADGEN_PODS="${LOADGEN_PODS:-12}"
USERS_PER_SHARD="${USERS_PER_SHARD:-10}"
SHARD_USER_COUNTS="${SHARD_USER_COUNTS:-}"
WARMUP_USERS="${WARMUP_USERS:-5}"
PER_USER_RATE="${PER_USER_RATE:-1}"
PER_POD_RATE="${PER_POD_RATE:-1}"
PER_POD_RATE_LABEL="${PER_POD_RATE_LABEL:-10 msg/sec per pod}"
WARMUP_DURATION_SECONDS="${WARMUP_DURATION_SECONDS:-60}"
MAIN_DURATION_SECONDS="${MAIN_DURATION_SECONDS:-600}"
AGGREGATE_TARGET_RATE="${AGGREGATE_TARGET_RATE:-120}"
KAFKA_TIMEOUT_SECONDS="${KAFKA_TIMEOUT_SECONDS:-1800}"
KAFKA_POLL_SECONDS="${KAFKA_POLL_SECONDS:-10}"
DELETION_OFFSET_SECONDS="${DELETION_OFFSET_SECONDS:-150}"
CLEAN_RESULT_DIR="${CLEAN_RESULT_DIR:-true}"

K3D_CONTEXT="$(kubectl config current-context)"
K3D_CLUSTER="${K3D_CONTEXT#k3d-}"
K3D_SERVER_CONTAINER="k3d-${K3D_CLUSTER}-server-0"

gateway_image="ohmf-gateway:poddelete-${RUN_STAMP}"
apps_image="ohmf-apps:poddelete-${RUN_STAMP}"
processor_image="ohmf-messages-processor:poddelete-${RUN_STAMP}"

OBS_DIR="${RESULT_DIR}/observations"
SHARDS_DIR="${RESULT_DIR}/shards"
MANIFEST_JSON="${OBS_DIR}/run-manifest.json"
DELETION_EVENT_JSON="${OBS_DIR}/deletion-event.json"
LAG_RECOVERY_JSON="${OBS_DIR}/kafka-lag-recovery.json"
MONITOR_PID=""
DELETION_PID=""
RUN_STATUS="passed"

require_cmd docker
require_cmd python
ensure_cluster

if [[ "${CLEAN_RESULT_DIR}" == "true" ]]; then
  rm -rf "${RESULT_DIR}"
fi
mkdir -p "${OBS_DIR}" "${SHARDS_DIR}"

import_image() {
  local image_ref="$1"
  if command -v k3d >/dev/null 2>&1; then
    k3d image import "${image_ref}" -c "${K3D_CLUSTER}"
    return
  fi
  docker save "${image_ref}" | MSYS_NO_PATHCONV=1 docker exec -i "${K3D_SERVER_CONTAINER}" ctr -n k8s.io images import - >/dev/null
}

current_kafka_lag() {
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" \
    | awk '$1=="messages-processor-v1" && $2=="msg.ingress.v1" && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}

capture_pods_state() {
  local stamp="$1"
  kubectl -n "${NS}" get pods -o wide > "${OBS_DIR}/pods-${stamp}.txt"
  kubectl -n "${NS}" get pods -o json > "${OBS_DIR}/pods-${stamp}.json"
  kubectl -n "${NS}" top pods > "${OBS_DIR}/top-pods-${stamp}.txt"
}

capture_kafka_state() {
  local stamp="$1"
  kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
    "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" \
    > "${OBS_DIR}/kafka-consumer-group-${stamp}.txt" || true
}

capture_processor_metrics() {
  local stamp="$1"
  local pod_name
  while IFS= read -r pod_name; do
    [[ -n "${pod_name}" ]] || continue
    kubectl -n "${NS}" exec "${pod_name}" -- wget -qO- http://localhost:18088/metrics > "${OBS_DIR}/messages-processor-metrics-${stamp}-${pod_name}.txt" || true
    kubectl -n "${NS}" exec "${pod_name}" -- wget -qO- http://localhost:18088/readyz > "${OBS_DIR}/messages-processor-readyz-${stamp}-${pod_name}.txt" || true
  done < <(kubectl -n "${NS}" get pods -l app.kubernetes.io/name=messages-processor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
}

capture_processor_logs() {
  local stamp="$1"
  local pod_name
  while IFS= read -r pod_name; do
    [[ -n "${pod_name}" ]] || continue
    kubectl -n "${NS}" logs "${pod_name}" --tail=1200 > "${OBS_DIR}/messages-processor-logs-${stamp}-${pod_name}.txt" || true
  done < <(kubectl -n "${NS}" get pods -l app.kubernetes.io/name=messages-processor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
}

capture_processor_rollup() {
  local stamp="$1"
  capture_pods_state "${stamp}"
  capture_kafka_state "${stamp}"
  capture_processor_metrics "${stamp}"
  capture_processor_logs "${stamp}"
}

verify_stage_events_metric_visible() {
  local pod_name
  while IFS= read -r pod_name; do
    [[ -n "${pod_name}" ]] || continue
    if ! kubectl -n "${NS}" exec "${pod_name}" -- sh -lc 'wget -qO- http://localhost:18088/metrics | grep -q "ohmf_messages_processor_stage_events_total"' ; then
      fail "stage events metric not visible from ${pod_name}"
    fi
  done < <(kubectl -n "${NS}" get pods -l app.kubernetes.io/name=messages-processor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
}

wait_for_consumer_group_assignment() {
  local expected_members="$1"
  local timeout_seconds="${2:-1800}"
  local poll_seconds="${3:-10}"
  local started now
  started="$(date +%s)"
  while true; do
    local group_state status_line member_count assigned_partitions total_partitions
    group_state="$(kubectl -n "${NS}" exec deploy/kafka -- sh -lc \
      "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group messages-processor-v1" 2>/dev/null || true)"
    status_line="$(python - "${expected_members}" "${group_state}" <<'PY'
import re
import sys

expected = int(sys.argv[1])
raw_text = sys.argv[2]
assignments = {}
assigned = 0
total = 0
for raw in raw_text.splitlines():
    line = raw.strip()
    if not line or line.startswith("GROUP") or line.startswith("Warning:"):
        continue
    fields = re.split(r"\s+", line)
    if len(fields) < 9 or fields[0] != "messages-processor-v1" or fields[1] != "msg.ingress.v1":
        continue
    total += 1
    member = fields[6]
    if member == "-":
        continue
    assigned += 1
    assignments.setdefault(member, 0)
    assignments[member] += 1
print(f"{len(assignments)} {assigned} {total}")
PY
)"
    read -r member_count assigned_partitions total_partitions <<<"${status_line}"
    if (( member_count >= expected_members && total_partitions > 0 && assigned_partitions == total_partitions )); then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "consumer group did not stabilize within ${timeout_seconds}s; members=${member_count}, assigned=${assigned_partitions}, total=${total_partitions}" >&2
      return 1
    fi
    sleep "${poll_seconds}"
  done
}

wait_for_zero_lag() {
  local timeout_seconds="$1"
  local poll_seconds="$2"
  local started_at_epoch now_epoch lag elapsed
  started_at_epoch="$(python - <<'PY'
import time
print(time.time())
PY
)"
  while true; do
    lag="$(current_kafka_lag)"
    if [[ "${lag}" == "0" ]]; then
      elapsed="$(python - "${started_at_epoch}" <<'PY'
import sys
import time

started = float(sys.argv[1])
print(round(time.time() - started, 6))
PY
)"
      printf '{"settled_to_zero": true, "lag_at_end": 0, "lag_zero_seconds": %s, "started_at_epoch": %s, "captured_at_utc": "%s"}\n' \
        "${elapsed}" "${started_at_epoch}" "$(date -u +%FT%TZ)" > "${LAG_RECOVERY_JSON}"
      return 0
    fi
    now_epoch="$(python - <<'PY'
import time
print(time.time())
PY
)"
    elapsed="$(python - "${started_at_epoch}" "${now_epoch}" <<'PY'
import sys

started = float(sys.argv[1])
now = float(sys.argv[2])
print(round(now - started, 6))
PY
)"
    if python - "${started_at_epoch}" "${now_epoch}" "${timeout_seconds}" <<'PY'
import sys

started = float(sys.argv[1])
now = float(sys.argv[2])
timeout_seconds = float(sys.argv[3])
raise SystemExit(0 if now - started >= timeout_seconds else 1)
PY
    then
      printf '{"settled_to_zero": false, "lag_at_end": %s, "lag_zero_seconds": %s, "started_at_epoch": %s, "captured_at_utc": "%s"}\n' \
        "${lag}" "${elapsed}" "${started_at_epoch}" "$(date -u +%FT%TZ)" > "${LAG_RECOVERY_JSON}"
      echo "lag did not drain to zero within ${timeout_seconds}s; remaining lag=${lag}" >&2
      return 1
    fi
    sleep "${poll_seconds}"
  done
}

build_and_import_images() {
  log "Building uniquely tagged images"
  docker build -t "${gateway_image}" "${REPO_ROOT}/ohmf/services/gateway"
  docker build -t "${apps_image}" -f "${REPO_ROOT}/ohmf/services/apps/Dockerfile" "${REPO_ROOT}"
  docker build -t "${processor_image}" "${REPO_ROOT}/ohmf/services/messages-processor"

  log "Importing images into ${K3D_CLUSTER}"
  import_image "${gateway_image}"
  import_image "${apps_image}"
  import_image "${processor_image}"
}

rollout_unique_images() {
  log "Forcing rollout onto uniquely tagged images"
  kubectl -n "${NS}" set image deploy/gateway gateway="${gateway_image}"
  kubectl -n "${NS}" set image deploy/apps apps="${apps_image}"
  kubectl -n "${NS}" set image deploy/messages-processor messages-processor="${processor_image}"

  kubectl -n "${NS}" rollout status deploy/gateway --timeout=300s
  kubectl -n "${NS}" rollout status deploy/apps --timeout=300s
  kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=300s
}

scale_processor_to_four() {
  log "Scaling messages-processor to 4 replicas"
  kubectl -n "${NS}" scale deploy/messages-processor --replicas=4
  kubectl -n "${NS}" rollout status deploy/messages-processor --timeout=300s
  wait_for_consumer_group_assignment 4 900 10
}

write_manifest() {
  cat > "${MANIFEST_JSON}" <<EOF
{
  "run_date": "${RUN_DATE}",
  "run_stamp": "${RUN_STAMP}",
  "scenario_name": "${SCENARIO_NAME}",
  "namespace": "${NS}",
  "cluster_context": "${K3D_CONTEXT}",
  "cluster_name": "${K3D_CLUSTER}",
  "result_dir": "${RESULT_DIR}",
  "deploy_result_md": "${DEPLOY_RESULT_MD}",
  "deploy_title": "${DEPLOY_TITLE}",
  "benchmark_label": "${BENCHMARK_LABEL}",
  "job_name": "${JOB_NAME}",
  "run_id_prefix": "${RUN_ID_PREFIX}",
  "loadgen_pods": ${LOADGEN_PODS},
  "users_per_shard": ${USERS_PER_SHARD},
  "warmup_users": ${WARMUP_USERS},
  "per_user_rate": ${PER_USER_RATE},
  "per_pod_rate": ${PER_POD_RATE},
  "per_pod_rate_label": "${PER_POD_RATE_LABEL}",
  "warmup_duration_seconds": ${WARMUP_DURATION_SECONDS},
  "main_duration_seconds": ${MAIN_DURATION_SECONDS},
  "aggregate_target_rate": ${AGGREGATE_TARGET_RATE},
  "kafka_timeout_seconds": ${KAFKA_TIMEOUT_SECONDS},
  "kafka_poll_seconds": ${KAFKA_POLL_SECONDS},
  "deletion_offset_seconds": ${DELETION_OFFSET_SECONDS},
  "gateway_image": "${gateway_image}",
  "apps_image": "${apps_image}",
  "processor_image": "${processor_image}"
}
EOF
}

cleanup() {
  if [[ -n "${MONITOR_PID}" ]] && kill -0 "${MONITOR_PID}" 2>/dev/null; then
    kill "${MONITOR_PID}" 2>/dev/null || true
    wait "${MONITOR_PID}" 2>/dev/null || true
  fi
  if [[ -n "${DELETION_PID}" ]] && kill -0 "${DELETION_PID}" 2>/dev/null; then
    kill "${DELETION_PID}" 2>/dev/null || true
    wait "${DELETION_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

cleanup_job() {
  kubectl -n "${NS}" delete job "${JOB_NAME}" --ignore-not-found=true >/dev/null 2>&1 || true
}

import_loadgen_image() {
  if command -v k3d >/dev/null 2>&1; then
    k3d image import ohmf-loadgen:dev -c "${K3D_CLUSTER}"
    return
  fi
  docker save ohmf-loadgen:dev | MSYS_NO_PATHCONV=1 docker exec -i "${K3D_SERVER_CONTAINER}" ctr -n k8s.io images import - >/dev/null
}

start_loadgen_job() {
  log "Building and importing loadgen image"
  docker build -t ohmf-loadgen:dev -f "${REPO_ROOT}/benchmarks/loadgen.Dockerfile" "${REPO_ROOT}"
  import_loadgen_image

  log "Creating indexed multisource loadgen job"
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
              value: "m4 sustained multisource local k3s validation"
EOF
}

start_monitor() {
  (
    local poll_index=0
    while [[ ! -f "${OBS_DIR}/loadgen-complete.flag" ]]; do
      local stamp
      stamp="$(date -u +%H%M%S)"
      capture_kafka_state "poll-${poll_index}-${stamp}"
      capture_pods_state "poll-${poll_index}-${stamp}"
      poll_index=$((poll_index + 1))
      sleep "${KAFKA_POLL_SECONDS}"
    done
  ) &
  MONITOR_PID=$!
}

delete_processor_pod_mid_run() {
  (
    sleep "${DELETION_OFFSET_SECONDS}"
    local pre_delete_stamp delete_stamp recovery_stamp target_pod deletion_requested_at
    pre_delete_stamp="pre-delete"
    delete_stamp="delete"
    recovery_stamp="during-rebalance"

    capture_pods_state "${pre_delete_stamp}"
    capture_kafka_state "${pre_delete_stamp}"
    capture_processor_metrics "${pre_delete_stamp}"
    capture_processor_logs "${pre_delete_stamp}"

    target_pod="$(kubectl -n "${NS}" get pods -l app.kubernetes.io/name=messages-processor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort | head -n 1)"
    deletion_requested_at="$(date -u +%FT%TZ)"

    cat > "${DELETION_EVENT_JSON}" <<EOF
{
  "deleted_pod": "${target_pod}",
  "deletion_requested_at_utc": "${deletion_requested_at}",
  "deletion_offset_seconds": ${DELETION_OFFSET_SECONDS}
}
EOF

    log "Deleting ${target_pod} at ${deletion_requested_at}"
    kubectl -n "${NS}" delete pod "${target_pod}" --wait=false >/dev/null

    capture_pods_state "${delete_stamp}"
    capture_kafka_state "${delete_stamp}"
    capture_processor_metrics "${delete_stamp}"
    capture_processor_logs "${delete_stamp}"

    wait_for_consumer_group_assignment 4 900 10 || RUN_STATUS="failed"

    capture_pods_state "${recovery_stamp}"
    capture_kafka_state "${recovery_stamp}"
    capture_processor_metrics "${recovery_stamp}"
    capture_processor_logs "${recovery_stamp}"
  ) &
  DELETION_PID=$!
}

main() {
  cleanup_job
  build_and_import_images
  rollout_unique_images
  scale_processor_to_four
  verify_stage_events_metric_visible

  write_manifest
  capture_processor_rollup "before"

  start_loadgen_job
  start_monitor
  delete_processor_pod_mid_run

  if ! kubectl -n "${NS}" wait --for=condition=complete "job/${JOB_NAME}" --timeout=2400s; then
    RUN_STATUS="failed"
  fi

  touch "${OBS_DIR}/loadgen-complete.flag"
  if [[ -n "${MONITOR_PID}" ]] && kill -0 "${MONITOR_PID}" 2>/dev/null; then
    wait "${MONITOR_PID}" || true
    MONITOR_PID=""
  fi
  if [[ -n "${DELETION_PID}" ]] && kill -0 "${DELETION_PID}" 2>/dev/null; then
    wait "${DELETION_PID}" || true
    DELETION_PID=""
  fi

  if ! wait_for_zero_lag "${KAFKA_TIMEOUT_SECONDS}" "${KAFKA_POLL_SECONDS}"; then
    RUN_STATUS="failed"
  fi

  capture_processor_rollup "settled"

  kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o wide > "${OBS_DIR}/loadgen-pods.txt" || true
  kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o json > "${OBS_DIR}/loadgen-pods.json" || true

  local loadgen_pod
  while IFS= read -r loadgen_pod; do
    [[ -n "${loadgen_pod}" ]] || continue
    kubectl -n "${NS}" logs "${loadgen_pod}" > "${SHARDS_DIR}/${loadgen_pod}.log" || true
  done < <(kubectl -n "${NS}" get pods -l job-name="${JOB_NAME}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)

  log "Aggregating pod-deletion resilience results"
  python "${REPO_ROOT}/benchmarks/scripts/summarize_processor_pod_deletion_resilience.py" \
    --result-dir "${RESULT_DIR}" \
    --namespace "${NS}" \
    --job-name "${JOB_NAME}" \
    --loadgen-pods "${LOADGEN_PODS}" \
    --per-pod-rate "${PER_POD_RATE}" \
    --main-duration-seconds "${MAIN_DURATION_SECONDS}" \
    --aggregate-target-rate "${AGGREGATE_TARGET_RATE}" \
    --overlay "deploy/k8s/overlays/local-k3s-full" \
    --cluster-name "${K3D_CLUSTER}" \
    --cluster-context "${K3D_CONTEXT}" \
    --system-under-test-commit "$(git rev-parse HEAD)" \
    --artifact-head "$(git rev-parse HEAD)" \
    --run-id-prefix "${RUN_ID_PREFIX}" \
    --run-date "${RUN_DATE}" \
    --principal-provisioning-mode "seed_db" \
    --scenario-name "${SCENARIO_NAME}" \
    --deploy-result-md "${DEPLOY_RESULT_MD}" \
    --deploy-title "${DEPLOY_TITLE}" \
    --per-pod-rate-label "${PER_POD_RATE_LABEL}" \
    --kafka-timeout-seconds "${KAFKA_TIMEOUT_SECONDS}" \
    --kafka-poll-seconds "${KAFKA_POLL_SECONDS}" \
    --run-status "${RUN_STATUS}"

  ok "Processor pod deletion resilience artifacts written to ${RESULT_DIR}"
}

main "$@"
