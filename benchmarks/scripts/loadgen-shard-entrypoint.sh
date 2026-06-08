#!/bin/sh

set -eu

shard_index="${JOB_COMPLETION_INDEX:-0}"
shard_pad="$(printf '%02d' "${shard_index}")"
namespace="${NAMESPACE:-ohmf}"
users_per_shard="${USERS_PER_SHARD:-10}"
shard_user_counts="${SHARD_USER_COUNTS:-}"
warmup_users="${WARMUP_USERS:-5}"
per_user_rate="${PER_USER_RATE:-1}"
warmup_duration="${WARMUP_DURATION_SECONDS:-60}"
main_duration="${MAIN_DURATION_SECONDS:-600}"
run_id_prefix="${RUN_ID_PREFIX:-multisource}"
message_text="${MESSAGE_TEXT:-m4 sustained multisource local k3s validation}"
base_url="${BASE_URL:-http://gateway.${namespace}.svc.cluster.local:8081}"
postgres_dsn="${POSTGRES_DSN:-postgres://ohmf:ohmf@postgres:5432/ohmf?sslmode=disable}"
jwt_secret="${JWT_SECRET:-dev-only-change-me}"

if [ -n "${shard_user_counts}" ]; then
  selected_count="$(printf '%s' "${shard_user_counts}" | tr ',' '\n' | sed -n "$((shard_index + 1))p")"
  if [ -n "${selected_count}" ]; then
    users_per_shard="${selected_count}"
  fi
fi

effective_warmup_users="${warmup_users}"
if [ "${effective_warmup_users}" -gt "${users_per_shard}" ]; then
  effective_warmup_users="${users_per_shard}"
fi

scenario="sustained-120msgsec-multisource-shard-${shard_pad}"
run_id="${run_id_prefix}-s${shard_pad}"
user_index_offset=0
if [ -n "${shard_user_counts}" ] && [ "${shard_index}" -gt 0 ]; then
  user_index_offset="$(printf '%s' "${shard_user_counts}" | tr ',' '\n' | head -n "${shard_index}" | awk '{sum += $1} END {print sum+0}')"
else
  user_index_offset=$((shard_index * users_per_shard))
fi
aggregate_main_rate=$((users_per_shard * per_user_rate))
aggregate_warm_rate=$((effective_warmup_users * per_user_rate))
output_dir="/tmp/loadgen-results/shard-${shard_pad}"

mkdir -p "${output_dir}"

cat > "${output_dir}/config.json" <<EOF
{
  "scenario": "${scenario}",
  "run_id": "${run_id}",
  "base_url": "${base_url}",
  "driver_location": "in-cluster-shard",
  "user_count": ${users_per_shard},
  "user_index_offset": ${user_index_offset},
  "per_user_rate": ${per_user_rate},
  "aggregate_target_rate": ${aggregate_main_rate},
  "conversations_per_user": 1,
  "principal_provisioning_mode": "seed_db",
  "jwt_secret": "${jwt_secret}",
  "message_text": "${message_text}",
  "auth_otp_code": "123456",
  "namespace": "${namespace}",
  "postgres_resource": "deploy/postgres",
  "postgres_user": "ohmf",
  "postgres_db": "ohmf",
  "postgres_dsn": "${postgres_dsn}",
  "cassandra_resource": "deploy/cassandra",
  "cassandra_keyspace": "ohmf_messages",
  "kafka_resource": "deploy/kafka",
  "kafka_consumer_group": "messages-processor-v1",
  "kafka_ingress_topic": "msg.ingress.v1",
  "kafka_lag_poll_seconds": 2,
  "kafka_lag_timeout_seconds": 180,
  "request_timeout_seconds": 15,
  "skip_reconcile": true,
  "include_latency_samples": true,
  "phases": [
    {
      "name": "warmup-${effective_warmup_users}x${per_user_rate}x${warmup_duration}",
      "user_count": ${effective_warmup_users},
      "per_user_rate": ${per_user_rate},
      "aggregate_target_rate": ${aggregate_warm_rate},
      "duration_seconds": ${warmup_duration}
    },
    {
      "name": "warmup-${users_per_shard}x${per_user_rate}x${warmup_duration}",
      "user_count": ${users_per_shard},
      "per_user_rate": ${per_user_rate},
      "aggregate_target_rate": ${aggregate_main_rate},
      "duration_seconds": ${warmup_duration}
    },
    {
      "name": "main-${users_per_shard}x${per_user_rate}x${main_duration}",
      "user_count": ${users_per_shard},
      "per_user_rate": ${per_user_rate},
      "aggregate_target_rate": ${aggregate_main_rate},
      "duration_seconds": ${main_duration}
    }
  ],
  "metadata": {
    "stage": "B1",
    "shard_id": "${shard_pad}",
    "run_id_prefix": "${run_id_prefix}",
    "source": "indexed-job"
  }
}
EOF

loadgen -config "${output_dir}/config.json" -output-dir "${output_dir}"

echo "SHARD_METADATA shard_index=${shard_index} run_id=${run_id} user_index_offset=${user_index_offset}"
echo "SHARD_SUMMARY_JSON_BEGIN"
cat "${output_dir}/summary.json"
echo "SHARD_SUMMARY_JSON_END"
