#!/usr/bin/env bash
set -euo pipefail

CLIENT_PORT="${CLIENT_PORT:-5173}"
CONTAINER_PORT="${CONTAINER_PORT:-8080}"
HOST_PORT="${HOST_PORT:-18080}"

find_docker() {
  if command -v docker >/dev/null 2>&1; then
    command -v docker
    return 0
  fi

  local win_path="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [ -x "$win_path" ]; then
    printf '%s\n' "$win_path"
    return 0
  fi

  printf '%s\n' "docker not found. Ensure Docker Desktop is installed and running." >&2
  return 1
}

port_available() {
  local port="$1"

  if command -v python3 >/dev/null 2>&1; then
    python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("127.0.0.1", port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
    return $?
  fi

  if command -v ss >/dev/null 2>&1; then
    ! ss -ltn "( sport = :$port )" | grep -q LISTEN
    return $?
  fi

  if command -v lsof >/dev/null 2>&1; then
    ! lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi

  if command -v netstat >/dev/null 2>&1; then
    ! netstat -an 2>/dev/null | grep -Eq "[\.\:]${port}[[:space:]].*LISTEN"
    return $?
  fi

  printf '%s\n' "No port scanning tool found. Install python3, ss, lsof, or netstat." >&2
  return 1
}

next_available_port() {
  local port="$1"
  shift

  while :; do
    local taken=0
    for reserved in "$@"; do
      if [ "$reserved" = "$port" ]; then
        taken=1
        break
      fi
    done

    if [ "$taken" -eq 0 ] && port_available "$port"; then
      printf '%s\n' "$port"
      return 0
    fi

    port=$((port + 1))
  done
}

write_runtime_config() {
  local runtime_file="$1"
  local frontend_port="$2"
  local api_host_port="$3"
  local asset_version="$4"

  cat >"$runtime_file" <<EOF
window.OHMF_RUNTIME_CONFIG = Object.freeze({
  frontend_port: "${frontend_port}",
  api_host_port: "${api_host_port}",
  api_base_url: "http://localhost:${api_host_port}",
  developer_mode: true,
  miniapp_sandbox_port: "${frontend_port}",
  miniapp_sandbox_url: "http://localhost:${frontend_port}",
  asset_version: "${asset_version}",
});
EOF
}

remove_existing_ohmf_containers() {
  local names=(
    ohmf-db
    ohmf-redis
    ohmf-cassandra
    ohmf-kafka
    ohmf-kafka-init
    ohmf-api
    ohmf-client
    ohmf-messages-processor
    ohmf-delivery-processor
    ohmf-sms-processor
    ohmf-prometheus
    ohmf-grafana
  )

  local name
  for name in "${names[@]}"; do
    if "$DOCKER_BIN" ps -aq -f "name=^${name}$" | grep -q .; then
      "$DOCKER_BIN" rm -f "$name" >/dev/null
    fi
  done
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/infra/docker/docker-compose.yml"
CLIENT_COMPOSE_FILE="${ROOT_DIR}/infra/docker/docker-compose.client.yml"
RUNTIME_CONFIG_FILE="${ROOT_DIR}/apps/web/runtime-config.js"
DOCKER_BIN="$(find_docker)"

selected_client_port="$(next_available_port "$CLIENT_PORT")"
selected_container_port="$(next_available_port "$CONTAINER_PORT" "$selected_client_port")"
selected_host_port="$(next_available_port "$HOST_PORT" "$selected_client_port" "$selected_container_port")"
asset_version="$(date -u +%s)"

printf '%s\n' "Stopping existing OHMF Docker containers..."
"$DOCKER_BIN" compose -f "$COMPOSE_FILE" -f "$CLIENT_COMPOSE_FILE" down --remove-orphans >/dev/null || true
remove_existing_ohmf_containers

write_runtime_config "$RUNTIME_CONFIG_FILE" "$selected_client_port" "$selected_host_port" "$asset_version"

export CLIENT_PORT="$selected_client_port"
export API_CONTAINER_PORT="$selected_container_port"
export API_HOST_PORT="$selected_host_port"

printf '%s\n' "Starting db, api, client, messages-processor, and delivery-processor containers..."
"$DOCKER_BIN" compose -f "$COMPOSE_FILE" -f "$CLIENT_COMPOSE_FILE" up -d --build db api client messages-processor delivery-processor

printf '\nSelected ports:\n'
printf 'CLIENT_PORT=%s\n' "$selected_client_port"
printf 'CONTAINER_PORT=%s\n' "$selected_container_port"
printf 'HOST_PORT=%s\n' "$selected_host_port"
printf '\nClient: http://localhost:%s\n' "$selected_client_port"
printf 'API:    http://localhost:%s\n' "$selected_host_port"
