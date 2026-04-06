#!/usr/bin/env bash
set -euo pipefail

OHMF_TEST_POSTGRES_MODE="${OHMF_TEST_POSTGRES_MODE:-external}"

# Locate a docker binary. Allows `docker` on PATH or common Docker Desktop Windows path.
find_docker_cmd() {
  if [ -n "${DOCKER_CMD:-}" ]; then
    echo "$DOCKER_CMD"
    return 0
  fi
  # Prefer docker from PATH
  if command -v docker >/dev/null 2>&1; then
    command -v docker
    return 0
  fi
  # Try common Docker Desktop Windows path (WSL environments)
  win_path="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  if [ -x "$win_path" ]; then
    echo "$win_path"
    return 0
  fi
  # Not found
  return 1
}

# Start the Postgres service via docker compose and wait until it's accepting connections.
start_postgres() {
  if [ -n "${TEST_DATABASE_URL:-}" ] || [ -n "${POSTGRES_URL:-}" ] || [ -n "${DB_DSN:-}" ]; then
    echo "External DB provided via environment; skipping postgres startup."
    OHMF_TEST_POSTGRES_MODE="external"
    return 0
  fi

  DOCKER_CMD=$(find_docker_cmd) || true

  # If docker CLI is runnable in this environment, use it. Otherwise try PowerShell on Windows host.
  USE_PWSH=0
  PWSH_CMD=""
  if [ -n "$DOCKER_CMD" ]; then
    # Test running docker
    if "$DOCKER_CMD" version >/dev/null 2>&1; then
      DOCKER_BIN="$DOCKER_CMD"
    else
      DOCKER_BIN=""
    fi
  else
    DOCKER_BIN=""
  fi

  if [ -z "${DOCKER_BIN:-}" ]; then
    # try PowerShell executables
    if command -v powershell.exe >/dev/null 2>&1; then
      PWSH_CMD="powershell.exe"
      USE_PWSH=1
    elif command -v pwsh >/dev/null 2>&1; then
      PWSH_CMD="pwsh"
      USE_PWSH=1
    else
      echo "docker command not found. Ensure Docker is installed and on PATH, or enable Docker Desktop WSL integration, or set DOCKER_CMD." >&2
      return 1
    fi
  fi

  run_docker() {
    if [ "${USE_PWSH:-0}" -eq 1 ]; then
      # join args (prefix with 'docker ' for PowerShell)
        cmd_str="docker "
      for a in "$@"; do
        cmd_str+="$a "
      done
      "$PWSH_CMD" -NoProfile -Command "$cmd_str"
    else
      "$DOCKER_BIN" "$@"
    fi
  }

  port_is_open() {
    (echo >/dev/tcp/127.0.0.1/5432) >/dev/null 2>&1
  }

  echo "Bringing up postgres with docker compose (or fallback to docker run)..."
  if run_docker compose up -d postgres >/dev/null 2>&1; then
    # compose-managed service
    cid=$(run_docker compose ps -q postgres)
    OHMF_TEST_POSTGRES_MODE="compose"
  else
    echo "No 'postgres' service in compose; falling back to a standalone postgres container"
    if port_is_open; then
      echo "Port 5432 is already in use; assuming an external Postgres is available."
      OHMF_TEST_POSTGRES_MODE="external"
      return 0
    fi
    # Start a standalone postgres container for tests
    if run_docker run -d --name ohmf_test_postgres -e POSTGRES_USER=dev -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=dev -p 5432:5432 postgres:15-alpine >/dev/null 2>&1; then
      cid=$(run_docker ps -q -f name=ohmf_test_postgres)
      OHMF_TEST_POSTGRES_MODE="standalone"
    elif port_is_open; then
      echo "Port 5432 is already in use; assuming an external Postgres is available."
      OHMF_TEST_POSTGRES_MODE="external"
      return 0
    else
      echo "Could not start postgres for tests" >&2
      return 1
    fi
  fi
  if [ -z "$cid" ]; then
    echo "Could not determine postgres container id" >&2
    return 1
  fi

  echo "Waiting for Postgres to accept connections..."
  for i in $(seq 1 30); do
    if run_docker exec "$cid" pg_isready >/dev/null 2>&1; then
      echo "Postgres is ready"
      return 0
    fi
    sleep 1
  done
  echo "Postgres did not become ready in time" >&2
  return 1
}

# Stop and remove the Postgres service created by docker compose.
stop_postgres() {
  if [ "${OHMF_TEST_POSTGRES_MODE:-external}" = "external" ]; then
    return 0
  fi
  DOCKER_CMD=$(find_docker_cmd) || true

  if [ "${OHMF_TEST_POSTGRES_MODE:-external}" = "compose" ] && command -v powershell.exe >/dev/null 2>&1; then
    PWSH_CMD="powershell.exe"
    "$PWSH_CMD" -NoProfile -Command "docker compose stop postgres" >/dev/null 2>&1 || true
    "$PWSH_CMD" -NoProfile -Command "docker compose rm -f postgres" >/dev/null 2>&1 || true
    return 0
  fi
  if [ "${OHMF_TEST_POSTGRES_MODE:-external}" = "compose" ] && command -v pwsh >/dev/null 2>&1; then
    PWSH_CMD="pwsh"
    "$PWSH_CMD" -NoProfile -Command "docker compose stop postgres" >/dev/null 2>&1 || true
    "$PWSH_CMD" -NoProfile -Command "docker compose rm -f postgres" >/dev/null 2>&1 || true
    return 0
  fi
  if [ "${OHMF_TEST_POSTGRES_MODE:-external}" = "compose" ] && [ -n "$DOCKER_CMD" ]; then
    "$DOCKER_CMD" compose stop postgres >/dev/null 2>&1 || true
    "$DOCKER_CMD" compose rm -f postgres >/dev/null 2>&1 || true
    return 0
  fi

  if [ -n "$DOCKER_CMD" ]; then
    cid=$($DOCKER_CMD ps -a -q -f name=ohmf_test_postgres 2>/dev/null || true)
    if [ -n "$cid" ]; then
      $DOCKER_CMD rm -f "$cid" >/dev/null 2>&1 || true
    fi
    return 0
  fi

  if command -v powershell.exe >/dev/null 2>&1; then
    PWSH_CMD=powershell.exe
    cid=$($PWSH_CMD -NoProfile -Command "docker ps -a -q -f name=ohmf_test_postgres" 2>/dev/null || true)
    if [ -n "$cid" ]; then
      $PWSH_CMD -NoProfile -Command "docker rm -f ohmf_test_postgres" >/dev/null 2>&1 || true
    fi
    return 0
  fi

  if command -v pwsh >/dev/null 2>&1; then
    PWSH_CMD=pwsh
    cid=$($PWSH_CMD -NoProfile -Command "docker ps -a -q -f name=ohmf_test_postgres" 2>/dev/null || true)
    if [ -n "$cid" ]; then
      $PWSH_CMD -NoProfile -Command "docker rm -f ohmf_test_postgres" >/dev/null 2>&1 || true
    fi
    return 0
  fi

  if command -v docker >/dev/null 2>&1; then
    cid=$(docker ps -a -q -f name=ohmf_test_postgres 2>/dev/null || true)
    if [ -n "$cid" ]; then
      docker rm -f "$cid" >/dev/null 2>&1 || true
    fi
  fi
}
