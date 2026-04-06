#!/usr/bin/env bash
set -euo pipefail

# Minimal test runner that starts Postgres, runs tests, and tears down.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/test-helpers.sh"

# detect requested mode
MODE="unit"
if [ "${1-}" = "--integration" ] || [ "${1-}" = "-i" ]; then
	MODE="integration"
fi

# Prefer project-local Go binary if present
if [ -x "${script_dir}/../ohmf/.tools/go/bin/go" ]; then
	GO_CMD="${script_dir}/../ohmf/.tools/go/bin/go"
elif [ -x "${script_dir}/../ohmf/.tools/go/bin/go.exe" ]; then
	GO_CMD="${script_dir}/../ohmf/.tools/go/bin/go.exe"
elif command -v go >/dev/null 2>&1; then
	GO_CMD="$(command -v go)"
else
	echo "go not found. Install Go or provide ohmf/.tools/go/bin/go executable." >&2
	exit 1
fi

if [ "$MODE" = "integration" ]; then
	echo "Running integration tests via docker compose (itest)..."
	# Use docker compose to run the itest service which runs the integration tests in-container
	DOCKER_CMD=$(find_docker_cmd) || true
	if [ -n "$DOCKER_CMD" ]; then
		"$DOCKER_CMD" compose up --build --abort-on-container-exit --exit-code-from itest itest
		rc=$?
		"$DOCKER_CMD" compose down -v || true
		exit $rc
	else
		echo "docker CLI not found; cannot run integration compose tests" >&2
		exit 1
	fi
else
	start_postgres
	trap stop_postgres EXIT
	echo "Running unit tests for ohmf module..."
	(cd ohmf && "$GO_CMD" test ./... -v)
fi

echo "All done."
