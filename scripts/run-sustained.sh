#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster
build_loadgen
start_gateway_port_forward "${DEFAULT_NAMESPACE}" "${DEFAULT_LOCAL_PORT}" "${DEFAULT_TARGET_PORT}"
run_loadgen "${REPO_ROOT}/benchmarks/scenarios/sustained-120.json"
