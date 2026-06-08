#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage C scaffold: backlog recovery."
log "Planned flow: scale messages-processor to 0, send with unique run_id, confirm Kafka lag and zero pg delta, then restore replicas and wait for lag -> 0."
