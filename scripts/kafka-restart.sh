#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage C scaffold: Kafka restart."
log "Planned flow: restart the single broker in the resilience overlay, then measure recovery and lag drain without claiming failover."
