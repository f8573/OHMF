#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage D scaffold: simulated local node failure."
log "Mandatory disclaimer for future artifacts: this is local-machine simulated node loss, not physical HA."
