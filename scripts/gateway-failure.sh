#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage C scaffold: gateway failure."
log "Planned flow: run live message load, delete one gateway pod mid-send, itemize hard failures and retries, then reconcile by run_id / test conversation."
