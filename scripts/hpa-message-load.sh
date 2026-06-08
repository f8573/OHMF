#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage D scaffold: HPA under real message load."
log "This will use /v1/messages traffic rather than synthetic /metrics traffic when Stage D is executed."
