#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

require_stage_a_tools
ensure_cluster

log "Stage C scaffold: Cassandra restart."
log "Planned flow: restart the single Cassandra node, treat Postgres as the authoritative pass path, and document any Cassandra gaps honestly."
