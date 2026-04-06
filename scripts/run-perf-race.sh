#!/bin/sh
set -eu

apk add --no-cache build-base >/dev/null
cd /src/ohmf/services/gateway
CGO_ENABLED=1 /usr/local/go/bin/go test -race ./internal/e2ee/... ./internal/messages/... ./internal/realtime/... -count=1 -timeout=10m
