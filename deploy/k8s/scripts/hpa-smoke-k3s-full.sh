#!/usr/bin/env bash
#
# hpa-smoke-k3s-full.sh — apply the gateway HPA baseline on top of the
# local-k3s-full profile, generate synthetic in-cluster load, and show whether
# replicas actually change.

set -euo pipefail

NS="ohmf"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERLAY="${SCRIPT_DIR}/../overlays/local-k3s-full-hpa"

log()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }

command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found" >&2; exit 1; }

if ! kubectl cluster-info --request-timeout=5s >/dev/null 2>&1; then
  echo "no reachable Kubernetes cluster" >&2
  exit 1
fi

log "Verifying Metrics Server availability"
kubectl top nodes

log "Applying full-pipeline HPA overlay"
kubectl apply -k "${OVERLAY}"

log "Initial gateway deployment + HPA state"
kubectl -n "${NS}" get deploy gateway
kubectl -n "${NS}" get hpa gateway
kubectl top pod -n "${NS}" | grep gateway || true

log "Starting synthetic in-cluster load against gateway /metrics"
cat <<'EOF' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gateway-load
  namespace: ohmf
spec:
  replicas: 8
  selector:
    matchLabels:
      app: gateway-load
  template:
    metadata:
      labels:
        app: gateway-load
    spec:
      containers:
        - name: busybox
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              while true; do
                wget -q -O- http://gateway:8081/metrics >/dev/null || true
              done
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 50m
              memory: 32Mi
EOF

log "Watching HPA and gateway replicas during load"
for _ in $(seq 1 8); do
  kubectl -n "${NS}" get hpa gateway
  kubectl -n "${NS}" get deploy gateway
  kubectl top pod -n "${NS}" | grep gateway || true
  sleep 15
done

log "Stopping synthetic load"
kubectl -n "${NS}" delete deployment gateway-load --ignore-not-found=true

log "Watching HPA and gateway replicas after load stops"
for _ in $(seq 1 10); do
  kubectl -n "${NS}" get hpa gateway
  kubectl -n "${NS}" get deploy gateway
  kubectl top pod -n "${NS}" | grep gateway || true
  sleep 20
done

log "HPA smoke complete"
