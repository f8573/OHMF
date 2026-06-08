# k3s results

This directory holds **recorded artifacts from real local-cluster runs**.
Those artifacts are the evidence boundary for any Kubernetes claim in this
repository.

Do not commit a passing result unless the command was actually run on a real
cluster and the recorded outputs match what happened.

## Current artifact types

- `*-local-k3s-smoke.md`
  - stage-1 smoke profile (`deploy/k8s/scripts/smoke-k3s.sh`)
- `*-local-k3s-full-smoke.md`
  - full local pipeline smoke (`deploy/k8s/scripts/smoke-k3s-full.sh`)
- `*-local-k3s-full-hpa-smoke.md`
  - gateway HPA smoke (`deploy/k8s/scripts/hpa-smoke-k3s-full.sh`)

## What a full-pipeline artifact should contain

- date and local time
- machine summary
- cluster flavor and versions
- images built/loaded
- exact commands run
- `kubectl get pods -n ohmf`
- `kubectl get svc -n ohmf`
- gateway health output
- proof of pipeline persistence:
  - authenticated send through the gateway
  - Postgres message count
  - Cassandra message count
  - Kafka consumer-group lag or offset state
- known limitations observed during the run

## What an HPA artifact should contain

- initial gateway replicas
- the HPA spec that was applied
- confirmation that Metrics Server was available
- load method used
- `kubectl get hpa`
- `kubectl top pod -n ohmf`
- replica changes during load
- replica changes after load stopped
- limitations:
  - local single-node only
  - synthetic load only
  - threshold chosen for visibility, not production tuning

## Existing artifacts

- [`2026-06-06-local-k3s-smoke.md`](2026-06-06-local-k3s-smoke.md)
  - stage-1 local smoke
