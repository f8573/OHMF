# k3s smoke results

This directory holds **recorded artifacts** from actually running the stage-1
k3s smoke deployment (`deploy/k8s/scripts/smoke-k3s.sh`). The point is evidence:
a claim like "OHMF deploys on Kubernetes" should be backed by a real, dated run
on a known machine, not by assertion.

> **Do not commit a "passing" result unless the smoke script was actually run
> and passed on a real cluster.** A fabricated result is worse than no result.
> If you have not run it yet, leave this directory with only this template.

## How to record a run

```bash
deploy/k8s/scripts/smoke-k3s.sh | tee deploy/k8s/results/$(date +%Y-%m-%d)-smoke.txt
```

Then capture the supporting output:

```bash
kubectl -n ohmf get pods -o wide
kubectl -n ohmf get svc
kubectl version --short
```

## What a completed artifact should contain

A result file (e.g. `2026-06-06-smoke.md` / `.txt`) should include:

1. **Date** of the run.
2. **Machine specs** — CPU cores, RAM, OS/version.
3. **Kubernetes flavor and version** — k3s/k3d/kind/Docker Desktop, plus
   `kubectl version` (client + server) and `kubectl get nodes`.
4. **Images used** — names and tags (`ohmf-gateway:dev`, `ohmf-apps:dev`, …) and
   how they were loaded into the cluster.
5. **`kubectl get pods -n ohmf` output** — showing pods `Running`/`Ready`
   (remember `messages-processor` is `replicas: 0`, so 0 pods is expected).
6. **`kubectl get svc -n ohmf` output**.
7. **Health check output** — the `gateway /healthz -> ok` line and the
   `Smoke check PASSED` line from the script.
8. **Known limitations observed** — e.g. gateway proxies to contacts/media
   returning 502 (those backends are not deployed), Postgres data being
   ephemeral, anything that needed a retry.

## Template

```markdown
# k3s smoke — <YYYY-MM-DD>

- Operator: <name>
- Machine: <e.g. 8-core / 16 GB / Windows 11 + WSL2 / Ubuntu 24.04>
- Cluster: <k3d v5.x | kind vX | k3s vX | Docker Desktop Kubernetes>
- kubectl: <client / server versions>
- Images: ohmf-gateway:dev, ohmf-apps:dev  (loaded via <k3d image import | kind load | ...>)

## kubectl get pods -n ohmf
<paste>

## kubectl get svc -n ohmf
<paste>

## Health check
<paste: "gateway /healthz -> ok" and "Smoke check PASSED">

## Known limitations / notes
- Postgres storage is emptyDir (ephemeral).
- contacts/media backends not deployed (gateway proxies to them 502; health unaffected).
- Kafka/Cassandra/processors not part of this profile (stage-2 / Docker Compose).
- <anything else observed>
```

## Status

_No recorded run yet._ Run the smoke script on a local cluster and add a dated
artifact here.
