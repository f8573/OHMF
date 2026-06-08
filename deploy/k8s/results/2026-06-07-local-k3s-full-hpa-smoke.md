# Local k3s gateway HPA smoke - 2026-06-07

## Scope

This artifact records a **real local single-node** run of
`deploy/k8s/overlays/local-k3s-full-hpa` and
`deploy/k8s/scripts/hpa-smoke-k3s-full.sh`.

It validates one thing only:

- the **gateway** HorizontalPodAutoscaler increased replicas under synthetic
  in-cluster load and later returned to 1 after load stopped

It does **not** validate:

- production HPA tuning
- autoscaling for Kafka, Cassandra, or `messages-processor`
- multi-node autoscaling behavior

## Run metadata

- Date: 2026-06-07
- Local completion time: 8:03:34 PM CDT
- Operator: James Faul
- Cluster: `k3d` single-node local cluster `ohmf-dev`
- Metrics Server: available

## Exact commands run

```bash
"C:\\Program Files\\Git\\bin\\bash.exe" deploy/k8s/scripts/hpa-smoke-k3s-full.sh
```

The script internally performed:

- `kubectl top nodes`
- `kubectl apply -k deploy/k8s/overlays/local-k3s-full-hpa`
- initial `kubectl get hpa gateway`
- synthetic load deployment with 8 BusyBox pods repeatedly fetching `http://gateway:8081/metrics`
- repeated `kubectl get hpa gateway`
- repeated `kubectl get deploy gateway`
- repeated `kubectl top pod -n ohmf | grep gateway`
- load deletion
- repeated post-load HPA / replica checks

## Initial state

Before synthetic load:

```text
gateway deployment: 1/1 available
gateway HPA target: cpu <unknown>/10%
gateway pods: 1
```

Metrics Server was reachable:

```text
NAME                    CPU(cores)   CPU%   MEMORY(bytes)   MEMORY%
k3d-ohmf-dev-server-0   575m         1%     2737Mi          8%
```

## HPA config applied

```yaml
scaleTargetRef:
  apiVersion: apps/v1
  kind: Deployment
  name: gateway
minReplicas: 1
maxReplicas: 4
metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 10
```

This threshold is intentionally tuned for **local visibility**, not production sizing.

## Scale-up evidence

Representative HPA / deployment observations during load:

```text
cpu: 4%/10%   replicas 1
cpu: 358%/10% replicas 1
gateway deployment -> 3 replicas
cpu: 358%/10% replicas 3
gateway deployment -> 4 replicas
cpu: 333%/10% replicas 4
cpu: 117%/10% replicas 4
cpu: 95%/10%  replicas 4
```

Representative pod CPU samples during load:

```text
gateway-7c9fbf9d9f-s5qsg   358m
gateway-7c9fbf9d9f-knd4z   130m
gateway-7c9fbf9d9f-2rdgk    47m
gateway-7c9fbf9d9f-dmtq2    41m
```

Highest observed supported gateway replica count:

```text
4/4 ready and available
```

## Load stop and scale-down evidence

After deleting `deployment/gateway-load`, replicas stayed high briefly while
the stabilization window elapsed, then reduced:

```text
cpu: 11%/10% replicas 4
cpu: 3%/10%  replicas 4
gateway deployment -> 2 replicas
cpu: 3%/10%  replicas 2
gateway deployment -> 1 replica
cpu: 4%/10%  replicas 2
final gateway deployment: 1/1 available
```

Final observed state:

```text
gateway replicas returned to 1
```

## Known limitations observed

- The first HPA sample showed `cpu: <unknown>/10%` before metrics populated; this resolved on the next polling interval.
- The synthetic load was a local-only BusyBox loop against `gateway /metrics`. It is useful for proving HPA wiring, not for performance claims.
- The threshold (`averageUtilization: 10`) is intentionally aggressive for a local smoke and should not be treated as production tuning.
- The Kustomize apply emitted a deprecation warning for `patchesStrategicMerge`. It did not block HPA behavior.

## Supported claim from this artifact

As of 2026-06-07, OHMF has a **local single-node gateway HPA baseline** on top
of `local-k3s-full`, and on this cluster the gateway scaled from **1 -> 4**
replicas under synthetic in-cluster CPU load and later scaled back down to
**1** after the load stopped.
