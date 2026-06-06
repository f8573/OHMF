# Local Kubernetes smoke — 2026-06-06

**Scope of this artifact:** it records a single, real run of the stage-1
`deploy/k8s/overlays/local-k3s` profile on a local single-node Kubernetes
cluster. It validates **local deployment smoke behavior only** — that the
manifests apply, the application pods become Ready, and the gateway serves
`/healthz` end-to-end through a Service. It does **not** validate performance,
latency, throughput, high availability, autoscaling, production readiness, or the
full Kafka/Cassandra event pipeline. No benchmark numbers are produced or implied.

## Run metadata

- **Date / time:** 2026-06-06, 15:17 CDT (UTC-05:00)
- **Operator:** James Faul (local workstation run)
- **Cluster flavor:** `kind` (Kubernetes-in-Docker), single node — used because
  `k3d` was not installed on this machine; `kind` is next in the documented
  preference order and produces an equivalent single-node local cluster.
- **Cluster name:** `ohmf-dev` (created fresh for this run, deleted afterwards)
- **Teardown:** **yes** — namespace deleted via `smoke-k3s.sh --down`, then the
  ephemeral `ohmf-dev` kind cluster was deleted and the prior kubectl context
  restored. Nothing was left running.

## Machine summary

| Field | Value |
| ----- | ----- |
| CPU   | AMD Ryzen 7 8845HS (8 cores / 16 threads) |
| RAM   | 15.3 GiB |
| OS    | Windows 11 Home (Insider Preview) 10.0.26300 |
| Shell | Git Bash (the smoke script is plain bash + kubectl + curl) |

## Tool / image versions

| Tool | Version |
| ---- | ------- |
| kind | v0.27.0 (go1.23.6, windows/amd64) |
| kind node image | `kindest/node:v1.32.2` |
| kubectl | client v1.30.5 / **server v1.32.2** |
| Docker | 27.4.0 |
| kustomize | built into kubectl (v5.0.4) |

Images built and loaded:

| Image | Built from | Loaded into cluster | Size (in node) |
| ----- | ---------- | ------------------- | -------------- |
| `ohmf-gateway:dev` | `docker build -t ohmf-gateway:dev ohmf/services/gateway` | `kind load docker-image ... --name ohmf-dev` | 31.2 MB |
| `ohmf-apps:dev`    | `docker build -t ohmf-apps:dev -f ohmf/services/apps/Dockerfile .` | `kind load docker-image ... --name ohmf-dev` | 12.8 MB |

The manifests use `imagePullPolicy: Never`, so the images had to be present in the
node's containerd. Confirmed via `crictl images` on `ohmf-dev-control-plane`:

```
docker.io/library/ohmf-apps      dev   193b2458fec6a   12.8MB
docker.io/library/ohmf-gateway   dev   8a01d7a4eb431   31.2MB
```

## Exact commands run

```bash
# 1. cluster (kind, single node)
kind create cluster --name ohmf-dev

# 2. build local images
docker build -t ohmf-gateway:dev ohmf/services/gateway
docker build -t ohmf-apps:dev -f ohmf/services/apps/Dockerfile .

# 3. load into the cluster (no registry in this profile)
kind load docker-image ohmf-gateway:dev ohmf-apps:dev --name ohmf-dev

# 4. offline manifest validation
kubectl kustomize deploy/k8s/overlays/local-k3s            # renders 12 docs
bash -n deploy/k8s/scripts/smoke-k3s.sh                    # bash syntax OK

# 5. live smoke (apply + wait + port-forward + /healthz)
deploy/k8s/scripts/smoke-k3s.sh

# 6. teardown
deploy/k8s/scripts/smoke-k3s.sh --down
kind delete cluster --name ohmf-dev
```

## `kubectl kustomize deploy/k8s/overlays/local-k3s`

Renders cleanly (exit 0). The only diagnostic is the known, benign deprecation
warning, which was left as-is to avoid breaking compatibility:

```
# Warning: 'patchesStrategicMerge' is deprecated. Please use 'patches' instead.
```

12 documents render: `Namespace`, `ConfigMap`, `Secret`, 4× `Service`, 5×
`Deployment` (`apps`, `gateway`, `messages-processor`, `postgres`, `redis`).
Validated as well-formed YAML with PyYAML (`yaml.safe_load_all` → 12 documents OK).

## Rollout status

```
deployment "postgres" successfully rolled out   [ok] deploy/postgres rolled out
deployment "redis"    successfully rolled out   [ok] deploy/redis rolled out
deployment "apps"     successfully rolled out   [ok] deploy/apps rolled out
deployment "gateway"  successfully rolled out   [ok] deploy/gateway rolled out
```

`messages-processor` is intentionally `replicas: 0` and is not waited on.

## `kubectl -n ohmf get pods -o wide`

```
NAME                       READY   STATUS    RESTARTS   AGE   IP           NODE                     NOMINATED NODE   READINESS GATES
apps-75b8b57fdb-d5jrl      1/1     Running   0          63s   10.244.0.6   ohmf-dev-control-plane   <none>           <none>
gateway-7c48bb5568-xhtvz   1/1     Running   0          63s   10.244.0.7   ohmf-dev-control-plane   <none>           <none>
postgres-97bf75d88-hjfm7   1/1     Running   0          62s   10.244.0.5   ohmf-dev-control-plane   <none>           <none>
redis-8bb87746d-w74nf      1/1     Running   0          62s   10.244.0.8   ohmf-dev-control-plane   <none>           <none>
```

(`messages-processor` shows 0 pods — expected, `replicas: 0`.)

## `kubectl -n ohmf get deploy`

```
NAME                 READY   UP-TO-DATE   AVAILABLE   AGE
apps                 1/1     1            1           64s
gateway              1/1     1            1           64s
messages-processor   0/0     0            0           64s
postgres             1/1     1            1           64s
redis                1/1     1            1           64s
```

## `kubectl -n ohmf get svc`

```
NAME       TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)     AGE
apps       ClusterIP   10.96.54.82     <none>        18086/TCP   63s
gateway    ClusterIP   10.96.4.43      <none>        8081/TCP    63s
postgres   ClusterIP   10.96.51.239    <none>        5432/TCP    63s
redis      ClusterIP   10.96.255.165   <none>        6379/TCP    63s
```

## App → in-cluster Postgres (the substantive part)

The `apps` init container blocked until Postgres was reachable, then `apps` came
up Postgres-backed — this is the bit that makes "runs on Kubernetes" concrete
rather than cosmetic:

```
# init container wait-for-postgres
waiting for postgres:5432...
postgres is reachable

# apps container
apps registry using postgres backend with migrations from /opt/ohmf/apps/migrations
apps registry listening on :18086
```

## Health check

Gateway runs in smoke mode (`APP_SMOKE_MODE=1`), reached via `kubectl
port-forward svc/gateway 18081:8081`:

```
==> Checking gateway health at http://127.0.0.1:18081/healthz
[ok] gateway /healthz -> ok

==> Smoke check PASSED
```

Smoke script exit code: **0**.

## Known limitations observed / by design

- **Single node, `replicas: 1`.** No HA, no multi-node scheduling, no
  anti-affinity. This is `kind` single-node by construction.
- **Ephemeral storage.** Postgres uses `emptyDir`; data is lost on pod restart.
- **No Ingress/TLS.** Services are ClusterIP only; the gateway is reached via
  `kubectl port-forward`.
- **No autoscaling** (no HPA/VPA), **no NetworkPolicy**, **no PodSecurity
  hardening**, **no real secret management** (the Secret holds dev placeholders).
- **Gateway runs in smoke mode**, not the full WebSocket/Kafka/Cassandra path.
  Its `/v1/{contacts,media}` proxies would return 502 (those backends are not in
  this profile); gateway health does not depend on them.
- **`messages-processor` is `replicas: 0`.** Kafka, Cassandra, and the
  processors are not deployed here — that remains Docker Compose / stage-2 work.
- **Not benchmark-validated.** No throughput, latency, client-count, or
  message-loss numbers were measured, and none should be inferred from this run.

## What this artifact establishes

The OHMF stage-1 manifests were **applied to and verified on a live local
single-node Kubernetes cluster** (kind v0.27.0, k8s v1.32.2): all four active
deployments reached Ready, the `apps` service connected to in-cluster Postgres
and applied its migrations, and the gateway served `/healthz` end-to-end through
its Service. Evidence level moves from "manifests render" to "manifests deploy
and serve health on a real local cluster." Nothing beyond local deployment smoke
is claimed.
