# Local k3s full-pipeline smoke - 2026-06-07

## Scope

This artifact records a **real local single-node** run of
`deploy/k8s/overlays/local-k3s-full` and
`deploy/k8s/scripts/smoke-k3s-full.sh`.

It validates:

- in-cluster Kafka
- in-cluster Cassandra
- `messages-processor` enabled
- full gateway mode
- one authenticated message flowing through:
  - gateway/API
  - Kafka ingress topic
  - `messages-processor`
  - Postgres
  - Cassandra

It does **not** validate:

- production readiness
- HA or multi-node behavior
- durable storage
- ingress/TLS
- benchmark throughput or latency

## Run metadata

- Date: 2026-06-07
- Local completion time: 7:57:52 PM CDT
- Operator: James Faul
- Cluster: `k3d` single-node local cluster `ohmf-dev`
- k3d: `v5.9.0`
- Kubernetes server: `v1.35.5+k3s1`
- kubectl client: `v1.29.2`

## Machine summary

- CPU: AMD Ryzen 9 3950X 16-Core Processor
- Cores / threads: 16 / 32
- RAM: 68,628,099,072 bytes (~63.9 GiB)
- OS: Microsoft Windows 11 Pro 64-bit, version `10.0.26200`

## Images built and used

- `ohmf-gateway:dev` - 74 MB
- `ohmf-apps:dev` - 26.8 MB
- `ohmf-messages-processor:dev` - 31.4 MB
- `postgres:16-alpine`
- `redis:7-alpine`
- `cassandra:5.0`
- `confluentinc/cp-kafka:7.6.1`

Local images were imported into the k3d cluster with:

```bash
k3d image import ohmf-gateway:dev ohmf-apps:dev ohmf-messages-processor:dev -c ohmf-dev
```

## Exact commands run

```bash
kubectl delete namespace ohmf --ignore-not-found=true
"C:\\Program Files\\Git\\bin\\bash.exe" deploy/k8s/scripts/smoke-k3s-full.sh
```

The smoke script internally performed:

- `kubectl apply -k deploy/k8s/overlays/local-k3s-full`
- rollout waits for `postgres`, `redis`, `cassandra`, `kafka`, `apps`, `messages-processor`, and `gateway`
- `kubectl wait --for=condition=complete job/kafka-init`
- gateway port-forward and `/healthz` check
- a live authenticated gateway send flow
- Postgres, Cassandra, and Kafka consumer-group checks

## Live workload state

`kubectl get pods -n ohmf`:

```text
NAME                                  READY   STATUS      RESTARTS   AGE   IP           NODE
apps-695b467944-x824b                 1/1     Running     0          93s   10.42.0.56   k3d-ohmf-dev-server-0
cassandra-669556d58f-ft9zm            1/1     Running     0          93s   10.42.0.57   k3d-ohmf-dev-server-0
gateway-7c9fbf9d9f-s5qsg              1/1     Running     0          93s   10.42.0.58   k3d-ohmf-dev-server-0
kafka-9fd8b548c-z68js                 1/1     Running     0          93s   10.42.0.59   k3d-ohmf-dev-server-0
kafka-init-n8cpn                      0/1     Completed   0          93s   10.42.0.61   k3d-ohmf-dev-server-0
messages-processor-7c6855b667-jlcp2   1/1     Running     0          93s   10.42.0.60   k3d-ohmf-dev-server-0
postgres-6f4c6cfddc-8slkb             1/1     Running     0          93s   10.42.0.62   k3d-ohmf-dev-server-0
redis-5649fbd7f-ssn8h                 1/1     Running     0          93s   10.42.0.63   k3d-ohmf-dev-server-0
```

`kubectl get svc -n ohmf`:

```text
NAME                 TYPE        CLUSTER-IP      PORT(S)
apps                 ClusterIP   10.43.98.30     18086/TCP
cassandra            ClusterIP   10.43.93.82     9042/TCP
gateway              ClusterIP   10.43.234.226   8081/TCP
kafka                ClusterIP   10.43.13.14     9092/TCP
messages-processor   ClusterIP   10.43.29.8      18088/TCP
postgres             ClusterIP   10.43.247.135   5432/TCP
redis                ClusterIP   10.43.142.89    6379/TCP
```

## Gateway health

```text
[ok] gateway /healthz -> ok
```

## Gateway -> Kafka -> processor -> persistence proof

The script executed a real authenticated send through the gateway and received:

```json
{"conversation_id":"a52fb731-b4ba-4610-a477-1e0374dcd71e","message_id":"48763cb1-6bba-43bc-bbb9-53baab712a93","server_order":1,"items_seen":1}
```

Observed persistence checks after that send:

Postgres:

```text
postgres_messages
-------------------
1
```

Cassandra:

```text
cassandra_messages
--------------------
1
```

Kafka consumer-group state:

```text
messages-processor-v1 msg.ingress.v1 partition 9 current-offset 1 log-end-offset 1 lag 0
```

Processor log excerpt:

```text
2026/06/08 00:57:36 messages-processor started
```

## Known limitations observed

- Cassandra count used an aggregation query without partition key; `cqlsh` warned about that. The query still returned `1` row for local validation.
- The Kustomize build emitted a deprecation warning for `patchesStrategicMerge`. It did not block the run.
- Storage remains ephemeral (`emptyDir`) for Kafka, Cassandra, and Postgres.
- This run proves one real message through the local async path, not performance or durability under failure.

## Supported claim from this artifact

As of 2026-06-07, OHMF has a **local single-node k3s/k3d full-pipeline baseline**
that deploys Kafka, Cassandra, the full gateway, and `messages-processor`, and
it has been verified with a real authenticated send flowing through
`gateway/API -> Kafka -> messages-processor -> Postgres/Cassandra`.
