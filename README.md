# OHMF — Distributed Real-Time Messaging Backend

OHMF is a distributed real-time messaging backend written in Go. It accepts messages over an
HTTP/WebSocket gateway, moves them through a Kafka event pipeline to background processors, and
persists conversation state durably in PostgreSQL (with Cassandra wired in as a secondary message
store). Redis carries presence and low-latency realtime fan-out. The whole stack runs locally under
Docker Compose.

The project exists to be a concrete, inspectable example of the correctness problems that show up in
a multi-service messaging system - idempotency under concurrent sends, per-conversation ordering
across multiple workers, at-least-once delivery with duplicate suppression, and cross-pod presence -
and to show those problems being solved with tests that pin the behavior down. It is a working
development environment and a correctness case study, **not** a finished, production-operated system.

> Status: actively developed. Core send/persist/deliver paths and reliability hardening are
> implemented and unit/integration tested. Local single-node Kubernetes evidence now includes a
> validated exact full-pipeline pass at `120 msg/sec` with `4` `messages-processor` replicas
> across `12` source IPs, including a backlog recovery validation confirming exact reconciliation
> after consumer group drain and restore. See [Limitations](#limitations) and
> [Benchmarks](#benchmarks-and-load-testing).

## Why this exists

Most "chat app" projects stop at create-read-update-delete over a database. The interesting and
hard part of messaging is everything around that: what happens when the same send is retried, when
two workers fan out the same conversation concurrently, when a delivery event is redelivered, or
when a user is connected to two pods at once. OHMF is built to make those failure paths explicit and
to demonstrate the engineering - not just the happy path. The reliability work is documented with
the specific test that proves each behavior in
[docs/reliability-hardening.md](docs/reliability-hardening.md).

## Stack

What is actually present and used in the code (verified against `go.mod` files and the compose stack):

| Layer | Technology | Where |
| --- | --- | --- |
| Language | Go 1.25 (multi-module) | `ohmf/services/*` |
| API / realtime | HTTP + WebSocket gateway (`go-chi/chi`, `gorilla/websocket`) | `ohmf/services/gateway` |
| Event pipeline | Apache Kafka (`segmentio/kafka-go`) | `ohmf/services/gateway/internal/bus`, processors |
| Authoritative store | PostgreSQL | gateway + processors |
| Secondary message store | Cassandra (`gocql`) - shadow-write enabled, reads off by default | `ohmf/services/gateway/internal/messages/cassandra_store.go` |
| Presence / realtime fan-out | Redis | gateway, processors |
| Observability | Prometheus + Grafana dashboards | `ohmf/infra/observability` |
| Local orchestration | Docker Compose | `docker-compose.yml`, `ohmf/infra/docker` |
| Web client | Static HTML/CSS/vanilla JS | `ohmf/apps/web` |
| CI | GitHub Actions (OpenAPI validation, Go tests, docker build, containerized integration test) | `.github/workflows` |

Kubernetes: plain-Kubernetes (Kustomize, no Helm) manifests for **local
single-node k3s/k3d validation** live under `deploy/k8s/`. The repo now
contains a lighter smoke profile (`overlays/local-k3s`), a fuller local
pipeline profile (`overlays/local-k3s-full`), and an optional local gateway HPA
layer (`overlays/local-k3s-full-hpa`). These support local single-node evidence
only: they are not a production deployment package, not multi-node resilience
evidence, not Helm, and not benchmark-validated. See `deploy/k8s/README.md`.

Not currently in the repo: Helm charts, and standalone WebSocket load-test
scripts or captured benchmark artifacts. These are referenced as design intent
only and are called out below.

## Architecture

```
                       WebSocket / HTTP clients
                                 |
                                 v
                      +----------------------+
                      |   Gateway (API)      |  auth, validation, idempotency,
                      |  chi + gorilla/ws    |  realtime fan-out
                      +----------------------+
                                 | produce: msg.ingress.v1
                                 v
                          +-------------+
                          |    Kafka    |  partitioned topics + DLQs
                          +-------------+
                                 | consume
                                 v
                +-----------------------------------+
                | messages-processor                |  persist + retry semantics
                +-----------------------------------+
                     |                 |
                     v                 v
              +------------+    +------------+
              | PostgreSQL |    | Cassandra  |  (authoritative)   (shadow write)
              +------------+    +------------+
                     | produce: msg.persisted.v1
                     v
                +-----------------------------------+
                | delivery-processor / sms-processor|  delivery receipts, SMS dispatch
                +-----------------------------------+
                                 | msg.delivery.v1
                                 v
                        Gateway --> clients (via Redis pub/sub + WS)
```

Redis sits alongside the gateway and processors for presence reference-counting, typing/user-event
fan-out, and async-send ack wake-ups. PostgreSQL is the source of truth for messages and delivery
receipts; Kafka and Redis are best-effort distribution paths around that authoritative state. A
fuller description of components, data flow, the concurrency model, failure assumptions, and
trade-offs is in [docs/architecture.md](docs/architecture.md).

## Quickstart

Prerequisites: Docker Desktop running, Git, and a shell (PowerShell on Windows, or any POSIX shell).
A system Go install is optional - a pinned toolchain is bundled at `ohmf/.tools/go/bin/go.exe`.

Bring up the local stack from the repo root:

```bash
docker compose up -d --build
docker compose ps
```

Health-check the gateway (it is not published to the host by default):

```bash
docker compose exec gateway wget -qO- http://localhost:8081/healthz
```

Run the Go tests for the gateway:

```bash
cd ohmf/services/gateway
../../.tools/go/bin/go.exe test ./...      # Windows bundled toolchain
# or, with a system Go: go test ./...
```

Run the repository test helper / containerized integration test:

```bash
./scripts/run-tests.sh --integration            # POSIX
docker compose run --rm itest                    # containerized integration tests
```

A larger compose stack that includes Kafka, Cassandra, Redis, and all processors lives at
`ohmf/infra/docker/docker-compose.yml`; see [Full stack](#full-ohmf-stack-kafka--cassandra--redis)
below. For the complete day-to-day local-hosting guide, see
[Local development guide](#local-development-guide).

## Benchmarks and load testing

**Honest status:** the repository still does not contain a WebSocket concurrency harness, but it now
does contain committed local benchmark artifacts under [`benchmarks/results/`](benchmarks/results/).
Those artifacts currently support a Stage A smoke, per-user and per-IP limiter validations, the
earlier unique-tag Stage B1 rerun ladder that passed exact full-pipeline reconciliation at `75`,
`90`, and `105 msg/sec` across `12` source IPs, a processor-scaling matrix that validated a
full-pipeline pass at `120 msg/sec` with `4` `messages-processor` replicas, and a backlog recovery
validation at `120 msg/sec` confirming exact full-pipeline reconciliation after the consumer group
was scaled to zero and restored mid-run. The `105 msg/sec` result remains the previous
single-processor-supported passing rung. These artifacts do **not**
substantiate large-client-count, client-observed HTTP accept latency, or production-throughput
claims. The old `ohmf/services/gateway/_tools/e2ee-load-test.go` remains an in-process simulation
of E2EE message *generation* - it does not open WebSocket connections, does not measure
client-observed HTTP accept latency, and does not measure end-to-end message loss. Treat it as a
micro-benchmark scaffold, not as evidence of system throughput.

Benchmark documentation is being consolidated under [benchmarks/](benchmarks/README.md), which
describes what a credible run must capture (driver, environment, metrics, how message loss is
defined, where sample output lives) so results can be reproduced rather than asserted.

## Project structure

```
.
|-- docker-compose.yml          # root local stack (Postgres + gateway + integration tests)
|-- docs/
|   |-- architecture.md         # components, data flow, concurrency, trade-offs, limits
|   `-- reliability-hardening.md# correctness case study, each fix linked to its test
|-- benchmarks/                 # load-test methodology + status (see README)
|-- scripts/                    # cross-platform test runners
|-- ohmf/
|   |-- services/
|   |   |-- gateway/            # HTTP + WebSocket API, realtime, messages, e2ee, presence
|   |   |-- messages-processor/ # Kafka consumer: persist + retry semantics
|   |   |-- delivery-processor/ # delivery receipts, idempotent
|   |   |-- sms-processor/      # SMS dispatch
|   |   `-- contacts | apps | media | auth | users | ...
|   |-- infra/
|   |   |-- docker/             # full stack: Kafka, Cassandra, Redis, processors
|   |   `-- observability/      # Prometheus + Grafana
|   |-- apps/                   # web client + Android scaffold
|   `-- packages/protocol/      # OpenAPI + SQL schema
`-- .github/workflows/          # CI: OpenAPI validation, Go tests, docker build, integration
```

## Limitations

These are stated up front so the repo is read accurately:

- **Delivery semantics are at-least-once, not exactly-once.** Postgres is authoritative; Kafka and
  Redis are best-effort around it. A crash between a side effect and its idempotency marker can
  duplicate that side effect on retry. (See `docs/reliability-hardening.md` section 5.)
- **Kubernetes is local-single-node only.** `deploy/k8s/` now contains:
  - a stage-1 smoke profile (`local-k3s`) for gateway-smoke + `apps` + Postgres/Redis
  - a fuller local pipeline profile (`local-k3s-full`) that deploys Kafka, Cassandra, the full gateway, and `messages-processor`
  - a local HPA layer (`local-k3s-full-hpa`) that was smoke-tested against the gateway

  Recorded artifacts under [`deploy/k8s/results/`](deploy/k8s/results/) show
  these profiles working on a real single-node cluster, including an async
  `gateway/API -> Kafka -> processor -> Postgres/Cassandra` proof and a gateway
  HPA smoke where replicas increased under synthetic load and later returned to
  1 after load stopped. These artifacts do **not** support production
  operations claims, Helm, multi-node resilience, durable storage, ingress/TLS,
  network policy, or benchmark/performance claims.
- **Cassandra is in shadow-write mode.** It is wired up and written to, but reads default to Postgres
  (`APP_USE_CASSANDRA_READS=false`). The Cassandra read path is not the live serving path.
- **No substantiated production load-test results.** See [Benchmarks](#benchmarks-and-load-testing).
- **Ordering is per-conversation, not global.** Fan-out preserves `server_order` within a
  conversation, not a total order across conversations.

---

## Local development guide

The sections below are the detailed local-hosting reference. The [Quickstart](#quickstart) above is
enough to get the stack running; this is for day-to-day work.

### About `postgres-data` (root stack)

You do not manually populate `postgres-data/`. It is populated automatically by the `postgres`
container on first startup because the root `docker-compose.yml` bind-mounts it:

```yaml
volumes:
  - ./postgres-data:/var/lib/postgresql/data
```

- First `docker compose up`: Postgres initializes data files in `postgres-data/`.
- Next runs: existing data is reused (persistent local state).
- Hard reset: `docker compose down -v`.

### Local service endpoints

Inside the Docker network (service-to-service):
- Contacts: `http://contacts:18085`
- Apps: `http://apps:18086`
- Media: `http://media:18087`
- Gateway: `http://gateway:8081`

From your host machine:
- Postgres: `localhost:5432`

To expose the gateway on your host, add to the `gateway` service in `docker-compose.yml`:

```yaml
ports:
  - "8081:8081"
```

Then recreate it: `docker compose up -d --build gateway`. Host access is then at
`http://localhost:8081/healthz`.

### Running tests

```powershell
# Windows, bundled toolchain
Push-Location .\ohmf\services\gateway
& ..\..\.tools\go\bin\go.exe test ./...
Pop-Location

# Repository helpers
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Integration
```

```bash
chmod +x scripts/*.sh
./scripts/run-tests.sh --integration
```
