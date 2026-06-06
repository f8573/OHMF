# e2ee-sim — E2EE payload micro-benchmark (NOT a load test)

`main.go` here is an **in-process simulation** of E2EE message *generation and
validation*. It was previously named `e2ee-load-test.go`, which was misleading: it
is not a WebSocket load test and produces no system-level performance evidence.

## What it does

- Generates synthetic encrypted/plaintext message payloads in memory: random
  session keys and nonces, one ephemeral ed25519 keypair per message, and
  per-recipient wrapped keys.
- "Validates" each payload with a fixed `time.Sleep`, then prints throughput and
  min/avg/max per-op timings.

## What it does NOT do

- It does **not** open WebSocket (or any network) connections to the gateway.
- It does **not** measure end-to-end p95 latency, message loss, or system
  throughput. The reported timings are dominated by a hardcoded sleep and the cost
  of key generation — not by the real messaging path.

So do not cite its output as evidence of concurrent-client capacity, latency, or
message-loss behavior.

## Running it

This lives under `_tools/`, which the Go toolchain excludes from normal builds, so
it is not part of the gateway module build. Run it directly:

```bash
go run ./ohmf/services/gateway/_tools/e2ee-sim
go run ./ohmf/services/gateway/_tools/e2ee-sim -messages 5000 -encrypted 0.8 -recipients 3
```

## For a real load test

See [`benchmarks/README.md`](../../../../../benchmarks/README.md) at the repository
root, which defines what a credible WebSocket load test must capture (driver,
environment manifest, latency percentiles, and a precise message-loss definition)
before any throughput/latency claim can be made.
