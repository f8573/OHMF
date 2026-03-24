# Messages Local Development

This repository contains the OHMF services and associated local infrastructure for development.

This guide explains how to:
- Set up your local environment
- Host the services locally with Docker Compose
- Verify the system is running
- Run tests

## Repository Layout

- `docker-compose.yml`: local compose stack at repo root
- `ohmf/`: core OHMF services, docs, and scripts
- `scripts/`: cross-platform test scripts
- `postgres-data/`: Postgres bind-mounted data directory used by root compose stack

## Prerequisites

Install the following first:
- Docker Desktop (running)
- Git
- Go 1.25+ (optional if you only run services via Docker)
- PowerShell (Windows) or a POSIX shell (Linux/macOS)

## 1) Start Local Hosting

From the repository root:

```powershell
docker compose up -d --build
```

Check status:

```powershell
docker compose ps
```

### About `postgres-data` (root stack)

You do not manually populate `postgres-data/`.
It is populated automatically by the `postgres` container on first startup because root `docker-compose.yml` uses:

```yaml
volumes:
  - ./postgres-data:/var/lib/postgresql/data
```

Behavior:
- First `docker compose up`: Postgres initializes data files in `postgres-data/`
- Next runs: existing data is reused (persistent local state)

Reset options:
- Root stack hard reset: `docker compose down -v`
- If needed, remove the local data directory contents and bring the stack up again

## 2) Verify Services

The gateway is not published to the host by default in the root compose file.
Use an in-container health check:

```powershell
docker compose exec gateway curl -f http://localhost:8081/healthz
```

If successful, the command returns HTTP 200.

## 3) Local Service Endpoints

Inside Docker network (service-to-service):
- Contacts: `http://contacts:18085`
- Apps: `http://apps:18086`
- Media: `http://media:18087`
- Gateway: `http://gateway:8081`

From your host machine:
- Postgres: `localhost:5432`

To expose gateway on your host (optional), add this to the `gateway` service in `docker-compose.yml`:

```yaml
ports:
  - "8081:8081"
```

Then recreate the service:

```powershell
docker compose up -d --build gateway
```

After that, host access works at `http://localhost:8081/healthz`.

## 4) Run Integration Test Container

The compose stack includes an `itest` container that runs integration tests.

Run once:

```powershell
docker compose run --rm itest
```

## 5) Run Repository Tests

Windows PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Integration
```

For gateway-only Go tests, prefer the bundled toolchain:

```powershell
Push-Location .\ohmf\services\gateway
& ..\..\.tools\go\bin\go.exe test ./...
Pop-Location
```

Linux/macOS:

```bash
chmod +x scripts/*.sh
./scripts/run-tests.sh --integration
```

## 6) Useful Daily Commands

View logs for all services:

```powershell
docker compose logs -f
```

View logs for one service:

```powershell
docker compose logs -f gateway
```

Restart one service:

```powershell
docker compose restart gateway
```

Refresh the full OHMF API automatically when gateway Go files change:

```powershell
powershell -ExecutionPolicy Bypass -File .\ohmf\scripts\watch-api.ps1
```

Stop the environment:

```powershell
docker compose down
```

Stop and remove volumes (clean reset):

```powershell
docker compose down -v
```

## 7) Full OHMF Stack (Alternative)

A larger compose setup also exists under:
- `ohmf/infra/docker/docker-compose.yml`

Start it from repo root with:

```powershell
docker compose -f .\ohmf\infra\docker\docker-compose.yml up -d --build
```

That stack includes additional components (Kafka, Redis, Cassandra, processors, and API variants) for broader end-to-end testing.

### Using `ohmf/scripts/run-dev.ps1` (recommended on Windows)

`ohmf/scripts/run-dev.ps1` is a convenience launcher for the OHMF stack.
It does all of the following:
- Finds Docker
- Stops/removes old OHMF containers
- Picks available ports if defaults are busy
- Writes `ohmf/apps/web/runtime-config.js`
- Starts: `db`, `api`, `client`, `messages-processor`, and `delivery-processor`

Run it:

```powershell
powershell -ExecutionPolicy Bypass -File .\ohmf\scripts\run-dev.ps1
```

Optional parameters:

```powershell
powershell -ExecutionPolicy Bypass -File .\ohmf\scripts\run-dev.ps1 -CLIENT_PORT 5173 -CONTAINER_PORT 8080 -HOST_PORT 18080
```

Important data note for this flow:
- `run-dev.ps1` uses `ohmf/infra/docker/docker-compose.yml`
- That compose file stores Postgres in Docker named volume `db_data` (not `postgres-data/`)

Reset OHMF DB volume used by `run-dev.ps1`:

```powershell
docker compose -f .\ohmf\infra\docker\docker-compose.yml -f .\ohmf\infra\docker\docker-compose.client.yml down -v
```

## Troubleshooting

- Docker build errors:
  - Ensure Docker Desktop is running
  - Retry with `docker compose build --no-cache`
- Gateway API not reflecting `.go` edits:
  - Run `.\ohmf\scripts\watch-api.ps1` so Docker rebuilds and restarts the API service on each change
- Postgres startup conflicts:
  - Confirm nothing else is using port 5432
  - Run `docker compose down -v` and start again
- Health check failures:
  - Run `docker compose logs -f <service>` to inspect failures

## Next Steps

- API and architecture docs: `ohmf/docs/`
- Setup reference for the OHMF folder: `ohmf/SETUP.md`
