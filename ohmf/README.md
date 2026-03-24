# OHMF Milestone 1 Foundation

**Phase 1 Status**: COMPLETE (28/29 items, 96.6%)

## Documentation

- **Platform spec**: [`OHMF_Complete_Platform_Spec_v1.md`](./OHMF_Complete_Platform_Spec_v1.md) - Single-document OHMF platform inventory with explicit feature statuses
- **iMessage reference**: [`IMESSAGE_COMPLETE_REFERENCE.md`](./IMESSAGE_COMPLETE_REFERENCE.md) - Current Apple Messages/iMessage capability reference used for parity decisions
- **Parity plan**: [`OHMF_IMESSAGE_PARITY_PLAN.md`](./OHMF_IMESSAGE_PARITY_PLAN.md) - Justified build/cut/add roadmap for OHMF relative to iMessage
- **Setup guide**: [`SETUP.md`](./SETUP.md) - Local development setup
- **Phase 1 Report**: [`FINAL_SESSION_REPORT.md`](./ohmf/FINAL_SESSION_REPORT.md) - Completion metrics and deliverables
- **Phase 2 Roadmap**: [`PHASE_2_ROADMAP.md`](./PHASE_2_ROADMAP.md) - Future work, blockers, priorities

## Local toolchain (non-admin)

Because system-wide installs were blocked by permissions, local binaries were installed under `.tools`:
- Go: `.tools/go/bin/go.exe`
- sqlc: `.tools/bin/sqlc.exe`
- migrate: `.tools/bin/migrate.exe`

## Build and test

```powershell
$env:PATH="C:\Users\James\Downloads\Messages\ohmf\.tools\go\bin;C:\Users\James\Downloads\Messages\ohmf\.tools\bin;$env:PATH"
cd C:\Users\James\Downloads\Messages\ohmf\services\gateway
go mod tidy
go build ./...
go test ./...
$env:OHMF_RUN_INTEGRATION="1"
go test ./integration -v
```

## Run with Docker Compose

1. Start Docker Desktop.
2. From repo root:

```powershell
docker compose -f .\infra\docker\docker-compose.yml up -d --build
```

API will be available at `http://localhost:18081` by default.

This stack now includes:
- Gateway (REST + WebSocket)
- Kafka (KRaft) + topic bootstrap
- Cassandra
- Redis
- `messages-processor`
- `delivery-processor`
- `sms-processor`

WebSocket endpoint: `ws://localhost:18081/v1/ws?access_token=<JWT>`

Feature flags (gateway):
- `APP_USE_KAFKA_SEND` (`true` by default in compose)
- `APP_USE_CASSANDRA_READS` (`false` by default in compose for phased rollout)
- `APP_ENABLE_WS_SEND` (`true` by default in compose)

## API endpoints

- `POST /v1/auth/phone/start`
- `POST /v1/auth/phone/verify`
- `POST /v1/auth/refresh`
- `POST /v1/auth/logout`
- `POST /v1/conversations`
- `GET /v1/conversations`
- `GET /v1/conversations/{id}`
- `POST /v1/messages`
- `POST /v1/messages/phone`
- `GET /v1/conversations/{id}/messages`
- `POST /v1/conversations/{id}/read`

OpenAPI: `packages/protocol/openapi/openapi.yaml`

User guide: `docs/user-signup-and-send-message.md`
