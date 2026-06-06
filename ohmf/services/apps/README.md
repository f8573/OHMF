# Mini-App Registry Service

This service is now the registry boundary for the OHMF mini-app platform.

## Responsibilities

**EXCLUSIVE ownership (control plane):**
- publisher app ownership (app metadata, ownership records)
- immutable releases keyed by `app_id + version`
- review workflow: `draft`, `submitted`, `under_review`, `needs_changes`, `approved`, `rejected`, `suspended`, `revoked`
- user install tracking (which user installed which app)
- update detection (installed_version vs latest_approved_version)
- publisher key registration, rotation, revocation
- catalog visibility rules (public/private/dev-only)
- review audit logging in `miniapp_registry_review_audit_log`

**NOT owned by this service** (delegated to gateway):
- mini-app sessions (ephemeral runtime state)
- session events (bridge call log)
- conversation shares (session initiation in messages)
- user state snapshots within sessions

**Integration with gateway:**
- compatibility support for `POST /v1/apps/register` in local developer mode
- PostgreSQL-backed control-plane persistence when `APP_DB_DSN` is configured
- gateway queries this service for app info, release versions, install state (read-only from gateway perspective)

## Main Endpoints

Host-facing:

- `GET /v1/apps`
- `GET /v1/apps/installed`
- `GET /v1/apps/{appID}`
- `POST /v1/apps/{appID}/install`
- `DELETE /v1/apps/{appID}/install`
- `GET /v1/apps/{appID}/updates`

Supported catalog query parameters:

- `q`
- `source_type`
- `visibility`
- `platform`
- `installed`
- `review_status`
- `limit`
- `cursor`
- `developer_mode`

Publisher-facing:

- `POST /v1/publisher/apps`
- `POST /v1/publisher/apps/{appID}/releases`
- `GET /v1/publisher/apps/{appID}/releases`
- `POST /v1/publisher/apps/{appID}/releases/{version}/submit`
- `POST /v1/publisher/apps/{appID}/releases/{version}/revoke`

Admin-facing:

- `POST /v1/admin/apps/{appID}/releases/{version}/start-review`
- `POST /v1/admin/apps/{appID}/releases/{version}/needs-changes`
- `POST /v1/admin/apps/{appID}/releases/{version}/approve`
- `POST /v1/admin/apps/{appID}/releases/{version}/reject`
- `POST /v1/admin/apps/{appID}/releases/{version}/suspend`

## Local Run

```bash
go run ./ohmf/services/apps
```

Environment variables:

- `APP_ENV` (values: `dev`, `staging`, `prod`; default: `dev`)
  - Controls persistence mode and validates configuration
  - **dev mode:** JSON file persistence allowed (for local testing)
  - **staging/prod:** Requires `APP_DB_DSN` (PostgreSQL mandatory)
- `APP_ADDR`
  - listen address, default `:18086`
- `APP_DB_DSN` (used in staging/prod; optional in dev mode)
  - PostgreSQL connection string; enables database-backed registry
  - **Required for all non-dev environments**
  - Example: `postgres://user:pass@localhost:5432/ohmf?sslmode=require`
- `DATA_FILE` (dev-only fallback)
  - JSON file path for file-backed persistence
  - Only used if `APP_DB_DSN` is not set AND `APP_ENV=dev`
  - default: `ohmf/services/apps/data/registry.json`
  - **Not used in production** (PostgreSQL is mandatory)
- `APP_MIGRATIONS_DIR`
  - migration directory, default `ohmf/services/apps/migrations`

### Persistence Modes

**Development Mode (`APP_ENV=dev`, `APP_DB_DSN` not set):**
- Uses JSON file at `DATA_FILE`
- Single-process only (not suitable for multi-instance deployments)
- Good for: local testing, prototyping, CI/CD pipelines
- Logs: `registry persistence: JSON file backend at ... (environment=dev)`

**Production Mode (`APP_ENV=staging|prod`, `APP_DB_DSN` set):**
- Uses PostgreSQL backend with connection pooling
- Cluster-safe (multiple instances sharing state)
- Required for: staging deployments, production deployments
- Logs: `registry persistence: PostgreSQL backend (environment=...)`

**Configuration Error:**
If `APP_ENV != dev` and `APP_DB_DSN` is not set, the service exits with error:
```
FATAL: APP_ENV=staging but APP_DB_DSN is not configured.
JSON persistence is only allowed in dev mode.
```

The gateway treats this service as the catalog/release source of truth while still owning app sessions and conversation sharing.

Persistence model:

**Enforcement (Startup Guard):**
- Apps service validates `APP_ENV` and `APP_DB_DSN` during startup
- If `APP_ENV != "dev"` and `APP_DB_DSN` is not set → **service exits with fatal error**
- This prevents accidental JSON-based deployments in production
- Clear error message directs operators to configure `APP_DB_DSN`

**Storage Modes:**
- **File-backed (dev only):** when `APP_ENV=dev` and `APP_DB_DSN` not set
  - JSON file at `DATA_FILE` (default `ohmf/services/apps/data/registry.json`)
  - Startup applies migrations from `APP_MIGRATIONS_DIR`
  - Registry writes are serialized with a single-process mutex
  - **Not cluster-safe; suitable for local development and testing only**

- **Database-backed (all environments):** when `APP_DB_DSN` is set
  - PostgreSQL connection string in `APP_DB_DSN`
  - Startup applies migrations from `APP_MIGRATIONS_DIR`
  - Registry writes are serialized with PostgreSQL advisory transaction lock
  - **Cluster-safe; required for staging and production**

**Audit & Reconciliation:**
- Release approval, rejection, revocation, install, uninstall, and app/release creation append review audit rows
- Operator backup/restore procedure is documented in `ohmf/docs/miniapp-registry-backup-restore.md`

Current install/update behavior:

- installs resolve to the latest approved release
- dev releases are hidden from normal users unless developer mode is explicitly enabled
- updates that add new permissions are flagged with `update_requires_consent` instead of silently changing the installed version
