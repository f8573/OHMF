# OHMF Test Gates

This repository exposes stable test entrypoints from the repo root:

```powershell
npm run test:unit
npm run test:integration
npm run test:web
npm run test:e2e
npm run test:live
npm run test:perf
npm run test:staging
```

List gates and suite-level tags:

```powershell
npm run test:list
```

## Gate Definitions

- `test:unit`: fast backend unit and contract coverage through the existing root Go test runner.
- `test:integration`: container-backed integration coverage through the existing Docker-based runner.
- `test:web`: fast web `node:test` coverage for shell helpers and browser-independent UI contracts.
- `test:e2e`: mocked Playwright coverage for deterministic browser flows.
- `test:live`: live Playwright coverage against a running OHMF stack. Requires a reachable API and frontend.
- `test:perf`: targeted race detection and benchmark coverage for gateway realtime, messaging, and E2EE paths.
- `test:staging`: staging/manual signoff gate. Prints the release checklist by default and optionally runs automation first when `OHMF_RUN_STAGING_AUTOMATION=1`.

## Environment Contract

These variables are the supported inputs for the standardized gates:

| Variable | Purpose |
|---|---|
| `OHMF_RUN_INTEGRATION` | Enables gateway integration tests where the Go suite expects integration mode. |
| `OHMF_E2E_LIVE` | Enables live Playwright browser flows. |
| `OHMF_API_BASE_URL` | Overrides the gateway base URL for web live tests. |
| `OHMF_E2E_BASE_URL` | Overrides the frontend base URL for Playwright. |
| `TEST_DATABASE_URL` | Overrides the database DSN for gateway DB-backed tests. |
| `POSTGRES_URL` / `DB_DSN` | Alternate DB DSN inputs already honored by existing scripts. |
| `OHMF_TEST_TAG` | Optional suite-level tag filter for any gate. Equivalent to `--tag`. |
| `OHMF_RUN_STAGING_AUTOMATION` | When set to `1`, `test:staging` runs integration and live automation before manual signoff. |

## Capability Tags

The standardized runner supports suite-level filtering for these tags:

- `auth`
- `messages`
- `conversations`
- `sync`
- `realtime`
- `devices`
- `privacy`
- `miniapp`
- `media`
- `relay`
- `e2ee`
- `search`
- `migration`
- `perf`

Example:

```powershell
npm run test:integration -- --tag auth
```

## CI Gate Intent

- PR gate: `test:unit`, `test:web`, and OpenAPI/schema validation.
- Merge gate: `test:integration` and `test:e2e`.
- Nightly gate: `test:live`, `test:perf`, and migration sweeps where infra is available.
- Pre-release gate: `test:staging` plus the manual checklist in [testing/STAGING_CHECKLIST.md](/Users/James/Downloads/Messages/testing/STAGING_CHECKLIST.md).

## Coverage Policy

A feature is only considered covered when it has:

- one happy-path automated test when runnable in this repo
- one validation or authorization failure assertion
- one state consistency or persistence assertion
- one manual script or checklist item if automation is not yet possible
