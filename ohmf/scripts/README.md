# 18 — Developer Scripts & Tooling

Mapping: OHMF spec section 18 (Developer tooling & scripts)

Purpose
- Document repository scripts for developer workflows: database refresh, migrations, build helpers, local dev start, and codegen.

Key scripts (examples)
- `scripts/refresh-db-build.ps1` — rebuild local DB state.
- `scripts/dev.ps1` — start local dev environment and API helpers.
- `scripts/watch-api.ps1` — rebuild and restart the Docker API service whenever `services/gateway/**/*.go` changes.

Usage examples (PowerShell)
```powershell
# reset local DB
.\scripts\refresh-db-build.ps1

# watch gateway Go files and refresh the API container on changes
.\scripts\dev.ps1 watch-api

# run gateway tests with the bundled toolchain
Push-Location .\services\gateway
& ..\..\.tools\go\bin\go.exe test ./...
Pop-Location
```

Implementation constraints
- Scripts should be idempotent and check prerequisites (docker, go, node).

Security considerations
- Do not hardcode credentials; read from env or .env files.

Observability and operational notes
- Provide verbose and non-verbose modes.
- Keep database-backed development flows in Docker; use scripts/watchers to refresh the API process instead of editing container state by hand.

Testing requirements
- Smoke tests for scripts in CI.

References
- infra/docker README for environment details.

# Spec validation helpers
You can run a small repository validator that checks that the codebase contains
the components referenced by OHMF spec section 1 (Purpose and Scope). The
checker emits a JSON report to `build/spec_section_1_report.json` and returns
non-zero if required items are missing.

Run (if you have a Go toolchain available):
```powershell
# from repository root
go run scripts/check_spec_section_1.go
```

The checker is lightweight — it validates presence of directories and README
signals for the services/features listed in section 1. Use the JSON report for
CI gating or audit automation.
