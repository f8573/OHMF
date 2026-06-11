# Mini-App Platform

This package holds the shared mini-app contract instead of only prose notes.

## Included

- `manifest.schema.json`
  - canonical manifest contract for registry publishing and host validation
- `bridge-contract.md`
  - canonical envelope and launch-context contract for web and Android hosts
- `docs/lifecycle.md`
  - registration, launch, teardown, and permissions lifecycle
- `docs/events.md`
  - host-to-app and app-to-host bridge event catalogue
- `docs/versioning.md`
  - bridge contract versioning and compatibility rules
- `sdk-web`
  - reusable web SDK package for bridge calls (JSDoc on all public methods)
- `sdk-types`
  - shared TypeScript declarations for launch context and bridge client shape
- `test-harness`
  - reusable mock host and local harness helpers
- `tools/miniapp-cli.mjs`
  - publisher-oriented CLI for validate, sign, package, upload-draft, and submit flows
- `examples/counter`
  - minimal state and transcript example (internal runtime dependency)
- `examples/eightball`
  - conversation-oriented demo game example (internal runtime dependency)
- `examples/ping`
  - minimal **external** fixture with zero internal deps; includes conformance tests

## Manifest Baseline

Required fields:

- `manifest_version`
- `app_id`
- `name`
- `version`
- `entrypoint`
- `message_preview`
- `permissions`
- `capabilities`

Optional but expected for production:

- `icons`
- `metadata`
- `signature`

## Developer Workflow

Validate:

```bash
node packages/miniapp/tools/miniapp-cli.mjs validate apps/web/miniapps/eightball/manifest.json
```

Sign:

```bash
node packages/miniapp/tools/miniapp-cli.mjs sign apps/web/miniapps/eightball/manifest.json --private-key ./keys/dev-ed25519.pem --kid dev
```

Package local asset metadata:

```bash
node packages/miniapp/tools/miniapp-cli.mjs package apps/web/miniapps/eightball/manifest.json --out ./dist/eightball
```

Upload a draft release through the host-facing publisher API:

```bash
node packages/miniapp/tools/miniapp-cli.mjs upload-draft apps/web/miniapps/eightball/manifest.json --api http://localhost:18080 --token <bearer>
```

Submit the uploaded release for review:

```bash
node packages/miniapp/tools/miniapp-cli.mjs submit app.ohmf.eightball 1.0.0 --api http://localhost:18080 --token <bearer>
```

## Runtime Rules

- Production apps should be registry-issued and reviewed.
- Developer-mode raw URL loading should stay opt-in.
- Hosts enforce granted capabilities at runtime even when the manifest declares more.
- Web and Android should use the same bridge envelope and launch-context shape.
- The launch context now carries `bridge_version`, `app_version`, and host-declared capabilities in addition to the app session identifiers and granted capabilities.
