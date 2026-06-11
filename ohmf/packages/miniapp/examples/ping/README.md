Ping — minimal external SDK fixture

The simplest possible mini-app that exercises the full bridge contract without
any dependency on internal web-app files.

## What it demonstrates

- Bootstrap via `createMiniAppClientFromLocation()` from the public SDK package
- `getLaunchContext()` — receive initial state on mount
- `getSessionStorage` / `setSessionStorage` — persist and reload a counter
- `updateSessionState` — push state to other participants
- `sendConversationMessage` — project a message into the host thread
- `on("session.stateUpdated", ...)` — receive remote state pushes
- `on("session.permissionsUpdated", ...)` — react to permission changes
- Graceful error display when the bridge is unavailable

## External-only dependency rule

`app.js` imports exclusively from `../../sdk-web/index.js`. It does not
reference anything under `apps/web/` or any other internal path.
The conformance test enforces this rule.

## Conformance tests

```
node --test packages/miniapp/examples/ping/conformance.test.mjs
```

Tests cover: launch context shape, session storage round-trip, message send,
unknown-method error handling, `session.stateUpdated` event delivery, channel
isolation, and `session.permissionsUpdated` event handling.

## Local preview

Serve the repo root with any static server, then open the miniapp runtime lab:

```
http://localhost:5174/miniapp-runtime.html
```

Load the manifest at `packages/miniapp/examples/ping/manifest.json` and point
the entrypoint base URL at the correct local port.
