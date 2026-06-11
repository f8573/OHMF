# Mini-App Lifecycle

---

## 1. Registration

Before a mini-app can be launched it must be registered with the host catalog.

**Production:** Submit a signed manifest through the publisher API
(`miniapp-cli.mjs submit`). The registry validates the signature, assigns an
immutable `(app_id, version)` release, and makes the app available in the
catalog.

**Developer mode:** Hosts with `developer_mode` enabled accept unsigned local
manifests at `?developer_mode=1`. The built-in catalog entries
(`app.ohmf.counter-lab`, `app.ohmf.eightball`) are bootstrapped this way at
startup.

**Manifest validation:** The host validates the manifest against
`packages/miniapp/manifest.schema.json` before accepting a registration.
Required fields: `manifest_version`, `app_id`, `name`, `version`,
`entrypoint`, `message_preview`, `permissions`, `capabilities`.

---

## 2. Installation

When a user first opens an app card in a conversation the host:

1. Fetches the manifest from the catalog (never from the app's own URL).
2. Validates the manifest schema.
3. Displays the requested `permissions` to the user.
4. Creates an app session via `POST /v1/apps/sessions` with `capabilities_granted`
   set to the user-approved subset.
5. Stores the granted set in local consent storage keyed by `app_id`.

On subsequent launches the host skips the permission prompt and uses the stored
consent, unless the manifest declares new permissions.

---

## 3. Launch

After installation the host:

1. Resolves the `entrypoint.url` through the sandbox base URL.
2. Appends `?channel=<token>&parent_origin=<host-origin>&asset_version=<v>`.
3. Creates an `<iframe sandbox="allow-scripts">` and sets `src` to the resolved URL.
4. Begins listening for bridge request messages from the iframe.

The app bootstraps by calling `createMiniAppClientFromLocation()` to parse
`channel` and `parent_origin`, then calls `getLaunchContext()` to receive its
initial state.

---

## 4. Active session

While the session is running:

- The app makes bridge calls to read/write storage, send messages, and update
  shared state.
- The host pushes `session.stateUpdated` events when another participant writes
  new state (see [events.md](events.md)).
- The host pushes `session.permissionsUpdated` if the user changes the app's
  permission grants.
- The `state_version` counter in the launch context and update responses
  provides optimistic locking for shared state. A host that rejects a stale
  write returns `state_version_conflict`; the app must re-fetch and retry.

---

## 5. Teardown

When the host closes the mini-app popup:

1. It optionally serialises the active session to restore storage (keyed by
   `(conversation_id, app_id, app_session_id)`).
2. It removes the iframe from the DOM.
3. The app's `destroy()` is implicitly called when the browsing context is
   removed. Apps should call `client.destroy()` explicitly if they have custom
   teardown logic (e.g. clearing timers).

On next open, if a stored session exists and the `state_version` matches the
server, the host restores it without a fresh `POST /v1/apps/sessions`.

---

## 6. Permissions

Capabilities are declared in the manifest `permissions` array and enforced
by the host at runtime regardless of what the manifest claims. Only
capabilities listed in `capabilities_granted` are usable.

Calling a method that requires a capability not in `capabilities_granted`
results in a `permission_denied` bridge error.

The app can inspect current grants via `launchContext.capabilities_granted`
and subscribe to `session.permissionsUpdated` for runtime changes.
