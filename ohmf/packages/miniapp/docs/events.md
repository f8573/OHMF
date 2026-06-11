# Mini-App Bridge Events

Bridge events are unsolicited messages pushed from the host to the app. They
share the same envelope as responses but use `bridge_event` instead of
`request_id` + `ok`.

Apps subscribe with `client.on(eventName, handler)` and unsubscribe by
calling the returned function or `client.off(eventName, handler)`.

---

## Host-to-app events

### `session.stateUpdated`

Fired when another participant's `session.updateState` call is committed by
the host. The app should merge the new snapshot into its local state and
re-render.

```typescript
interface StateUpdatedPayload {
  state_snapshot: Record<string, unknown>;
  state_version: number;
}
```

Example:

```js
client.on("session.stateUpdated", (payload) => {
  localState.counter = Number(payload.state_snapshot.counter) ?? 0;
  render();
});
```

---

### `session.permissionsUpdated`

Fired when the host user changes the capability grants for this app session.
The app should update its UI to reflect the new permission set.

```typescript
interface PermissionsUpdatedPayload {
  capabilities_granted: string[];
}
```

Example:

```js
client.on("session.permissionsUpdated", (payload) => {
  grantedCapabilities = payload.capabilities_granted ?? [];
  updatePermissionUI();
});
```

---

## App-to-host calls (bridge methods)

Apps initiate these calls via bridge methods documented in
[bridge-contract.md](../bridge-contract.md). There is no app-to-host
push mechanism — all app-initiated communication goes through the
request/response bridge.

---

## Event delivery guarantees

- Events are delivered on a best-effort basis. A missed `session.stateUpdated`
  event does not break state integrity because the app can always call
  `getLaunchContext()` to re-fetch the current snapshot and version.
- The host does not replay missed events after reconnection. Apps that need
  eventual consistency should re-fetch launch context on reconnect.
- The `bridge_event` field is absent on response messages; `request_id` is
  absent on event messages. Apps must not confuse the two shapes.
