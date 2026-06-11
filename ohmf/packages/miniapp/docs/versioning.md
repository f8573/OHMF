# Bridge Contract Versioning

## Current version

`BRIDGE_VERSION = "1.0"`

The version is carried in every bridge message envelope. Hosts validate the
`bridge_version` field and reject frames that advertise an incompatible version.

---

## Compatibility rules

**Backward-compatible changes** (minor, no version bump):

- Adding a new optional field to a response shape.
- Adding a new bridge method.
- Adding a new `bridge_event` type.
- Relaxing a validation constraint.

**Breaking changes** (require a version bump):

- Removing or renaming a bridge method.
- Removing or renaming a field in the request or response envelope.
- Changing the semantics of an existing method.
- Tightening a validation constraint in a way that rejects previously valid input.

---

## Version negotiation

The launch context includes `bridge_version` so the app can detect the host's
declared version at runtime:

```js
const ctx = await client.getLaunchContext();
if (ctx.bridge_version !== BRIDGE_VERSION) {
  // host is running a different version — degrade gracefully
}
```

Apps should degrade gracefully when running against an older host that does not
support a method, rather than hard-failing. Catch the `method_not_found` error
and fall back to a reduced feature set.

---

## SDK package versioning

The `packages/miniapp/sdk-web` package is versioned independently of the bridge
wire protocol:

- **SDK patch/minor bumps**: bug fixes and new convenience helpers that do not
  change the wire protocol.
- **SDK major bump**: always accompanied by a `BRIDGE_VERSION` increment.

Apps should pin the SDK version they test against and upgrade deliberately.

---

## Manifest compatibility

The manifest `manifest_version` field tracks the schema version
(`packages/miniapp/manifest.schema.json`), not the bridge version. A host must
accept all manifests it can validate against its copy of the schema, regardless
of whether the `bridge_version` in the runtime matches.
