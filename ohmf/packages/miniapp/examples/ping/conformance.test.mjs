/**
 * Conformance tests for the external ping fixture and the sdk-web package.
 *
 * Verifies the bridge contract without a browser: the test installs a minimal
 * message-bus shim on globalThis so that OHMFMiniAppClient can exchange
 * postMessage envelopes with an inline test host in the same Node.js process.
 *
 * Run with:  node --test packages/miniapp/examples/ping/conformance.test.mjs
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

import { OHMFMiniAppClient, BRIDGE_VERSION, KNOWN_CAPABILITIES } from "../../sdk-web/index.js";

const __dir = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Test message bus
//
// OHMFMiniAppClient registers a handler via globalThis.addEventListener and
// calls targetWindow.postMessage() to send requests. The fake host below
// intercepts postMessage, processes the call, and synchronously delivers the
// response by calling the registered globalThis handlers directly — no real
// cross-frame messaging needed.
// ---------------------------------------------------------------------------

function createTestBridge(contextOverrides = {}) {
  const ORIGIN = "https://host.ohmf.test";
  const channel = `chan_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

  // Collect globalThis "message" handlers so we can deliver responses.
  const globalHandlers = [];
  const origAdd = globalThis.addEventListener;
  const origRemove = globalThis.removeEventListener;

  globalThis.addEventListener = function (type, handler, ...rest) {
    if (type === "message") { globalHandlers.push(handler); return; }
    origAdd?.call(this, type, handler, ...rest);
  };
  globalThis.removeEventListener = function (type, handler, ...rest) {
    if (type === "message") {
      const i = globalHandlers.indexOf(handler);
      if (i >= 0) globalHandlers.splice(i, 1);
      return;
    }
    origRemove?.call(this, type, handler, ...rest);
  };

  function deliverToApp(data) {
    const evt = { source: hostWindow, origin: ORIGIN, data };
    for (const h of [...globalHandlers]) h(evt);
  }

  // In-memory host state
  const sessionStorage = {};
  let stateVersion = 1;
  let stateSnapshot = {};

  const baseCtx = {
    bridge_version: BRIDGE_VERSION,
    app_id: "app.ohmf.ping",
    app_version: "1.0.0",
    app_session_id: "aps_conformance_001",
    conversation_id: "conv_conformance_001",
    viewer: { user_id: "usr_001", role: "PLAYER", display_name: "Conformance User" },
    participants: [{ user_id: "usr_001", role: "PLAYER", display_name: "Conformance User" }],
    capabilities_granted: [
      "conversation.read_context",
      "conversation.send_message",
      "storage.session",
      "realtime.session",
    ],
    host_capabilities: [],
    state_snapshot: stateSnapshot,
    state_version: stateVersion,
    joinable: true,
    ...contextOverrides,
  };

  // Fake host window: receives SDK calls and dispatches responses synchronously
  const hostWindow = {
    postMessage(data, _targetOrigin) {
      const { request_id, method, params = {} } = data;
      let result;
      let error;
      try {
        switch (method) {
          case "host.getLaunchContext":
            result = { ...baseCtx, state_snapshot: { ...stateSnapshot }, state_version: stateVersion };
            break;
          case "conversation.readContext":
            result = { conversation_id: baseCtx.conversation_id, title: "Conformance Chat", recent_messages: [] };
            break;
          case "conversation.sendMessage":
            result = { message_id: `msg_${Date.now()}`, state_version: stateVersion };
            break;
          case "storage.session.get":
            result = { key: params.key, value: sessionStorage[params.key] ?? null };
            break;
          case "storage.session.set":
            sessionStorage[params.key] = params.value;
            stateVersion += 1;
            result = { key: params.key, value: params.value, state_version: stateVersion };
            break;
          case "session.updateState":
            Object.assign(stateSnapshot, params);
            stateVersion += 1;
            result = { state_snapshot: { ...stateSnapshot }, state_version: stateVersion };
            break;
          default:
            throw Object.assign(new Error(`Unknown method: ${method}`), { code: "method_not_found" });
        }
      } catch (err) {
        error = { code: err.code || "bridge_error", message: err.message };
      }
      const response = error
        ? { bridge_version: BRIDGE_VERSION, channel, request_id, ok: false, error }
        : { bridge_version: BRIDGE_VERSION, channel, request_id, ok: true, result };
      deliverToApp(response);
    },
  };

  const client = new OHMFMiniAppClient({ channel, targetOrigin: ORIGIN, targetWindow: hostWindow });

  return {
    client,
    channel,
    origin: ORIGIN,
    /** Push a bridge_event directly to the app's listeners. */
    dispatchBridgeEvent(name, payload) {
      deliverToApp({ bridge_version: BRIDGE_VERSION, channel, bridge_event: name, payload });
    },
    cleanup() {
      client.destroy();
      globalThis.addEventListener = origAdd;
      globalThis.removeEventListener = origRemove;
    },
  };
}

// ---------------------------------------------------------------------------
// Static / structural tests
// ---------------------------------------------------------------------------

test("SDK exports BRIDGE_VERSION, KNOWN_CAPABILITIES, and OHMFMiniAppClient", () => {
  assert.equal(BRIDGE_VERSION, "1.0");
  assert.ok(Array.isArray(KNOWN_CAPABILITIES));
  assert.ok(KNOWN_CAPABILITIES.length > 0);
  assert.equal(typeof OHMFMiniAppClient, "function");
});

test("manifest.json is schema-valid", () => {
  const manifest = JSON.parse(readFileSync(join(__dir, "manifest.json"), "utf8"));

  assert.match(manifest.manifest_version, /^\d+\.\d+$/);
  assert.match(manifest.app_id, /^[a-z0-9]+(\.[a-z0-9-]+)+$/);
  assert.equal(typeof manifest.name, "string");
  assert.ok(manifest.name.length > 0);
  assert.match(manifest.version, /^\d+\.\d+\.\d+/);
  assert.equal(typeof manifest.entrypoint, "object");
  assert.ok(["url", "inline", "web_bundle"].includes(manifest.entrypoint.type));
  assert.equal(typeof manifest.entrypoint.url, "string");
  assert.equal(typeof manifest.message_preview, "object");
  assert.ok(["static_image", "live"].includes(manifest.message_preview.type));
  assert.ok(Array.isArray(manifest.permissions));
  assert.equal(typeof manifest.capabilities, "object");
});

test("app.js imports only from sdk-web — no internal web-app paths", () => {
  const source = readFileSync(join(__dir, "app.js"), "utf8");

  // Must import from the SDK package
  assert.match(source, /from ['"].*sdk-web\/index\.js['"]/);

  // Must not reference any internal app paths
  assert.doesNotMatch(source, /from ['"].*apps\/web/);
  assert.doesNotMatch(source, /from ['"].*miniapp-sdk\.js['"]/);
  assert.doesNotMatch(source, /require\(['"].*apps\/web/);
});

test("app.js uses createMiniAppClientFromLocation for standard bootstrap", () => {
  const source = readFileSync(join(__dir, "app.js"), "utf8");
  assert.match(source, /createMiniAppClientFromLocation/);
});

// ---------------------------------------------------------------------------
// Bridge conformance — lifecycle
// ---------------------------------------------------------------------------

test("bridge: getLaunchContext returns expected shape", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    const ctx = await client.getLaunchContext();
    assert.equal(ctx.bridge_version, BRIDGE_VERSION);
    assert.equal(ctx.app_id, "app.ohmf.ping");
    assert.equal(typeof ctx.app_session_id, "string");
    assert.equal(typeof ctx.conversation_id, "string");
    assert.equal(typeof ctx.viewer, "object");
    assert.ok(Array.isArray(ctx.participants));
    assert.ok(Array.isArray(ctx.capabilities_granted));
    assert.equal(typeof ctx.state_snapshot, "object");
    assert.equal(typeof ctx.state_version, "number");
    assert.equal(typeof ctx.joinable, "boolean");
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------------
// Bridge conformance — session storage (persist / reload)
// ---------------------------------------------------------------------------

test("bridge: session storage persist and reload round-trip", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    // Write
    const setResult = await client.setSessionStorage("ping_count", 42);
    assert.equal(setResult.key, "ping_count");
    assert.equal(setResult.value, 42);
    assert.equal(typeof setResult.state_version, "number");

    // Read back
    const getResult = await client.getSessionStorage("ping_count");
    assert.equal(getResult.key, "ping_count");
    assert.equal(getResult.value, 42);
  } finally {
    cleanup();
  }
});

test("bridge: reading an absent storage key returns null value", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    const result = await client.getSessionStorage("does_not_exist");
    assert.equal(result.key, "does_not_exist");
    assert.equal(result.value, null);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------------
// Bridge conformance — host messages
// ---------------------------------------------------------------------------

test("bridge: sendConversationMessage returns message_id and state_version", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    const result = await client.sendConversationMessage({
      content_type: "app_event",
      content: { event_name: "PING", body: { count: 1 } },
      text: "Ping count is now 1.",
    });
    assert.equal(typeof result.message_id, "string");
    assert.ok(result.message_id.length > 0);
    assert.equal(typeof result.state_version, "number");
  } finally {
    cleanup();
  }
});

test("bridge: updateSessionState merges and returns updated snapshot", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    const result = await client.updateSessionState({ ping_count: 7 });
    assert.equal(result.state_snapshot.ping_count, 7);
    assert.equal(typeof result.state_version, "number");
    assert.ok(result.state_version >= 1);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------------
// Bridge conformance — error handling
// ---------------------------------------------------------------------------

test("bridge: unknown method rejects with method_not_found code", async () => {
  const { client, cleanup } = createTestBridge();
  try {
    await assert.rejects(
      () => client.call("host.nonExistentMethod"),
      (error) => {
        assert.equal(error.code, "method_not_found");
        assert.equal(typeof error.message, "string");
        return true;
      },
    );
  } finally {
    cleanup();
  }
});

test("bridge: destroy rejects all pending calls with Bridge disposed", async () => {
  // Use a host that never responds
  const ORIGIN = "https://host.ohmf.test";
  const channel = "chan_destroy_test";
  const globalHandlers = [];
  const origAdd = globalThis.addEventListener;
  const origRemove = globalThis.removeEventListener;
  globalThis.addEventListener = (t, h) => { if (t === "message") globalHandlers.push(h); else origAdd?.call(globalThis, t, h); };
  globalThis.removeEventListener = (t, h) => { if (t === "message") { const i = globalHandlers.indexOf(h); if (i >= 0) globalHandlers.splice(i, 1); } else origRemove?.call(globalThis, t, h); };

  const silentHost = { postMessage() { /* intentionally never responds */ } };
  const client = new OHMFMiniAppClient({ channel, targetOrigin: ORIGIN, targetWindow: silentHost });

  const pending = client.getLaunchContext();
  client.destroy();

  await assert.rejects(pending, /Bridge disposed/);

  globalThis.addEventListener = origAdd;
  globalThis.removeEventListener = origRemove;
});

// ---------------------------------------------------------------------------
// Bridge conformance — event delivery
// ---------------------------------------------------------------------------

test("bridge: session.stateUpdated event delivers to on() handler", async () => {
  const { client, dispatchBridgeEvent, cleanup } = createTestBridge();
  try {
    const received = await new Promise((resolve) => {
      client.on("session.stateUpdated", resolve);
      dispatchBridgeEvent("session.stateUpdated", { state_snapshot: { ping_count: 5 }, state_version: 3 });
    });
    assert.equal(received.state_snapshot.ping_count, 5);
    assert.equal(received.state_version, 3);
  } finally {
    cleanup();
  }
});

test("bridge: session.permissionsUpdated event delivers updated capabilities_granted", async () => {
  const { client, dispatchBridgeEvent, cleanup } = createTestBridge();
  try {
    const received = await new Promise((resolve) => {
      client.on("session.permissionsUpdated", resolve);
      dispatchBridgeEvent("session.permissionsUpdated", {
        capabilities_granted: ["conversation.read_context"],
      });
    });
    assert.deepEqual(received.capabilities_granted, ["conversation.read_context"]);
  } finally {
    cleanup();
  }
});

test("bridge: on() returns an unsubscribe function that stops delivery", () => {
  const { client, dispatchBridgeEvent, cleanup } = createTestBridge();
  try {
    let callCount = 0;
    const unsub = client.on("session.stateUpdated", () => { callCount += 1; });
    dispatchBridgeEvent("session.stateUpdated", { state_version: 1 });
    assert.equal(callCount, 1);

    unsub();
    dispatchBridgeEvent("session.stateUpdated", { state_version: 2 });
    assert.equal(callCount, 1, "handler should not fire after unsubscribe");
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------------
// Bridge conformance — channel isolation
// ---------------------------------------------------------------------------

test("bridge: messages on a different channel are ignored", async () => {
  const ORIGIN = "https://host.ohmf.test";
  const channel = "chan_isolation_test";
  const globalHandlers = [];
  const origAdd = globalThis.addEventListener;
  const origRemove = globalThis.removeEventListener;
  globalThis.addEventListener = (t, h) => { if (t === "message") globalHandlers.push(h); else origAdd?.call(globalThis, t, h); };
  globalThis.removeEventListener = (t, h) => { if (t === "message") { const i = globalHandlers.indexOf(h); if (i >= 0) globalHandlers.splice(i, 1); } else origRemove?.call(globalThis, t, h); };

  let resolved = false;
  const hostWindow = { postMessage() {} };
  const client = new OHMFMiniAppClient({ channel, targetOrigin: ORIGIN, targetWindow: hostWindow });

  client.on("session.stateUpdated", () => { resolved = true; });

  // Deliver an event on a different channel
  const wrongChannelEvt = {
    source: hostWindow,
    origin: ORIGIN,
    data: { bridge_version: BRIDGE_VERSION, channel: "chan_wrong", bridge_event: "session.stateUpdated", payload: {} },
  };
  for (const h of [...globalHandlers]) h(wrongChannelEvt);

  assert.equal(resolved, false, "event on wrong channel must not reach app handlers");

  client.destroy();
  globalThis.addEventListener = origAdd;
  globalThis.removeEventListener = origRemove;
});

test("bridge: messages from a different origin are ignored", async () => {
  const ORIGIN = "https://host.ohmf.test";
  const channel = "chan_origin_test";
  const globalHandlers = [];
  const origAdd = globalThis.addEventListener;
  const origRemove = globalThis.removeEventListener;
  globalThis.addEventListener = (t, h) => { if (t === "message") globalHandlers.push(h); else origAdd?.call(globalThis, t, h); };
  globalThis.removeEventListener = (t, h) => { if (t === "message") { const i = globalHandlers.indexOf(h); if (i >= 0) globalHandlers.splice(i, 1); } else origRemove?.call(globalThis, t, h); };

  let resolved = false;
  const hostWindow = { postMessage() {} };
  const client = new OHMFMiniAppClient({ channel, targetOrigin: ORIGIN, targetWindow: hostWindow });

  client.on("session.stateUpdated", () => { resolved = true; });

  // Deliver an event from the wrong origin
  const wrongOriginEvt = {
    source: hostWindow,
    origin: "https://attacker.example",
    data: { bridge_version: BRIDGE_VERSION, channel, bridge_event: "session.stateUpdated", payload: {} },
  };
  for (const h of [...globalHandlers]) h(wrongOriginEvt);

  assert.equal(resolved, false, "event from wrong origin must not reach app handlers");

  client.destroy();
  globalThis.addEventListener = origAdd;
  globalThis.removeEventListener = origRemove;
});
