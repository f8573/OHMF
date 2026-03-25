const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./device-management.js");

test("index exposes linked-device entrypoints in auth and app shells", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");

  for (const id of [
    "pair-device-auth-form",
    "pair-device-code-input",
    "linked-devices-btn",
    "device-manager-window",
    "linked-device-list",
  ]) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
});

test("pairing errors map invalid and expired codes to distinct user copy", () => {
  const invalid = helpers.describePairingError({ code: "invalid_pairing_code" });
  const expired = helpers.describePairingError({ code: "pairing_expired" });

  assert.equal(invalid.kind, "invalid");
  assert.match(invalid.title, /Invalid pairing code/i);
  assert.equal(expired.kind, "expired");
  assert.match(expired.title, /Pairing code expired/i);
  assert.notEqual(invalid.message, expired.message);
});

test("linked-device normalization keeps current device first and converges after refresh", () => {
  const initial = helpers.normalizeLinkedDevices([
    {
      id: "device-b",
      platform: "ios",
      device_name: "Phone",
      client_version: "1.2.0",
      capabilities: ["e2ee_ott_v2", "web_push_v1"],
      last_seen_at: "2026-03-24T12:05:00Z",
      attestation_state: "verified",
      ignored_secret: "should-not-surface",
    },
    {
      id: "device-a",
      platform: "web",
      device_name: "This browser",
      last_seen_at: "2026-03-24T12:10:00Z",
    },
  ], "device-a");

  assert.equal(initial[0].id, "device-a");
  assert.equal(initial[0].isCurrent, true);
  assert.deepEqual(Object.keys(initial[1]).sort(), [
    "attestationState",
    "capabilities",
    "clientVersion",
    "deviceName",
    "id",
    "isCurrent",
    "lastSeenAt",
    "platform",
  ].sort());

  const afterRefresh = helpers.normalizeLinkedDevices([
    {
      id: "device-a",
      platform: "web",
      device_name: "This browser",
      last_seen_at: "2026-03-24T12:15:00Z",
    },
  ], "device-a");

  assert.equal(afterRefresh.length, 1);
  assert.equal(afterRefresh.some((device) => device.id === "device-b"), false);
});
