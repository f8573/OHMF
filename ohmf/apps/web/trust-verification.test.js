const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./trust-verification.js");

test("index exposes trust verification entrypoints in the chat shell", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");

  for (const id of [
    "chat-trust-panel",
    "chat-trust-summary",
    "chat-trust-status",
    "chat-trust-refresh-btn",
    "chat-trust-list",
  ]) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
});

test("normalizeTrustDevices exposes verify and revoke actions from backend trust state", () => {
  const devices = helpers.normalizeTrustDevices([
    { device_id: "device-a", fingerprint: "aaa" },
    { device_id: "device-b", fingerprint: "bbb" },
  ], {
    "device-a": { effective_trust_state: "VERIFIED", current_fingerprint: "aaa" },
    "device-b": { effective_trust_state: "REVOKED", current_fingerprint: "bbb" },
  });

  assert.equal(devices[0].deviceId, "device-b");
  assert.equal(devices[0].canVerify, true);
  assert.equal(devices[0].canRevoke, false);
  assert.match(devices[0].warning, /revoked/i);

  const verified = devices.find((item) => item.deviceId === "device-a");
  assert.equal(verified.canVerify, false);
  assert.equal(verified.canRevoke, true);
  assert.equal(verified.trustStateLabel, "Verified");
});

test("mismatch warning remains visible after reload normalization", () => {
  const bundles = [{ device_id: "device-a", fingerprint: "new-fingerprint" }];
  const trustMap = {
    "device-a": {
      effective_trust_state: "MISMATCH",
      current_fingerprint: "new-fingerprint",
      recorded_fingerprint: "old-fingerprint",
    },
  };

  const first = helpers.normalizeTrustDevices(bundles, trustMap);
  const second = helpers.normalizeTrustDevices(bundles, trustMap);

  assert.equal(first[0].trustState, "MISMATCH");
  assert.match(first[0].warning, /fingerprint changed/i);
  assert.equal(second[0].warning, first[0].warning);
});

test("stale trust refresh re-reads the latest backend state", () => {
  const bundles = [{ device_id: "device-a", fingerprint: "aaa" }];
  const initial = helpers.normalizeTrustDevices(bundles, {
    "device-a": { effective_trust_state: "VERIFIED", current_fingerprint: "aaa" },
  });
  const refreshed = helpers.normalizeTrustDevices(bundles, {
    "device-a": { effective_trust_state: "REVOKED", current_fingerprint: "aaa" },
  });

  assert.equal(initial[0].trustState, "VERIFIED");
  assert.equal(refreshed[0].trustState, "REVOKED");
  assert.match(helpers.summarizeTrustDevices(refreshed), /revoked/i);
});
