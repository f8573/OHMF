const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./transport-continuity.js");

test("index loads transport continuity helpers before app bootstrap", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  assert.match(html, /<script src="\.\/transport-continuity\.js"><\/script>/);
});

test("pre-onboarding phone threads stay explicitly on phone delivery", () => {
  const summary = helpers.threadTransportSummary({
    kind: "phone",
    e2eeReady: false,
    messages: [{ transport: "SMS" }],
  });

  assert.equal(summary.label, "Phone delivery");
  assert.equal(summary.subtitle, "Phone delivery");
  assert.doesNotMatch(summary.label, /secure/i);
  assert.doesNotMatch(summary.subtitle, /secure/i);
});

test("promotion switches the visible thread state to secure OHMF", () => {
  const summary = helpers.threadTransportSummary({
    kind: "dm",
    e2eeReady: true,
    messages: [{ transport: "SMS" }, { transport: "OHMF" }],
  });

  assert.equal(summary.label, "Secure OHMF");
  assert.match(summary.subtitle, /Promoted from phone delivery/);
  assert.equal(summary.promoted, true);
});

test("wrong-device and decrypt failures stay visible as distinct secure states", () => {
  const wrongDevice = helpers.messageFailureDetail({ transport: "OHMF", decryptStatus: "other_device" });
  const decryptError = helpers.messageFailureDetail({ transport: "OHMF", decryptStatus: "error" });

  assert.match(wrongDevice.label, /another linked device/i);
  assert.match(decryptError.label, /could not be decrypted/i);
  assert.notEqual(wrongDevice.label, decryptError.label);
});

test("retry and resend copy stays distinct between phone and secure failures", () => {
  const phoneFailed = helpers.formatOutgoingStatusLabel({ transport: "SMS", status: "FAIL_SEND" }, "Sent");
  const secureFailed = helpers.formatOutgoingStatusLabel({ transport: "OHMF", status: "FAIL_SEND" }, "Sent");
  const phoneSent = helpers.formatOutgoingStatusLabel({ transport: "SMS", status: "SENT" }, "Sent");

  assert.match(phoneFailed, /Phone delivery failed/i);
  assert.match(secureFailed, /Secure OHMF failed/i);
  assert.equal(phoneSent, "Sent");
  assert.notEqual(phoneFailed, secureFailed);
});

test("refresh or reconnect convergence replaces stale phone labels with secure ones", () => {
  const stale = helpers.threadTransportSummary({
    kind: "phone",
    e2eeReady: false,
    messages: [{ transport: "SMS" }],
  });
  const refreshed = helpers.threadTransportSummary({
    kind: "dm",
    e2eeReady: true,
    messages: [{ transport: "SMS" }, { transport: "OHMF" }],
  });

  assert.equal(stale.label, "Phone delivery");
  assert.equal(refreshed.label, "Secure OHMF");
  assert.notEqual(stale.subtitle, refreshed.subtitle);
});

test("direct messages do not show secure wording before readiness exists", () => {
  const summary = helpers.threadTransportSummary({
    kind: "dm",
    e2eeReady: false,
    messages: [],
  });

  assert.doesNotMatch(summary.label, /secure/i);
  assert.doesNotMatch(summary.subtitle, /secure/i);
  assert.match(summary.subtitle, /waiting for ohmf setup/i);
});
