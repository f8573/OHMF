const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./permissions.js");

test("eightball index loads rules and permissions before booting the app", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  assert.match(html, /<script src="\.\/rules\.js"><\/script>/);
  assert.match(html, /<script src="\.\/permissions\.js"><\/script>/);
});

test("blocked actions disable every mandatory host capability", () => {
  const blocked = helpers.describeBlockedActions([
    "conversation.read_context",
    "storage.session",
  ]);

  assert.equal(blocked.writeDisabled, true);
  assert.equal(blocked.projectDisabled, true);
  assert.equal(blocked.draftDisabled, false);
  assert.equal(blocked.refreshDisabled, false);
  assert.equal(blocked.missing.join(","), "conversation.send_message,realtime.session");
  assert.equal(blocked.blockedSummary, "Blocked: host denied projecting the latest summary, recording shots.");
});

test("permission denial copy names the blocked capability", () => {
  const message = helpers.permissionErrorMessage({
    message: "Permission required: conversation.send_message",
    details: { required_capability: "conversation.send_message" },
  });

  assert.equal(message, "Blocked: host denied conversation.send_message.");
});
