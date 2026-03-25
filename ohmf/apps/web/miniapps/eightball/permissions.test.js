const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./permissions.js");

test("eightball index loads the permission helper before booting the app", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  assert.match(html, /<script src="\.\/permissions\.js"><\/script>/);
});

test("blocked actions stay visible when the host denies required capabilities", () => {
  const blocked = helpers.describeBlockedActions([
    "conversation.read_context",
    "storage.session",
  ]);

  assert.equal(blocked.askDisabled, true);
  assert.equal(blocked.sendDisabled, true);
  assert.equal(blocked.draftDisabled, false);
  assert.equal(blocked.refreshDisabled, false);
  assert.match(blocked.blockedSummary, /Blocked: host denied/i);
});

test("permission denial copy names the blocked capability", () => {
  const message = helpers.permissionErrorMessage({
    message: "Permission required: conversation.send_message",
    details: { required_capability: "conversation.send_message" },
  });

  assert.match(message, /conversation\.send_message/);
  assert.match(message, /Blocked:/);
});
