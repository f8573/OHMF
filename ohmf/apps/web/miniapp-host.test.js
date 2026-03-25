const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const helpers = require("./miniapp-host.js");

test("index loads miniapp host helpers before the main app bundle", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  assert.match(html, /<script src="\.\/miniapp-host\.js"><\/script>/);
});

test("conversation launch flow installs from catalog before creating a session", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /async function ensureMiniappSession[\s\S]*ensureMiniappInstalled\(entry\.appId/);
  assert.match(source, /async function ensureMiniappSession[\s\S]*apiRequest\("\/v1\/apps\/sessions"/);
  assert.match(source, /conversation_id: thread\.id/);
  assert.match(source, /capabilities_granted: Array\.from\(state\.miniapp\.grantedPermissions\)/);
});

test("restore helpers persist only an active popup session and round-trip through storage", () => {
  const serialized = helpers.buildRestoreState({
    threadId: "conv-1",
    appId: "app.ohmf.eightball",
    appSessionId: "aps-1",
    popupOpen: true,
  });

  assert.deepEqual(helpers.parseRestoreState(JSON.stringify(serialized)), {
    threadId: "conv-1",
    appId: "app.ohmf.eightball",
    appSessionId: "aps-1",
    popupOpen: true,
  });

  assert.equal(helpers.buildRestoreState({
    threadId: "conv-1",
    appId: "app.ohmf.eightball",
    appSessionId: "aps-1",
    popupOpen: false,
  }), null);
});

test("session event helpers refresh only the active session", () => {
  const activeEvent = {
    session_id: "aps-1",
    event_type: "snapshot_written",
    event_seq: 7,
    body: { state_version: 5 },
  };
  const staleEvent = {
    session_id: "aps-2",
    event_type: "snapshot_written",
    event_seq: 8,
    body: { state_version: 6 },
  };

  assert.equal(helpers.shouldRefreshSession(activeEvent, "aps-1"), true);
  assert.equal(helpers.shouldRefreshSession(staleEvent, "aps-1"), false);
  assert.equal(helpers.normalizeSessionEvent(activeEvent).eventType, "snapshot_written");
});
