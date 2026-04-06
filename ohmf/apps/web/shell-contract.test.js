const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

test("index exposes the core auth, shell, and composer surfaces", () => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");

  for (const id of [
    "auth-shell",
    "app-shell",
    "empty-state",
    "chat",
    "conversation-settings-btn",
    "conversation-settings-window",
    "logout-btn",
    "composer",
    "composer-input",
    "composer-attachment",
    "group-manager-window",
  ]) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
});

test("app source keeps refresh, realtime fallback, and attachment flows wired", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");

  assert.match(source, /async function refreshAuthTokens\(/);
  assert.match(source, /function logout\(\)/);
  assert.match(source, /function scheduleLiveRefreshLoop\(delayMs = LIVE_SYNC_INTERVAL_MS\)/);
  assert.match(source, /let eventStreamAbort = null;/);
  assert.match(source, /async function sendAttachmentDraft\(\)/);
  assert.match(source, /async function refreshGroupManager\(options = \{\}\)/);
  assert.match(source, /async function refreshTrustPanel\(options = \{\}\)/);
  assert.match(source, /function openConversationSettings\(\)/);
  assert.match(source, /function renderConversationSettings\(\)/);
});
