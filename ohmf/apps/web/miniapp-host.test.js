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

test("miniapp session persistence retries with the latest server version after a conflict", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /function buildMiniappSnapshotRequest\(nextVersion\)/);
  assert.match(source, /if \(error\?\.\s*code !== "state_version_conflict"\)/);
  assert.match(source, /const latest = await apiRequest\(`\/v1\/apps\/sessions\/\$\{encodeURIComponent\(sessionId\)\}`/);
  assert.match(source, /Number\(latest\?\.state_version \|\| latest\?\.launch_context\?\.state_version \|\| 0\) \+ 1/);
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

test("frontend config exposes developer mode so built-in apps appear in the selector", () => {
  const source = fs.readFileSync(path.join(__dirname, "frontend-config.js"), "utf8");
  assert.match(source, /developer_mode: Boolean\(runtimeConfig\.developer_mode\)/);
});

test("miniapp launcher falls back to the active frontend origin when no separate sandbox is configured", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /function miniappSandboxBaseURL\(\)/);
  assert.match(source, /function miniappFrameSandboxFlags\(\)/);
  assert.match(source, /window\.OHMF_RUNTIME_CONFIG\?\.miniapp_sandbox_url \|\| window\.location\.origin/);
  assert.match(source, /url\.hostname !== window\.location\.hostname \|\| url\.port !== window\.location\.port/);
  assert.match(source, /new URL\(manifestUrl, miniappSandboxBaseURL\(\) \+ "\/"\)\.toString\(\)/);
  assert.match(source, /const entrypointUrl = rewriteLocalDevEntrypoint\(state\.miniapp\.manifest\.entrypoint\.url\)/);
  assert.match(source, /new URL\(entrypointUrl, miniappSandboxBaseURL\(\) \+ "\/"\)/);
  assert.match(source, /url\.searchParams\.set\("asset_version", sanitizeText\(window\.OHMF_RUNTIME_CONFIG\?\.asset_version, 80\) \|\| "dev"\)/);
  assert.match(source, /return "allow-scripts";/);
  assert.match(source, /el\.miniappFrame\.setAttribute\("sandbox", miniappFrameSandboxFlags\(\)\)/);
});

test("built-in miniapp bootstrap checks the developer catalog before re-registering local apps", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /const devQuery = shouldBootstrapBuiltinMiniapps\(\) \? "\?developer_mode=1" : ""/);
  assert.match(source, /apiRequest\(`\/v1\/apps\$\{devQuery\}`/);
  assert.match(source, /errorCode !== "invalid_manifest" \|\| !errorMessage\.includes\("already published"\)/);
  assert.match(source, /apiRequest\(`\/v1\/apps\/\$\{encodeURIComponent\(appId\)\}\$\{devQuery\}`/);
});

test("miniapp message previews rewrite local dev asset ports before rendering sent cards", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /function normalizePreviewURL\(value\)/);
  assert.match(source, /new URL\(rewriteLocalDevAssetURL\(String\(value\)\), window\.location\.href\)/);
});

test("local web shell CSP allows miniapp frames and preview assets on localhost and 127.0.0.1", () => {
  const shellHtml = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  const runtimeHtml = fs.readFileSync(path.join(__dirname, "miniapp-runtime.html"), "utf8");

  assert.match(shellHtml, /img-src 'self' data: blob: http:\/\/localhost:\* http:\/\/127\.0\.0\.1:\*/);
  assert.match(shellHtml, /frame-src 'self' http:\/\/localhost:\* http:\/\/127\.0\.0\.1:\*/);
  assert.match(runtimeHtml, /img-src 'self' data: blob: http:\/\/localhost:\* http:\/\/127\.0\.0\.1:\*/);
  assert.match(runtimeHtml, /frame-src 'self' http:\/\/localhost:\* http:\/\/127\.0\.0\.1:\*/);
});

test("miniapp boot loaders use the asset_version query param instead of reaching into window.parent", () => {
  const eightballBoot = fs.readFileSync(path.join(__dirname, "miniapps", "eightball", "boot.js"), "utf8");
  const counterBoot = fs.readFileSync(path.join(__dirname, "miniapps", "counter", "boot.js"), "utf8");
  const runtimeSource = fs.readFileSync(path.join(__dirname, "miniapp-runtime.js"), "utf8");

  assert.match(eightballBoot, /runtimeParams\.get\("asset_version"\)/);
  assert.doesNotMatch(eightballBoot, /window\.parent\.OHMF_RUNTIME_CONFIG/);
  assert.match(counterBoot, /runtimeParams\.get\("asset_version"\)/);
  assert.doesNotMatch(counterBoot, /window\.parent\.OHMF_RUNTIME_CONFIG/);
  assert.match(runtimeSource, /url\.searchParams\.set\("asset_version", window\.OHMF_RUNTIME_CONFIG\?\.asset_version \|\| "dev"\)/);
  assert.match(runtimeSource, /url\.hostname !== window\.location\.hostname \|\| url\.port !== FRONTEND_PORT/);
  assert.match(runtimeSource, /let baseUrl = rewriteLocalDevEntrypoint\(state\.manifest\.entrypoint\.url\)/);
  assert.match(runtimeSource, /function frameSandboxFlags\(\)/);
  assert.match(runtimeSource, /"allow-scripts allow-same-origin"/);
});

test("live sync signs the user out when refresh and sync both fail with 401", () => {
  const source = fs.readFileSync(path.join(__dirname, "app.js"), "utf8");
  assert.match(source, /function handleUnauthorizedSession\(message = "Session expired\. Sign in again\."\)/);
  assert.match(source, /if \(Number\(error\?\.status \|\| 0\) === 401\) \{\s*handleUnauthorizedSession\(\);/);
});
