/**
 * Ping — minimal external miniapp fixture.
 *
 * This file imports ONLY from the public SDK package. It has zero dependencies
 * on anything inside apps/web or any other internal path.
 */
import { createMiniAppClientFromLocation, BRIDGE_VERSION } from "../../sdk-web/index.js";

const el = {
  status: document.getElementById("status"),
  pingCount: document.getElementById("ping-count"),
  pingBtn: document.getElementById("ping-btn"),
  sendBtn: document.getElementById("send-btn"),
  reloadBtn: document.getElementById("reload-btn"),
  log: document.getElementById("log"),
};

const STORAGE_KEY = "ping_count";

let bridge = null;
let pingCount = 0;

function setStatus(text, isError = false) {
  el.status.textContent = text;
  el.status.classList.toggle("error", isError);
}

function addLog(label, detail) {
  const li = document.createElement("li");
  li.textContent = detail === undefined
    ? label
    : `${label}: ${JSON.stringify(detail)}`;
  el.log.prepend(li);
}

async function savePingCount() {
  const result = await bridge.setSessionStorage(STORAGE_KEY, pingCount);
  addLog("storage.session.set", { key: STORAGE_KEY, state_version: result?.state_version });
}

async function loadPingCount() {
  const result = await bridge.getSessionStorage(STORAGE_KEY);
  const stored = typeof result?.value === "number" ? result.value : 0;
  pingCount = stored;
  el.pingCount.textContent = String(pingCount);
  addLog("storage.session.get", { key: STORAGE_KEY, value: stored });
}

async function ping() {
  pingCount += 1;
  el.pingCount.textContent = String(pingCount);
  await savePingCount();
  await bridge.updateSessionState({ ping_count: pingCount });
  setStatus(`Ping ${pingCount} committed.`);
  addLog("session.updateState", { ping_count: pingCount });
}

async function sendMessage() {
  const result = await bridge.sendConversationMessage({
    content_type: "app_event",
    content: { event_name: "PING", body: { count: pingCount } },
    text: `Ping count is now ${pingCount}.`,
  });
  addLog("conversation.sendMessage", { message_id: result?.message_id });
  setStatus("Projected ping message into conversation.");
}

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  if (!params.get("channel") || !params.get("parent_origin")) {
    setStatus("No bridge context — open via the miniapp runtime.", true);
    addLog("bridge_version", BRIDGE_VERSION);
    return;
  }

  bridge = createMiniAppClientFromLocation();

  bridge.on("session.stateUpdated", (payload) => {
    addLog("event session.stateUpdated", payload);
    if (typeof payload?.state_snapshot?.ping_count === "number") {
      pingCount = payload.state_snapshot.ping_count;
      el.pingCount.textContent = String(pingCount);
      setStatus(`Remote update: ping count is ${pingCount}.`);
    }
  });

  bridge.on("session.permissionsUpdated", (payload) => {
    addLog("event session.permissionsUpdated", payload);
    setStatus("Permission grants updated.");
  });

  try {
    const ctx = await bridge.getLaunchContext();
    addLog("host.getLaunchContext", {
      app_id: ctx.app_id,
      bridge_version: ctx.bridge_version,
      capabilities_granted: ctx.capabilities_granted,
    });

    await loadPingCount();
    pingCount = typeof ctx.state_snapshot?.ping_count === "number"
      ? ctx.state_snapshot.ping_count
      : pingCount;
    el.pingCount.textContent = String(pingCount);

    setStatus(`Bridge ready. Session: ${ctx.app_session_id}`);
  } catch (error) {
    setStatus(`Bridge error [${error.code ?? "error"}]: ${error.message}`, true);
    addLog("bootstrap error", { code: error.code, message: error.message, details: error.details });
  }
}

el.pingBtn.addEventListener("click", async () => {
  if (!bridge) { setStatus("No bridge.", true); return; }
  try { await ping(); }
  catch (error) { setStatus(`Ping failed [${error.code ?? "error"}]: ${error.message}`, true); }
});

el.sendBtn.addEventListener("click", async () => {
  if (!bridge) { setStatus("No bridge.", true); return; }
  try { await sendMessage(); }
  catch (error) { setStatus(`Send failed [${error.code ?? "error"}]: ${error.message}`, true); }
});

el.reloadBtn.addEventListener("click", async () => {
  if (!bridge) { setStatus("No bridge.", true); return; }
  try { await loadPingCount(); setStatus("Count reloaded from session storage."); }
  catch (error) { setStatus(`Reload failed [${error.code ?? "error"}]: ${error.message}`, true); }
});

bootstrap();
