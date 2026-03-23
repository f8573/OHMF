"use strict";

const DEFAULT_MANIFEST_URL = "./miniapps/counter/manifest.json";
const FRONTEND_PORT = String(window.OHMF_WEB_CONFIG?.frontend_port || window.location.port || "5174");
const DEFAULT_API_BASE_URL = (window.OHMF_WEB_CONFIG?.api_base_url || window.localStorage.getItem("ohmf.apiBaseUrl") || "http://localhost:18081").replace(/\/+$/, "");
const AUTH_STORAGE_KEY = "ohmf.auth.session.v1";
const STORAGE_PREFIX = "ohmf.miniapp.runtime.v2";
const PERMISSION_DESCRIPTIONS = Object.freeze({
  "conversation.read_context": "Read bounded thread metadata and a recent message window.",
  "conversation.send_message": "Project user-approved app messages into the conversation transcript.",
  "participants.read_basic": "Read participant ids, display names, and assigned roles.",
  "storage.session": "Persist app-private session key/value state through the host.",
  "storage.shared_conversation": "Persist conversation-scoped shared app state through the host.",
  "realtime.session": "Update shared session state and receive state change events.",
  "media.pick_user": "Open the host media picker after explicit user action.",
  "notifications.in_app": "Display host-mediated in-app prompts and status badges.",
});

// P4.3: WebSocket v2 protocol event types
const WS_V2_EVENTS = Object.freeze({
  HELLO: "hello",
  HELLO_ACK: "hello_ack",
  SUBSCRIBE_SESSION: "subscribe_session",
  SUBSCRIBE_SESSION_ACK: "subscribe_session_ack",
  SESSION_EVENT: "session_event",
  ERROR: "error",
});

// P4.3: Session event types
const SESSION_EVENT_TYPES = Object.freeze({
  SESSION_CREATED: "session_created",
  STORAGE_UPDATED: "storage_updated",
  SNAPSHOT_WRITTEN: "snapshot_written",
  MESSAGE_PROJECTED: "message_projected",
});

// P4.3: Module-scope WebSocket lifecycle (not application state)
let ws = null;
const activeSessionSubscriptions = new Set();

const state = {
  manifest: null,
  auth: null,
  apiBaseUrl: DEFAULT_API_BASE_URL,
  grantedPermissions: new Set(),
  frameWindow: null,
  channelId: "",
  launchContext: null,
  sessionState: null,
  sessionMode: "local",
  appOrigin: null, // P3.2: Isolated runtime origin (e.g., "a7f3e1c5.miniapp.local")
  logEntries: [],
};

const el = {
  status: document.getElementById("runtime-status"),
  manifestForm: document.getElementById("manifest-form"),
  manifestUrl: document.getElementById("manifest-url"),
  relaunchBtn: document.getElementById("relaunch-btn"),
  clearSessionBtn: document.getElementById("clear-session-btn"),
  permissionsList: document.getElementById("permissions-list"),
  contextJson: document.getElementById("context-json"),
  manifestJson: document.getElementById("manifest-json"),
  transcriptList: document.getElementById("transcript-list"),
  logList: document.getElementById("log-list"),
  frame: document.getElementById("app-frame"),
};

function nowTime() {
  return new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
}

function sanitizeText(value, limit = 240) {
  return String(value || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, limit);
}

function randomId(prefix) {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return `${prefix}_${window.crypto.randomUUID().replace(/-/g, "")}`;
  }
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function cloneJson(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

function storageKey(appId) {
  return `${STORAGE_PREFIX}.${appId}.session`;
}

function conversationKey(appId) {
  return `${STORAGE_PREFIX}.${appId}.conversation_id`;
}

function defaultTranscript() {
  return [
    {
      id: "msg_seed_1",
      author: "Avery",
      text: "Shared lists work better if the app can project summaries back into the thread.",
      createdAt: new Date(Date.now() - 1000 * 60 * 13).toISOString(),
    },
    {
      id: "msg_seed_2",
      author: "Jordan",
      text: "Use the counter app to test state sync, storage, and message projection.",
      createdAt: new Date(Date.now() - 1000 * 60 * 9).toISOString(),
    },
  ];
}

function defaultSessionState() {
  return {
    stateVersion: 1,
    stateSnapshot: { counter: 0, updated_by: "host" },
    storage: {},
    sharedConversationStorage: {},
    transcript: defaultTranscript(),
  };
}

function normalizeSessionState(raw) {
  if (!raw || typeof raw !== "object") {
    return defaultSessionState();
  }
  if ("snapshot" in raw || "session_storage" in raw || "shared_conversation_storage" in raw || "projected_messages" in raw) {
    return {
      stateVersion: Number(raw.state_version) > 0 ? Number(raw.state_version) : 1,
      stateSnapshot: raw.snapshot && typeof raw.snapshot === "object" ? raw.snapshot : { counter: 0, updated_by: "host" },
      storage: raw.session_storage && typeof raw.session_storage === "object" ? raw.session_storage : {},
      sharedConversationStorage: raw.shared_conversation_storage && typeof raw.shared_conversation_storage === "object" ? raw.shared_conversation_storage : {},
      transcript: Array.isArray(raw.projected_messages) && raw.projected_messages.length ? raw.projected_messages : defaultTranscript(),
    };
  }
  return {
    stateVersion: Number(raw.stateVersion) > 0 ? Number(raw.stateVersion) : 1,
    stateSnapshot: raw.stateSnapshot && typeof raw.stateSnapshot === "object" ? raw.stateSnapshot : { counter: 0, updated_by: "host" },
    storage: raw.storage && typeof raw.storage === "object" ? raw.storage : {},
    sharedConversationStorage: raw.sharedConversationStorage && typeof raw.sharedConversationStorage === "object" ? raw.sharedConversationStorage : {},
    transcript: Array.isArray(raw.transcript) && raw.transcript.length ? raw.transcript : defaultTranscript(),
  };
}

function sessionEnvelopeFromState() {
  return {
    snapshot: cloneJson(state.sessionState?.stateSnapshot || {}),
    session_storage: cloneJson(state.sessionState?.storage || {}),
    shared_conversation_storage: cloneJson(state.sessionState?.sharedConversationStorage || {}),
    projected_messages: cloneJson(state.sessionState?.transcript || []),
  };
}

function loadSavedSession(appId) {
  const raw = window.localStorage.getItem(storageKey(appId));
  if (!raw) return defaultSessionState();
  try {
    return normalizeSessionState(JSON.parse(raw));
  } catch {
    return defaultSessionState();
  }
}

function saveSession() {
  if (!state.manifest?.app_id || !state.sessionState) return;
  window.localStorage.setItem(storageKey(state.manifest.app_id), JSON.stringify(state.sessionState));
}

function clearSavedSession() {
  if (!state.manifest?.app_id) return;
  window.localStorage.removeItem(storageKey(state.manifest.app_id));
}

function loadConversationID(appId) {
  const key = conversationKey(appId);
  const existing = window.localStorage.getItem(key);
  if (existing) return existing;
  const generated = window.crypto?.randomUUID?.() || `${Date.now()}00000000-0000-4000-8000-000000000000`.slice(0, 36);
  window.localStorage.setItem(key, generated);
  return generated;
}

function loadRuntimeAuth() {
  const raw = window.sessionStorage.getItem(AUTH_STORAGE_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed?.accessToken || !parsed?.userId) return null;
    return parsed;
  } catch {
    return null;
  }
}

function setStatus(message) {
  el.status.textContent = sanitizeText(message, 280) || "Ready.";
  el.status.classList.remove("error");
}

function setErrorStatus(message) {
  el.status.textContent = sanitizeText(message, 280) || "Ready.";
  el.status.classList.add("error");
} // removed: boolean status flag split into named helpers

function addLog(kind, summary, detail) {
  state.logEntries.unshift({
    id: randomId("log"),
    kind,
    summary: sanitizeText(summary, 280),
    detail: detail === undefined ? "" : JSON.stringify(detail, null, 2),
    time: nowTime(),
  });
  state.logEntries = state.logEntries.slice(0, 60);
  renderLog();
}

function renderLog() {
  el.logList.replaceChildren();
  if (!state.logEntries.length) {
    const li = document.createElement("li");
    li.className = "log-item";
    li.textContent = "No bridge traffic yet.";
    el.logList.append(li);
    return;
  }

  state.logEntries.forEach((entry) => {
    const item = document.createElement("li");
    item.className = `log-item ${entry.kind}`;
    const header = document.createElement("header");
    const title = document.createElement("strong");
    title.textContent = entry.summary;
    const time = document.createElement("span");
    time.textContent = entry.time;
    header.append(title, time);
    item.append(header);
    if (entry.detail) {
      const pre = document.createElement("pre");
      pre.textContent = entry.detail;
      item.append(pre);
    }
    el.logList.append(item);
  });
}

function renderTranscript() {
  el.transcriptList.replaceChildren();
  const transcript = state.sessionState?.transcript || [];
  if (!transcript.length) {
    const li = document.createElement("li");
    li.className = "transcript-item";
    li.textContent = "No projected messages yet.";
    el.transcriptList.append(li);
    return;
  }

  transcript.forEach((message) => {
    const item = document.createElement("li");
    item.className = "transcript-item";
    const header = document.createElement("header");
    const author = document.createElement("strong");
    author.textContent = sanitizeText(message.author, 60) || "System";
    const time = document.createElement("span");
    const d = new Date(message.createdAt);
    time.textContent = Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    header.append(author, time);
    const body = document.createElement("div");
    body.textContent = sanitizeText(message.text, 280) || "(empty)";
    item.append(header, body);
    el.transcriptList.append(item);
  });
}

function renderManifest() {
  el.manifestJson.textContent = state.manifest ? JSON.stringify(state.manifest, null, 2) : "";
}

function renderContext() {
  el.contextJson.textContent = state.launchContext ? JSON.stringify(state.launchContext, null, 2) : "";
}

function currentGrantedPermissions() {
  return Array.from(state.grantedPermissions).sort();
}

function syncLaunchContextPermissions() {
  if (!state.launchContext) return;
  state.launchContext.capabilities_granted = currentGrantedPermissions();
}

function renderPermissions() {
  el.permissionsList.replaceChildren();
  const permissions = Array.isArray(state.manifest?.permissions) ? state.manifest.permissions : [];
  if (!permissions.length) {
    const empty = document.createElement("p");
    empty.className = "empty-note";
    empty.textContent = "No permissions declared.";
    el.permissionsList.append(empty);
    return;
  }

  permissions.forEach((permission) => {
    const item = document.createElement("label");
    item.className = "permission-item";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = state.grantedPermissions.has(permission);
    input.addEventListener("change", () => {
      if (input.checked) {
        state.grantedPermissions.add(permission);
      } else {
        state.grantedPermissions.delete(permission);
      }
      syncLaunchContextPermissions();
      pushBridgeEvent("session.permissionsUpdated", {
        app_session_id: state.launchContext?.app_session_id,
        capabilities_granted: currentGrantedPermissions(),
      });
      renderContext();
      setStatus(`Updated permission grants for ${state.manifest?.name || "app"}.`);
      void persistSessionState(0, "PERMISSIONS_UPDATED", {
        capabilities_granted: currentGrantedPermissions(),
      }).catch((error) => {
        addLog("error", "runtime.permissions_persist_failed", { message: error.message || String(error) });
      });
    });

    const copy = document.createElement("div");
    copy.className = "permission-copy";
    const title = document.createElement("strong");
    title.textContent = permission;
    const description = document.createElement("span");
    description.textContent = PERMISSION_DESCRIPTIONS[permission] || "Custom permission declared by the manifest.";
    copy.append(title, description);

    item.append(input, copy);
    el.permissionsList.append(item);
  });
}

function rewriteLocalDevEntrypoint(rawUrl) {
  const url = new URL(rawUrl, window.location.href);
  const localHosts = new Set(["localhost", "127.0.0.1"]);
  if (localHosts.has(url.hostname) && localHosts.has(window.location.hostname) && url.port !== FRONTEND_PORT) {
    url.protocol = window.location.protocol;
    url.host = `${window.location.hostname}:${FRONTEND_PORT}`;
  }
  return url.toString();
}

function validateManifest(manifest) {
  if (!manifest || typeof manifest !== "object") throw new Error("Manifest must be a JSON object.");
  if (!sanitizeText(manifest.app_id, 120)) throw new Error("Manifest is missing app_id.");
  if (!sanitizeText(manifest.name, 120)) throw new Error("Manifest is missing name.");
  if (!sanitizeText(manifest.version, 40)) throw new Error("Manifest is missing version.");
  if (!manifest.entrypoint || typeof manifest.entrypoint !== "object") throw new Error("Manifest entrypoint is required.");
  if (!["url", "inline", "web_bundle"].includes(sanitizeText(manifest.entrypoint.type, 40))) throw new Error("Manifest entrypoint.type is invalid.");
  if (!sanitizeText(manifest.entrypoint.url, 400)) throw new Error("Manifest entrypoint.url is required.");
  if (!Array.isArray(manifest.permissions)) throw new Error("Manifest permissions must be an array.");
  if (!manifest.capabilities || typeof manifest.capabilities !== "object") throw new Error("Manifest capabilities must be an object.");
  if (manifest.signature !== undefined && manifest.signature !== null && typeof manifest.signature !== "object") {
    throw new Error("Manifest signature must be an object when provided.");
  }
}

async function fetchManifest(url) {
  // Convert relative URLs to absolute URLs from mini-app sandbox origin
  let resolvedUrl = url;
  if (!url.startsWith("http://") && !url.startsWith("https://")) {
    const miniappSandboxUrl = window.OHMF_RUNTIME_CONFIG?.miniapp_sandbox_url || "http://localhost:5174";
    resolvedUrl = new URL(url, miniappSandboxUrl + "/").toString();
  }
  const response = await fetch(resolvedUrl, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Manifest request failed with ${response.status}.`);
  }
  const manifest = await response.json();
  validateManifest(manifest);
  manifest.entrypoint.url = rewriteLocalDevEntrypoint(manifest.entrypoint.url);
  if (!manifest.manifest_version) {
    manifest.manifest_version = "1.0";
  }
  return manifest;
}

function buildViewer() {
  state.auth = loadRuntimeAuth();
  if (state.auth?.userId) {
    return {
      user_id: state.auth.userId,
      role: "PLAYER",
      display_name: state.auth.phoneE164 || state.auth.userId,
    };
  }
  return { user_id: "usr_demo_1", role: "PLAYER", display_name: "Avery" };
}

function buildParticipants(viewer) {
  return [
    viewer,
    { user_id: "usr_demo_2", role: "PLAYER", display_name: "Jordan" },
  ];
}

function buildLocalLaunchContext() {
  const saved = loadSavedSession(state.manifest.app_id);
  const viewer = buildViewer();
  state.sessionMode = "local";
  state.sessionState = saved;
  state.launchContext = {
    app_id: state.manifest.app_id,
    app_session_id: randomId("aps"),
    conversation_id: loadConversationID(state.manifest.app_id),
    viewer,
    participants: buildParticipants(viewer),
    capabilities_granted: currentGrantedPermissions(),
    state_snapshot: saved.stateSnapshot,
    state_version: saved.stateVersion,
  };
}

function applySessionRecord(record, mode) {
  state.sessionMode = mode;
  state.sessionState = normalizeSessionState({
    state_version: record?.state_version,
    snapshot: record?.state?.snapshot,
    session_storage: record?.state?.session_storage,
    shared_conversation_storage: record?.state?.shared_conversation_storage,
    projected_messages: record?.state?.projected_messages,
  });
  // P3.2: Extract isolated origin from session response
  state.appOrigin = record?.app_origin || null;
  state.launchContext = record?.launch_context || {
    app_id: state.manifest.app_id,
    app_session_id: record?.app_session_id || randomId("aps"),
    conversation_id: record?.conversation_id || loadConversationID(state.manifest.app_id),
    viewer: buildViewer(),
    participants: buildParticipants(buildViewer()),
    capabilities_granted: Array.isArray(record?.capabilities_granted) ? record.capabilities_granted : currentGrantedPermissions(),
    state_snapshot: state.sessionState.stateSnapshot,
    state_version: state.sessionState.stateVersion,
  };
  state.grantedPermissions = new Set(Array.isArray(record?.capabilities_granted) && record.capabilities_granted.length ? record.capabilities_granted : state.manifest.permissions);
  syncLaunchContextPermissions();
  saveSession();
}

async function gatewayRequest(path, options = {}) {
  state.auth = loadRuntimeAuth();
  if (!state.auth?.accessToken) {
    throw new Error("No OHMF web auth session is available.");
  }

  const headers = new Headers(options.headers || {});
  headers.set("Authorization", `Bearer ${state.auth.accessToken}`);
  let body = options.body;
  if (body !== undefined) {
    headers.set("Content-Type", "application/json");
    body = JSON.stringify(body);
  }

  const response = await fetch(`${state.apiBaseUrl}${path}`, {
    method: options.method || "GET",
    headers,
    body,
  });

  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }

  if (!response.ok) {
    const err = new Error(payload?.message || `Gateway request failed with ${response.status}.`);
    err.code = payload?.code || "gateway_error";
    err.status = response.status;
    err.details = payload?.details;
    throw err;
  }
  return payload;
}

async function ensureManifestRegistered() {
  try {
    return await gatewayRequest(`/v1/apps/${encodeURIComponent(state.manifest.app_id)}`);
  } catch (error) {
    if (error.status !== 404) throw error;
  }

  await gatewayRequest("/v1/apps/register", {
    method: "POST",
    body: { manifest: state.manifest },
  });
  return gatewayRequest(`/v1/apps/${encodeURIComponent(state.manifest.app_id)}`);
}

async function ensureGatewaySession() {
  await ensureManifestRegistered();
  const viewer = buildViewer();
  const localSeed = loadSavedSession(state.manifest.app_id);
  const record = await gatewayRequest("/v1/apps/sessions", {
    method: "POST",
    body: {
      app_id: state.manifest.app_id,
      conversation_id: loadConversationID(state.manifest.app_id),
      viewer,
      participants: buildParticipants(viewer),
      capabilities_granted: currentGrantedPermissions(),
      state_snapshot: localSeed.stateSnapshot,
      resume_existing: true,
    },
  });
  applySessionRecord(record, "gateway");

  // P4.3: Subscribe to session events via WebSocket v2 if session established
  if (state.launchContext?.app_session_id && ws && ws.readyState === WebSocket.OPEN) {
    subscribeToSessionEvents(state.launchContext.app_session_id);
  } else if (state.launchContext?.app_session_id) {
    // Queue subscription for after WebSocket connects
    addLog("debug", "runtime.session_created_queued_for_subscription", {
      app_session_id: state.launchContext.app_session_id,
    });
  }
}

async function initializeSession() {
  state.auth = loadRuntimeAuth();
  if (state.auth?.accessToken) {
    try {
      await ensureGatewaySession();
      addLog("ok", "runtime.gateway_session", {
        app_session_id: state.launchContext?.app_session_id,
        conversation_id: state.launchContext?.conversation_id,
      });
      // P4.3: Establish WebSocket v2 connection for real-time session events
      await ensureWebSocketConnected();
      return;
    } catch (error) {
      addLog("error", "runtime.gateway_session_failed", { message: error.message || String(error), code: error.code });
    }
  }
  buildLocalLaunchContext();
  addLog("ok", "runtime.local_session", {
    app_session_id: state.launchContext?.app_session_id,
    conversation_id: state.launchContext?.conversation_id,
  });
}

// P4.3: WebSocket v2 Session Subscription Implementation
function ensureWebSocketConnected() {
  return new Promise((resolve) => {
    // If WebSocket is already connected, resolve immediately
    if (ws && ws.readyState === WebSocket.OPEN) {
      resolve();
      return;
    }

    // If WebSocket is connecting, wait for it using event-based listener
    if (ws && ws.readyState === WebSocket.CONNECTING) {
      const onOpen = () => {
        ws.removeEventListener("open", onOpen);
        resolve();
      };
      ws.addEventListener("open", onOpen);
      return;
    }

    // Create new WebSocket connection
    startWebSocketConnection().then(resolve);
  });
}

function startWebSocketConnection() {
  return new Promise((resolve) => {
    state.auth = loadRuntimeAuth();
    if (!state.auth?.accessToken) {
      addLog("warn", "runtime.websocket_skipped", { reason: "no_auth_token" });
      resolve();
      return;
    }

    const wsURL = new URL(state.apiBaseUrl.replace(/^http/i, "ws") + "/v2/ws");
    wsURL.searchParams.set("access_token", state.auth.accessToken);
    const socket = new WebSocket(wsURL.toString());
    ws = socket;

    socket.addEventListener("open", () => {
      addLog("ok", "runtime.websocket_connected", {});
      // Send hello handshake
      socket.send(JSON.stringify({
        event: WS_V2_EVENTS.HELLO,
        data: { device_id: randomId("dev") },
      }));
      // Resubscribe to active sessions after reconnection
      handleReconnect();
      resolve();
    });

    socket.addEventListener("message", (event) => {
      try {
        const message = JSON.parse(event.data);
        const eventName = message?.event;
        switch (eventName) {
          case WS_V2_EVENTS.HELLO_ACK:
            addLog("ok", "runtime.websocket_hello_ack", {});
            break;
          case WS_V2_EVENTS.SUBSCRIBE_SESSION_ACK:
            // Subscription successful
            addLog("ok", "runtime.session_subscribed", { session_id: message.data?.session_id });
            break;
          case WS_V2_EVENTS.SESSION_EVENT:
            // P4.3: Handle real-time session events
            handleSessionEvent(message.data);
            break;
          case WS_V2_EVENTS.ERROR:
            if (message.data?.code === "too_many_subscriptions") {
              addLog("warn", "runtime.too_many_subscriptions", { detail: message.data });
            } else {
              addLog("error", "runtime.websocket_error", { code: message.data?.code, detail: message.data });
            }
            break;
          default:
            addLog("debug", `runtime.websocket_message:${eventName}`, message.data);
        }
      } catch (error) {
        addLog("error", "runtime.websocket_parse_error", { message: error.message });
      }
    });

    socket.addEventListener("close", () => {
      if (ws === socket) {
        ws = null;
        addLog("warn", "runtime.websocket_closed", {});
      }
    });

    socket.addEventListener("error", (error) => {
      addLog("error", "runtime.websocket_error", { message: error.message || "WebSocket error" });
      socket.close();
    });
  });
}

function subscribeToSessionEvents(sessionID) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    addLog("warn", "runtime.session_subscribe_skipped", { session_id: sessionID, reason: "websocket_not_ready" });
    return;
  }

  // Skip redundant subscriptions
  if (activeSessionSubscriptions.has(sessionID)) {
    return;
  }

  ws.send(JSON.stringify({
    event: WS_V2_EVENTS.SUBSCRIBE_SESSION,
    data: { session_id: sessionID },
  }));

  // Track subscription for reconnection
  activeSessionSubscriptions.add(sessionID);
  addLog("debug", "runtime.session_subscribed_request_sent", { session_id: sessionID });
}

function emitSessionEvent(eventType, data) {
  window.postMessage({
    type: WS_V2_EVENTS.SESSION_EVENT,
    payload: { event_type: eventType, data },
  }, state.appOrigin || "*");
}

function handleSessionEvent(evt) {
  // Extract event details
  const { event_seq, event_type, actor_id, body, created_at } = evt;

  // Update local app state based on event_type
  switch (event_type) {
    case SESSION_EVENT_TYPES.SESSION_CREATED:
      // Emit event for mini-app to react to
      emitSessionEvent(SESSION_EVENT_TYPES.SESSION_CREATED, body);
      addLog("ok", "runtime.session_event:session_created", { event_seq, body });
      break;

    case SESSION_EVENT_TYPES.STORAGE_UPDATED:
      // Update local session storage state from event
      if (body && typeof body === "object") {
        state.sessionState.storage = { ...state.sessionState.storage, ...body };
        saveSession();
      }
      emitSessionEvent(SESSION_EVENT_TYPES.STORAGE_UPDATED, body);
      addLog("ok", "runtime.session_event:storage_updated", { event_seq, body });
      break;

    case SESSION_EVENT_TYPES.SNAPSHOT_WRITTEN:
      // Update local snapshot state from event
      if (body && typeof body === "object") {
        state.sessionState.stateSnapshot = body;
        state.launchContext.state_snapshot = cloneJson(body);
        saveSession();
      }
      emitSessionEvent(SESSION_EVENT_TYPES.SNAPSHOT_WRITTEN, body);
      addLog("ok", "runtime.session_event:snapshot_written", { event_seq, body });
      break;

    case SESSION_EVENT_TYPES.MESSAGE_PROJECTED:
      // Handle message event
      if (body && typeof body === "object") {
        appendProjectedMessage(body.text || "(message event)", body.content_type || "app_event", body.content || null);
      }
      emitSessionEvent(SESSION_EVENT_TYPES.MESSAGE_PROJECTED, body);
      addLog("ok", "runtime.session_event:message_projected", { event_seq, body });
      break;

    default:
      addLog("debug", `runtime.session_event:${event_type}`, { event_seq, body });
  }
}

function handleReconnect() {
  // Resubscribe to all active sessions after reconnection (removed: redundant null/size checks)
  if (activeSessionSubscriptions.size > 0) {
    addLog("debug", "runtime.reconnect_resubscribing", { count: activeSessionSubscriptions.size });
    for (const sessionID of activeSessionSubscriptions) {
      subscribeToSessionEvents(sessionID);
    }
  }
}

function buildFrameUrl() {
  // Use mini-app sandbox origin (separate from main app)
  // This ensures the mini-app runs in an isolated security context
  const miniappSandboxUrl = window.OHMF_RUNTIME_CONFIG?.miniapp_sandbox_url || "http://localhost:5174";
  let baseUrl = state.manifest.entrypoint.url;

  // In production with isolated origin infrastructure, the app_origin would be used as:
  // const protocol = new URL(state.manifest.entrypoint.url).protocol;
  // baseUrl = `${protocol}//${state.appOrigin}` if state.appOrigin exists

  const url = new URL(baseUrl, miniappSandboxUrl + "/");
  url.searchParams.set("channel", state.channelId);
  url.searchParams.set("parent_origin", window.location.origin);
  url.searchParams.set("app_id", state.manifest.app_id);
  url.searchParams.set("app_origin", state.appOrigin || ""); // P3.2: Include origin for client-side validation
  return url.toString();
}

function launchFrame() {
  state.channelId = randomId("chan");
  state.frameWindow = null;
  // P3.2: Sandbox attributes enforce isolation
  // allow-scripts: required for app code execution
  // allow-same-origin removed (P3.1): no direct host access via cookies/storage
  el.frame.setAttribute("sandbox", "allow-scripts");
  el.frame.src = buildFrameUrl();
  const suffix = state.sessionMode === "gateway" ? " using gateway session." : " using local fallback session.";
  const originNote = state.appOrigin ? ` (isolated: ${state.appOrigin})` : "";
  setStatus(`Launching ${state.manifest.name}${suffix}${originNote}`);
  addLog("ok", "runtime.launch", {
    entrypoint: state.manifest.entrypoint.url,
    app_origin: state.appOrigin,
    channel: state.channelId,
    mode: state.sessionMode,
    sandbox: "allow-scripts",
  });
}

function requirePermission(permission) {
  if (!permission) return;
  if (!state.grantedPermissions.has(permission)) {
    const err = new Error(`Permission denied: ${permission}`);
    err.code = "miniapp_capability_denied";
    err.details = { required_capability: permission };
    throw err;
  }
}

function pushBridgeEvent(name, payload) {
  if (!state.frameWindow) return;
  state.frameWindow.postMessage(
    {
      bridge_version: "1.0",
      bridge_event: name,
      channel: state.channelId,
      payload: cloneJson(payload),
    },
    "*"
  );
  addLog("ok", name, payload);
}

function sendBridgeResponse(targetWindow, requestId, ok, result, error) {
  targetWindow.postMessage(
    {
      bridge_version: "1.0",
      channel: state.channelId,
      request_id: requestId,
      ok,
      result: ok ? cloneJson(result) : undefined,
      error: ok ? undefined : error,
    },
    "*"
  );
}

function appendProjectedMessage(text, contentType = "app_event", content = null) {
  state.sessionState.transcript.push({
    id: randomId("msg"),
    author: state.manifest.name,
    text,
    content_type: contentType,
    content: cloneJson(content),
    createdAt: new Date().toISOString(),
  });
  state.sessionState.transcript = state.sessionState.transcript.slice(-20);
  renderTranscript();
  saveSession();
}

async function appendSessionEvent(eventName, body) {
  if (state.sessionMode !== "gateway" || !state.launchContext?.app_session_id) return null;
  return gatewayRequest(`/v1/apps/sessions/${encodeURIComponent(state.launchContext.app_session_id)}/events`, {
    method: "POST",
    body: { event_name: eventName, body },
  });
}

async function persistSessionState(version, eventName, eventBody) {
  if (!state.sessionState) return 0;
  saveSession();

  if (state.sessionMode !== "gateway" || !state.launchContext?.app_session_id) {
    if (version > 0) {
      state.sessionState.stateVersion = version;
      state.launchContext.state_version = version;
    }
    return state.sessionState.stateVersion;
  }

  try {
    const payload = await gatewayRequest(`/v1/apps/sessions/${encodeURIComponent(state.launchContext.app_session_id)}/snapshot`, {
      method: "POST",
      body: {
        state: sessionEnvelopeFromState(),
        state_version: version,
        capabilities_granted: currentGrantedPermissions(),
      },
    });
    state.sessionState.stateVersion = Number(payload?.state_version) > 0 ? Number(payload.state_version) : state.sessionState.stateVersion;
    state.launchContext.state_version = state.sessionState.stateVersion;
    state.launchContext.state_snapshot = cloneJson(state.sessionState.stateSnapshot);
    saveSession();
    if (eventName) {
      await appendSessionEvent(eventName, eventBody);
    }
    return state.sessionState.stateVersion;
  } catch (error) {
    if (error.status === 409 && state.launchContext?.app_session_id) {
      const refreshed = await gatewayRequest(`/v1/apps/sessions/${encodeURIComponent(state.launchContext.app_session_id)}`);
      applySessionRecord(refreshed, "gateway");
    }
    throw error;
  }
}

async function applyStateUpdate(params) {
  requirePermission("realtime.session");
  const requested = params && typeof params === "object" ? params : {}; // removed: deep clone not needed, spread will copy
  if ("counter" in requested) {
    requested.counter = Math.max(0, Math.min(9999, Number(requested.counter) || 0));
  }

  state.sessionState.stateSnapshot = {
    ...(state.sessionState.stateSnapshot || {}), // removed: deep clone, shallow spread suffices
    ...requested,
    updated_by: state.launchContext?.viewer?.display_name || state.launchContext?.viewer?.user_id || "app",
    updated_at: new Date().toISOString(),
  };
  state.sessionState.stateVersion += 1;
  state.launchContext.state_snapshot = state.sessionState.stateSnapshot; // removed: redundant clone of newly created object
  state.launchContext.state_version = state.sessionState.stateVersion;

  const persistedVersion = await persistSessionState(state.sessionState.stateVersion, "STATE_UPDATED", {
    app_session_id: state.launchContext.app_session_id,
    delta: requested,
    state_version: state.sessionState.stateVersion,
  });

  renderContext();
  pushBridgeEvent("session.stateUpdated", {
    app_session_id: state.launchContext.app_session_id,
    state_version: persistedVersion,
    delta: requested,
    state_snapshot: state.sessionState.stateSnapshot,
  });
  return { state_version: persistedVersion, state_snapshot: state.sessionState.stateSnapshot };
}

function summarizeMessagePayload(params) {
  const explicitText = sanitizeText(params?.text, 220);
  if (explicitText) return explicitText;
  const bodyText = sanitizeText(params?.content?.body?.text, 220);
  if (bodyText) return bodyText;
  const eventName = sanitizeText(params?.content?.event_name, 80);
  if (eventName) return `${state.manifest.name}: ${eventName}`;
  return `${state.manifest.name} posted an update.`;
}

async function pickUserMedia(params) {
  requirePermission("media.pick_user");
  const input = document.createElement("input");
  input.type = "file";
  input.accept = sanitizeText(params?.accept, 200);
  input.multiple = Boolean(params?.multiple);

  const files = await new Promise((resolve) => {
    input.addEventListener("change", () => resolve(Array.from(input.files || [])), { once: true });
    input.click();
  });

  const items = await Promise.all(
    files.slice(0, input.multiple ? 5 : 1).map(
      (file) =>
        new Promise((resolve) => {
          if (file.size > 256 * 1024) {
            resolve({ name: file.name, type: file.type, size_bytes: file.size, last_modified: file.lastModified });
            return;
          }
          const reader = new FileReader();
          reader.addEventListener("load", () => {
            resolve({
              name: file.name,
              type: file.type,
              size_bytes: file.size,
              last_modified: file.lastModified,
              data_url: typeof reader.result === "string" ? reader.result : "",
            });
          });
          reader.readAsDataURL(file);
        })
    )
  );

  await appendSessionEvent("MEDIA_PICKED", { count: items.length, names: items.map((item) => item.name) });
  return { items };
}

async function showInAppNotification(params) {
  requirePermission("notifications.in_app");
  const title = sanitizeText(params?.title || "Mini-app notice", 80);
  const message = sanitizeText(params?.message || "", 180);
  const notificationID = randomId("note");
  setStatus(`${title}${message ? `: ${message}` : ""}`);
  await appendSessionEvent("NOTIFICATION_SHOWN", { notification_id: notificationID, title, message });
  return { notification_id: notificationID, displayed: true };
}

async function handleBridgeCall(message) {
  const method = sanitizeText(message.method, 120);
  switch (method) {
    case "host.getLaunchContext":
      syncLaunchContextPermissions();
      return cloneJson(state.launchContext);
    case "conversation.readContext":
      requirePermission("conversation.read_context");
      return {
        conversation_id: state.launchContext.conversation_id,
        title: "Mini-App Runtime Test Thread",
        recent_messages: cloneJson((state.sessionState?.transcript || []).slice(-6)),
      };
    case "conversation.sendMessage": {
      requirePermission("conversation.send_message");
      const text = summarizeMessagePayload(message.params);
      appendProjectedMessage(text, sanitizeText(message.params?.content_type, 60) || "app_event", message.params?.content || null);
      state.sessionState.stateVersion += 1;
      const persistedVersion = await persistSessionState(state.sessionState.stateVersion, "MESSAGE_PROJECTED", {
        app_session_id: state.launchContext.app_session_id,
        content_type: sanitizeText(message.params?.content_type, 60) || "app_event",
        content: cloneJson(message.params?.content || null),
        text,
      });
      return { message_id: randomId("msg"), state_version: persistedVersion };
    }
    case "conversation.readParticipants":
    case "participants.readBasic":
      requirePermission("participants.read_basic");
      return { participants: cloneJson(state.launchContext?.participants || []) };
    case "storage.session.get": {
      requirePermission("storage.session");
      const key = sanitizeText(message.params?.key, 80);
      if (!key) throw new Error("storage.session.get requires params.key");
      return { key, value: cloneJson(state.sessionState.storage[key]) };
    }
    case "storage.session.set": {
      requirePermission("storage.session");
      const key = sanitizeText(message.params?.key, 80);
      if (!key) throw new Error("storage.session.set requires params.key");
      state.sessionState.storage[key] = cloneJson(message.params?.value);
      state.sessionState.stateVersion += 1;
      const persistedVersion = await persistSessionState(state.sessionState.stateVersion, "SESSION_STORAGE_UPDATED", { key });
      return { key, value: cloneJson(state.sessionState.storage[key]), state_version: persistedVersion };
    }
    case "storage.sharedConversation.get": {
      requirePermission("storage.shared_conversation");
      const key = sanitizeText(message.params?.key, 80);
      if (!key) throw new Error("storage.sharedConversation.get requires params.key");
      return { key, value: cloneJson(state.sessionState.sharedConversationStorage[key]) };
    }
    case "storage.sharedConversation.set": {
      requirePermission("storage.shared_conversation");
      const key = sanitizeText(message.params?.key, 80);
      if (!key) throw new Error("storage.sharedConversation.set requires params.key");
      state.sessionState.sharedConversationStorage[key] = cloneJson(message.params?.value);
      state.sessionState.stateVersion += 1;
      const persistedVersion = await persistSessionState(state.sessionState.stateVersion, "SHARED_STORAGE_UPDATED", { key });
      return { key, value: cloneJson(state.sessionState.sharedConversationStorage[key]), state_version: persistedVersion };
    }
    case "session.updateState":
      return applyStateUpdate(message.params);
    case "media.pickUser":
      return pickUserMedia(message.params);
    case "notifications.inApp.show":
      return showInAppNotification(message.params);
    default: {
      const err = new Error(`Unknown bridge method: ${method}`);
      err.code = "method_not_found";
      throw err;
    }
  }
}

async function loadAndLaunch(manifestUrl) {
  const cleanUrl = sanitizeText(manifestUrl, 300) || DEFAULT_MANIFEST_URL;
  state.manifest = await fetchManifest(cleanUrl);
  state.grantedPermissions = new Set(state.manifest.permissions);
  await initializeSession();
  renderManifest();
  renderPermissions();
  renderContext();
  renderTranscript();
  launchFrame();
  el.manifestUrl.value = cleanUrl;
}

window.addEventListener("message", async (event) => {
  if (event.source !== el.frame.contentWindow) return;

  // P3.2: Validate origin if isolated origin is available
  // In production, this validates that the iframe is running at the expected isolated origin
  if (state.appOrigin) {
    const expectedOriginUrl = new URL(`http://${state.appOrigin}`);
    const messageOrigin = new URL(event.origin);
    if (messageOrigin.host !== expectedOriginUrl.host) {
      addLog("error", "runtime.invalid_origin", {
        expected: expectedOriginUrl.host,
        received: messageOrigin.host,
      });
      return;
    }
  } else {
    // Fallback: validate against manifest entrypoint origin
    if (state.manifest?.entrypoint?.url) {
      const expectedOrigin = new URL(state.manifest.entrypoint.url, window.location.href).origin;
      if (event.origin !== expectedOrigin) return;
    }
  }

  const message = event.data;
  if (!message || typeof message !== "object") return;
  if (message.channel !== state.channelId) return;

  state.frameWindow = event.source;
  const requestId = sanitizeText(message.request_id, 80);
  if (!requestId) return;

  addLog("ok", `request ${sanitizeText(message.method, 120)}`, message);
  try {
    const result = await handleBridgeCall(message);
    sendBridgeResponse(event.source, requestId, true, result);
  } catch (error) {
    const payload = {
      code: sanitizeText(error.code || "bridge_error", 80),
      message: sanitizeText(error.message || "Bridge call failed", 220),
      details: error.details && typeof error.details === "object" ? error.details : undefined,
    };
    addLog("error", `error ${sanitizeText(message.method, 120)}`, payload);
    sendBridgeResponse(event.source, requestId, false, null, payload);
  }
});

el.manifestForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await loadAndLaunch(el.manifestUrl.value);
  } catch (error) {
    setErrorStatus(error.message || "Failed to load manifest.");
    addLog("error", "runtime.load_failed", { message: error.message || String(error) });
  }
});

el.relaunchBtn.addEventListener("click", () => {
  if (!state.manifest) {
    setErrorStatus("Load a manifest before relaunching.");
    return;
  }
  renderContext();
  renderTranscript();
  launchFrame();
});

el.clearSessionBtn.addEventListener("click", async () => {
  if (!state.manifest) {
    setErrorStatus("Load a manifest before clearing session state.");
    return;
  }
  try {
    if (state.sessionMode === "gateway" && state.launchContext?.app_session_id) {
      // P4.3: Remove session subscription before deleting
      activeSessionSubscriptions.delete(state.launchContext.app_session_id);
      await gatewayRequest(`/v1/apps/sessions/${encodeURIComponent(state.launchContext.app_session_id)}`, { method: "DELETE" });
    }
  } catch (error) {
    addLog("error", "runtime.end_session_failed", { message: error.message || String(error) });
  }
  clearSavedSession();
  await initializeSession();
  renderPermissions();
  renderContext();
  renderTranscript();
  launchFrame();
  setStatus(`Cleared persisted session for ${state.manifest.name}.`);
});

renderLog();
loadAndLaunch(DEFAULT_MANIFEST_URL).catch((error) => {
  setErrorStatus(error.message || "Failed to start runtime.");
  addLog("error", "runtime.bootstrap_failed", { message: error.message || String(error) });
});
