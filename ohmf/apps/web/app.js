"use strict";

function normalizeAPIBaseURL(value) {
  const fallback = (window.OHMF_WEB_CONFIG?.api_base_url || "http://localhost:18080").replace(/\/+$/, "");
  if (!value) return fallback;
  try {
    const url = new URL(value);
    const localHosts = new Set(["localhost", "127.0.0.1"]);
    const targetPort = String(window.OHMF_WEB_CONFIG?.api_host_port || "18080");
    if (localHosts.has(url.hostname) && url.port !== targetPort) {
      url.port = targetPort;
      const normalized = url.toString().replace(/\/+$/, "");
      window.localStorage.setItem("ohmf.apiBaseUrl", normalized);
      return normalized;
    }
    return url.toString().replace(/\/+$/, "");
  } catch {
    return fallback;
  }
}

const API_BASE_URL = normalizeAPIBaseURL(window.OHMF_WEB_CONFIG?.api_base_url || window.localStorage.getItem("ohmf.apiBaseUrl") || "http://localhost:18080");
const AUTH_STORAGE_KEY = "ohmf.auth.session.v1";
const STORE_VERSION = 2;
const SYNC_CURSOR_VERSION = 1;
const SYNC_DEVICE_KEY = "ohmf.sync.device.v1";
const CRYPTO_STORAGE_PREFIX = "ohmf.crypto.device.v1";
const TRANSPORT_SMS = "SMS";
const TRANSPORT_OHMF = "OHMF";
const CONTENT_TYPE_TEXT = "text";
const CONTENT_TYPE_ATTACHMENT = "attachment";
const CONTENT_TYPE_APP_CARD = "app_card";
const CONTENT_TYPE_APP_EVENT = "app_event";
const CONTENT_TYPE_ENCRYPTED = "encrypted";
const SIGNAL_ENCRYPTION_SCHEME = "OHMF_SIGNAL_V1";
const MLS_ENCRYPTION_SCHEME = "OHMF_MLS_V1";
const ENCRYPTION_SCHEME = SIGNAL_ENCRYPTION_SCHEME;
const LEGACY_ENCRYPTION_SCHEME = "OHMF_DOUBLE_RATCHET_P256_AESGCM_V3";
const ATTACHMENT_ENCRYPTION_SCHEME = "OHMF_ATTACHMENT_AESGCM_V1";
const SIGNAL_PREKEY_BATCH_SIZE = 100;
const SIGNAL_PREKEY_REPLENISH_AT = 20;
const CRYPTO_DB_NAME = "ohmf.crypto";
const CRYPTO_DB_VERSION = 1;
const CRYPTO_DEVICE_STORE = "device_state";
const MINIAPP_CONSENT_STORAGE_PREFIX = "ohmf.miniapp.consent.v1";
const DECRYPTED_MESSAGE_CACHE_LIMIT = 500;
const SMS_DELIVERY_STATUSES = Object.freeze({
  SENT: "SENT",
  FAIL_SEND: "FAIL_SEND",
});
const OHMF_DELIVERY_STATUSES = Object.freeze({
  SENT: "SENT",
  DELIVERED: "DELIVERED",
  READ: "READ",
  FAIL_DELIVERY: "FAIL_DELIVERY",
  FAIL_SEND: "FAIL_SEND",
});
const LIVE_SYNC_INTERVAL_MS = 5000;
const BUILTIN_DEV_MINIAPP_CATALOG = Object.freeze([
  {
    appId: "app.ohmf.counter-lab",
    manifestUrl: "./miniapps/counter/manifest.json",
    title: "Counter Lab",
    summary: "Shared state demo with projected messages.",
  },
  {
    appId: "app.ohmf.eightball",
    manifestUrl: "./miniapps/eightball/manifest.json",
    title: "Mystic 8-Ball",
    summary: "Open-source SDK demo with shared answers and projected summaries.",
  },
]);

const state = {
  auth: null,
  challengeId: "",
  query: "",
  activeThreadId: null,
  threads: [],
  typingDraft: "",
  remoteTypingByThread: {},
  replyTarget: null,
  openMessageMenu: null,
  messageMetadata: {
    open: false,
    threadId: "",
    messageId: "",
    loading: false,
    error: "",
    edits: [],
    reactions: [],
    recipientDeliveryAt: "",
    recipientReadAt: "",
    requestToken: 0,
  },
  miniapp: {
    drawerOpen: false,
    popupOpen: false,
    selectedAppId: "",
    catalog: [],
    catalogLoaded: false,
    manifest: null,
    launchContext: null,
    sessionState: null,
    grantedPermissions: new Set(),
    frameWindow: null,
    channelId: "",
    sessionMode: "idle",
    consentRequired: false,
    lastShareError: "",
    loading: false,
    loadTimer: 0,
  },
  sync: {
    deviceId: "",
    lastUserCursor: 0,
  },
  profiles: {},
  selfProfile: null,
  crypto: {
    device: null,
    published: false,
    bundleCache: {},
    decryptedMessageCache: {},
    decryptChain: Promise.resolve(),
  },
};
let liveSyncInFlight = false;
let eventStreamAbort = null;
let eventStreamReconnectTimer = 0;
let refreshAuthInFlight = null;
let eventStreamDisabled = false;
let realtimeSocket = null;
let realtimeReconnectTimer = 0;
let realtimeConnectFailures = 0;
let liveRefreshTimer = 0;
let typingStopTimer = 0;
let localTypingThreadId = "";
let localTypingSent = false;
const pendingDeliveredThroughByThread = Object.create(null);
const pendingDeliveredFlushTimers = Object.create(null);

const el = {
  authShell: document.getElementById("auth-shell"),
  appShell: document.getElementById("app-shell"),
  authStatus: document.getElementById("auth-status"),
  authHint: document.getElementById("auth-hint"),
  phoneStartForm: document.getElementById("phone-start-form"),
  phoneVerifyForm: document.getElementById("phone-verify-form"),
  countryCodeSelect: document.getElementById("country-code-select"),
  phoneInput: document.getElementById("phone-input"),
  phoneE164Preview: document.getElementById("phone-e164-preview"),
  otpInput: document.getElementById("otp-input"),
  threadList: document.getElementById("thread-list"),
  messageList: document.getElementById("message-list"),
  searchInput: document.getElementById("search-input"),
  title: document.getElementById("chat-title"),
  subtitle: document.getElementById("chat-subtitle"),
  composer: document.getElementById("composer"),
  composerReply: document.getElementById("composer-reply"),
  composerReplyLabel: document.getElementById("composer-reply-label"),
  composerReplyText: document.getElementById("composer-reply-text"),
  composerReplyCancel: document.getElementById("composer-reply-cancel"),
  composerInput: document.getElementById("composer-input"),
  emptyState: document.getElementById("empty-state"),
  messageMetadataWindow: document.getElementById("message-metadata-window"),
  messageMetadataBackdrop: document.getElementById("message-metadata-backdrop"),
  messageMetadataCloseBtn: document.getElementById("message-metadata-close-btn"),
  messageMetadataTitle: document.getElementById("message-metadata-title"),
  messageMetadataSubtitle: document.getElementById("message-metadata-subtitle"),
  messageMetadataBody: document.getElementById("message-metadata-body"),
  backBtn: document.getElementById("back-btn"),
  newChatBtn: document.getElementById("new-chat-btn"),
  newGroupBtn: document.getElementById("new-group-btn"),
  logoutBtn: document.getElementById("logout-btn"),
  newChatForm: document.getElementById("new-chat-form"),
  newCountryCodeSelect: document.getElementById("new-country-code-select"),
  newPhoneInput: document.getElementById("new-phone-input"),
  nicknameBtn: document.getElementById("nickname-btn"),
  groupEncryptionBtn: document.getElementById("group-encryption-btn"),
  blockBtn: document.getElementById("block-btn"),
  closeThreadBtn: document.getElementById("close-thread-btn"),
  attachBtn: document.getElementById("attach-btn"),
  miniappLauncher: document.getElementById("miniapp-launcher"),
  miniappWindow: document.getElementById("miniapp-window"),
  miniappBackdrop: document.getElementById("miniapp-backdrop"),
  miniappCloseBtn: document.getElementById("miniapp-close-btn"),
  miniappWindowCloseBtn: document.getElementById("miniapp-window-close-btn"),
  miniappPicker: document.getElementById("miniapp-picker"),
  miniappUploadCard: document.getElementById("miniapp-upload-card"),
  miniappCatalogCards: document.getElementById("miniapp-catalog-cards"),
  miniappStage: document.getElementById("miniapp-stage"),
  miniappPreviewTitle: document.getElementById("miniapp-preview-title"),
  miniappPreviewSubtitle: document.getElementById("miniapp-preview-subtitle"),
  miniappPermissions: document.getElementById("miniapp-permissions"),
  miniappContextCopy: document.getElementById("miniapp-context-copy"),
  miniappLaunchMode: document.getElementById("miniapp-launch-mode"),
  miniappShareBtn: document.getElementById("miniapp-share-btn"),
  miniappOpenBtn: document.getElementById("miniapp-open-btn"),
  miniappResetBtn: document.getElementById("miniapp-reset-btn"),
  miniappWindowTitle: document.getElementById("miniapp-window-title"),
  miniappWindowSubtitle: document.getElementById("miniapp-window-subtitle"),
  miniappFrame: document.getElementById("miniapp-frame"),
  attachmentInput: document.getElementById("attachment-input"),
};

function nowISO() {
  return new Date().toISOString();
}

function formatShortTime(value) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function formatDateTime(value) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
}

function messageSnippet(message, limit = 72) {
  if (!message) return "";
  if (message.deleted) return "Message deleted";
  if (isAppCardMessage(message)) return sanitizeText(message?.content?.title || "Shared app", limit);
  return sanitizeText(message.text, limit);
}

function normalizeReplyReference(raw) {
  if (!raw || typeof raw !== "object") return null;
  const messageId = sanitizeText(raw.message_id, 80);
  if (!messageId) return null;
  return {
    messageId,
    senderUserId: sanitizeText(raw.sender_user_id, 80),
    contentType: sanitizeText(raw.content_type, 40) || CONTENT_TYPE_TEXT,
    text: sanitizeText(raw.text, 180) || "Original message",
  };
}

function buildReplyReference(thread, message) {
  return {
    message_id: message.id,
    sender_user_id: sanitizeText(message.senderUserId, 80) || (message.direction === "out" ? state.auth?.userId || "" : ""),
    content_type: sanitizeText(message.contentType, 40) || CONTENT_TYPE_TEXT,
    text: messageSnippet(message, 180) || "Original message",
  };
}

function sanitizeText(value, limit = 1000) {
  return String(value || "")
    .replace(/[\u0000-\u001f\u007f]/g, "")
    .trim()
    .slice(0, limit);
}

function normalizePreviewURL(value) {
  if (!value) return "";
  try {
    const url = new URL(String(value), window.location.href);
    if (url.protocol !== "http:" && url.protocol !== "https:") return "";
    return url.toString();
  } catch {
    return "";
  }
}

function normalizeMiniappMessagePreview(raw) {
  if (!raw || typeof raw !== "object") return null;
  const type = String(raw.type || "").trim();
  const url = normalizePreviewURL(raw.url);
  if (!url || (type !== "static_image" && type !== "live")) return null;
  return {
    type,
    url,
    altText: sanitizeText(raw.alt_text, 140),
    fitMode: String(raw.fit_mode || "scale").trim() === "crop" ? "crop" : "scale",
  };
}

function appCardDisplayName(content, limit = 120) {
  return sanitizeText(content?.title || content?.app_id || "Shared app", limit);
} // removed: single-use app card wrappers inlined at render sites

function appCardPresentationModes(thread) {
  const modes = {};
  const seenAppIds = new Set();
  for (let index = (thread?.messages || []).length - 1; index >= 0; index -= 1) {
    const message = thread.messages[index];
    if (!isAppCardMessage(message) || message.deleted) continue;
    const appId = sanitizeText(message?.content?.app_id, 120);
    if (!appId) {
      modes[message.id] = "expanded";
      continue;
    }
    if (seenAppIds.has(appId)) modes[message.id] = "compact";
    else {
      seenAppIds.add(appId);
      modes[message.id] = "expanded";
    }
  }
  return modes;
}

function replyIndexByTarget(thread) {
  const index = {};
  for (const message of thread?.messages || []) {
    if (message.deleted) continue;
    const replyReference = normalizeReplyReference(message.content?.reply_to);
    if (!replyReference) continue;
    if (!index[replyReference.messageId]) index[replyReference.messageId] = [];
    index[replyReference.messageId].push(message);
  }
  return index;
}

function normalizeReactionCounts(raw) {
  if (!raw || typeof raw !== "object") return {};
  const normalized = {};
  for (const [emoji, count] of Object.entries(raw)) {
    const key = sanitizeText(emoji, 8);
    const value = Number(count);
    if (!key || !Number.isFinite(value) || value <= 0) continue;
    normalized[key] = Math.trunc(value);
  }
  return normalized;
}

function onlyDigits(value, limit = 32) {
  return String(value || "").replace(/\D/g, "").slice(0, limit);
}

function formatPhoneLocal(value) {
  const digits = onlyDigits(value, 10);
  if (digits.length <= 3) return digits ? `(${digits}` : "";
  if (digits.length <= 6) return `(${digits.slice(0, 3)})-${digits.slice(3)}`;
  return `(${digits.slice(0, 3)})-${digits.slice(3, 6)}-${digits.slice(6)}`;
}

function toE164(countryCode, localValue) {
  const prefix = String(countryCode || "").trim();
  const digits = onlyDigits(localValue, 15);
  if (!/^\+\d{1,4}$/.test(prefix)) return "";
  const raw = `${prefix}${digits}`;
  if (!/^\+\d{8,15}$/.test(raw)) return "";
  return raw;
}

function makeIdempotencyKey(prefix = "msg") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

async function uploadMediaFile(file) {
  if (!file) throw new Error("no file");
  const buffer = await file.arrayBuffer();
  const checksum = await sha256Hex(buffer);
  const init = await apiRequest(`/v1/media/uploads`, {
    method: "POST",
    body: JSON.stringify({
      mime_type: file.type || "application/octet-stream",
      size_bytes: file.size,
      file_name: sanitizeText(file.name || "attachment", 140),
      checksum_sha256: checksum,
    }),
  });
  const uploadUrl = init?.upload_url;
  const uploadId = init?.upload_id;
  const token = init?.token;
  const attachmentId = init?.attachment_id;
  if (!uploadUrl) throw new Error("no upload url returned");

  const response = await fetch(uploadUrl, {
    method: init?.method || "PUT",
    headers: init?.headers || { "Content-Type": file.type || "application/octet-stream" },
    body: buffer,
  });
  if (!response.ok) throw new Error("upload failed");
  if (!token) throw new Error("missing upload token");
  await apiRequest(`/v1/media/uploads/${encodeURIComponent(token)}/complete`, {
    method: "POST",
    body: JSON.stringify({}),
  });

  return {
    upload_id: uploadId,
    upload_url: uploadUrl,
    attachment_id: attachmentId,
    checksum_sha256: checksum,
    file_name: sanitizeText(file.name || "attachment", 140),
    mime_type: file.type || "application/octet-stream",
    size_bytes: file.size,
  };
}

async function sha256Hex(buffer) {
  const digest = await window.crypto.subtle.digest("SHA-256", buffer);
  return Array.from(new Uint8Array(digest))
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

async function encryptAttachmentBytes(buffer) {
  const keyBytes = window.crypto.getRandomValues(new Uint8Array(32));
  const nonce = window.crypto.getRandomValues(new Uint8Array(12));
  const key = await window.crypto.subtle.importKey("raw", keyBytes, { name: "AES-GCM" }, false, ["encrypt"]);
  const ciphertext = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, key, buffer);
  return {
    ciphertext,
    fileKey: bytesToBase64(keyBytes),
    fileNonce: bytesToBase64(nonce),
    scheme: ATTACHMENT_ENCRYPTION_SCHEME,
  };
}

async function decryptAttachmentBytes(buffer, descriptor) {
  const fileKey = sanitizeText(descriptor?.file_key, 4000);
  const fileNonce = sanitizeText(descriptor?.file_nonce, 4000);
  if (!fileKey || !fileNonce) return buffer;
  const key = await window.crypto.subtle.importKey("raw", base64ToBytes(fileKey), { name: "AES-GCM" }, false, ["decrypt"]);
  return window.crypto.subtle.decrypt({ name: "AES-GCM", iv: base64ToBytes(fileNonce) }, key, buffer);
}

async function uploadEncryptedMediaFile(thread, file) {
  if (!thread || thread.kind !== "dm") return uploadMediaFile(file);
  const recipients = await ensureEncryptedConversation(thread);
  if (!recipients) return uploadMediaFile(file);

  const sourceBuffer = await file.arrayBuffer();
  const encrypted = await encryptAttachmentBytes(sourceBuffer);
  const checksum = await sha256Hex(encrypted.ciphertext);
  const init = await apiRequest(`/v1/media/uploads`, {
    method: "POST",
    body: JSON.stringify({
      mime_type: "application/octet-stream",
      size_bytes: encrypted.ciphertext.byteLength,
      file_name: "",
      checksum_sha256: checksum,
    }),
  });
  const uploadUrl = init?.upload_url;
  const uploadId = init?.upload_id;
  const token = init?.token;
  const attachmentId = init?.attachment_id;
  if (!uploadUrl) throw new Error("no upload url returned");

  const response = await fetch(uploadUrl, {
    method: init?.method || "PUT",
    headers: init?.headers || { "Content-Type": "application/octet-stream" },
    body: encrypted.ciphertext,
  });
  if (!response.ok) throw new Error("upload failed");
  if (!token) throw new Error("missing upload token");
  await apiRequest(`/v1/media/uploads/${encodeURIComponent(token)}/complete`, {
    method: "POST",
    body: JSON.stringify({}),
  });

  return {
    upload_id: uploadId,
    upload_url: uploadUrl,
    attachment_id: attachmentId,
    checksum_sha256: checksum,
    file_name: sanitizeText(file.name || "attachment", 140),
    mime_type: file.type || "application/octet-stream",
    size_bytes: file.size,
    file_key: encrypted.fileKey,
    file_nonce: encrypted.fileNonce,
    encryption_scheme: encrypted.scheme,
    stored_mime_type: "application/octet-stream",
    stored_size_bytes: encrypted.ciphertext.byteLength,
    encrypted: true,
  };
}

function initials(name) {
  return sanitizeText(name, 24)
    .split(/\s+/)
    .map((part) => part[0] || "")
    .join("")
    .slice(0, 2)
    .toUpperCase();
}

function authStoreSet(session) {
  state.auth = session;
  window.sessionStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(session));
}

function authStoreClear() {
  state.auth = null;
  window.sessionStorage.removeItem(AUTH_STORAGE_KEY);
}

function authStoreLoad() {
  const raw = window.sessionStorage.getItem(AUTH_STORAGE_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || !parsed.accessToken || !parsed.refreshToken || !parsed.userId) return null;
    return parsed;
  } catch {
    return null;
  }
}

function conversationStoreKey() {
  return `ohmf.conversations.${state.auth?.userId || "anon"}.v${STORE_VERSION}`;
}

function syncCursorStoreKey() {
  return `ohmf.sync.cursor.${state.auth?.userId || "anon"}.v${SYNC_CURSOR_VERSION}`;
}

function ensureSyncDeviceId() {
  let current = sanitizeText(window.localStorage.getItem(SYNC_DEVICE_KEY), 120);
  if (!current) {
    current = randomId("web");
    window.localStorage.setItem(SYNC_DEVICE_KEY, current);
  }
  state.sync.deviceId = current;
  return current;
}

function saveSyncCursor() {
  if (!state.auth?.userId) return;
  window.localStorage.setItem(syncCursorStoreKey(), JSON.stringify({
    version: SYNC_CURSOR_VERSION,
    last_user_cursor: Number(state.sync.lastUserCursor || 0),
  }));
}

function loadSyncCursor() {
  const raw = window.localStorage.getItem(syncCursorStoreKey());
  state.sync.lastUserCursor = 0;
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    state.sync.lastUserCursor = Number(parsed?.last_user_cursor || 0);
  } catch {
    state.sync.lastUserCursor = 0;
  }
}

function cryptoStoreKey() {
  return `${CRYPTO_STORAGE_PREFIX}.${sanitizeText(state.auth?.userId, 80)}.${sanitizeText(state.auth?.deviceId, 80)}`;
}

let cryptoDBPromise = null;

function openCryptoDB() {
  if (cryptoDBPromise) return cryptoDBPromise;
  cryptoDBPromise = new Promise((resolve, reject) => {
    const request = window.indexedDB.open(CRYPTO_DB_NAME, CRYPTO_DB_VERSION);
    request.onerror = () => reject(request.error || new Error("indexeddb_open_failed"));
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(CRYPTO_DEVICE_STORE)) {
        db.createObjectStore(CRYPTO_DEVICE_STORE);
      }
    };
    request.onsuccess = () => resolve(request.result);
  });
  return cryptoDBPromise;
}

async function cryptoStoreLoad() {
  try {
    const db = await openCryptoDB();
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(CRYPTO_DEVICE_STORE, "readonly");
      const store = tx.objectStore(CRYPTO_DEVICE_STORE);
      const request = store.get(cryptoStoreKey());
      request.onerror = () => reject(request.error || new Error("crypto_store_read_failed"));
      request.onsuccess = () => resolve(request.result || null);
    });
  } catch {
    try {
      const raw = window.localStorage.getItem(cryptoStoreKey());
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }
}

async function cryptoStoreSave(value) {
  try {
    const db = await openCryptoDB();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(CRYPTO_DEVICE_STORE, "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error || new Error("crypto_store_write_failed"));
      tx.objectStore(CRYPTO_DEVICE_STORE).put(value, cryptoStoreKey());
    });
    window.localStorage.removeItem(cryptoStoreKey());
  } catch {
    window.localStorage.setItem(cryptoStoreKey(), JSON.stringify(value));
  }
}

function normalizeDecryptedMessageCache(cache) {
  return cache && typeof cache === "object" ? cache : {};
}

async function hydrateCryptoClientState() {
  const stored = await cryptoStoreLoad();
  state.crypto.decryptedMessageCache = normalizeDecryptedMessageCache(stored?.decryptedMessageCache);
  return stored;
}

function bytesToBase64(bytes) {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let binary = "";
  for (const value of view) binary += String.fromCharCode(value);
  return window.btoa(binary);
}

function base64ToBytes(value) {
  const raw = window.atob(String(value || ""));
  const bytes = new Uint8Array(raw.length);
  for (let index = 0; index < raw.length; index += 1) bytes[index] = raw.charCodeAt(index);
  return bytes;
}

async function exportPublicKey(publicKey) {
  const spki = await window.crypto.subtle.exportKey("spki", publicKey);
  return bytesToBase64(spki);
}

async function importECDHPublicKey(spkiB64) {
  return window.crypto.subtle.importKey(
    "spki",
    base64ToBytes(spkiB64),
    { name: "ECDH", namedCurve: "P-256" },
    true,
    []
  );
}

async function importECDSAPublicKey(spkiB64) {
  return window.crypto.subtle.importKey(
    "spki",
    base64ToBytes(spkiB64),
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["verify"]
  );
}

async function importECDHPrivateKey(jwk) {
  return window.crypto.subtle.importKey(
    "jwk",
    jwk,
    { name: "ECDH", namedCurve: "P-256" },
    true,
    ["deriveBits"]
  );
}

async function importECDSAPrivateKey(jwk) {
  return window.crypto.subtle.importKey(
    "jwk",
    jwk,
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign"]
  );
}

async function deriveWrapKey(privateKey, peerPublicKey) {
  const bits = await window.crypto.subtle.deriveBits({ name: "ECDH", public: peerPublicKey }, privateKey, 256);
  const digest = await window.crypto.subtle.digest("SHA-256", bits);
  return window.crypto.subtle.importKey("raw", digest, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function deriveSharedSecretBytes(privateKey, peerPublicKey) {
  return new Uint8Array(await window.crypto.subtle.deriveBits({ name: "ECDH", public: peerPublicKey }, privateKey, 256));
}

function utf8Bytes(value) {
  return new TextEncoder().encode(String(value || ""));
}

function concatBytes(...parts) {
  const total = parts.reduce((sum, part) => sum + (part?.length || 0), 0);
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    if (!part?.length) continue;
    output.set(part, offset);
    offset += part.length;
  }
  return output;
}

async function hmacSHA256(keyBytes, dataBytes) {
  const key = await window.crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await window.crypto.subtle.sign("HMAC", key, dataBytes));
}

async function sha256Base64(dataBytes) {
  return bytesToBase64(new Uint8Array(await window.crypto.subtle.digest("SHA-256", dataBytes)));
}

async function hkdfExpand(ikmBytes, saltBytes, info, length) {
  const salt = saltBytes?.length ? saltBytes : new Uint8Array(32);
  const prk = await hmacSHA256(salt, ikmBytes);
  const infoBytes = utf8Bytes(info);
  const blocks = [];
  let previous = new Uint8Array(0);
  let generated = 0;
  let counter = 1;
  while (generated < length) {
    previous = await hmacSHA256(prk, concatBytes(previous, infoBytes, new Uint8Array([counter])));
    blocks.push(previous);
    generated += previous.length;
    counter += 1;
  }
  return concatBytes(...blocks).slice(0, length);
}

async function kdfRoot(rootKeyB64, sharedSecretBytes) {
  const output = await hkdfExpand(sharedSecretBytes, base64ToBytes(rootKeyB64), "OHMF_DOUBLE_RATCHET_ROOT_V1", 64);
  return {
    rootKey: bytesToBase64(output.slice(0, 32)),
    chainKey: bytesToBase64(output.slice(32, 64)),
  };
}

async function kdfChain(chainKeyB64) {
  const chainKey = base64ToBytes(chainKeyB64);
  const nextChainKey = await hmacSHA256(chainKey, utf8Bytes("chain"));
  const messageKey = await hmacSHA256(chainKey, utf8Bytes("message"));
  return {
    nextChainKey: bytesToBase64(nextChainKey),
    messageKey: bytesToBase64(messageKey),
  };
}

async function importAESKey(rawBytes, usages = ["encrypt", "decrypt"]) {
  return window.crypto.subtle.importKey("raw", rawBytes, { name: "AES-GCM" }, false, usages);
}

function ratchetSessionId(userId, deviceId) {
  return `${sanitizeText(userId, 80)}:${sanitizeText(deviceId, 80)}`;
}

function normalizeRatchetSession(session, device) {
  return {
    rootKey: sanitizeText(session?.rootKey, 4000),
    sendChainKey: sanitizeText(session?.sendChainKey, 4000),
    receiveChainKey: sanitizeText(session?.receiveChainKey, 4000),
    sendCount: Number(session?.sendCount || 0),
    receiveCount: Number(session?.receiveCount || 0),
    previousSendCount: Number(session?.previousSendCount || 0),
    localRatchetPublicKey: sanitizeText(session?.localRatchetPublicKey || device?.agreementPublicKey, 4000),
    localRatchetPrivateKeyJwk: session?.localRatchetPrivateKeyJwk || device?.agreementPrivateKeyJwk || null,
    remoteRatchetPublicKey: sanitizeText(session?.remoteRatchetPublicKey, 4000),
    pendingRatchet: Boolean(session?.pendingRatchet),
    skippedKeys: session?.skippedKeys && typeof session.skippedKeys === "object" ? session.skippedKeys : {},
  };
}

function persistCryptoDeviceState(device) {
  const next = {
    ...device,
    ratchetSessions: device?.ratchetSessions && typeof device.ratchetSessions === "object" ? device.ratchetSessions : {},
    signalRatchetSessions: device?.signalRatchetSessions && typeof device.signalRatchetSessions === "object" ? device.signalRatchetSessions : {},
    mlsEpochSecrets: device?.mlsEpochSecrets && typeof device.mlsEpochSecrets === "object" ? device.mlsEpochSecrets : {},
    trustPins: device?.trustPins && typeof device.trustPins === "object" ? device.trustPins : {},
    legacyRatchetSessions: device?.legacyRatchetSessions && typeof device.legacyRatchetSessions === "object" ? device.legacyRatchetSessions : {},
    decryptedMessageCache: normalizeDecryptedMessageCache(state.crypto.decryptedMessageCache),
  };
  void cryptoStoreSave(next);
  state.crypto.device = next;
  return next;
}

function getRatchetSession(device, userId, deviceId) {
  if (!device) return null;
  const session = device.ratchetSessions?.[ratchetSessionId(userId, deviceId)];
  return session ? normalizeRatchetSession(session, device) : null;
}

function setRatchetSession(device, userId, deviceId, session) {
  const next = {
    ...device,
    ratchetSessions: {
      ...(device?.ratchetSessions || {}),
      [ratchetSessionId(userId, deviceId)]: normalizeRatchetSession(session, device),
    },
  };
  return persistCryptoDeviceState(next);
}

function mlsEpochSecretId(conversationId, epoch) {
  return `${sanitizeText(conversationId, 80)}:${Number(epoch || 0)}`;
}

function normalizeMLSEpochSecret(secret) {
  return {
    epoch: Number(secret?.epoch || 0),
    digest: sanitizeText(secret?.digest, 200),
    treeHash: sanitizeText(secret?.treeHash, 512),
    secretKey: sanitizeText(secret?.secretKey, 4000),
    updatedAt: sanitizeText(secret?.updatedAt, 80) || nowISO(),
  };
}

function getMLSEpochSecret(device, conversationId, epoch, digest = "") {
  if (!device) return null;
  const stored = device.mlsEpochSecrets?.[mlsEpochSecretId(conversationId, epoch)];
  if (!stored) return null;
  const normalized = normalizeMLSEpochSecret(stored);
  if (sanitizeText(digest, 200) && normalized.digest !== sanitizeText(digest, 200)) return null;
  return normalized.secretKey ? normalized : null;
}

function setMLSEpochSecret(device, conversationId, secret) {
  const normalized = normalizeMLSEpochSecret(secret);
  const next = {
    ...device,
    mlsEpochSecrets: {
      ...(device?.mlsEpochSecrets || {}),
      [mlsEpochSecretId(conversationId, normalized.epoch)]: normalized,
    },
  };
  return persistCryptoDeviceState(next);
}

function takeSkippedMessageKey(session, ratchetPublicKey, messageNumber) {
  const key = `${String(ratchetPublicKey || "")}|${Number(messageNumber || 0)}`;
  if (!session?.skippedKeys?.[key]) return "";
  const messageKey = sanitizeText(session.skippedKeys[key], 4000);
  delete session.skippedKeys[key];
  return messageKey;
}

function stashSkippedMessageKey(session, ratchetPublicKey, messageNumber, messageKey) {
  const key = `${String(ratchetPublicKey || "")}|${Number(messageNumber || 0)}`;
  const next = {
    ...(session.skippedKeys || {}),
    [key]: sanitizeText(messageKey, 4000),
  };
  const keys = Object.keys(next);
  while (keys.length > 64) {
    delete next[keys.shift()];
  }
  session.skippedKeys = next;
}

async function signalExportPublicKey(publicKey) {
  return bytesToBase64(await window.crypto.subtle.exportKey("raw", publicKey));
}

async function signalImportAgreementPublicKey(rawB64) {
  return window.crypto.subtle.importKey("raw", base64ToBytes(rawB64), { name: "X25519" }, true, []);
}

async function signalImportSigningPublicKey(rawB64) {
  return window.crypto.subtle.importKey("raw", base64ToBytes(rawB64), { name: "Ed25519" }, true, ["verify"]);
}

async function signalImportAgreementPrivateKey(jwk) {
  return window.crypto.subtle.importKey("jwk", jwk, { name: "X25519" }, true, ["deriveBits"]);
}

async function signalImportSigningPrivateKey(jwk) {
  return window.crypto.subtle.importKey("jwk", jwk, { name: "Ed25519" }, true, ["sign"]);
}

async function signalGenerateAgreementKeyPair() {
  return window.crypto.subtle.generateKey({ name: "X25519" }, true, ["deriveBits"]);
}

async function signalGenerateSigningKeyPair() {
  return window.crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
}

async function signalDeriveSharedSecretBytes(privateKey, peerPublicKey) {
  return new Uint8Array(await window.crypto.subtle.deriveBits({ name: "X25519", public: peerPublicKey }, privateKey, 256));
}

async function signalDeriveWrapKey(privateKey, peerPublicKey) {
  const bits = await signalDeriveSharedSecretBytes(privateKey, peerPublicKey);
  const digest = await window.crypto.subtle.digest("SHA-256", bits);
  return window.crypto.subtle.importKey("raw", digest, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function signalSignDetached(privateKey, payload) {
  return bytesToBase64(await window.crypto.subtle.sign("Ed25519", privateKey, utf8Bytes(payload)));
}

async function signalVerifyDetached(publicKey, payload, signatureB64) {
  return window.crypto.subtle.verify("Ed25519", publicKey, base64ToBytes(signatureB64), utf8Bytes(payload));
}

function signalRatchetSessionId(userId, deviceId) {
  return `signal:${sanitizeText(userId, 80)}:${sanitizeText(deviceId, 80)}`;
}

function normalizeSignalRatchetSession(session, device) {
  return {
    rootKey: sanitizeText(session?.rootKey, 4000),
    sendChainKey: sanitizeText(session?.sendChainKey, 4000),
    receiveChainKey: sanitizeText(session?.receiveChainKey, 4000),
    sendCount: Number(session?.sendCount || 0),
    receiveCount: Number(session?.receiveCount || 0),
    previousSendCount: Number(session?.previousSendCount || 0),
    localRatchetPublicKey: sanitizeText(session?.localRatchetPublicKey || device?.signedPrekeyPublicKey || "", 4000),
    localRatchetPrivateKeyJwk: session?.localRatchetPrivateKeyJwk || device?.signedPrekeyPrivateKeyJwk || null,
    remoteRatchetPublicKey: sanitizeText(session?.remoteRatchetPublicKey, 4000),
    pendingRatchet: Boolean(session?.pendingRatchet),
    skippedKeys: session?.skippedKeys && typeof session.skippedKeys === "object" ? session.skippedKeys : {},
  };
}

function getSignalRatchetSession(device, userId, deviceId) {
  if (!device) return null;
  const session = device.signalRatchetSessions?.[signalRatchetSessionId(userId, deviceId)];
  return session ? normalizeSignalRatchetSession(session, device) : null;
}

function setSignalRatchetSession(device, userId, deviceId, session) {
  const next = {
    ...device,
    signalRatchetSessions: {
      ...(device?.signalRatchetSessions || {}),
      [signalRatchetSessionId(userId, deviceId)]: normalizeSignalRatchetSession(session, device),
    },
  };
  return persistCryptoDeviceState(next);
}

function trustPinId(userId, deviceId) {
  return `${sanitizeText(userId, 80)}:${sanitizeText(deviceId, 80)}`;
}

function signalBundleSupported(bundle) {
  return sanitizeText(bundle?.bundle_version, 80) === ENCRYPTION_SCHEME
    && sanitizeText(bundle?.agreement_identity_public_key, 4000)
    && sanitizeText(bundle?.signing_public_key, 4000)
    && sanitizeText(bundle?.fingerprint, 128);
}

function signedPrekeyForBundle(bundle) {
  const signedPrekey = bundle?.signed_prekey && typeof bundle.signed_prekey === "object" ? bundle.signed_prekey : {};
  return {
    prekey_id: Number(signedPrekey?.prekey_id || bundle?.signed_prekey_id || 0),
    public_key: sanitizeText(signedPrekey?.public_key || bundle?.signed_prekey_public_key, 4000),
    signature: sanitizeText(signedPrekey?.signature || bundle?.signed_prekey_signature, 8000),
  };
}

async function claimDeviceBundles(userId) {
  const payload = await apiRequest(`/v1/device-keys/${encodeURIComponent(sanitizeText(userId, 80))}/claim`, { method: "POST" });
  return Array.isArray(payload?.items) ? payload.items : [];
}

function confirmTrustedRemoteBundles(device, bundles) {
  const nextPins = { ...(device?.trustPins || {}) };
  const changed = [];
  for (const bundle of bundles) {
    const userId = sanitizeText(bundle?.user_id, 80);
    const deviceId = sanitizeText(bundle?.device_id, 80);
    const fingerprint = sanitizeText(bundle?.fingerprint, 128);
    if (!userId || !deviceId || !fingerprint) continue;
    const key = trustPinId(userId, deviceId);
    if (!nextPins[key]) nextPins[key] = fingerprint;
    else if (nextPins[key] !== fingerprint) changed.push({ userId, deviceId, fingerprint });
  }
  if (changed.length) {
    const accepted = window.confirm(`Device key changed for ${changed.length} device(s). Trust the new keys and continue?`);
    if (!accepted) {
      throw new Error("Untrusted device key change. Message send cancelled.");
    }
    for (const item of changed) {
      nextPins[trustPinId(item.userId, item.deviceId)] = item.fingerprint;
    }
  }
  return persistCryptoDeviceState({ ...device, trustPins: nextPins });
} // removed: prompt flag eliminated because only the confirm path is used

async function generateSignalPrekey(prekeyId) {
  const keyPair = await signalGenerateAgreementKeyPair();
  return {
    prekey_id: Number(prekeyId),
    public_key: await signalExportPublicKey(keyPair.publicKey),
    private_key_jwk: await window.crypto.subtle.exportKey("jwk", keyPair.privateKey),
  };
}

async function topUpSignalPrekeys(device, minimum = SIGNAL_PREKEY_BATCH_SIZE) {
  const next = { ...device, oneTimePrekeys: { ...(device?.oneTimePrekeys || {}) } };
  const available = Object.values(next.oneTimePrekeys).filter((item) => item && !item.consumed_at).length;
  let nextPrekeyId = Number(next.nextPrekeyId || 1);
  for (let index = available; index < minimum; index += 1) {
    const generated = await generateSignalPrekey(nextPrekeyId);
    next.oneTimePrekeys[String(generated.prekey_id)] = generated;
    nextPrekeyId += 1;
  }
  next.nextPrekeyId = nextPrekeyId;
  return persistCryptoDeviceState(next);
}

async function initializeOutboundRatchetSession(device, bundle) {
  const peerAgreementKey = agreementKeyForBundle(bundle);
  if (!peerAgreementKey) throw new Error("Missing peer agreement key");
  const agreementPrivateKey = await importECDHPrivateKey(device.agreementPrivateKeyJwk);
  const peerPublicKey = await importECDHPublicKey(peerAgreementKey);
  const handshakeSharedSecret = await deriveSharedSecretBytes(agreementPrivateKey, peerPublicKey);
  const handshake = await kdfRoot("", handshakeSharedSecret);
  const localRatchetKeyPair = await window.crypto.subtle.generateKey(
    { name: "ECDH", namedCurve: "P-256" },
    true,
    ["deriveBits"]
  );
  const sharedSecret = await deriveSharedSecretBytes(localRatchetKeyPair.privateKey, peerPublicKey);
  const seed = await kdfRoot(handshake.rootKey, sharedSecret);
  return normalizeRatchetSession({
    rootKey: seed.rootKey,
    sendChainKey: seed.chainKey,
    receiveChainKey: "",
    sendCount: 0,
    receiveCount: 0,
    previousSendCount: 0,
    localRatchetPublicKey: await exportPublicKey(localRatchetKeyPair.publicKey),
    localRatchetPrivateKeyJwk: await window.crypto.subtle.exportKey("jwk", localRatchetKeyPair.privateKey),
    remoteRatchetPublicKey: peerAgreementKey,
    pendingRatchet: false,
    skippedKeys: {},
  }, device);
}

async function rotateSendingRatchet(session) {
  const localRatchetKeyPair = await window.crypto.subtle.generateKey(
    { name: "ECDH", namedCurve: "P-256" },
    true,
    ["deriveBits"]
  );
  const localPrivateKey = localRatchetKeyPair.privateKey;
  const remotePublicKey = await importECDHPublicKey(session.remoteRatchetPublicKey);
  const sharedSecret = await deriveSharedSecretBytes(localPrivateKey, remotePublicKey);
  const next = await kdfRoot(session.rootKey, sharedSecret);
  session.rootKey = next.rootKey;
  session.sendChainKey = next.chainKey;
  session.previousSendCount = Number(session.sendCount || 0);
  session.sendCount = 0;
  session.localRatchetPublicKey = await exportPublicKey(localRatchetKeyPair.publicKey);
  session.localRatchetPrivateKeyJwk = await window.crypto.subtle.exportKey("jwk", localPrivateKey);
  session.pendingRatchet = false;
  return session;
}

async function advanceSendingRatchet(device, bundle) {
  let session = getRatchetSession(device, bundle.user_id, bundle.device_id);
  if (!session) {
    session = await initializeOutboundRatchetSession(device, bundle);
  } else if (session.pendingRatchet || !session.sendChainKey) {
    session = await rotateSendingRatchet(session);
  }
  const step = await kdfChain(session.sendChainKey);
  const header = {
    ratchet_public_key: sanitizeText(session.localRatchetPublicKey, 4000),
    previous_chain_length: Number(session.previousSendCount || 0),
    message_number: Number(session.sendCount || 0),
  };
  session.sendChainKey = step.nextChainKey;
  session.sendCount = Number(session.sendCount || 0) + 1;
  return {
    device: setRatchetSession(device, bundle.user_id, bundle.device_id, session),
    header,
    messageKey: step.messageKey,
  };
}

function recipientHeaderSummary(recipients) {
  return (Array.isArray(recipients) ? recipients : [])
    .map((item) => [
      sanitizeText(item?.user_id, 80),
      sanitizeText(item?.device_id, 80),
      sanitizeText(item?.ratchet_public_key, 4000),
      String(Number(item?.previous_chain_length || 0)),
      String(Number(item?.message_number || 0)),
      sanitizeText(item?.initial_session?.sender_ephemeral_public_key, 4000),
      String(Number(item?.initial_session?.signed_prekey_id || 0)),
      String(Number(item?.initial_session?.one_time_prekey_id || 0)),
    ].join(":"))
    .sort()
    .join(";");
}

function encryptedRecipientCacheKey(recipient) {
  return [
    sanitizeText(recipient?.user_id, 80),
    sanitizeText(recipient?.device_id, 80),
    sanitizeText(recipient?.wrap_nonce, 4000),
    sanitizeText(recipient?.wrapped_key, 12000),
    sanitizeText(recipient?.ratchet_public_key, 4000),
    String(Number(recipient?.previous_chain_length || 0)),
    String(Number(recipient?.message_number || 0)),
    sanitizeText(recipient?.initial_session?.sender_ephemeral_public_key, 4000),
    String(Number(recipient?.initial_session?.signed_prekey_id || 0)),
    String(Number(recipient?.initial_session?.one_time_prekey_id || 0)),
  ].join(":");
}

function mlsEpochPackageCacheKey(pkg) {
  return [
    sanitizeText(pkg?.user_id, 80),
    sanitizeText(pkg?.device_id, 80),
    sanitizeText(pkg?.wrap_nonce, 4000),
    sanitizeText(pkg?.wrapped_key, 12000),
    sanitizeText(pkg?.ratchet_public_key, 4000),
    String(Number(pkg?.previous_chain_length || 0)),
    String(Number(pkg?.message_number || 0)),
    sanitizeText(pkg?.initial_session?.sender_ephemeral_public_key, 4000),
    String(Number(pkg?.initial_session?.signed_prekey_id || 0)),
    String(Number(pkg?.initial_session?.one_time_prekey_id || 0)),
  ].join(":");
}

function encryptedEnvelopeFingerprint(message) {
  if (sanitizeText(message?.contentType, 40) !== CONTENT_TYPE_ENCRYPTED) return "";
  const envelope = message?.content && typeof message.content === "object" ? message.content : {};
  const encryption = envelope.encryption && typeof envelope.encryption === "object" ? envelope.encryption : {};
  const recipients = Array.isArray(encryption.recipients) ? encryption.recipients : [];
  const epochSecretBoxes = Array.isArray(encryption.epoch_secret_boxes) ? encryption.epoch_secret_boxes : [];
  return [
    sanitizeText(message?.id, 80),
    sanitizeText(encryption.scheme, 120),
    sanitizeText(encryption.sender_user_id, 80),
    sanitizeText(encryption.sender_device_id, 80),
    sanitizeText(encryption.sender_signature, 8000),
    String(Number(encryption.conversation_epoch || 1)),
    String(Number(encryption.mls_epoch || 0)),
    sanitizeText(encryption.tree_hash, 512),
    sanitizeText(encryption.epoch_secret_digest, 200),
    sanitizeText(envelope.nonce, 4000),
    sanitizeText(envelope.ciphertext, 16000),
    recipients.map(encryptedRecipientCacheKey).sort().join(";"),
    epochSecretBoxes.map(mlsEpochPackageCacheKey).sort().join(";"),
  ].join("|");
}

function applyDecryptedMessageView(message, cached) {
  if (!message || !cached) return message;
  const contentType = sanitizeText(cached.contentType, 40) || CONTENT_TYPE_TEXT;
  const content = cached.content && typeof cached.content === "object"
    ? cloneJson(cached.content)
    : { text: sanitizeText(cached.content, 1000) };
  return {
    ...message,
    text: sanitizeText(cached.text || content?.text || "[Encrypted message]", 1000),
    contentType,
    content,
    isEncrypted: true,
    decryptStatus: sanitizeText(cached.decryptStatus, 24) || "ok",
  };
}

function cachedDecryptedMessage(messageOrFingerprint) {
  const fingerprint = typeof messageOrFingerprint === "string"
    ? sanitizeText(messageOrFingerprint, 24000)
    : encryptedEnvelopeFingerprint(messageOrFingerprint);
  if (!fingerprint) return null;
  const cached = state.crypto.decryptedMessageCache?.[fingerprint];
  return cached && typeof cached === "object" ? cached : null;
}

function rememberDecryptedMessage(message) {
  const fingerprint = sanitizeText(message?.encryptedEnvelopeFingerprint, 24000) || encryptedEnvelopeFingerprint(message);
  if (!fingerprint || sanitizeText(message?.decryptStatus, 24) !== "ok") return;
  const next = {
    ...(state.crypto.decryptedMessageCache || {}),
    [fingerprint]: {
      text: sanitizeText(message.text, 1000),
      contentType: sanitizeText(message.contentType, 40) || CONTENT_TYPE_TEXT,
      content: cloneJson(message.content || {}),
      decryptStatus: "ok",
      cachedAt: nowISO(),
    },
  };
  const keys = Object.keys(next);
  if (keys.length > DECRYPTED_MESSAGE_CACHE_LIMIT) {
    keys
      .sort((a, b) => new Date(next[a]?.cachedAt || 0).getTime() - new Date(next[b]?.cachedAt || 0).getTime())
      .slice(0, keys.length - DECRYPTED_MESSAGE_CACHE_LIMIT)
      .forEach((key) => {
        delete next[key];
      });
  }
  state.crypto.decryptedMessageCache = next;
  if (state.crypto.device) persistCryptoDeviceState(state.crypto.device);
}

function cacheEncryptedMessagePlaintext(messageId, envelope, plainContentType, plainContent, fallbackText) {
  const fingerprint = encryptedEnvelopeFingerprint({
    id: sanitizeText(messageId, 80),
    contentType: CONTENT_TYPE_ENCRYPTED,
    content: envelope && typeof envelope === "object" ? envelope : {},
  });
  if (!fingerprint) return;
  const next = {
    ...(state.crypto.decryptedMessageCache || {}),
    [fingerprint]: {
      text: sanitizeText(
        fallbackText || plainContent?.text || plainContent?.file_name || plainContent?.attachment_id || "[Encrypted message]",
        1000
      ),
      contentType: sanitizeText(plainContentType, 40) || CONTENT_TYPE_TEXT,
      content: cloneJson(plainContent || {}),
      decryptStatus: "ok",
      cachedAt: nowISO(),
    },
  };
  const keys = Object.keys(next);
  if (keys.length > DECRYPTED_MESSAGE_CACHE_LIMIT) {
    keys
      .sort((a, b) => new Date(next[a]?.cachedAt || 0).getTime() - new Date(next[b]?.cachedAt || 0).getTime())
      .slice(0, keys.length - DECRYPTED_MESSAGE_CACHE_LIMIT)
      .forEach((key) => {
        delete next[key];
      });
  }
  state.crypto.decryptedMessageCache = next;
  if (state.crypto.device) persistCryptoDeviceState(state.crypto.device);
}

function queueCryptoDecrypt(task) {
  const chain = state.crypto?.decryptChain instanceof Promise ? state.crypto.decryptChain : Promise.resolve();
  const next = chain
    .catch(() => {})
    .then(() => task());
  state.crypto.decryptChain = next.catch(() => {});
  return next;
}

function canReuseDecryptedMessage(existingMessage, mappedMessage) {
  return Boolean(
    existingMessage
    && mappedMessage
    && sanitizeText(existingMessage.decryptStatus, 24) === "ok"
    && sanitizeText(existingMessage.encryptedEnvelopeFingerprint, 24000)
    && existingMessage.encryptedEnvelopeFingerprint === mappedMessage.encryptedEnvelopeFingerprint
  );
}

function ratchetSignaturePayloadForEnvelope({ scheme, conversationEpoch, nonce, ciphertext, recipients }) {
  return [
    String(scheme || ""),
    String(Number(conversationEpoch || 1)),
    String(nonce || ""),
    String(ciphertext || ""),
    recipientHeaderSummary(recipients),
  ].join("|");
}

function mlsSignaturePayloadForEnvelope({ scheme, conversationEpoch, mlsEpoch, treeHash, epochSecretDigest, nonce, ciphertext, epochSecretBoxes }) {
  return [
    sanitizeText(scheme, 120),
    String(Number(conversationEpoch || 1)),
    String(Number(mlsEpoch || conversationEpoch || 1)),
    sanitizeText(treeHash, 512),
    sanitizeText(epochSecretDigest, 200),
    sanitizeText(nonce, 4000),
    sanitizeText(ciphertext, 16000),
    recipientHeaderSummary(epochSecretBoxes),
  ].join("|");
}

function signaturePayloadForEnvelope({ scheme, conversationEpoch, senderEphemeralPublicKey, nonce, ciphertext }) {
  return [
    String(scheme || ""),
    String(Number(conversationEpoch || 1)),
    String(senderEphemeralPublicKey || ""),
    String(nonce || ""),
    String(ciphertext || ""),
  ].join("|");
}

async function signDetached(privateKey, payload) {
  const data = new TextEncoder().encode(String(payload || ""));
  const signature = await window.crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, privateKey, data);
  return bytesToBase64(signature);
}

async function verifyDetached(publicKey, payload, signatureB64) {
  const data = new TextEncoder().encode(String(payload || ""));
  return window.crypto.subtle.verify(
    { name: "ECDSA", hash: "SHA-256" },
    publicKey,
    base64ToBytes(signatureB64),
    data
  );
}

function agreementKeyForBundle(bundle) {
  return sanitizeText(bundle?.agreement_identity_public_key || bundle?.identity_public_key, 4000);
}

async function ensureCryptoDeviceState() {
  if (!state.auth?.deviceId) {
    await ensureCurrentDeviceId();
  }
  if (state.crypto.device?.deviceId === state.auth?.deviceId) return state.crypto.device;

  const stored = await hydrateCryptoClientState();
  if (stored?.agreementPublicKey && stored?.agreementPrivateKeyJwk && stored?.signingPublicKey && stored?.signingPrivateKeyJwk && stored?.signedPrekeyPublicKey && stored?.signedPrekeyPrivateKeyJwk) {
    const normalized = {
      ...stored,
      deviceId: state.auth?.deviceId || stored.deviceId || "",
      ratchetSessions: stored?.ratchetSessions && typeof stored.ratchetSessions === "object" ? stored.ratchetSessions : {},
      signalRatchetSessions: stored?.signalRatchetSessions && typeof stored.signalRatchetSessions === "object" ? stored.signalRatchetSessions : {},
      legacyRatchetSessions: stored?.legacyRatchetSessions && typeof stored.legacyRatchetSessions === "object" ? stored.legacyRatchetSessions : {},
      trustPins: stored?.trustPins && typeof stored.trustPins === "object" ? stored.trustPins : {},
      oneTimePrekeys: stored?.oneTimePrekeys && typeof stored.oneTimePrekeys === "object" ? stored.oneTimePrekeys : {},
      nextPrekeyId: Number(stored?.nextPrekeyId || 1),
      decryptedMessageCache: normalizeDecryptedMessageCache(stored?.decryptedMessageCache),
    };
    state.crypto.device = normalized;
    return normalized;
  }

  const legacyRaw = stored || (() => {
    try {
      const raw = window.localStorage.getItem(cryptoStoreKey());
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  })();
  const agreementKeyPair = await signalGenerateAgreementKeyPair();
  const signingKeyPair = await signalGenerateSigningKeyPair();
  const signedPrekeyPair = await signalGenerateAgreementKeyPair();
  const next = {
    deviceId: state.auth?.deviceId || legacyRaw?.deviceId || "",
    agreementPublicKey: await signalExportPublicKey(agreementKeyPair.publicKey),
    agreementPrivateKeyJwk: await window.crypto.subtle.exportKey("jwk", agreementKeyPair.privateKey),
    signingPublicKey: await signalExportPublicKey(signingKeyPair.publicKey),
    signingPrivateKeyJwk: await window.crypto.subtle.exportKey("jwk", signingKeyPair.privateKey),
    signedPrekeyId: Number(legacyRaw?.signedPrekeyId || 1),
    signedPrekeyPublicKey: await signalExportPublicKey(signedPrekeyPair.publicKey),
    signedPrekeyPrivateKeyJwk: await window.crypto.subtle.exportKey("jwk", signedPrekeyPair.privateKey),
    publishedAt: "",
    ratchetSessions: {},
    signalRatchetSessions: {},
    legacyRatchetSessions: legacyRaw?.ratchetSessions && typeof legacyRaw.ratchetSessions === "object" ? legacyRaw.ratchetSessions : {},
    trustPins: {},
    oneTimePrekeys: {},
    nextPrekeyId: 1,
    legacyAgreementPublicKey: sanitizeText(legacyRaw?.agreementPublicKey || legacyRaw?.publicKey, 4000),
    legacyAgreementPrivateKeyJwk: legacyRaw?.agreementPrivateKeyJwk || legacyRaw?.privateKeyJwk || null,
    legacySigningPublicKey: sanitizeText(legacyRaw?.signingPublicKey, 4000),
    legacySigningPrivateKeyJwk: legacyRaw?.signingPrivateKeyJwk || null,
  };
  const withPrekeys = await topUpSignalPrekeys(next, SIGNAL_PREKEY_BATCH_SIZE);
  state.crypto.device = withPrekeys;
  return withPrekeys;
}

async function publishCryptoBundle() {
  if (!state.auth?.deviceId) return null;
  let device = await ensureCryptoDeviceState();
  device = await topUpSignalPrekeys(device, SIGNAL_PREKEY_BATCH_SIZE);
  if (state.crypto.published && device.publishedAt) {
    const available = Object.values(device.oneTimePrekeys || {}).filter((item) => item && !item.consumed_at).length;
    if (available >= SIGNAL_PREKEY_REPLENISH_AT) return device;
  }
  const signingPrivateKey = await signalImportSigningPrivateKey(device.signingPrivateKeyJwk);
  const signedPrekeySignature = await signalSignDetached(signingPrivateKey, `OHMF_SIGNAL_V1|signed_prekey|${Number(device.signedPrekeyId || 1)}|${device.signedPrekeyPublicKey}`);
  const oneTimePrekeys = Object.values(device.oneTimePrekeys || {})
    .filter((item) => item && !item.consumed_at)
    .map((item) => ({
      prekey_id: Number(item.prekey_id || 0),
      public_key: sanitizeText(item.public_key, 4000),
    }));
  const payload = await apiRequest(`/v1/device-keys/${encodeURIComponent(state.auth.deviceId)}`, {
    method: "PUT",
    body: JSON.stringify({
      bundle_version: ENCRYPTION_SCHEME,
      identity_key_alg: "X25519",
      identity_public_key: device.agreementPublicKey,
      agreement_identity_public_key: device.agreementPublicKey,
      signing_key_alg: "Ed25519",
      signing_public_key: device.signingPublicKey,
      signed_prekey: {
        prekey_id: Number(device.signedPrekeyId || 1),
        public_key: device.signedPrekeyPublicKey,
        signature: signedPrekeySignature,
      },
      key_version: 1,
      trust_level: "TRUSTED_SELF",
      one_time_prekeys: oneTimePrekeys,
    }),
  });
  const next = {
    ...device,
    publishedAt: payload?.updated_at || nowISO(),
    fingerprint: sanitizeText(payload?.fingerprint, 128),
  };
  persistCryptoDeviceState(next);
  state.crypto.published = true;
  await apiRequest(`/v1/devices/${encodeURIComponent(state.auth.deviceId)}`, {
    method: "PATCH",
    body: JSON.stringify({
      platform: "WEB",
      capabilities: ["MINI_APPS", "E2EE_OTT_V2", "WEB_PUSH_V1"],
    }),
  }).catch((error) => {
    console.error(error);
  });
  return next;
}

async function fetchDeviceBundles(userId) {
  const cacheKey = sanitizeText(userId, 80);
  if (state.crypto.bundleCache[cacheKey]) return state.crypto.bundleCache[cacheKey];
  const payload = await apiRequest(`/v1/device-keys/${encodeURIComponent(cacheKey)}`, { method: "GET" });
  const items = Array.isArray(payload?.items) ? payload.items : [];
  state.crypto.bundleCache[cacheKey] = items;
  return items;
}

async function refetchDeviceBundles(userId) {
  const cacheKey = sanitizeText(userId, 80);
  delete state.crypto.bundleCache[cacheKey];
  return fetchDeviceBundles(cacheKey);
} // removed: boolean cache bypass replaced with named refresh helper

function participantUserIdsForThread(thread) {
  const selfUserId = sanitizeText(state.auth?.userId, 80);
  const seen = new Set();
  const ordered = [];
  for (const userId of [selfUserId, ...(Array.isArray(thread?.participants) ? thread.participants : [])]) {
    const normalized = sanitizeText(userId, 80);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    ordered.push(normalized);
  }
  return ordered;
}

function remoteParticipantUserIdsForThread(thread) {
  const selfUserId = sanitizeText(state.auth?.userId, 80);
  return participantUserIdsForThread(thread).filter((userId) => userId !== selfUserId);
}

function buildEncryptedGroupNotReadyError(thread, fallbackMessage = "Encrypted group is not ready yet.") {
  const blocked = Array.isArray(thread?.e2eeBlockedMemberIds) ? thread.e2eeBlockedMemberIds : [];
  const labels = blocked
    .map((userId) => displayNameForUser(userId) || (userId ? `User ${userId.slice(0, 8)}` : ""))
    .filter(Boolean);
  const error = new Error(labels.length ? `Encrypted group is waiting on ${labels.join(", ")}.` : fallbackMessage);
  error.code = "encrypted_group_not_ready";
  return error;
}

async function ensureEncryptedConversation(thread) {
  if (!thread || thread.kind === "phone" || thread.kind === "draft_phone") return null;
  const selfUserId = sanitizeText(state.auth?.userId, 80);
  const remoteUserIds = remoteParticipantUserIdsForThread(thread);
  if (!selfUserId || !remoteUserIds.length) return null;
  let device = await publishCryptoBundle();
  const participantUserIds = participantUserIdsForThread(thread);
  const bundlesByUser = Object.fromEntries(await Promise.all(
    participantUserIds.map(async (userId) => [
      userId,
      (await refetchDeviceBundles(userId)).filter(signalBundleSupported),
    ])
  ));
  const signalSelfBundles = bundlesByUser[selfUserId] || [];
  const signalRecipientBundles = remoteUserIds.flatMap((userId) => bundlesByUser[userId] || []);
  if (!signalRecipientBundles.length) {
    if (thread.kind === "group" && sanitizeText(thread.encryptionState, 40) === "ENCRYPTED") {
      throw buildEncryptedGroupNotReadyError(thread);
    }
    return null;
  }
  device = confirmTrustedRemoteBundles(device, signalRecipientBundles);
  if (thread.kind === "group") {
    if (sanitizeText(thread.encryptionState, 40) !== "ENCRYPTED") return null;
    if (!thread.e2eeReady) {
      throw buildEncryptedGroupNotReadyError(thread);
    }
    for (const userId of remoteUserIds) {
      if ((bundlesByUser[userId] || []).length === 0) {
        throw buildEncryptedGroupNotReadyError(thread, "Encrypted group members must publish secure messaging keys first.");
      }
    }
  }
  if (thread.kind === "dm" && sanitizeText(thread.encryptionState, 40) !== "ENCRYPTED") {
    const updated = await apiRequest(`/v1/conversations/${encodeURIComponent(thread.id)}/metadata`, {
      method: "PATCH",
      body: JSON.stringify({ encryption_state: "ENCRYPTED" }),
    });
    const existing = getThreadById(thread.id) || thread;
    upsertThread({ ...existing, ...mapConversation(updated), messages: existing.messages, loadedMessages: existing.loadedMessages });
    saveConversationStore();
  }
  return {
    device,
    selfBundles: signalSelfBundles,
    recipientBundles: signalRecipientBundles,
    recipientUserIds: remoteUserIds,
    bundlesByUser,
  };
}

function threadUsesMLS(thread) {
  return thread?.kind === "group"
    && sanitizeText(thread?.encryptionState, 40) === "ENCRYPTED"
    && Boolean(thread?.mlsEnabled);
}

async function signalDeriveX3DHRoot(device, bundle, claimedBundle) {
  const identityPrivateKey = await signalImportAgreementPrivateKey(device.agreementPrivateKeyJwk);
  const ephemeralKeyPair = await signalGenerateAgreementKeyPair();
  const recipientIdentityPublicKey = await signalImportAgreementPublicKey(agreementKeyForBundle(bundle));
  const recipientSignedPrekey = signedPrekeyForBundle(bundle);
  const recipientSignedPrekeyPublicKey = await signalImportAgreementPublicKey(recipientSignedPrekey.public_key);
  const segments = [];
  segments.push(await signalDeriveSharedSecretBytes(identityPrivateKey, recipientSignedPrekeyPublicKey));
  segments.push(await signalDeriveSharedSecretBytes(ephemeralKeyPair.privateKey, recipientIdentityPublicKey));
  segments.push(await signalDeriveSharedSecretBytes(ephemeralKeyPair.privateKey, recipientSignedPrekeyPublicKey));
  const claimedPrekey = claimedBundle?.claimed_one_time_prekey && typeof claimedBundle.claimed_one_time_prekey === "object"
    ? claimedBundle.claimed_one_time_prekey
    : null;
  if (claimedPrekey?.public_key) {
    const oneTimePrekeyPublicKey = await signalImportAgreementPublicKey(sanitizeText(claimedPrekey.public_key, 4000));
    segments.push(await signalDeriveSharedSecretBytes(ephemeralKeyPair.privateKey, oneTimePrekeyPublicKey));
  }
  const rootMaterial = await hkdfExpand(concatBytes(...segments), new Uint8Array(32), "OHMF_SIGNAL_X3DH_V1", 32);
  return {
    rootKey: bytesToBase64(rootMaterial),
    senderEphemeralPublicKey: await signalExportPublicKey(ephemeralKeyPair.publicKey),
  };
}

async function initializeSignalOutboundSession(device, bundle, claimedBundle = null) {
  const recipientSignedPrekey = signedPrekeyForBundle(bundle);
  if (!recipientSignedPrekey.public_key) throw new Error("Missing recipient signed prekey");
  const localRatchetKeyPair = await signalGenerateAgreementKeyPair();
  const x3dh = await signalDeriveX3DHRoot(device, bundle, claimedBundle);
  const recipientSignedPrekeyPublicKey = await signalImportAgreementPublicKey(recipientSignedPrekey.public_key);
  const ratchetSharedSecret = await signalDeriveSharedSecretBytes(localRatchetKeyPair.privateKey, recipientSignedPrekeyPublicKey);
  const seeded = await kdfRoot(x3dh.rootKey, ratchetSharedSecret);
  return {
    session: normalizeSignalRatchetSession({
      rootKey: seeded.rootKey,
      sendChainKey: seeded.chainKey,
      receiveChainKey: "",
      sendCount: 0,
      receiveCount: 0,
      previousSendCount: 0,
      localRatchetPublicKey: await signalExportPublicKey(localRatchetKeyPair.publicKey),
      localRatchetPrivateKeyJwk: await window.crypto.subtle.exportKey("jwk", localRatchetKeyPair.privateKey),
      remoteRatchetPublicKey: recipientSignedPrekey.public_key,
      pendingRatchet: false,
      skippedKeys: {},
    }, device),
    initialSession: {
      sender_ephemeral_public_key: x3dh.senderEphemeralPublicKey,
      signed_prekey_id: recipientSignedPrekey.prekey_id,
      one_time_prekey_id: Number(claimedBundle?.claimed_one_time_prekey?.prekey_id || 0) || undefined,
    },
  };
}

async function rotateSignalSendingRatchet(session) {
  const localRatchetKeyPair = await signalGenerateAgreementKeyPair();
  const localPrivateKey = localRatchetKeyPair.privateKey;
  const remotePublicKey = await signalImportAgreementPublicKey(session.remoteRatchetPublicKey);
  const sharedSecret = await signalDeriveSharedSecretBytes(localPrivateKey, remotePublicKey);
  const next = await kdfRoot(session.rootKey, sharedSecret);
  session.rootKey = next.rootKey;
  session.sendChainKey = next.chainKey;
  session.previousSendCount = Number(session.sendCount || 0);
  session.sendCount = 0;
  session.localRatchetPublicKey = await signalExportPublicKey(localRatchetKeyPair.publicKey);
  session.localRatchetPrivateKeyJwk = await window.crypto.subtle.exportKey("jwk", localPrivateKey);
  session.pendingRatchet = false;
  return session;
}

async function advanceSignalSendingRatchet(device, bundle, claimedBundle = null) {
  let session = getSignalRatchetSession(device, bundle.user_id, bundle.device_id);
  let initialSession = null;
  if (!session) {
    const initialized = await initializeSignalOutboundSession(device, bundle, claimedBundle);
    session = initialized.session;
    initialSession = initialized.initialSession;
  } else if (session.pendingRatchet || !session.sendChainKey) {
    session = await rotateSignalSendingRatchet(session);
  }
  const step = await kdfChain(session.sendChainKey);
  const header = {
    ratchet_public_key: sanitizeText(session.localRatchetPublicKey, 4000),
    previous_chain_length: Number(session.previousSendCount || 0),
    message_number: Number(session.sendCount || 0),
  };
  if (initialSession) header.initial_session = initialSession;
  session.sendChainKey = step.nextChainKey;
  session.sendCount = Number(session.sendCount || 0) + 1;
  return {
    device: setSignalRatchetSession(device, bundle.user_id, bundle.device_id, session),
    header,
    messageKey: step.messageKey,
  };
}

async function buildMLSEpochSecretBoxes(encryptionContext, device, epochSecretBytes) {
  const remoteClaimedBundles = (await Promise.all(
    (Array.isArray(encryptionContext.recipientUserIds) ? encryptionContext.recipientUserIds : [])
      .map((userId) => claimDeviceBundles(userId).catch(() => []))
  )).flat();
  const claimedByDevice = Object.fromEntries(
    remoteClaimedBundles
      .filter(signalBundleSupported)
      .map((item) => [sanitizeText(item?.device_id, 80), item])
  );
  const epochSecretBoxes = [];
  const recipients = [
    ...encryptionContext.selfBundles.filter((item) => sanitizeText(item?.device_id, 80) !== sanitizeText(state.auth.deviceId, 80)),
    ...encryptionContext.recipientBundles,
  ];
  let workingDevice = device;
  for (const bundle of recipients) {
    const claimedBundle = sanitizeText(bundle.user_id, 80) === sanitizeText(state.auth.userId, 80)
      ? null
      : claimedByDevice[sanitizeText(bundle.device_id, 80)] || null;
    const advanced = await advanceSignalSendingRatchet(workingDevice, bundle, claimedBundle);
    workingDevice = advanced.device;
    const wrapNonce = window.crypto.getRandomValues(new Uint8Array(12));
    const wrapKey = await importAESKey(base64ToBytes(advanced.messageKey), ["encrypt"]);
    const wrappedKey = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: wrapNonce }, wrapKey, epochSecretBytes);
    epochSecretBoxes.push({
      user_id: sanitizeText(bundle.user_id, 80),
      device_id: sanitizeText(bundle.device_id, 80),
      wrapped_key: bytesToBase64(wrappedKey),
      wrap_nonce: bytesToBase64(wrapNonce),
      ratchet_public_key: advanced.header.ratchet_public_key,
      previous_chain_length: advanced.header.previous_chain_length,
      message_number: advanced.header.message_number,
      ...(advanced.header.initial_session ? { initial_session: advanced.header.initial_session } : {}),
    });
  }
  const selfPrivateKey = await signalImportAgreementPrivateKey(workingDevice.agreementPrivateKeyJwk);
  const selfPublicKey = await signalImportAgreementPublicKey(workingDevice.agreementPublicKey);
  const selfWrapKey = await signalDeriveWrapKey(selfPrivateKey, selfPublicKey);
  const selfWrapNonce = window.crypto.getRandomValues(new Uint8Array(12));
  const selfWrappedKey = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: selfWrapNonce }, selfWrapKey, epochSecretBytes);
  epochSecretBoxes.push({
    user_id: sanitizeText(state.auth.userId, 80),
    device_id: sanitizeText(state.auth.deviceId, 80),
    wrapped_key: bytesToBase64(selfWrappedKey),
    wrap_nonce: bytesToBase64(selfWrapNonce),
  });
  return { device: workingDevice, epochSecretBoxes };
}

async function encryptMLSConversationContent(thread, plainContent, innerContentType, encryptionContext) {
  let device = confirmTrustedRemoteBundles(encryptionContext.device, encryptionContext.recipientBundles);
  const signingPrivateKey = await signalImportSigningPrivateKey(device.signingPrivateKeyJwk);
  const conversationEpoch = Number(thread.encryptionEpoch || 1);
  const mlsEpoch = Number(thread.mlsEpoch || conversationEpoch || 1);
  const plaintext = new TextEncoder().encode(JSON.stringify({
    content_type: sanitizeText(innerContentType, 40) || CONTENT_TYPE_TEXT,
    body: plainContent,
  }));
  let epochSecret = getMLSEpochSecret(device, thread.id, mlsEpoch);
  let epochSecretBoxes = [];
  if (!epochSecret || epochSecret.treeHash !== sanitizeText(thread.mlsTreeHash, 512)) {
    const epochSecretBytes = window.crypto.getRandomValues(new Uint8Array(32));
    const built = await buildMLSEpochSecretBoxes(encryptionContext, device, epochSecretBytes);
    device = built.device;
    epochSecretBoxes = built.epochSecretBoxes;
    epochSecret = {
      epoch: mlsEpoch,
      digest: await sha256Base64(epochSecretBytes),
      treeHash: sanitizeText(thread.mlsTreeHash, 512),
      secretKey: bytesToBase64(epochSecretBytes),
      updatedAt: nowISO(),
    };
    device = setMLSEpochSecret(device, thread.id, epochSecret);
  }
  const contentNonce = window.crypto.getRandomValues(new Uint8Array(12));
  const epochSecretKey = await importAESKey(base64ToBytes(epochSecret.secretKey), ["encrypt"]);
  const ciphertext = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: contentNonce }, epochSecretKey, plaintext);
  const ciphertextB64 = bytesToBase64(ciphertext);
  const nonceB64 = bytesToBase64(contentNonce);
  const senderSignature = await signalSignDetached(signingPrivateKey, mlsSignaturePayloadForEnvelope({
    scheme: MLS_ENCRYPTION_SCHEME,
    conversationEpoch,
    mlsEpoch,
    treeHash: sanitizeText(thread.mlsTreeHash, 512),
    epochSecretDigest: epochSecret.digest,
    nonce: nonceB64,
    ciphertext: ciphertextB64,
    epochSecretBoxes,
  }));
  return {
    ciphertext: ciphertextB64,
    nonce: nonceB64,
    encryption: {
      scheme: MLS_ENCRYPTION_SCHEME,
      sender_device_id: sanitizeText(state.auth.deviceId, 80),
      sender_user_id: sanitizeText(state.auth.userId, 80),
      sender_signature: senderSignature,
      signature_alg: "Ed25519",
      sender_identity_public_key: device.agreementPublicKey,
      conversation_epoch: conversationEpoch,
      mls_epoch: mlsEpoch,
      tree_hash: sanitizeText(thread.mlsTreeHash, 512),
      epoch_secret_digest: epochSecret.digest,
      ...(epochSecretBoxes.length ? { epoch_secret_boxes: epochSecretBoxes } : {}),
    },
  };
}

async function encryptConversationContent(thread, plainContent, innerContentType = CONTENT_TYPE_TEXT) {
  const encryptionContext = await ensureEncryptedConversation(thread);
  if (!encryptionContext?.device) return null;
  if (threadUsesMLS(thread)) {
    return encryptMLSConversationContent(thread, plainContent, innerContentType, encryptionContext);
  }
  let device = encryptionContext.device;
  device = confirmTrustedRemoteBundles(device, encryptionContext.recipientBundles);
  const signingPrivateKey = await signalImportSigningPrivateKey(device.signingPrivateKeyJwk);
  const remoteClaimedBundles = (await Promise.all(
    (Array.isArray(encryptionContext.recipientUserIds) ? encryptionContext.recipientUserIds : [])
      .map((userId) => claimDeviceBundles(userId).catch(() => []))
  )).flat();
  const claimedByDevice = Object.fromEntries(
    remoteClaimedBundles
      .filter(signalBundleSupported)
      .map((item) => [sanitizeText(item?.device_id, 80), item])
  );
  const contentKeyBytes = window.crypto.getRandomValues(new Uint8Array(32));
  const contentNonce = window.crypto.getRandomValues(new Uint8Array(12));
  const contentKey = await importAESKey(contentKeyBytes, ["encrypt"]);
  const plaintext = new TextEncoder().encode(JSON.stringify({
    content_type: sanitizeText(innerContentType, 40) || CONTENT_TYPE_TEXT,
    body: plainContent,
  }));
  const ciphertext = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: contentNonce }, contentKey, plaintext);
  const ciphertextB64 = bytesToBase64(ciphertext);
  const nonceB64 = bytesToBase64(contentNonce);
  const conversationEpoch = Number(thread.encryptionEpoch || 1);

  const recipientEntries = [];
  const recipients = [
    ...encryptionContext.selfBundles.filter((item) => sanitizeText(item?.device_id, 80) !== sanitizeText(state.auth.deviceId, 80)),
    ...encryptionContext.recipientBundles,
  ];
  for (const bundle of recipients) {
    const peerAgreementKey = agreementKeyForBundle(bundle);
    if (!peerAgreementKey) continue;
    const wrapNonce = window.crypto.getRandomValues(new Uint8Array(12));
    const claimedBundle = sanitizeText(bundle.user_id, 80) === sanitizeText(state.auth.userId, 80)
      ? null
      : claimedByDevice[sanitizeText(bundle.device_id, 80)] || null;
    const advanced = await advanceSignalSendingRatchet(device, bundle, claimedBundle);
    device = advanced.device;
    const wrapKey = await importAESKey(base64ToBytes(advanced.messageKey), ["encrypt"]);
    const wrappedKey = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: wrapNonce }, wrapKey, contentKeyBytes);
    recipientEntries.push({
      user_id: sanitizeText(bundle.user_id, 80),
      device_id: sanitizeText(bundle.device_id, 80),
      wrapped_key: bytesToBase64(wrappedKey),
      wrap_nonce: bytesToBase64(wrapNonce),
      ratchet_public_key: advanced.header.ratchet_public_key,
      previous_chain_length: advanced.header.previous_chain_length,
      message_number: advanced.header.message_number,
      ...(advanced.header.initial_session ? { initial_session: advanced.header.initial_session } : {}),
    });
  }
  const selfPrivateKey = await signalImportAgreementPrivateKey(device.agreementPrivateKeyJwk);
  const selfPublicKey = await signalImportAgreementPublicKey(device.agreementPublicKey);
  const selfWrapKey = await signalDeriveWrapKey(selfPrivateKey, selfPublicKey);
  const selfWrapNonce = window.crypto.getRandomValues(new Uint8Array(12));
  const selfWrappedKey = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: selfWrapNonce }, selfWrapKey, contentKeyBytes);
  recipientEntries.push({
    user_id: sanitizeText(state.auth.userId, 80),
    device_id: sanitizeText(state.auth.deviceId, 80),
    wrapped_key: bytesToBase64(selfWrappedKey),
    wrap_nonce: bytesToBase64(selfWrapNonce),
  });

  const senderSignature = await signalSignDetached(signingPrivateKey, ratchetSignaturePayloadForEnvelope({
    scheme: ENCRYPTION_SCHEME,
    conversationEpoch,
    nonce: nonceB64,
    ciphertext: ciphertextB64,
    recipients: recipientEntries,
  }));

  return {
    ciphertext: ciphertextB64,
    nonce: nonceB64,
    encryption: {
      scheme: ENCRYPTION_SCHEME,
      sender_device_id: sanitizeText(state.auth.deviceId, 80),
      sender_user_id: sanitizeText(state.auth.userId, 80),
      sender_signature: senderSignature,
      signature_alg: "Ed25519",
      sender_identity_public_key: device.agreementPublicKey,
      conversation_epoch: conversationEpoch,
      recipients: recipientEntries,
    },
  };
}

async function unwrapSignalWrappedKey(device, recipient, encryption) {
  const agreementPrivateKey = await signalImportAgreementPrivateKey(device.agreementPrivateKeyJwk);
  const ratchetPublicKey = sanitizeText(recipient?.ratchet_public_key, 4000);
  if (ratchetPublicKey && sanitizeText(encryption?.sender_signature, 8000)) {
    const senderUserId = sanitizeText(encryption?.sender_user_id, 80);
    const senderDeviceId = sanitizeText(encryption?.sender_device_id, 80);
    const initialSession = recipient?.initial_session && typeof recipient.initial_session === "object" ? recipient.initial_session : null;
    const decryptWithSignalSession = async (useFreshSession = false) => {
      const workingDevice = cloneJson(device || {});
      let session = useFreshSession
        ? normalizeSignalRatchetSession(null, workingDevice)
        : (getSignalRatchetSession(workingDevice, senderUserId, senderDeviceId) || normalizeSignalRatchetSession(null, workingDevice));
      let messageKey = takeSkippedMessageKey(session, ratchetPublicKey, Number(recipient?.message_number || 0));
      if (!messageKey) {
        if (session.remoteRatchetPublicKey !== ratchetPublicKey || !session.receiveChainKey) {
          let localPrivateKey;
          let rootBase = session.rootKey;
          if (!session.receiveChainKey && initialSession?.sender_ephemeral_public_key) {
            const senderIdentityPublicKey = await signalImportAgreementPublicKey(sanitizeText(encryption?.sender_identity_public_key, 4000));
            const senderEphemeralPublicKey = await signalImportAgreementPublicKey(sanitizeText(initialSession.sender_ephemeral_public_key, 4000));
            const signedPrekeyPrivateKey = await signalImportAgreementPrivateKey(workingDevice.signedPrekeyPrivateKeyJwk);
            const segments = [
              await signalDeriveSharedSecretBytes(signedPrekeyPrivateKey, senderIdentityPublicKey),
              await signalDeriveSharedSecretBytes(agreementPrivateKey, senderEphemeralPublicKey),
              await signalDeriveSharedSecretBytes(signedPrekeyPrivateKey, senderEphemeralPublicKey),
            ];
            const oneTimePrekeyId = String(Number(initialSession.one_time_prekey_id || 0));
            if (oneTimePrekeyId && workingDevice.oneTimePrekeys?.[oneTimePrekeyId]?.private_key_jwk) {
              const oneTimePrivateKey = await signalImportAgreementPrivateKey(workingDevice.oneTimePrekeys[oneTimePrekeyId].private_key_jwk);
              segments.push(await signalDeriveSharedSecretBytes(oneTimePrivateKey, senderEphemeralPublicKey));
              workingDevice.oneTimePrekeys = {
                ...(workingDevice.oneTimePrekeys || {}),
                [oneTimePrekeyId]: {
                  ...(workingDevice.oneTimePrekeys?.[oneTimePrekeyId] || {}),
                  consumed_at: nowISO(),
                },
              };
            }
            const rootMaterial = await hkdfExpand(concatBytes(...segments), new Uint8Array(32), "OHMF_SIGNAL_X3DH_V1", 32);
            rootBase = bytesToBase64(rootMaterial);
            localPrivateKey = signedPrekeyPrivateKey;
          } else {
            localPrivateKey = await signalImportAgreementPrivateKey(session.localRatchetPrivateKeyJwk || workingDevice.signedPrekeyPrivateKeyJwk);
          }
          const remotePublicKey = await signalImportAgreementPublicKey(ratchetPublicKey);
          const sharedSecret = await signalDeriveSharedSecretBytes(localPrivateKey, remotePublicKey);
          const next = await kdfRoot(rootBase, sharedSecret);
          session.rootKey = next.rootKey;
          session.receiveChainKey = next.chainKey;
          session.receiveCount = 0;
          session.remoteRatchetPublicKey = ratchetPublicKey;
          session.pendingRatchet = true;
        }
        const targetNumber = Number(recipient?.message_number || 0);
        if (targetNumber < Number(session.receiveCount || 0)) {
          throw new Error("Missing skipped message key");
        }
        while (Number(session.receiveCount || 0) < targetNumber) {
          const skipped = await kdfChain(session.receiveChainKey);
          stashSkippedMessageKey(session, ratchetPublicKey, session.receiveCount, skipped.messageKey);
          session.receiveChainKey = skipped.nextChainKey;
          session.receiveCount = Number(session.receiveCount || 0) + 1;
        }
        const current = await kdfChain(session.receiveChainKey);
        messageKey = current.messageKey;
        session.receiveChainKey = current.nextChainKey;
        session.receiveCount = Number(session.receiveCount || 0) + 1;
      }
      const wrapKey = await importAESKey(base64ToBytes(messageKey), ["decrypt"]);
      const wrappedKeyBytes = await window.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: base64ToBytes(recipient.wrap_nonce) },
        wrapKey,
        base64ToBytes(recipient.wrapped_key)
      );
      workingDevice.signalRatchetSessions = {
        ...(workingDevice.signalRatchetSessions || {}),
        [signalRatchetSessionId(senderUserId, senderDeviceId)]: normalizeSignalRatchetSession(session, workingDevice),
      };
      return {
        keyBytes: new Uint8Array(wrappedKeyBytes),
        device: workingDevice,
      };
    };
    try {
      return await decryptWithSignalSession(false);
    } catch (error) {
      const canRetryFreshSession = error?.name === "OperationError"
        && Boolean(initialSession?.sender_ephemeral_public_key)
        && Boolean(getSignalRatchetSession(device, senderUserId, senderDeviceId));
      if (!canRetryFreshSession) throw error;
      return decryptWithSignalSession(true);
    }
  }
  const peerPublicKey = await signalImportAgreementPublicKey(device.agreementPublicKey);
  const wrapKey = await signalDeriveWrapKey(agreementPrivateKey, peerPublicKey);
  const wrappedKeyBytes = await window.crypto.subtle.decrypt(
    { name: "AES-GCM", iv: base64ToBytes(recipient.wrap_nonce) },
    wrapKey,
    base64ToBytes(recipient.wrapped_key)
  );
  return {
    keyBytes: new Uint8Array(wrappedKeyBytes),
    device,
  };
}

async function decryptConversationContent(message) {
  if (sanitizeText(message?.contentType, 40) !== CONTENT_TYPE_ENCRYPTED) return message;
  const cached = cachedDecryptedMessage(message);
  if (cached) return applyDecryptedMessageView(message, cached);
  const envelope = message.content && typeof message.content === "object" ? message.content : {};
  const encryption = envelope.encryption && typeof envelope.encryption === "object" ? envelope.encryption : {};
  const scheme = sanitizeText(encryption.scheme || ENCRYPTION_SCHEME, 120);
  const recipients = Array.isArray(encryption.recipients) ? encryption.recipients : [];
  const epochSecretBoxes = Array.isArray(encryption.epoch_secret_boxes) ? encryption.epoch_secret_boxes : [];
  const recipientEntries = scheme === MLS_ENCRYPTION_SCHEME ? epochSecretBoxes : recipients;
  const recipient = recipientEntries.find((item) => sanitizeText(item?.device_id, 80) === sanitizeText(state.auth?.deviceId, 80));
  if (!recipient && scheme !== MLS_ENCRYPTION_SCHEME) {
    return {
      ...message,
      text: "[Encrypted message for another device]",
      content: { text: "[Encrypted message for another device]" },
      isEncrypted: true,
      decryptStatus: "other_device",
    };
  }

  return queueCryptoDecrypt(async () => {
    try {
      let device = await ensureCryptoDeviceState();
      if (scheme === MLS_ENCRYPTION_SCHEME) {
        const senderUserId = sanitizeText(encryption.sender_user_id, 80);
        const senderDeviceId = sanitizeText(encryption.sender_device_id, 80);
        const senderBundles = await refetchDeviceBundles(senderUserId);
        const senderBundle = senderBundles.find((item) => sanitizeText(item?.device_id, 80) === senderDeviceId);
        const signingPublicKeyValue = sanitizeText(senderBundle?.signing_public_key, 4000);
        if (!signingPublicKeyValue) throw new Error("Missing MLS sender signing key");
        const signingPublicKey = await signalImportSigningPublicKey(signingPublicKeyValue);
        const valid = await signalVerifyDetached(
          signingPublicKey,
          mlsSignaturePayloadForEnvelope({
            scheme,
            conversationEpoch: Number(encryption.conversation_epoch || 1),
            mlsEpoch: Number(encryption.mls_epoch || encryption.conversation_epoch || 1),
            treeHash: sanitizeText(encryption.tree_hash, 512),
            epochSecretDigest: sanitizeText(encryption.epoch_secret_digest, 200),
            nonce: String(envelope.nonce || ""),
            ciphertext: String(envelope.ciphertext || ""),
            epochSecretBoxes,
          }),
          sanitizeText(encryption.sender_signature, 8000)
        );
        if (!valid) throw new Error("Invalid MLS sender signature");
        let epochSecret = getMLSEpochSecret(
          device,
          sanitizeText(message?.conversationId, 80),
          Number(encryption.mls_epoch || encryption.conversation_epoch || 1),
          sanitizeText(encryption.epoch_secret_digest, 200)
        );
        if (!epochSecret) {
          if (!recipient) {
            return {
              ...message,
              text: "[Encrypted message for another device]",
              content: { text: "[Encrypted message for another device]" },
              isEncrypted: true,
              decryptStatus: "other_device",
            };
          }
          const unwrapped = await unwrapSignalWrappedKey(device, recipient, encryption);
          device = setMLSEpochSecret(unwrapped.device, sanitizeText(message?.conversationId, 80), {
            epoch: Number(encryption.mls_epoch || encryption.conversation_epoch || 1),
            digest: sanitizeText(encryption.epoch_secret_digest, 200),
            treeHash: sanitizeText(encryption.tree_hash, 512),
            secretKey: bytesToBase64(unwrapped.keyBytes),
            updatedAt: nowISO(),
          });
          epochSecret = getMLSEpochSecret(
            device,
            sanitizeText(message?.conversationId, 80),
            Number(encryption.mls_epoch || encryption.conversation_epoch || 1),
            sanitizeText(encryption.epoch_secret_digest, 200)
          );
        }
        if (!epochSecret?.secretKey) throw new Error("Missing MLS epoch secret");
        const epochSecretKey = await importAESKey(base64ToBytes(epochSecret.secretKey), ["decrypt"]);
        const plaintext = await window.crypto.subtle.decrypt(
          { name: "AES-GCM", iv: base64ToBytes(envelope.nonce) },
          epochSecretKey,
          base64ToBytes(envelope.ciphertext)
        );
        const decoded = JSON.parse(new TextDecoder().decode(plaintext));
        const innerContentType = sanitizeText(decoded?.content_type, 40)
          || (decoded?.body && typeof decoded.body === "object" && decoded.body?.attachment_id ? CONTENT_TYPE_ATTACHMENT : "")
          || (decoded && typeof decoded === "object" && decoded?.attachment_id ? CONTENT_TYPE_ATTACHMENT : "")
          || CONTENT_TYPE_TEXT;
        const innerContent = decoded && typeof decoded.body === "object"
          ? decoded.body
          : decoded?.body !== undefined
          ? decoded.body
          : decoded && typeof decoded === "object"
          ? decoded
          : { text: sanitizeText(decoded, 1000) };
        const fallbackText = innerContentType === CONTENT_TYPE_ATTACHMENT
          ? sanitizeText(innerContent?.file_name || innerContent?.attachment_id || "Attachment", 1000)
          : sanitizeText(innerContent?.text || "[Encrypted message]", 1000);
        const decrypted = {
          ...message,
          text: fallbackText,
          contentType: innerContentType,
          content: innerContent && typeof innerContent === "object" ? innerContent : { text: sanitizeText(innerContent, 1000) },
          isEncrypted: true,
          decryptStatus: "ok",
        };
        rememberDecryptedMessage(decrypted);
        return decrypted;
      }
      if (scheme === LEGACY_ENCRYPTION_SCHEME) {
        if (!device.legacyAgreementPrivateKeyJwk) throw new Error("Missing legacy key material");
        const legacyDevice = {
          agreementPrivateKeyJwk: device.legacyAgreementPrivateKeyJwk,
          ratchetSessions: device.legacyRatchetSessions || {},
        };
        const legacyAgreementPrivateKey = await importECDHPrivateKey(legacyDevice.agreementPrivateKeyJwk);
        let wrapKey;
        const ratchetPublicKey = sanitizeText(recipient.ratchet_public_key, 4000);
        if (ratchetPublicKey) {
          const senderUserId = sanitizeText(encryption.sender_user_id, 80);
          const senderDeviceId = sanitizeText(encryption.sender_device_id, 80);
          const senderBundles = await refetchDeviceBundles(senderUserId);
          const senderBundle = senderBundles.find((item) => sanitizeText(item?.device_id, 80) === senderDeviceId) || {};
          const signingPublicKeyValue = sanitizeText(senderBundle?.signing_public_key, 4000);
          if (signingPublicKeyValue) {
            try {
              const signingPublicKey = await importECDSAPublicKey(signingPublicKeyValue);
              const valid = await verifyDetached(
                signingPublicKey,
                ratchetSignaturePayloadForEnvelope({
                  scheme,
                  conversationEpoch: Number(encryption.conversation_epoch || 1),
                  nonce: String(envelope.nonce || ""),
                  ciphertext: String(envelope.ciphertext || ""),
                  recipients,
                }),
                sanitizeText(encryption.sender_signature, 8000)
              );
              if (!valid) throw new Error("Invalid legacy sender signature");
            } catch {}
          }
          let session = getRatchetSession(legacyDevice, senderUserId, senderDeviceId) || normalizeRatchetSession(null, legacyDevice);
          let messageKey = takeSkippedMessageKey(session, ratchetPublicKey, Number(recipient.message_number || 0));
          if (!messageKey) {
            if (session.remoteRatchetPublicKey !== ratchetPublicKey || !session.receiveChainKey) {
              const initialInbound = !session.receiveChainKey;
              const localPrivateKey = await importECDHPrivateKey(
                initialInbound ? legacyDevice.agreementPrivateKeyJwk : (session.localRatchetPrivateKeyJwk || legacyDevice.agreementPrivateKeyJwk)
              );
              const remotePublicKey = await importECDHPublicKey(ratchetPublicKey);
              const sharedSecret = await deriveSharedSecretBytes(localPrivateKey, remotePublicKey);
              let rootBase = session.rootKey;
              if (initialInbound) {
                const senderAgreementPublicKey = await importECDHPublicKey(sanitizeText(encryption.sender_identity_public_key, 4000));
                const handshakeSharedSecret = await deriveSharedSecretBytes(legacyAgreementPrivateKey, senderAgreementPublicKey);
                const handshake = await kdfRoot("", handshakeSharedSecret);
                rootBase = handshake.rootKey;
              }
              const next = await kdfRoot(rootBase, sharedSecret);
              session.rootKey = next.rootKey;
              session.receiveChainKey = next.chainKey;
              session.receiveCount = 0;
              session.remoteRatchetPublicKey = ratchetPublicKey;
              session.pendingRatchet = true;
            }
            const targetNumber = Number(recipient.message_number || 0);
            while (Number(session.receiveCount || 0) < targetNumber) {
              const skipped = await kdfChain(session.receiveChainKey);
              stashSkippedMessageKey(session, ratchetPublicKey, session.receiveCount, skipped.messageKey);
              session.receiveChainKey = skipped.nextChainKey;
              session.receiveCount = Number(session.receiveCount || 0) + 1;
            }
            const current = await kdfChain(session.receiveChainKey);
            messageKey = current.messageKey;
            session.receiveChainKey = current.nextChainKey;
            session.receiveCount = Number(session.receiveCount || 0) + 1;
          }
          device = persistCryptoDeviceState({
            ...device,
            legacyRatchetSessions: {
              ...(device.legacyRatchetSessions || {}),
              ...legacyDevice.ratchetSessions,
              [ratchetSessionId(senderUserId, senderDeviceId)]: session,
            },
          });
          wrapKey = await importAESKey(base64ToBytes(messageKey), ["decrypt"]);
        } else {
          const peerPublicKey = await importECDHPublicKey(sanitizeText(encryption.sender_identity_public_key, 4000));
          wrapKey = await deriveWrapKey(legacyAgreementPrivateKey, peerPublicKey);
        }
        const contentKeyRaw = await window.crypto.subtle.decrypt(
          { name: "AES-GCM", iv: base64ToBytes(recipient.wrap_nonce) },
          wrapKey,
          base64ToBytes(recipient.wrapped_key)
        );
        const contentKey = await importAESKey(new Uint8Array(contentKeyRaw), ["decrypt"]);
        const plaintext = await window.crypto.subtle.decrypt(
          { name: "AES-GCM", iv: base64ToBytes(envelope.nonce) },
          contentKey,
          base64ToBytes(envelope.ciphertext)
        );
        const decoded = JSON.parse(new TextDecoder().decode(plaintext));
        const innerContentType = sanitizeText(decoded?.content_type, 40) || CONTENT_TYPE_TEXT;
        const innerContent = decoded && typeof decoded.body === "object" ? decoded.body : decoded?.body || decoded || {};
        const decrypted = {
          ...message,
          text: sanitizeText(innerContent?.text || "[Legacy encrypted message]", 1000),
          contentType: innerContentType,
          content: innerContent && typeof innerContent === "object" ? innerContent : { text: sanitizeText(innerContent, 1000) },
          isEncrypted: true,
          decryptStatus: "ok",
        };
        rememberDecryptedMessage(decrypted);
        return decrypted;
      }

      const agreementPrivateKey = await signalImportAgreementPrivateKey(device.agreementPrivateKeyJwk);
      let plaintext;
      const ratchetPublicKey = sanitizeText(recipient.ratchet_public_key, 4000);
      if (ratchetPublicKey && sanitizeText(encryption.sender_signature, 8000)) {
        const senderUserId = sanitizeText(encryption.sender_user_id, 80);
        const senderDeviceId = sanitizeText(encryption.sender_device_id, 80);
        const initialSession = recipient.initial_session && typeof recipient.initial_session === "object" ? recipient.initial_session : null;
        const senderBundles = await refetchDeviceBundles(senderUserId);
        const senderBundle = senderBundles.find((item) => sanitizeText(item?.device_id, 80) === senderDeviceId);
        const signingPublicKeyValue = sanitizeText(senderBundle?.signing_public_key, 4000);
        if (!signingPublicKeyValue) throw new Error("Missing sender signing key");
        const signingPublicKey = await signalImportSigningPublicKey(signingPublicKeyValue);
        const valid = await signalVerifyDetached(
          signingPublicKey,
          ratchetSignaturePayloadForEnvelope({
            scheme,
            conversationEpoch: Number(encryption.conversation_epoch || 1),
            nonce: String(envelope.nonce || ""),
            ciphertext: String(envelope.ciphertext || ""),
            recipients,
          }),
          sanitizeText(encryption.sender_signature, 8000)
        );
        if (!valid) throw new Error("Invalid sender signature");
        const decryptWithSignalSession = async (useFreshSession = false) => {
          const workingDevice = cloneJson(device || {});
          let session = useFreshSession
            ? normalizeSignalRatchetSession(null, workingDevice)
            : (getSignalRatchetSession(workingDevice, senderUserId, senderDeviceId) || normalizeSignalRatchetSession(null, workingDevice));
          let messageKey = takeSkippedMessageKey(session, ratchetPublicKey, Number(recipient.message_number || 0));
          if (!messageKey) {
            if (session.remoteRatchetPublicKey !== ratchetPublicKey || !session.receiveChainKey) {
              let localPrivateKey;
              let rootBase = session.rootKey;
              if (!session.receiveChainKey && initialSession?.sender_ephemeral_public_key) {
                const senderIdentityPublicKey = await signalImportAgreementPublicKey(sanitizeText(encryption.sender_identity_public_key, 4000));
                const senderEphemeralPublicKey = await signalImportAgreementPublicKey(sanitizeText(initialSession.sender_ephemeral_public_key, 4000));
                const signedPrekeyPrivateKey = await signalImportAgreementPrivateKey(workingDevice.signedPrekeyPrivateKeyJwk);
                const segments = [
                  await signalDeriveSharedSecretBytes(signedPrekeyPrivateKey, senderIdentityPublicKey),
                  await signalDeriveSharedSecretBytes(agreementPrivateKey, senderEphemeralPublicKey),
                  await signalDeriveSharedSecretBytes(signedPrekeyPrivateKey, senderEphemeralPublicKey),
                ];
                const oneTimePrekeyId = String(Number(initialSession.one_time_prekey_id || 0));
                if (oneTimePrekeyId && workingDevice.oneTimePrekeys?.[oneTimePrekeyId]?.private_key_jwk) {
                  const oneTimePrivateKey = await signalImportAgreementPrivateKey(workingDevice.oneTimePrekeys[oneTimePrekeyId].private_key_jwk);
                  segments.push(await signalDeriveSharedSecretBytes(oneTimePrivateKey, senderEphemeralPublicKey));
                  workingDevice.oneTimePrekeys = {
                    ...(workingDevice.oneTimePrekeys || {}),
                    [oneTimePrekeyId]: {
                      ...(workingDevice.oneTimePrekeys?.[oneTimePrekeyId] || {}),
                      consumed_at: nowISO(),
                    },
                  };
                }
                const rootMaterial = await hkdfExpand(concatBytes(...segments), new Uint8Array(32), "OHMF_SIGNAL_X3DH_V1", 32);
                rootBase = bytesToBase64(rootMaterial);
                localPrivateKey = signedPrekeyPrivateKey;
              } else {
                localPrivateKey = await signalImportAgreementPrivateKey(session.localRatchetPrivateKeyJwk || workingDevice.signedPrekeyPrivateKeyJwk);
              }
              const remotePublicKey = await signalImportAgreementPublicKey(ratchetPublicKey);
              const sharedSecret = await signalDeriveSharedSecretBytes(localPrivateKey, remotePublicKey);
              const next = await kdfRoot(rootBase, sharedSecret);
              session.rootKey = next.rootKey;
              session.receiveChainKey = next.chainKey;
              session.receiveCount = 0;
              session.remoteRatchetPublicKey = ratchetPublicKey;
              session.pendingRatchet = true;
            }
            const targetNumber = Number(recipient.message_number || 0);
            if (targetNumber < Number(session.receiveCount || 0)) {
              throw new Error("Missing skipped message key");
            }
            while (Number(session.receiveCount || 0) < targetNumber) {
              const skipped = await kdfChain(session.receiveChainKey);
              stashSkippedMessageKey(session, ratchetPublicKey, session.receiveCount, skipped.messageKey);
              session.receiveChainKey = skipped.nextChainKey;
              session.receiveCount = Number(session.receiveCount || 0) + 1;
            }
            const current = await kdfChain(session.receiveChainKey);
            messageKey = current.messageKey;
            session.receiveChainKey = current.nextChainKey;
            session.receiveCount = Number(session.receiveCount || 0) + 1;
          }
          const wrapKey = await importAESKey(base64ToBytes(messageKey), ["decrypt"]);
          const contentKeyRaw = await window.crypto.subtle.decrypt(
            { name: "AES-GCM", iv: base64ToBytes(recipient.wrap_nonce) },
            wrapKey,
            base64ToBytes(recipient.wrapped_key)
          );
          const contentKey = await importAESKey(new Uint8Array(contentKeyRaw), ["decrypt"]);
          const decryptedPlaintext = await window.crypto.subtle.decrypt(
            { name: "AES-GCM", iv: base64ToBytes(envelope.nonce) },
            contentKey,
            base64ToBytes(envelope.ciphertext)
          );
          workingDevice.signalRatchetSessions = {
            ...(workingDevice.signalRatchetSessions || {}),
            [signalRatchetSessionId(senderUserId, senderDeviceId)]: normalizeSignalRatchetSession(session, workingDevice),
          };
          return {
            plaintext: decryptedPlaintext,
            device: workingDevice,
          };
        };
        let decryptedSignalPayload;
        try {
          decryptedSignalPayload = await decryptWithSignalSession(false);
        } catch (error) {
          const canRetryFreshSession = error?.name === "OperationError"
            && Boolean(initialSession?.sender_ephemeral_public_key)
            && Boolean(getSignalRatchetSession(device, senderUserId, senderDeviceId));
          if (!canRetryFreshSession) throw error;
          decryptedSignalPayload = await decryptWithSignalSession(true);
        }
        plaintext = decryptedSignalPayload.plaintext;
        device = persistCryptoDeviceState(decryptedSignalPayload.device);
      } else {
        const peerPublicKey = await signalImportAgreementPublicKey(device.agreementPublicKey);
        const wrapKey = await signalDeriveWrapKey(agreementPrivateKey, peerPublicKey);
        const contentKeyRaw = await window.crypto.subtle.decrypt(
          { name: "AES-GCM", iv: base64ToBytes(recipient.wrap_nonce) },
          wrapKey,
          base64ToBytes(recipient.wrapped_key)
        );
        const contentKey = await importAESKey(new Uint8Array(contentKeyRaw), ["decrypt"]);
        plaintext = await window.crypto.subtle.decrypt(
          { name: "AES-GCM", iv: base64ToBytes(envelope.nonce) },
          contentKey,
          base64ToBytes(envelope.ciphertext)
        );
      }
      const decoded = JSON.parse(new TextDecoder().decode(plaintext));
      const innerContentType = sanitizeText(decoded?.content_type, 40)
        || (decoded?.body && typeof decoded.body === "object" && decoded.body?.attachment_id ? CONTENT_TYPE_ATTACHMENT : "")
        || (decoded && typeof decoded === "object" && decoded?.attachment_id ? CONTENT_TYPE_ATTACHMENT : "")
        || CONTENT_TYPE_TEXT;
      const innerContent = decoded && typeof decoded.body === "object"
        ? decoded.body
        : decoded?.body !== undefined
        ? decoded.body
        : decoded && typeof decoded === "object"
        ? decoded
        : { text: sanitizeText(decoded, 1000) };
      const fallbackText = innerContentType === CONTENT_TYPE_ATTACHMENT
        ? sanitizeText(innerContent?.file_name || innerContent?.attachment_id || "Attachment", 1000)
        : sanitizeText(innerContent?.text || "[Encrypted message]", 1000);
      const decrypted = {
        ...message,
        text: fallbackText,
        contentType: innerContentType,
        content: innerContent && typeof innerContent === "object" ? innerContent : { text: sanitizeText(innerContent, 1000) },
        isEncrypted: true,
        decryptStatus: "ok",
      };
      rememberDecryptedMessage(decrypted);
      return decrypted;
    } catch (error) {
      console.error(error);
      return {
        ...message,
        text: "[Unable to decrypt message]",
        content: { text: "[Unable to decrypt message]" },
        isEncrypted: true,
        decryptStatus: "error",
      };
    }
  });
}

function saveConversationStore() {
  if (!state.auth?.userId) return;
  window.localStorage.setItem(
    conversationStoreKey(),
    JSON.stringify({
      version: STORE_VERSION,
      savedAt: nowISO(),
      threads: state.threads,
    })
  );
}

function isForegroundThread(threadId) {
  const conversationId = sanitizeText(threadId, 80);
  return Boolean(
    state.auth
    && conversationId
    && state.activeThreadId === conversationId
    && !document.hidden
    && typeof document.hasFocus === "function"
    && document.hasFocus()
  );
}

function queueConversationDelivered(threadId, throughServerOrder) {
  const conversationId = sanitizeText(threadId, 80);
  const through = Number(throughServerOrder || 0);
  if (!state.auth || !conversationId || !through) return;
  const thread = getThreadById(conversationId);
  if (!thread || thread.kind === "draft_phone") return;
  const currentThrough = Number(thread.deliveredThroughServerOrder || 0);
  const pendingThrough = Number(pendingDeliveredThroughByThread[conversationId] || 0);
  if (through <= currentThrough && through <= pendingThrough) return;
  pendingDeliveredThroughByThread[conversationId] = Math.max(through, pendingThrough);
  if (pendingDeliveredFlushTimers[conversationId]) return;
  pendingDeliveredFlushTimers[conversationId] = window.setTimeout(() => {
    pendingDeliveredFlushTimers[conversationId] = 0;
    void flushConversationDelivered(conversationId);
  }, 150);
}

async function flushConversationDelivered(threadId) {
  const conversationId = sanitizeText(threadId, 80);
  const through = Number(pendingDeliveredThroughByThread[conversationId] || 0);
  delete pendingDeliveredThroughByThread[conversationId];
  if (!state.auth || !conversationId || !through) return;
  const thread = getThreadById(conversationId);
  if (!thread || thread.kind === "draft_phone") return;
  const currentThrough = Number(thread.deliveredThroughServerOrder || 0);
  if (through <= currentThrough) return;
  try {
    await apiRequest(`/v2/conversations/${encodeURIComponent(conversationId)}/delivered`, {
      method: "POST",
      body: JSON.stringify({
        through_server_order: through,
        device_id: state.sync.deviceId,
      }),
    });
    const refreshed = getThreadById(conversationId);
    if (!refreshed) return;
    upsertThread({
      ...refreshed,
      deliveredThroughServerOrder: Math.max(Number(refreshed.deliveredThroughServerOrder || 0), through),
      deliveredStatusUpdatedAt: refreshed.deliveredStatusUpdatedAt || nowISO(),
    });
    saveConversationStore();
  } catch {} // removed: descriptive catch comment
}

function loadConversationStore() {
  const raw = window.localStorage.getItem(conversationStoreKey());
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.threads)) return;
    state.threads = parsed.threads.map((thread) => ({
      id: sanitizeText(thread.id, 80),
      kind: sanitizeText(thread.kind, 24) || "dm",
      serverTitle: sanitizeText(thread.serverTitle, 80),
      title: sanitizeText(thread.title, 80) || "Conversation",
      subtitle: sanitizeText(thread.subtitle, 120),
      nickname: sanitizeText(thread.nickname, 80),
      encryptionState: sanitizeText(thread.encryptionState, 40),
      encryptionEpoch: Number(thread.encryptionEpoch || 1),
      e2eeReady: Boolean(thread.e2eeReady),
      e2eeBlockedMemberIds: Array.isArray(thread.e2eeBlockedMemberIds) ? thread.e2eeBlockedMemberIds.map((id) => sanitizeText(id, 80)).filter(Boolean) : [],
      blockedByViewer: Boolean(thread.blockedByViewer),
      blockedByOther: Boolean(thread.blockedByOther),
      updatedAt: thread.updatedAt || nowISO(),
      blocked: Boolean(thread.blockedByViewer) || Boolean(thread.blockedByOther) || Boolean(thread.blocked),
      closed: Boolean(thread.closed),
      previewText: sanitizeText(thread.previewText, 180),
      unreadCount: Number(thread.unreadCount || 0),
      deliveredThroughServerOrder: Number(thread.deliveredThroughServerOrder || 0),
      deliveredStatusUpdatedAt: thread.deliveredStatusUpdatedAt || "",
      readThroughServerOrder: Number(thread.readThroughServerOrder || 0),
      readStatusUpdatedAt: thread.readStatusUpdatedAt || "",
      externalPhones: Array.isArray(thread.externalPhones) ? thread.externalPhones.map((p) => sanitizeText(p, 32)) : [],
      participants: Array.isArray(thread.participants) ? thread.participants.map((p) => sanitizeText(p, 80)) : [],
      messages: Array.isArray(thread.messages)
        ? thread.messages.map((message) => {
            const transport = normalizeTransport(message.transport);
            return {
              ...message,
              transport,
              status: normalizeDeliveryStatus(transport, message.status),
            };
          })
        : [],
      loadedMessages: Boolean(thread.loadedMessages),
    }));
  } catch {
    state.threads = [];
  }
}

function setAuthMessage(message) {
  el.authStatus.textContent = sanitizeText(message, 200);
  el.authStatus.classList.remove("error");
}

function setAuthError(message) {
  el.authStatus.textContent = sanitizeText(message, 200);
  el.authStatus.classList.add("error");
} // removed: boolean auth status flag split into named helpers

function syncAuthHint() {
  if (!el.authHint) return;
  el.authHint.innerHTML = "";
  const text = document.createTextNode("Choose your country code and enter your number.");
  el.authHint.appendChild(text);
  if (!window.OHMF_WEB_CONFIG?.use_real_otp_provider) {
    el.authHint.appendChild(document.createTextNode(" In local dev, OTP is "));
    const code = document.createElement("code");
    code.textContent = "123456";
    el.authHint.appendChild(code);
    el.authHint.appendChild(document.createTextNode("."));
  }
}

function base64URLToUint8Array(value) {
  const normalized = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
  const raw = window.atob(padded);
  const output = new Uint8Array(raw.length);
  for (let index = 0; index < raw.length; index += 1) output[index] = raw.charCodeAt(index);
  return output;
}

async function ensureCurrentDeviceId() {
  if (state.auth?.deviceId) return state.auth.deviceId;
  const payload = await apiRequest("/v1/devices");
  const webDevice = (payload?.devices || []).find((device) => sanitizeText(device.platform, 24).toUpperCase() === "WEB");
  if (webDevice?.device_id) {
    authStoreSet({ ...state.auth, deviceId: sanitizeText(webDevice.device_id, 80) });
    return state.auth.deviceId;
  }
  return "";
}

async function registerServiceWorkerAndPush() {
  if (!state.auth || !window.OHMF_WEB_CONFIG?.web_push_enabled) return;
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) return;
  const vapidKey = sanitizeText(window.OHMF_WEB_CONFIG?.web_push_vapid_public_key, 400);
  if (!vapidKey) return;
  const registration = await navigator.serviceWorker.register("./sw.js");
  const permission = await Notification.requestPermission();
  if (permission !== "granted") return;
  let subscription = await registration.pushManager.getSubscription();
  if (!subscription) {
    subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: base64URLToUint8Array(vapidKey),
    });
  }
  const deviceId = await ensureCurrentDeviceId();
  if (!deviceId) return;
  await apiRequest(`/v1/devices/${encodeURIComponent(deviceId)}`, {
    method: "PATCH",
    body: JSON.stringify({
      platform: "WEB",
      push_provider: "WEBPUSH",
      push_subscription: JSON.stringify(subscription),
      capabilities: ["MINI_APPS", "WEB_PUSH_V1"],
    }),
  });
}

async function apiRequest(path, options = {}) {
  const request = () => {
    const headers = new Headers(options.headers || {});
    headers.set("Content-Type", "application/json");
    if (state.auth?.accessToken) headers.set("Authorization", `Bearer ${state.auth.accessToken}`);
    return fetch(`${API_BASE_URL}${path}`, { ...options, headers, credentials: "omit" });
  };

  let response = await request();
  if (response.status === 401 && state.auth?.refreshToken && await refreshAuthTokens()) {
    response = await request();
  }

  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { message: text };
    }
  }
  if (!response.ok) {
    const error = new Error(payload?.message || "Request failed");
    error.status = response.status;
    error.code = payload?.code || `http_${response.status}`;
    throw error;
  }
  return payload;
}

async function refreshAuthTokens() {
  if (!state.auth?.refreshToken) return false;
  if (refreshAuthInFlight) return refreshAuthInFlight;

  const sessionAtStart = state.auth;
  refreshAuthInFlight = (async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/v1/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: sessionAtStart.refreshToken }),
        credentials: "omit",
      });
      if (!response.ok) return false;
      const json = await response.json();
      const tokens = json?.tokens;
      if (!tokens?.access_token || !tokens?.refresh_token) return false;
      if (!state.auth || state.auth.userId !== sessionAtStart.userId) return false;
      authStoreSet({
        ...state.auth,
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token,
      });
      return true;
    } catch {
      return false;
    } finally {
      refreshAuthInFlight = null;
    }
  })();

  return refreshAuthInFlight;
}

function cloneJson(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

function decodeJWTPayload(token) {
  const raw = sanitizeText(token, 4000);
  if (!raw || !raw.includes(".")) return null;
  try {
    const payload = raw.split(".")[1];
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
    return JSON.parse(window.atob(padded));
  } catch {
    return null;
  }
}

function accessTokenExpiresSoon(skewSec = 60) {
  const payload = decodeJWTPayload(state.auth?.accessToken || "");
  const exp = Number(payload?.exp || 0);
  if (!exp) return false;
  return exp <= Math.floor(Date.now() / 1000) + skewSec;
}

async function ensureFreshAccessToken() {
  if (!state.auth?.refreshToken) return false;
  if (!accessTokenExpiresSoon()) return true;
  return Boolean(await refreshAuthTokens());
}

async function forceRefreshAccessToken() {
  if (!state.auth?.refreshToken) return false;
  return Boolean(await refreshAuthTokens());
} // removed: retry and refresh boolean flags now use named flows

function randomId(prefix) {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return `${prefix}_${window.crypto.randomUUID().replace(/-/g, "")}`;
  }
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function getMiniappCatalogEntry(appId = state.miniapp.selectedAppId) {
  return state.miniapp.catalog.find((item) => item.appId === appId) || null;
}

function miniappSupportReason(thread = getActiveThread()) {
  if (!thread) return "Select a saved OHMF conversation to launch an app.";
  if (thread.closed) return "Reopen or choose an active conversation to launch an app.";
  if (thread.kind === "draft_phone") return "Save the conversation before sending an app.";
  return "";
} // removed: support wrapper collapsed into direct reason checks

function rewriteLocalDevEntrypoint(rawUrl) {
  const url = new URL(rawUrl, window.location.href);
  const localHosts = new Set(["localhost", "127.0.0.1"]);
  if (localHosts.has(url.hostname) && localHosts.has(window.location.hostname) && url.port !== window.location.port) {
    url.protocol = window.location.protocol;
    url.host = `${window.location.hostname}:${window.location.port}`;
  }
  return url.toString();
}

function shouldBootstrapBuiltinMiniapps() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("dev_apps") === "1") return true;
  if (window.localStorage.getItem("ohmf.dev_apps") === "1") return true;
  return new Set(["localhost", "127.0.0.1"]).has(window.location.hostname) && Boolean(window.OHMF_RUNTIME_CONFIG?.developer_mode);
}

function normalizeMiniappCatalogEntry(raw) {
  const manifest = raw?.manifest && typeof raw.manifest === "object" ? cloneJson(raw.manifest) : null;
  if (manifest?.entrypoint?.url) {
    manifest.entrypoint.url = rewriteLocalDevEntrypoint(manifest.entrypoint.url);
  }
  const install = raw?.install && typeof raw.install === "object" ? raw.install : {};
  return {
    appId: sanitizeText(raw?.app_id || manifest?.app_id, 120),
    title: sanitizeText(manifest?.name || raw?.title || raw?.app_id, 120),
    summary: sanitizeText(manifest?.metadata?.summary || raw?.summary || "", 220),
    sourceType: sanitizeText(raw?.source_type, 40) || "external",
    version: sanitizeText(raw?.version || manifest?.version, 40),
    publishedAt: sanitizeText(raw?.published_at, 80),
    reviewStatus: sanitizeText(raw?.review_status, 40) || "approved",
    latestApprovedVersion: sanitizeText(raw?.latest_approved_version, 40),
    latestVersion: sanitizeText(raw?.latest_version, 40),
    updateAvailable: Boolean(raw?.update_available),
    updateRequiresConsent: Boolean(raw?.update_requires_consent),
    installState: sanitizeText(raw?.install_state, 60),
    permissionDelta: {
      added: Array.isArray(raw?.permission_delta?.added) ? raw.permission_delta.added.map((value) => sanitizeText(value, 120)).filter(Boolean) : [],
      removed: Array.isArray(raw?.permission_delta?.removed) ? raw.permission_delta.removed.map((value) => sanitizeText(value, 120)).filter(Boolean) : [],
    },
    manifest,
    install: {
      installed: Boolean(install?.installed),
      installedVersion: sanitizeText(install?.installed_version, 40),
      autoUpdate: Boolean(install?.auto_update),
      enabled: install?.enabled !== false,
    },
  };
}

function normalizeMiniappSessionState(raw) {
  if (!raw || typeof raw !== "object") {
    return {
      stateVersion: 1,
      stateSnapshot: {},
      storage: {},
      sharedConversationStorage: {},
      transcript: [],
    };
  }
  return {
    stateVersion: Number(raw.state_version || raw.stateVersion || 1) || 1,
    stateSnapshot: raw.snapshot || raw.stateSnapshot || {},
    storage: raw.session_storage || raw.storage || {},
    sharedConversationStorage: raw.shared_conversation_storage || raw.sharedConversationStorage || {},
    transcript: Array.isArray(raw.projected_messages || raw.transcript) ? (raw.projected_messages || raw.transcript) : [],
  };
}

function miniappConsentKey(manifest) {
  if (!manifest?.app_id || !manifest?.version) return "";
  return `${MINIAPP_CONSENT_STORAGE_PREFIX}.${manifest.app_id}.${manifest.version}`;
}

function hasMiniappConsent(manifest) {
  const key = miniappConsentKey(manifest);
  return Boolean(key) && window.localStorage.getItem(key) === "granted";
}

function grantMiniappConsent(manifest) {
  const key = miniappConsentKey(manifest);
  if (key) {
    window.localStorage.setItem(key, "granted");
  }
}

function consentCopyForMiniapp(manifest, entry) {
  const permissions = Array.isArray(manifest?.permissions) ? manifest.permissions : [];
  const lines = [`${manifest?.name || entry?.title || "This app"} needs approval before it can run in this conversation.`];
  if (permissions.length) {
    lines.push("");
    lines.push("Permissions:");
    for (const permission of permissions) {
      lines.push(`- ${permission}`);
    }
  }
  if (entry?.updateRequiresConsent && entry.permissionDelta.added.length) {
    lines.push("");
    lines.push("New permissions in this update:");
    for (const permission of entry.permissionDelta.added) {
      lines.push(`- ${permission}`);
    }
  }
  lines.push("");
  lines.push("Continue?");
  return lines.join("\n");
}

async function ensureMiniappConsent(manifest, entry) {
  if (!manifest) throw new Error("Manifest is required before launch.");
  const permissions = Array.isArray(manifest.permissions) ? manifest.permissions : [];
  if (!permissions.length && !entry?.updateRequiresConsent) {
    return true;
  }
  if (hasMiniappConsent(manifest)) {
    return true;
  }
  const approved = window.confirm(consentCopyForMiniapp(manifest, entry));
  if (!approved) {
    throw new Error("Mini-app launch cancelled because permissions were not approved.");
  }
  grantMiniappConsent(manifest);
  return true;
}

async function fetchMiniappManifest(manifestUrl) {
  // Convert relative URLs to absolute URLs from mini-app sandbox origin
  let resolvedUrl = manifestUrl;
  if (!manifestUrl.startsWith("http://") && !manifestUrl.startsWith("https://")) {
    const miniappSandboxUrl = window.OHMF_RUNTIME_CONFIG?.miniapp_sandbox_url || "http://localhost:5174";
    resolvedUrl = new URL(manifestUrl, miniappSandboxUrl + "/").toString();
  }
  const response = await fetch(resolvedUrl, { cache: "no-store" });
  if (!response.ok) throw new Error(`Manifest request failed with ${response.status}`);
  const manifest = await response.json();
  if (!manifest?.app_id || !manifest?.entrypoint?.url) throw new Error("invalid_manifest");
  manifest.entrypoint.url = rewriteLocalDevEntrypoint(manifest.entrypoint.url);
  return manifest;
}

async function bootstrapBuiltinMiniappCatalog() {
  if (!state.auth || !shouldBootstrapBuiltinMiniapps()) return;
  for (const entry of BUILTIN_DEV_MINIAPP_CATALOG) {
    try {
      const manifest = await fetchMiniappManifest(entry.manifestUrl);
      await ensureMiniappManifestRegistered(manifest);
    } catch (error) {
      console.error(error);
    }
  }
}

async function ensureMiniappManifestRegistered(manifest) {
  const catalog = await apiRequest("/v1/apps", { method: "GET" });
  const existing = Array.isArray(catalog?.items)
    ? catalog.items.find((item) => sanitizeText(item?.manifest?.app_id || item?.app_id, 120) === sanitizeText(manifest?.app_id, 120))
    : null;
  if (existing) {
    return existing;
  }
  await apiRequest("/v1/apps/register", { method: "POST", body: JSON.stringify({ manifest }) });
  return apiRequest(`/v1/apps/${encodeURIComponent(manifest.app_id)}`, { method: "GET" });
}

async function loadMiniappCatalog(options = {}) {
  if (!state.auth) return [];
  if (options.bootstrapDev !== false) {
    await bootstrapBuiltinMiniappCatalog();
  }
  const response = await apiRequest(`/v1/apps${shouldBootstrapBuiltinMiniapps() ? "?developer_mode=1" : ""}`, { method: "GET" });
  const items = Array.isArray(response?.items)
    ? response.items
      .map(normalizeMiniappCatalogEntry)
      .filter((item) => item.appId)
      .sort((a, b) => {
        if (a.install.installed !== b.install.installed) return a.install.installed ? -1 : 1;
        if (a.updateAvailable !== b.updateAvailable) return a.updateAvailable ? -1 : 1;
        return a.title.localeCompare(b.title);
      })
    : [];
  state.miniapp.catalog = items;
  state.miniapp.catalogLoaded = true;
  if (!state.miniapp.selectedAppId && items.length) {
    state.miniapp.selectedAppId = items[0].appId;
  }
  return items;
}

async function loadMiniappManifestByAppId(appId) {
  const response = await apiRequest(`/v1/apps/${encodeURIComponent(appId)}${shouldBootstrapBuiltinMiniapps() ? "?developer_mode=1" : ""}`, { method: "GET" });
  const manifest = response?.manifest;
  if (!manifest?.app_id || !manifest?.entrypoint?.url) throw new Error("invalid_manifest");
  manifest.entrypoint.url = rewriteLocalDevEntrypoint(manifest.entrypoint.url);
  return manifest;
}

async function ensureMiniappInstalled(appId, options = {}) {
  if (!appId) return null;
  const body = options.acceptPermissionChanges ? JSON.stringify({ accept_permission_changes: true }) : undefined;
  const response = await apiRequest(`/v1/apps/${encodeURIComponent(appId)}/install`, { method: "POST", body });
  const normalized = normalizeMiniappCatalogEntry(response);
  const nextCatalog = state.miniapp.catalog.map((item) => (item.appId === normalized.appId ? { ...item, ...normalized } : item));
  if (!nextCatalog.some((item) => item.appId === normalized.appId)) {
    nextCatalog.push(normalized);
  }
  state.miniapp.catalog = nextCatalog;
  return normalized;
}

function buildMiniappViewer(thread) {
  return {
    user_id: state.auth?.userId || "",
    role: "PLAYER",
    display_name: thread?.title || state.auth?.phoneE164 || state.auth?.userId || "You",
  };
}

function buildMiniappParticipants(thread) {
  const ids = Array.isArray(thread?.participants) ? thread.participants : [];
  const viewer = buildMiniappViewer(thread);
  const participants = [{ ...viewer }];
  for (const id of ids) {
    if (!id || id === viewer.user_id) continue;
    participants.push({ user_id: id, role: "PLAYER", display_name: displayNameForUser(id) || `User ${id.slice(0, 8)}` });
  }
  if (participants.length === 1 && thread?.externalPhones?.[0]) {
    participants.push({ user_id: `phone:${thread.externalPhones[0]}`, role: "PLAYER", display_name: thread.externalPhones[0] });
  }
  return participants;
}

function applyMiniappSessionRecord(record, manifest) {
  state.miniapp.sessionMode = "gateway";
  state.miniapp.manifest = manifest;
  state.miniapp.sessionState = normalizeMiniappSessionState({
    state_version: record?.state_version,
    snapshot: record?.state?.snapshot,
    session_storage: record?.state?.session_storage,
    shared_conversation_storage: record?.state?.shared_conversation_storage,
    projected_messages: record?.state?.projected_messages,
  });
  state.miniapp.launchContext = record?.launch_context || null;
  state.miniapp.consentRequired = Boolean(record?.consent_required || record?.launch_context?.consent_required);
  state.miniapp.grantedPermissions = new Set(
    Array.isArray(record?.capabilities_granted) && record.capabilities_granted.length ? record.capabilities_granted : (manifest.permissions || [])
  );
  state.miniapp.lastShareError = "";
}

async function ensureMiniappSession() {
  const thread = getActiveThread();
  if (miniappSupportReason(thread)) throw new Error("Select a saved conversation first.");
  const entry = getMiniappCatalogEntry();
  if (!entry) throw new Error("Select an app first.");
  const manifest = state.miniapp.manifest || entry.manifest || await loadMiniappManifestByAppId(entry.appId);
  await ensureMiniappConsent(manifest, entry);
  const install = await ensureMiniappInstalled(entry.appId, { acceptPermissionChanges: entry?.updateRequiresConsent });
  state.miniapp.manifest = manifest;
  const record = await apiRequest("/v1/apps/sessions", {
    method: "POST",
    body: JSON.stringify({
      app_id: manifest.app_id,
      conversation_id: thread.id,
      viewer: buildMiniappViewer(thread),
      participants: buildMiniappParticipants(thread),
      capabilities_granted: Array.from(state.miniapp.grantedPermissions),
      state_snapshot: cloneJson(state.miniapp.sessionState?.stateSnapshot || {}),
      resume_existing: true,
    }),
  });
  applyMiniappSessionRecord(record, manifest);
}

async function fetchMiniappSession(sessionId) {
  const record = await apiRequest(`/v1/apps/sessions/${encodeURIComponent(sessionId)}`, { method: "GET" });
  const appId = sanitizeText(record?.app_id, 120);
  if (!appId) throw new Error("invalid_session");
  const manifest = state.miniapp.manifest?.app_id === appId ? state.miniapp.manifest : await loadMiniappManifestByAppId(appId);
  state.miniapp.selectedAppId = appId;
  applyMiniappSessionRecord(record, manifest);
  return record;
}

async function joinMiniappSession(sessionId) {
  const record = await apiRequest(`/v1/apps/sessions/${encodeURIComponent(sessionId)}/join`, {
    method: "POST",
    body: JSON.stringify({ capabilities_granted: Array.from(state.miniapp.grantedPermissions) }),
  });
  const appId = sanitizeText(record?.app_id, 120);
  const manifest = state.miniapp.manifest?.app_id === appId ? state.miniapp.manifest : await loadMiniappManifestByAppId(appId);
  applyMiniappSessionRecord(record, manifest);
  return record;
}

async function persistMiniappSession(version, eventName, eventBody) {
  if (!state.miniapp.launchContext?.app_session_id) return 0;
  const nextVersion = Math.max(1, Number(version || 0) + 1);
  const payload = await apiRequest(`/v1/apps/sessions/${encodeURIComponent(state.miniapp.launchContext.app_session_id)}/snapshot`, {
    method: "POST",
    body: JSON.stringify({
      state: {
        snapshot: cloneJson(state.miniapp.sessionState?.stateSnapshot || {}),
        session_storage: cloneJson(state.miniapp.sessionState?.storage || {}),
        shared_conversation_storage: cloneJson(state.miniapp.sessionState?.sharedConversationStorage || {}),
        projected_messages: cloneJson(state.miniapp.sessionState?.transcript || []),
      },
      state_version: nextVersion,
      capabilities_granted: Array.from(state.miniapp.grantedPermissions),
    }),
  });
  if (eventName) {
    await apiRequest(`/v1/apps/sessions/${encodeURIComponent(state.miniapp.launchContext.app_session_id)}/events`, {
      method: "POST",
      body: JSON.stringify({ event_name: eventName, body: eventBody || {} }),
    });
  }
  return Number(payload?.state_version || nextVersion || 1);
}

async function shareMiniappToConversation() {
  const thread = getActiveThread();
  const supportReason = miniappSupportReason(thread);
  if (supportReason) {
    state.miniapp.lastShareError = supportReason;
    renderMiniappLauncher();
    return;
  }
  const entry = getMiniappCatalogEntry();
  if (!entry) return;
  const manifest = state.miniapp.manifest || entry.manifest || await loadMiniappManifestByAppId(entry.appId);
  await ensureMiniappConsent(manifest, entry);
  const install = await ensureMiniappInstalled(entry.appId, { acceptPermissionChanges: entry?.updateRequiresConsent });
  state.miniapp.manifest = manifest;
  state.miniapp.lastShareError = "";
  let payload;
  try {
    payload = await apiRequest("/v1/apps/shares", {
      method: "POST",
      body: JSON.stringify({
        conversation_id: thread.id,
        app_id: manifest.app_id,
        capabilities_granted: Array.from(state.miniapp.grantedPermissions),
        state_snapshot: cloneJson(state.miniapp.sessionState?.stateSnapshot || {}),
        resume_existing: true,
      }),
    });
  } catch (error) {
    if (error.status === 409 && error.code === "miniapp_unsupported") {
      state.miniapp.lastShareError = "This conversation is not mini-app capable yet. Every participant needs an OHMF device with mini-app support.";
      renderMiniappLauncher();
      return;
    }
    throw error;
  }
  if (payload?.message) {
    upsertThreadMessage(thread.id, mapMessage(payload.message));
  }
  applyMiniappSessionRecord(payload, manifest);
  closeMiniappLauncher();
  renderAll();
}

async function openMiniappCard(message) {
  const sessionId = sanitizeText(message?.content?.app_session_id, 120);
  if (!sessionId) return;
  state.miniapp.drawerOpen = true;
  const record = await fetchMiniappSession(sessionId);
  renderAll();
  if (record?.joinable !== false) {
    await openEmbeddedMiniapp();
  }
}

function appendProjectedMiniappMessage(text, contentType = "app_event", content = null) {
  const thread = getActiveThread();
  if (!thread) return;
  const message = {
    id: randomId("appmsg"),
    direction: "out",
    text: sanitizeText(text, 280),
    createdAt: nowISO(),
    serverOrder: Number.MAX_SAFE_INTEGER - Date.now(),
    status: OHMF_DELIVERY_STATUSES.SENT,
    statusUpdatedAt: nowISO(),
    transport: TRANSPORT_OHMF,
    reactions: {},
    editedAt: "",
    deleted: false,
    contentType,
    content,
  };
  upsertThread({ ...thread, messages: [...(thread.messages || []), message], updatedAt: message.createdAt });
  saveConversationStore();
  renderAll();
}

function buildMiniappFrameURL() {
  // Build URL relative to mini-app sandbox origin (separate from main app)
  const miniappSandboxUrl = window.OHMF_RUNTIME_CONFIG?.miniapp_sandbox_url || "http://localhost:5174";
  const url = new URL(state.miniapp.manifest.entrypoint.url, miniappSandboxUrl + "/");
  state.miniapp.channelId = randomId("chan");
  url.searchParams.set("channel", state.miniapp.channelId);
  url.searchParams.set("parent_origin", window.location.origin);
  url.searchParams.set("app_id", state.miniapp.manifest.app_id);
  return url.toString();
}

function clearMiniappLoadTimeout() {
  if (state.miniapp.loadTimer) {
    window.clearTimeout(state.miniapp.loadTimer);
    state.miniapp.loadTimer = 0;
  }
  state.miniapp.loading = false;
}

function startMiniappLoadTimeout() {
  clearMiniappLoadTimeout();
  state.miniapp.loading = true;
  state.miniapp.loadTimer = window.setTimeout(() => {
    state.miniapp.loadTimer = 0;
    if (!state.miniapp.popupOpen) return;
    state.miniapp.loading = false;
    state.miniapp.lastShareError = "The mini-app runtime timed out while loading. Retry or close the app window.";
    renderAll();
  }, 10000);
}

function summarizeMiniappMessage(params) {
  const explicit = sanitizeText(params?.text, 220);
  if (explicit) return explicit;
  const eventName = sanitizeText(params?.content?.event_name, 80);
  if (eventName) return `${state.miniapp.manifest?.name || "App"}: ${eventName}`;
  return `${state.miniapp.manifest?.name || "App"} posted an update.`;
}

function requireMiniappPermission(permission) {
  if (state.miniapp.grantedPermissions.has(permission)) return;
  const error = new Error(`Permission required: ${permission}`);
  error.code = "permission_denied";
  throw error;
}

async function handleMiniappBridgeCall(message) {
  const method = sanitizeText(message.method, 120);
  switch (method) {
    case "host.getLaunchContext":
      return cloneJson(state.miniapp.launchContext);
    case "host.getRuntimeConfig":
      return {
        asset_version: window.OHMF_RUNTIME_CONFIG?.asset_version || "dev",
        api_base_url: window.OHMF_WEB_CONFIG?.api_base_url || API_BASE_URL || "http://localhost:18080",
        developer_mode: window.OHMF_WEB_CONFIG?.developer_mode || false,
      };
    case "conversation.readContext":
      requireMiniappPermission("conversation.read_context");
      return {
        conversation_id: state.miniapp.launchContext?.conversation_id,
        title: getActiveThread()?.title || "Conversation",
        recent_messages: cloneJson((getActiveThread()?.messages || []).slice(-6).map((item) => ({
          author: item.direction === "out" ? "You" : getActiveThread()?.title || "Participant",
          text: item.text,
          createdAt: item.createdAt,
        }))),
      };
    case "conversation.sendMessage": {
      requireMiniappPermission("conversation.send_message");
      const text = summarizeMiniappMessage(message.params);
      appendProjectedMiniappMessage(text, sanitizeText(message.params?.content_type, 60) || "app_event", message.params?.content || null);
      state.miniapp.sessionState.transcript.push({ author: state.miniapp.manifest.name, text, createdAt: nowISO() });
      state.miniapp.sessionState.stateVersion += 1;
      const persistedVersion = await persistMiniappSession(state.miniapp.sessionState.stateVersion, "MESSAGE_PROJECTED", {
        text,
        content_type: sanitizeText(message.params?.content_type, 60) || "app_event",
      });
      state.miniapp.launchContext.state_version = persistedVersion;
      return { message_id: randomId("msg"), state_version: persistedVersion };
    }
    case "participants.readBasic":
      requireMiniappPermission("participants.read_basic");
      return { participants: cloneJson(state.miniapp.launchContext?.participants || []) };
    case "storage.session.get": {
      requireMiniappPermission("storage.session");
      const key = sanitizeText(message.params?.key, 80);
      return { key, value: cloneJson(state.miniapp.sessionState.storage[key]) };
    }
    case "storage.session.set": {
      requireMiniappPermission("storage.session");
      const key = sanitizeText(message.params?.key, 80);
      state.miniapp.sessionState.storage[key] = cloneJson(message.params?.value);
      state.miniapp.sessionState.stateVersion += 1;
      const persistedVersion = await persistMiniappSession(state.miniapp.sessionState.stateVersion, "SESSION_STORAGE_UPDATED", { key });
      state.miniapp.launchContext.state_version = persistedVersion;
      return { key, value: cloneJson(state.miniapp.sessionState.storage[key]), state_version: persistedVersion };
    }
    case "session.updateState": {
      requireMiniappPermission("realtime.session");
      const next = cloneJson(message.params || {});
      state.miniapp.sessionState.stateSnapshot = {
        ...(state.miniapp.sessionState.stateSnapshot || {}),
        ...next,
        updated_at: nowISO(),
      };
      state.miniapp.sessionState.stateVersion += 1;
      const persistedVersion = await persistMiniappSession(state.miniapp.sessionState.stateVersion, "STATE_UPDATED", { delta: next });
      state.miniapp.launchContext.state_snapshot = cloneJson(state.miniapp.sessionState.stateSnapshot);
      state.miniapp.launchContext.state_version = persistedVersion;
      return { state_version: persistedVersion, state_snapshot: cloneJson(state.miniapp.sessionState.stateSnapshot) };
    }
    default: {
      const error = new Error(`Unknown bridge method: ${method}`);
      error.code = "method_not_found";
      throw error;
    }
  }
}

function sendMiniappBridgeResponse(targetWindow, requestId, ok, result, error) {
  targetWindow.postMessage(
    {
      bridge_version: "1.0",
      channel: state.miniapp.channelId,
      request_id: requestId,
      ok,
      result: ok ? cloneJson(result) : undefined,
      error: ok ? undefined : error,
    },
    "*"
  );
}

function profileForUser(userId) {
  return state.profiles[sanitizeText(userId, 80)] || null;
}

function displayNameForUser(userId) {
  const profile = profileForUser(userId);
  return sanitizeText(profile?.display_name || profile?.primary_phone_e164 || "", 80);
}

function pickTitle(conversation) {
  if (conversation.nickname) return conversation.nickname;
  if (sanitizeText(conversation.serverTitle, 80)) return sanitizeText(conversation.serverTitle, 80);
  if (conversation.externalPhones?.length) return conversation.externalPhones[0];
  const others = (conversation.participants || []).filter((id) => id !== state.auth?.userId);
  if (others.length === 0) return `Conversation ${conversation.id.slice(0, 8)}`;
  if (conversation.kind === "group") {
    const labels = others.map((id) => displayNameForUser(id) || `User ${id.slice(0, 8)}`).slice(0, 3);
    return labels.join(", ");
  }
  return displayNameForUser(others[0]) || `User ${others[0].slice(0, 8)}`;
}

function pickSubtitle(conversation) {
  if (conversation.blockedByViewer) return "Blocked by you";
  if (conversation.blockedByOther) return "Blocked";
  if (conversation.kind === "phone") return "Phone conversation (OTT preferred)";
  if (conversation.kind === "group" && sanitizeText(conversation.encryptionState, 40) === "ENCRYPTED" && !conversation.e2eeReady) {
    const blockedCount = Array.isArray(conversation.e2eeBlockedMemberIds) ? conversation.e2eeBlockedMemberIds.length : 0;
    return blockedCount > 0 ? `Encrypted group waiting on ${blockedCount} member${blockedCount === 1 ? "" : "s"}` : "Encrypted group syncing";
  }
  if (sanitizeText(conversation.encryptionState, 40) === "ENCRYPTED") return "Encrypted conversation";
  if (conversation.kind === "group") {
    const total = Array.isArray(conversation.participants) ? conversation.participants.length : 0;
    return total <= 1 ? "Only you" : `${total} participants`;
  }
  const others = (conversation.participants || []).filter((id) => id !== state.auth?.userId).length;
  return others <= 0 ? "Only you" : `${others} participant${others > 1 ? "s" : ""}`;
}

function mapConversation(item) {
  const kind = item.type === "PHONE_DM" || (Array.isArray(item.external_phones) && item.external_phones.length > 0)
    ? "phone"
    : item.type === "GROUP"
    ? "group"
    : "dm";
  const blockedByViewer = Boolean(item.blocked_by_viewer);
  const blockedByOther = Boolean(item.blocked_by_other);
  const thread = {
    id: sanitizeText(item.conversation_id, 80),
    kind,
    serverTitle: sanitizeText(item.title, 80),
    title: "",
    subtitle: "",
    nickname: sanitizeText(item.nickname, 80),
    encryptionState: sanitizeText(item.encryption_state, 40),
    encryptionEpoch: Number(item.encryption_epoch || 1),
    mlsEnabled: Boolean(item.mls_enabled),
    mlsEpoch: Number(item.mls_epoch || 0),
    mlsTreeHash: sanitizeText(item.mls_tree_hash, 512),
    e2eeReady: Boolean(item.e2ee_ready),
    e2eeBlockedMemberIds: Array.isArray(item.e2ee_blocked_member_ids) ? item.e2ee_blocked_member_ids.map((v) => sanitizeText(v, 80)).filter(Boolean) : [],
    updatedAt: item.updated_at || nowISO(),
    blockedByViewer,
    blockedByOther,
    blocked: blockedByViewer || blockedByOther || Boolean(item.blocked),
    closed: Boolean(item.closed),
    participants: Array.isArray(item.participants) ? item.participants.map((v) => sanitizeText(v, 80)) : [],
    externalPhones: Array.isArray(item.external_phones) ? item.external_phones.map((v) => sanitizeText(v, 32)) : [],
    previewText: sanitizeText(item.last_message_preview, 180),
    unreadCount: Number(item.unread_count || 0),
    deliveredThroughServerOrder: 0,
    deliveredStatusUpdatedAt: "",
    readThroughServerOrder: 0,
    readStatusUpdatedAt: "",
    messages: [],
    loadedMessages: false,
  };
  thread.title = pickTitle(thread);
  thread.subtitle = pickSubtitle(thread);
  return thread;
}

function mapMessage(item) {
  const transport = normalizeTransport(item.transport);
  const status = normalizeDeliveryStatus(
    transport,
    item.status || (item.sender_user_id === state.auth?.userId ? OHMF_DELIVERY_STATUSES.SENT : "")
  );
  const contentType = sanitizeText(item?.content_type, 40) || CONTENT_TYPE_TEXT;
  const content = item?.content && typeof item.content === "object" ? item.content : {};
  const encryptedEnvelopeFingerprintValue = contentType === CONTENT_TYPE_ENCRYPTED
    ? encryptedEnvelopeFingerprint({
      id: sanitizeText(item?.message_id, 80),
      contentType,
      content,
    })
    : "";
  const deleted = Boolean(item?.deleted) || Boolean(item?.deleted_at) || sanitizeText(item?.visibility_state, 40) === "SOFT_DELETED";
  const genericFallbackText = contentType === CONTENT_TYPE_APP_EVENT
    ? "App event"
    : contentType === CONTENT_TYPE_ENCRYPTED
    ? "[Encrypted message]"
    : "Message";
  const fallbackText = deleted
    ? "Message deleted"
    : contentType === CONTENT_TYPE_APP_CARD
    ? sanitizeText(content.title || "Shared app", 1000)
    : contentType === CONTENT_TYPE_ATTACHMENT
    ? sanitizeText(content.file_name || content.attachment_id || "Attachment", 1000)
    : sanitizeText(item?.content?.text, 1000) || (Object.keys(content || {}).length ? sanitizeText(JSON.stringify(content || {}), 1000) : genericFallbackText);
  return {
    id: sanitizeText(item?.message_id, 80),
    senderUserId: sanitizeText(item?.sender_user_id, 80),
    direction: item.sender_user_id === state.auth?.userId ? "out" : "in",
    text: fallbackText,
    createdAt: item.created_at || nowISO(),
    sentAt: item.sent_at || item.created_at || nowISO(),
    deliveredAt: item.delivered_at || "",
    readAt: item.read_at || "",
    serverOrder: Number(item.server_order || 0),
    status,
    statusUpdatedAt: item.status_updated_at || item.deleted_at || item.read_at || item.delivered_at || item.sent_at || item.created_at || nowISO(),
    transport,
    reactions: normalizeReactionCounts(item?.reactions),
    editedAt: item.edited_at || "",
    deleted,
    deletedAt: item.deleted_at || "",
    contentType,
    content,
    encryptedEnvelopeFingerprint: encryptedEnvelopeFingerprintValue,
    decryptStatus: "",
  };
}

async function materializeMessage(item, existingMessage = null) {
  const mapped = mapMessage(item);
  if (mapped.contentType !== CONTENT_TYPE_ENCRYPTED) return mapped;
  if (canReuseDecryptedMessage(existingMessage, mapped)) {
    return applyDecryptedMessageView(mapped, existingMessage);
  }
  const cached = cachedDecryptedMessage(mapped);
  if (cached) return applyDecryptedMessageView(mapped, cached);
  return decryptConversationContent(mapped);
}

function applyMessageReactions(payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  const messageId = sanitizeText(payload?.message_id, 80);
  if (!conversationId || !messageId) return;
  const thread = getThreadById(conversationId);
  if (!thread) return;
  const reactions = normalizeReactionCounts(payload?.reactions);
  let found = false;
  const nextMessages = (thread.messages || []).map((message) => {
    if (message.id !== messageId) return message;
    found = true;
    return {
      ...message,
      reactions,
      statusUpdatedAt: payload?.acted_at || message.statusUpdatedAt || nowISO(),
    };
  });
  upsertThread({
    ...thread,
    messages: nextMessages,
    updatedAt: payload?.acted_at || thread.updatedAt,
  });
  saveConversationStore();
  refreshOpenMessageMetadata(conversationId, messageId);
  if (!found && thread.loadedMessages && state.activeThreadId === conversationId) {
    void loadMessagesForThread(conversationId).catch((error) => {
      console.error(error);
    });
  }
}

function isAppCardMessage(message) {
  return sanitizeText(message?.contentType, 40) === CONTENT_TYPE_APP_CARD;
}

function isAttachmentMessage(message) {
  return sanitizeText(message?.contentType, 40) === CONTENT_TYPE_ATTACHMENT;
}

function upsertThreadMessage(threadId, nextMessage) {
  const thread = getThreadById(threadId);
  if (!thread) return;
  const nextMessages = [...(thread.messages || []).filter((message) => message.id !== nextMessage.id), nextMessage].sort(
    (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
  );
  const isUnreadIncoming = nextMessage.direction === "in" && state.activeThreadId !== threadId;
  upsertThread({
    ...thread,
    messages: nextMessages,
    updatedAt: nextMessage.createdAt || nowISO(),
    previewText: nextMessage.deleted ? "Message deleted" : nextMessage.text,
    unreadCount: isUnreadIncoming ? Number(thread.unreadCount || 0) + 1 : Number(thread.unreadCount || 0),
  });
  saveConversationStore();
}

function normalizeTransport(value) {
  return sanitizeText(value, 24) === TRANSPORT_SMS ? TRANSPORT_SMS : TRANSPORT_OHMF;
}

function legacyStatusToCurrent(value, transport) {
  const status = sanitizeText(value, 40).toUpperCase();
  if (!status) return "";
  if (status === "FAILED") return "FAIL_SEND";
  if (status === "PENDING") return "SENT";
  if (status.startsWith("SENT (SMS")) return "SENT";
  if (status === "SENT (SMS FALLBACK)") return "SENT";
  if (transport === TRANSPORT_SMS && status === "DELIVERED") return "SENT";
  if (transport === TRANSPORT_SMS && status === "READ") return "SENT";
  return status;
}

function normalizeDeliveryStatus(transport, status) {
  const resolvedTransport = normalizeTransport(transport);
  const normalized = legacyStatusToCurrent(status, resolvedTransport);
  if (!normalized) return "";
  const allowed = resolvedTransport === TRANSPORT_SMS ? SMS_DELIVERY_STATUSES : OHMF_DELIVERY_STATUSES;
  return Object.values(allowed).includes(normalized) ? normalized : resolvedTransport === TRANSPORT_SMS ? SMS_DELIVERY_STATUSES.SENT : OHMF_DELIVERY_STATUSES.SENT;
}

function deliveryIndicatorLabel(message) {
  if (normalizeTransport(message.transport) !== TRANSPORT_OHMF || message.direction !== "out") return "";
  const status = normalizeDeliveryStatus(message.transport, message.status);
  switch (status) {
    case OHMF_DELIVERY_STATUSES.SENT:
      return message.sentAt ? `Sent ${formatShortTime(message.sentAt)}` : "Sent";
    case OHMF_DELIVERY_STATUSES.DELIVERED:
      return message.deliveredAt ? `Delivered ${formatShortTime(message.deliveredAt)}` : "Delivered";
    case OHMF_DELIVERY_STATUSES.READ:
      return message.readAt ? `Read ${formatShortTime(message.readAt)}` : "Read";
    case OHMF_DELIVERY_STATUSES.FAIL_DELIVERY:
      return "Failed delivery";
    case OHMF_DELIVERY_STATUSES.FAIL_SEND:
      return "Failed to send";
    default:
      return "";
  }
}

function threadSort(a, b) {
  return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
}

function getThreadById(id) {
  return state.threads.find((thread) => thread.id === id) || null;
}

function getActiveThread() {
  return getThreadById(state.activeThreadId);
}

function getMessageById(threadId, messageId) {
  const thread = getThreadById(threadId);
  if (!thread) return null;
  return (thread.messages || []).find((message) => message.id === messageId) || null;
}

function replyAuthorLabel(thread, message) {
  if (message?.direction === "out") return "you";
  return sanitizeText(thread?.title || "this message", 60);
}

function toggleMessageMenu(threadId, messageId) {
  if (
    state.openMessageMenu &&
    state.openMessageMenu.threadId === threadId &&
    state.openMessageMenu.messageId === messageId
  ) {
    state.openMessageMenu = null;
  } else {
    state.openMessageMenu = { threadId, messageId };
  }
  renderMessages();
}

function clearMessageMenu() {
  if (!state.openMessageMenu) return;
  state.openMessageMenu = null;
  renderMessages();
}

function clearMessageMenuWithoutRender() {
  if (!state.openMessageMenu) return;
  state.openMessageMenu = null;
} // removed: boolean render flag split into named menu clearers

function resetMessageMetadataState() {
  state.messageMetadata = {
    open: false,
    threadId: "",
    messageId: "",
    loading: false,
    error: "",
    edits: [],
    reactions: [],
    recipientDeliveryAt: "",
    recipientReadAt: "",
    requestToken: 0,
  };
}

function closeMessageMetadata() {
  if (!state.messageMetadata.open) return;
  state.messageMetadata.open = false;
  renderAll();
}

function messageHistoryContentSummary(content) {
  if (!content || typeof content !== "object") return sanitizeText(content, 180);
  if (sanitizeText(content.text, 180)) return sanitizeText(content.text, 180);
  if (sanitizeText(content.file_name || content.attachment_id, 180)) {
    return `Attachment: ${sanitizeText(content.file_name || content.attachment_id, 160)}`;
  }
  if (sanitizeText(content.title, 180)) return sanitizeText(content.title, 180);
  if (sanitizeText(content.ciphertext, 40) && content.encryption && typeof content.encryption === "object") {
    return "[Encrypted payload]";
  }
  const raw = sanitizeText(JSON.stringify(content), 180);
  return raw || "No content snapshot";
}

async function summarizeHistoricalMessageContent(messageId, content) {
  if (!content || typeof content !== "object") return messageHistoryContentSummary(content);
  const encryption = content.encryption && typeof content.encryption === "object" ? content.encryption : null;
  if (!sanitizeText(content.ciphertext, 40) || !encryption) {
    return messageHistoryContentSummary(content);
  }
  const historicalMessage = {
    id: sanitizeText(messageId, 80),
    senderUserId: sanitizeText(encryption.sender_user_id, 80),
    direction: "out",
    text: "[Encrypted payload]",
    createdAt: nowISO(),
    sentAt: "",
    deliveredAt: "",
    readAt: "",
    serverOrder: 0,
    status: "",
    statusUpdatedAt: nowISO(),
    transport: TRANSPORT_OHMF,
    reactions: {},
    editedAt: "",
    deleted: false,
    contentType: CONTENT_TYPE_ENCRYPTED,
    content,
    encryptedEnvelopeFingerprint: "",
    decryptStatus: "",
  };
  const liveMessage = getMessageById(state.messageMetadata.threadId, messageId);
  const historicalFingerprint = encryptedEnvelopeFingerprint(historicalMessage);
  if (
    liveMessage
    && sanitizeText(liveMessage.decryptStatus, 24) === "ok"
    && historicalFingerprint
    && historicalFingerprint === sanitizeText(liveMessage.encryptedEnvelopeFingerprint, 24000)
  ) {
    return messageHistoryContentSummary(liveMessage.content);
  }
  const cached = cachedDecryptedMessage(historicalFingerprint || historicalMessage);
  if (cached) {
    return messageHistoryContentSummary(cached.content);
  }
  try {
    const decrypted = await decryptConversationContent(historicalMessage);
    if (sanitizeText(decrypted?.decryptStatus, 24) === "ok") {
      return messageHistoryContentSummary(decrypted.content);
    }
  } catch (error) {
    const message = sanitizeText(error?.message, 120);
    if (message !== "Missing skipped message key") {
      console.error(error);
    }
  }
  return "[Encrypted payload]";
}

function recipientDeliveredAt(thread, message) {
  if (!thread || !message || message.direction !== "in") return "";
  if (sanitizeText(state.messageMetadata.recipientDeliveryAt, 80)) return state.messageMetadata.recipientDeliveryAt;
  const through = Number(thread.deliveredThroughServerOrder || 0);
  if (!through || Number(message.serverOrder || 0) > through) return "";
  return thread.deliveredStatusUpdatedAt || "";
}

function recipientReadAt(thread, message) {
  if (!thread || !message || message.direction !== "in") return "";
  if (sanitizeText(state.messageMetadata.recipientReadAt, 80)) return state.messageMetadata.recipientReadAt;
  const through = Number(thread.readThroughServerOrder || 0);
  if (!through || Number(message.serverOrder || 0) > through) return "";
  return thread.readStatusUpdatedAt || "";
}

async function openMessageMetadata(threadId, messageId) {
  const thread = getThreadById(threadId);
  const message = getMessageById(threadId, messageId);
  if (!thread || !message) return;
  const requestToken = Date.now();
  state.messageMetadata = {
    open: true,
    threadId,
    messageId,
    loading: true,
    error: "",
    edits: [],
    reactions: [],
    recipientDeliveryAt: "",
    recipientReadAt: "",
    requestToken,
  };
  renderAll();
  try {
    const [editsPayload, reactionsPayload, readStatusPayload] = await Promise.all([
      apiRequest(`/v1/messages/${encodeURIComponent(messageId)}/edits`),
      apiRequest(`/v1/messages/${encodeURIComponent(messageId)}/reactions/history`),
      apiRequest(`/v1/conversations/${encodeURIComponent(threadId)}/read-status`),
    ]);
    if (
      !state.messageMetadata.open ||
      state.messageMetadata.threadId !== threadId ||
      state.messageMetadata.messageId !== messageId ||
      state.messageMetadata.requestToken !== requestToken
    ) {
      return;
    }
    state.messageMetadata.loading = false;
    const edits = Array.isArray(editsPayload?.edits) ? editsPayload.edits : [];
    if (edits.length) {
      const summarizedEdits = await Promise.all(edits.map(async (item, index) => {
        const isLatestEdit = index === 0;
        return {
          ...item,
          previous_summary: await summarizeHistoricalMessageContent(messageId, item?.previous_content),
          new_summary: isLatestEdit && sanitizeText(message.decryptStatus, 24) === "ok"
            ? messageHistoryContentSummary(message.content)
            : await summarizeHistoricalMessageContent(messageId, item?.new_content),
        };
      }));
      for (let index = 1; index < summarizedEdits.length; index += 1) {
        const newerEdit = summarizedEdits[index - 1];
        const newerBefore = sanitizeText(newerEdit?.previous_summary, 180);
        if (newerBefore) {
          summarizedEdits[index].new_summary = newerBefore;
        }
      }
      state.messageMetadata.edits = summarizedEdits;
    } else {
      state.messageMetadata.edits = [];
    }
    state.messageMetadata.reactions = Array.isArray(reactionsPayload?.history) ? reactionsPayload.history : [];
    const selfStatus = (Array.isArray(readStatusPayload?.members) ? readStatusPayload.members : [])
      .find((item) => sanitizeText(item?.user_id, 80) === sanitizeText(state.auth?.userId, 80));
    state.messageMetadata.recipientDeliveryAt = sanitizeText(selfStatus?.delivery_at, 80);
    state.messageMetadata.recipientReadAt = sanitizeText(selfStatus?.read_at, 80);
    renderAll();
  } catch (error) {
    console.error(error);
    if (
      !state.messageMetadata.open ||
      state.messageMetadata.threadId !== threadId ||
      state.messageMetadata.messageId !== messageId ||
      state.messageMetadata.requestToken !== requestToken
    ) {
      return;
    }
    state.messageMetadata.loading = false;
    state.messageMetadata.error = sanitizeText(error.message || "Unable to load message details.", 220);
    renderAll();
  }
}

function refreshOpenMessageMetadata(threadId, messageId) {
  if (
    !state.messageMetadata.open ||
    state.messageMetadata.threadId !== threadId ||
    state.messageMetadata.messageId !== messageId
  ) {
    return;
  }
  void openMessageMetadata(threadId, messageId);
}

function appendMetadataValueRow(container, label, value, valueClass = "") {
  const row = document.createElement("div");
  row.className = "message-metadata-row";

  const labelEl = document.createElement("span");
  labelEl.className = "message-metadata-label";
  labelEl.textContent = label;

  const valueEl = document.createElement("span");
  valueEl.className = valueClass ? `message-metadata-value ${valueClass}` : "message-metadata-value";
  valueEl.textContent = sanitizeText(value || "Not available", 4000) || "Not available";

  row.append(labelEl, valueEl);
  container.appendChild(row);
}

function buildMessageMetadataSection(title) {
  const section = document.createElement("section");
  section.className = "message-metadata-section";

  const heading = document.createElement("h3");
  heading.className = "message-metadata-section-title";
  heading.textContent = title;
  section.appendChild(heading);

  return section;
}

function renderMessageMetadataWindow() {
  const isOpen = Boolean(state.messageMetadata.open);
  el.messageMetadataWindow.classList.toggle("hidden", !isOpen);
  if (!isOpen) return;

  const thread = getThreadById(state.messageMetadata.threadId);
  const message = getMessageById(state.messageMetadata.threadId, state.messageMetadata.messageId);
  el.messageMetadataTitle.textContent = message ? messageSnippet(message, 80) || "Message details" : "Message details";
  el.messageMetadataSubtitle.textContent = thread
    ? `${thread.title} - #${sanitizeText(state.messageMetadata.messageId, 12)}`
    : sanitizeText(state.messageMetadata.messageId, 80);
  el.messageMetadataBody.replaceChildren();

  if (!thread || !message) {
    const empty = document.createElement("p");
    empty.className = "message-metadata-empty";
    empty.textContent = "This message is no longer available in the current thread view.";
    el.messageMetadataBody.appendChild(empty);
    return;
  }

  const currentSection = buildMessageMetadataSection("Current State");
  appendMetadataValueRow(currentSection, "Sent", formatDateTime(message.sentAt));
  appendMetadataValueRow(currentSection, "Delivered", formatDateTime(message.direction === "out" ? message.deliveredAt : recipientDeliveredAt(thread, message)));
  appendMetadataValueRow(currentSection, "Read", formatDateTime(message.direction === "out" ? message.readAt : recipientReadAt(thread, message)));
  appendMetadataValueRow(currentSection, "Edited", formatDateTime(message.editedAt));
  appendMetadataValueRow(currentSection, "Status", sanitizeText(message.status, 40));
  appendMetadataValueRow(currentSection, "Transport", sanitizeText(message.transport, 40));
  appendMetadataValueRow(currentSection, "Server order", message.serverOrder > 0 ? String(message.serverOrder) : "Pending");
  el.messageMetadataBody.appendChild(currentSection);

  if (state.messageMetadata.loading) {
    const loading = document.createElement("p");
    loading.className = "message-metadata-empty";
    loading.textContent = "Loading edit and reaction history...";
    el.messageMetadataBody.appendChild(loading);
    return;
  }

  if (state.messageMetadata.error) {
    const error = document.createElement("p");
    error.className = "message-metadata-empty error";
    error.textContent = state.messageMetadata.error;
    el.messageMetadataBody.appendChild(error);
  }

  const editsSection = buildMessageMetadataSection("Edit History");
  if (!state.messageMetadata.edits.length) {
    const empty = document.createElement("p");
    empty.className = "message-metadata-empty";
    empty.textContent = "No edits recorded for this message.";
    editsSection.appendChild(empty);
  } else {
    const list = document.createElement("div");
    list.className = "message-metadata-history-list";
    for (const item of state.messageMetadata.edits) {
      const card = document.createElement("article");
      card.className = "message-metadata-history-card";
      appendMetadataValueRow(card, "Edited", formatDateTime(item?.edited_at));
      appendMetadataValueRow(card, "By", sanitizeText(item?.edited_by, 80));
      appendMetadataValueRow(card, "Sent", formatDateTime(item?.sent_at));
      appendMetadataValueRow(card, "Delivered", formatDateTime(item?.delivered_at));
      appendMetadataValueRow(card, "Read", formatDateTime(item?.read_at));
      appendMetadataValueRow(card, "Before", sanitizeText(item?.previous_summary, 180) || messageHistoryContentSummary(item?.previous_content), "history");
      appendMetadataValueRow(card, "After", sanitizeText(item?.new_summary, 180) || messageHistoryContentSummary(item?.new_content), "history");
      list.appendChild(card);
    }
    editsSection.appendChild(list);
  }
  el.messageMetadataBody.appendChild(editsSection);

  const reactionsSection = buildMessageMetadataSection("Reaction History");
  if (!state.messageMetadata.reactions.length) {
    const empty = document.createElement("p");
    empty.className = "message-metadata-empty";
    empty.textContent = "No reaction changes recorded for this message.";
    reactionsSection.appendChild(empty);
  } else {
    const list = document.createElement("div");
    list.className = "message-metadata-history-list";
    for (const item of state.messageMetadata.reactions) {
      const card = document.createElement("article");
      card.className = "message-metadata-history-card";
      appendMetadataValueRow(card, "Action", `${sanitizeText(item?.emoji, 8) || "Reaction"} ${sanitizeText(item?.action, 20)}`);
      appendMetadataValueRow(card, "By", sanitizeText(item?.acted_by, 80));
      appendMetadataValueRow(card, "Changed", formatDateTime(item?.acted_at));
      appendMetadataValueRow(card, "Sent", formatDateTime(item?.sent_at));
      appendMetadataValueRow(card, "Delivered", formatDateTime(item?.delivered_at));
      appendMetadataValueRow(card, "Read", formatDateTime(item?.read_at));
      list.appendChild(card);
    }
    reactionsSection.appendChild(list);
  }
  el.messageMetadataBody.appendChild(reactionsSection);
}

function setReplyTarget(thread, message) {
  state.replyTarget = {
    threadId: thread.id,
    messageId: message.id,
    label: replyAuthorLabel(thread, message),
    snippet: messageSnippet(message),
    reference: buildReplyReference(thread, message),
  };
  state.openMessageMenu = null;
  renderMessages();
  el.composerInput.focus();
}

function clearReplyTarget() {
  if (!state.replyTarget) return;
  state.replyTarget = null;
  renderMessages();
}

function clearReplyTargetWithoutRender() {
  if (!state.replyTarget) return;
  state.replyTarget = null;
} // removed: boolean render flag split into named reply clearers

function composerMessageContent(rawText) {
  const text = sanitizeText(rawText, 1000);
  if (!text) return null;
  const content = { text };
  const activeThread = getActiveThread();
  const replyTarget = state.replyTarget;
  if (!activeThread || !replyTarget || replyTarget.threadId !== activeThread.id) return content;
  const replyReference = normalizeReplyReference(replyTarget.reference);
  if (replyReference) content.reply_to = {
    message_id: replyReference.messageId,
    sender_user_id: replyReference.senderUserId,
    content_type: replyReference.contentType,
    text: replyReference.text,
  };
  return content;
}

function renderComposerReply(thread) {
  if (!thread || !state.replyTarget || state.replyTarget.threadId !== thread.id) {
    state.replyTarget = null;
    el.composerReply.classList.add("hidden");
    el.composerReplyLabel.textContent = "Replying";
    el.composerReplyText.textContent = "";
    return;
  }
  el.composerReplyLabel.textContent = `Replying to ${state.replyTarget.label}`;
  el.composerReplyText.textContent = state.replyTarget.snippet;
  el.composerReply.classList.remove("hidden");
}

function visibleThreads() {
  const q = sanitizeText(state.query, 120).toLowerCase();
  return [...state.threads]
    .filter((thread) => !thread.closed)
    .filter((thread) => {
      if (!q) return true;
      const combined = `${thread.title} ${thread.subtitle} ${thread.previewText || ""} ${(thread.messages || []).map((m) => m.text).join(" ")}`.toLowerCase();
      return combined.includes(q);
    })
    .sort(threadSort);
}

function upsertThread(thread) {
  thread.title = pickTitle(thread);
  thread.subtitle = pickSubtitle(thread);
  const idx = state.threads.findIndex((item) => item.id === thread.id);
  if (idx === -1) state.threads.push(thread);
  else state.threads[idx] = { ...state.threads[idx], ...thread };
  state.threads.sort(threadSort);
}

function setRemoteTyping(conversationId, userId, active) {
  if (!conversationId || !userId || userId === state.auth?.userId) return;
  const next = { ...(state.remoteTypingByThread[conversationId] || {}) };
  if (active) next[userId] = nowISO();
  else delete next[userId];
  if (Object.keys(next).length === 0) delete state.remoteTypingByThread[conversationId];
  else state.remoteTypingByThread[conversationId] = next;
}

function remoteTypingUsers(threadId) {
  return Object.keys(state.remoteTypingByThread[threadId] || {}).filter((userId) => userId !== state.auth?.userId);
}

function typingIndicatorText(threadId) {
  const thread = getThreadById(threadId);
  const users = remoteTypingUsers(threadId);
  if (!thread || users.length === 0) return "";
  if (users.length === 1) {
    if (thread.kind === "dm" || thread.kind === "phone") return `${thread.title} is typing...`;
    return "Someone is typing...";
  }
  return `${users.length} people are typing...`;
}

function sendTypingEvent(eventName, conversationId) {
  if (!conversationId || !realtimeSocket || realtimeSocket.readyState !== window.WebSocket.OPEN) return;
  realtimeSocket.send(JSON.stringify({
    event: eventName,
    data: { conversation_id: conversationId },
  }));
}

function stopLocalTyping() {
  if (typingStopTimer) {
    window.clearTimeout(typingStopTimer);
    typingStopTimer = 0;
  }
  if (localTypingSent && localTypingThreadId) {
    sendTypingEvent("typing.stopped", localTypingThreadId);
  }
  localTypingSent = false;
  localTypingThreadId = "";
}

function syncLocalTypingSignal() {
  const thread = getActiveThread();
  const conversationId = thread?.id || "";
  const text = sanitizeText(el.composerInput?.value || "", 1000);
  const canSignal = Boolean(thread && !thread.blocked && thread.kind !== "draft_phone" && text);
  if (!canSignal) {
    stopLocalTyping();
    return;
  }
  if (localTypingSent && localTypingThreadId && localTypingThreadId !== conversationId) {
    sendTypingEvent("typing.stopped", localTypingThreadId);
    localTypingSent = false;
  }
  if (!localTypingSent || localTypingThreadId !== conversationId) {
    sendTypingEvent("typing.started", conversationId);
    localTypingSent = true;
    localTypingThreadId = conversationId;
  }
  if (typingStopTimer) window.clearTimeout(typingStopTimer);
  typingStopTimer = window.setTimeout(() => {
    stopLocalTyping();
  }, 3000);
}

async function loadConversationsFromApi() {
  const payload = await apiRequest("/v2/conversations", { method: "GET" });
  const items = Array.isArray(payload?.items) ? payload.items : [];
  await resolveProfilesForUsers(items.flatMap((item) => Array.isArray(item.participants) ? item.participants : []));
  for (const item of items) {
    const mapped = mapConversation(item);
    const existing = getThreadById(mapped.id);
    if (existing) {
      upsertThread({
        ...mapped,
        messages: existing.messages,
        loadedMessages: existing.loadedMessages,
      });
    } else {
      upsertThread(mapped);
    }
  }
  const visible = visibleThreads();
  if (state.activeThreadId && !visible.some((thread) => thread.id === state.activeThreadId)) {
    state.activeThreadId = visible.length ? visible[0].id : null;
  }
  if (!state.activeThreadId && visible.length > 0) state.activeThreadId = visible[0].id;
  saveConversationStore();
}

async function loadSelfProfile() {
  try {
    state.selfProfile = await apiRequest("/v1/me", { method: "GET" });
    if (state.selfProfile?.user_id) {
      state.profiles[sanitizeText(state.selfProfile.user_id, 80)] = cloneJson(state.selfProfile);
    }
  } catch (error) {
    console.error(error);
  }
}

async function resolveProfilesForUsers(userIds) {
  const unique = [...new Set((userIds || []).map((item) => sanitizeText(item, 80)).filter(Boolean))]
    .filter((userId) => !state.profiles[userId]);
  if (!unique.length) return;
  try {
    const payload = await apiRequest("/v1/users/resolve", {
      method: "POST",
      body: JSON.stringify({ user_ids: unique }),
    });
    for (const item of payload?.items || []) {
      const userId = sanitizeText(item?.user_id, 80);
      if (!userId) continue;
      state.profiles[userId] = cloneJson(item);
    }
  } catch (error) {
    console.error(error);
  }
}

async function loadMessagesForThread(threadId) {
  const thread = getThreadById(threadId);
  if (!thread || thread.kind === "draft_phone") return;
  const payload = await apiRequest(`/v1/conversations/${encodeURIComponent(threadId)}/messages`, { method: "GET" });
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const existingMessages = new Map((thread.messages || []).map((message) => [message.id, message]));
  const messages = await Promise.all(items.map((item) => materializeMessage(
    item,
    existingMessages.get(sanitizeText(item?.message_id, 80)) || null
  )));
  upsertThread({
    ...thread,
    messages,
    loadedMessages: true,
    updatedAt: messages.length ? messages[messages.length - 1].createdAt : thread.updatedAt,
    unreadCount: 0,
    previewText: messages.length ? messages[messages.length - 1].text : thread.previewText,
  });
  if (state.activeThreadId === threadId) {
    renderAll();
  }
  const refreshedThread = getThreadById(threadId) || thread;
  await markConversationDelivered(refreshedThread);
  if (isForegroundThread(threadId)) {
    await markConversationRead(refreshedThread);
  }
  saveConversationStore();
}

async function refreshLiveState() {
  await syncFromCursor();
}

async function syncThreadReceipts(threadId) {
  const thread = getThreadById(threadId);
  if (!thread || thread.kind === "draft_phone") return;
  await markConversationDelivered(thread);
  if (isForegroundThread(threadId)) {
    await markConversationRead(thread);
  }
  saveConversationStore();
}

function ensureThreadFromEvent(payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  if (!conversationId) return null;
  const existing = getThreadById(conversationId);
  if (existing) {
    if (
      payload?.nickname !== undefined
      || payload?.closed !== undefined
      || payload?.blocked !== undefined
      || payload?.blocked_by_viewer !== undefined
      || payload?.blocked_by_other !== undefined
    ) {
      return applyConversationStateUpdate(payload);
    }
    return existing;
  }
  const mapped = mapConversation({
    conversation_id: conversationId,
    type: sanitizeText(payload?.conversation_type, 24) || "DM",
    title: sanitizeText(payload?.title, 80),
    encryption_state: sanitizeText(payload?.encryption_state, 40),
    encryption_epoch: Number(payload?.encryption_epoch || 0),
    e2ee_ready: Boolean(payload?.e2ee_ready),
    e2ee_blocked_member_ids: Array.isArray(payload?.e2ee_blocked_member_ids) ? payload.e2ee_blocked_member_ids : [],
    updated_at: payload?.message?.created_at || payload?.status_updated_at || nowISO(),
    participants: Array.isArray(payload?.participants) ? payload.participants : [],
    external_phones: Array.isArray(payload?.external_phones) ? payload.external_phones : [],
    last_message_preview: sanitizeText(payload?.preview, 180),
    nickname: sanitizeText(payload?.nickname, 80),
    closed: Boolean(payload?.closed),
    blocked: Boolean(payload?.blocked),
    blocked_by_viewer: Boolean(payload?.blocked_by_viewer),
    blocked_by_other: Boolean(payload?.blocked_by_other),
    unread_count: 0,
  });
  upsertThread(mapped);
  return getThreadById(conversationId);
}

function applyConversationStateUpdate(payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  if (!conversationId) return null;
  const thread = getThreadById(conversationId);
  if (!thread) return null;
  const blockedByViewer = payload?.blocked_by_viewer !== undefined
    ? Boolean(payload.blocked_by_viewer)
    : Boolean(thread.blockedByViewer);
  const blockedByOther = payload?.blocked_by_other !== undefined
    ? Boolean(payload.blocked_by_other)
    : Boolean(thread.blockedByOther);
  const next = {
    ...thread,
    serverTitle: payload?.title !== undefined ? sanitizeText(payload.title, 80) : thread.serverTitle,
    nickname: payload?.nickname !== undefined ? sanitizeText(payload.nickname, 80) : thread.nickname,
    closed: payload?.closed !== undefined ? Boolean(payload.closed) : thread.closed,
    encryptionState: payload?.encryption_state !== undefined ? sanitizeText(payload.encryption_state, 40) : thread.encryptionState,
    encryptionEpoch: payload?.encryption_epoch !== undefined ? Number(payload.encryption_epoch || 0) : thread.encryptionEpoch,
    e2eeReady: payload?.e2ee_ready !== undefined ? Boolean(payload.e2ee_ready) : Boolean(thread.e2eeReady),
    e2eeBlockedMemberIds: Array.isArray(payload?.e2ee_blocked_member_ids)
      ? payload.e2ee_blocked_member_ids.map((id) => sanitizeText(id, 80)).filter(Boolean)
      : (Array.isArray(thread.e2eeBlockedMemberIds) ? thread.e2eeBlockedMemberIds : []),
    participants: Array.isArray(payload?.participants)
      ? payload.participants.map((id) => sanitizeText(id, 80)).filter(Boolean)
      : (Array.isArray(thread.participants) ? thread.participants : []),
    externalPhones: Array.isArray(payload?.external_phones)
      ? payload.external_phones.map((phone) => sanitizeText(phone, 32)).filter(Boolean)
      : (Array.isArray(thread.externalPhones) ? thread.externalPhones : []),
    blockedByViewer,
    blockedByOther,
    blocked: payload?.blocked !== undefined
      ? Boolean(payload.blocked)
      : (blockedByViewer || blockedByOther),
    updatedAt: payload?.updated_at || thread.updatedAt,
  };
  next.title = pickTitle(next);
  next.subtitle = pickSubtitle(next);
  upsertThread(next);
  const visible = visibleThreads();
  if (next.closed && state.activeThreadId === next.id) {
    state.activeThreadId = visible.length ? visible[0].id : null;
  } else if (!next.closed && !state.activeThreadId) {
    state.activeThreadId = next.id;
  }
  saveConversationStore();
  return getThreadById(conversationId);
}

function applyReceiptCheckpoint(kind, conversationId, throughServerOrder, updatedAt) {
  const thread = getThreadById(conversationId);
  if (!thread) return;
  const checkpointAt = updatedAt || nowISO();
  const nextMessages = (thread.messages || []).map((message) => {
    if (message.direction !== "out" || Number(message.serverOrder || 0) > throughServerOrder) return message;
    if (kind === "READ") {
      return {
        ...message,
        status: OHMF_DELIVERY_STATUSES.READ,
        readAt: checkpointAt,
        statusUpdatedAt: checkpointAt,
      };
    }
    if (normalizeDeliveryStatus(message.transport, message.status) === OHMF_DELIVERY_STATUSES.READ) return message;
    return {
      ...message,
      status: OHMF_DELIVERY_STATUSES.DELIVERED,
      deliveredAt: checkpointAt,
      statusUpdatedAt: checkpointAt,
    };
  });
  const nextThread = {
    ...thread,
    messages: nextMessages,
    updatedAt: checkpointAt || thread.updatedAt,
    deliveredThroughServerOrder: Math.max(Number(thread.deliveredThroughServerOrder || 0), throughServerOrder),
    deliveredStatusUpdatedAt: checkpointAt,
  };
  if (kind === "READ") {
    nextThread.readThroughServerOrder = Math.max(Number(thread.readThroughServerOrder || 0), throughServerOrder);
    nextThread.readStatusUpdatedAt = checkpointAt;
  }
  upsertThread(nextThread);
  saveConversationStore();
  renderAll();
}

function applyStoredReceiptCheckpoint(thread, message) {
  if (!thread || !message || message.direction !== "out") return message;
  const serverOrder = Number(message.serverOrder || 0);
  if (!serverOrder) return message;
  const readThrough = Number(thread.readThroughServerOrder || 0);
  const deliveredThrough = Number(thread.deliveredThroughServerOrder || 0);
  if (readThrough && serverOrder <= readThrough) {
    const readAt = thread.readStatusUpdatedAt || message.readAt || message.statusUpdatedAt || nowISO();
    return {
      ...message,
      status: OHMF_DELIVERY_STATUSES.READ,
      readAt,
      statusUpdatedAt: readAt,
    };
  }
  if (deliveredThrough && serverOrder <= deliveredThrough) {
    if (normalizeDeliveryStatus(message.transport, message.status) === OHMF_DELIVERY_STATUSES.READ) return message;
    const deliveredAt = thread.deliveredStatusUpdatedAt || message.deliveredAt || message.statusUpdatedAt || nowISO();
    return {
      ...message,
      status: OHMF_DELIVERY_STATUSES.DELIVERED,
      deliveredAt,
      statusUpdatedAt: deliveredAt,
    };
  }
  return message;
}

function applyMessageDeleted(payload) {
  const conversationId = sanitizeText(payload?.conversation_id || payload?.message?.conversation_id, 80);
  if (!conversationId) return;
  const thread = ensureThreadFromEvent(payload) || getThreadById(conversationId);
  if (!thread) return;

  const messagePayload = payload?.message && typeof payload.message === "object" ? payload.message : payload;
  const messageId = sanitizeText(messagePayload?.message_id, 80);
  if (!messageId) return;

  const deletedAt = messagePayload?.deleted_at || payload?.deleted_at || nowISO();
  let found = false;
  const nextMessages = (thread.messages || []).map((message) => {
    if (message.id !== messageId) return message;
    found = true;
    return {
      ...message,
      text: "Message deleted",
      content: {},
      deleted: true,
      deletedAt,
      statusUpdatedAt: deletedAt || message.statusUpdatedAt,
    };
  });

  const lastMessage = nextMessages[nextMessages.length - 1];
  const previewText = sanitizeText(payload?.preview, 180)
    || (lastMessage ? (lastMessage.deleted ? "Message deleted" : lastMessage.text) : thread.previewText);
  upsertThread({
    ...thread,
    messages: nextMessages,
    previewText,
    updatedAt: deletedAt || thread.updatedAt,
  });
  saveConversationStore();
  refreshOpenMessageMetadata(conversationId, messageId);

  if (!found && thread.loadedMessages && state.activeThreadId === conversationId) {
    void loadMessagesForThread(conversationId).catch((error) => {
      console.error(error);
    });
  }
}

async function applyMessageEdited(payload) {
  const conversationId = sanitizeText(payload?.conversation_id || payload?.message?.conversation_id, 80);
  if (!conversationId) return;
  const thread = ensureThreadFromEvent(payload) || getThreadById(conversationId);
  if (!thread) return;

  const messagePayload = payload?.message && typeof payload.message === "object" ? payload.message : payload;
  const messageId = sanitizeText(messagePayload?.message_id, 80);
  if (!messageId) return;

  const editedAt = messagePayload?.edited_at || payload?.edited_at || nowISO();
  const existingMessage = thread.messages?.find((message) => message.id === messageId) || null;
  const updatedMessage = await materializeMessage(messagePayload, existingMessage);
  updatedMessage.editedAt = editedAt;

  let found = false;
  const nextMessages = (thread.messages || []).map((message) => {
    if (message.id !== messageId) return message;
    found = true;
    return {
      ...message,
      text: updatedMessage.text,
      content: updatedMessage.content,
      editedAt,
      statusUpdatedAt: editedAt,
    };
  });

  const lastMessage = nextMessages[nextMessages.length - 1];
  const previewText = sanitizeText(payload?.preview, 180)
    || (lastMessage ? (lastMessage.deleted ? "Message deleted" : lastMessage.text) : thread.previewText);
  upsertThread({
    ...thread,
    messages: nextMessages,
    previewText,
    updatedAt: editedAt || thread.updatedAt,
  });
  saveConversationStore();
  refreshOpenMessageMetadata(conversationId, messageId);

  if (!found && thread.loadedMessages && state.activeThreadId === conversationId) {
    void loadMessagesForThread(conversationId).catch((error) => {
      console.error(error);
    });
  }
}

async function applyUserEvent(event) {
  const eventType = sanitizeText(event?.type, 80);
  const payload = event?.payload && typeof event.payload === "object" ? event.payload : {};
  if (eventType === "conversation_message_appended") {
    const thread = ensureThreadFromEvent(payload);
    if (!thread) return;
    const messagePayload = payload?.message || {};
    const existingMessage = thread.messages?.find((message) => message.id === sanitizeText(messagePayload?.message_id, 80)) || null;
    const nextMessage = await materializeMessage(messagePayload, existingMessage);
    upsertThreadMessage(thread.id, nextMessage);
    if (!state.activeThreadId) {
      state.activeThreadId = thread.id;
    }
    const refreshed = getThreadById(thread.id) || thread;
    if (isForegroundThread(thread.id) && nextMessage.direction === "in") {
      void markConversationDelivered(refreshed);
      void markConversationRead(refreshed);
      upsertThread({ ...refreshed, unreadCount: 0, previewText: nextMessage.text });
    } else if (nextMessage.direction === "in") {
      queueConversationDelivered(thread.id, nextMessage.serverOrder);
      upsertThread({ ...refreshed, unreadCount: Number(refreshed.unreadCount || 0) + 1, previewText: nextMessage.text });
    }
    return;
  }
  if (eventType === "conversation_message_edited") {
    await applyMessageEdited(payload);
    return;
  }
  if (eventType === "conversation_message_deleted") {
    applyMessageDeleted(payload);
    return;
  }
  if (eventType === "conversation_message_reactions_updated") {
    applyMessageReactions(payload);
    return;
  }
  if (eventType === "conversation_receipt_updated") {
    const conversationId = sanitizeText(payload?.conversation_id, 80);
    const through = Number(payload?.through_server_order || 0);
    const kind = sanitizeText(payload?.receipt_kind, 24).toUpperCase();
    if (!conversationId || !through || !kind) return;
    applyReceiptCheckpoint(kind, conversationId, through, payload?.status_updated_at || nowISO());
    return;
  }
  if (eventType === "conversation_typing_updated") {
    const conversationId = sanitizeText(payload?.conversation_id, 80);
    const userId = sanitizeText(payload?.user_id, 80);
    if (!conversationId || !userId) return;
    setRemoteTyping(conversationId, userId, sanitizeText(payload?.state, 40) === "typing_started");
    if (state.activeThreadId === conversationId) {
      renderMessages();
    }
    return;
  }
  if (eventType === "conversation_preview_updated") {
    ensureThreadFromEvent(payload);
    return;
  }
  if (eventType === "conversation_state_updated") {
    applyConversationStateUpdate(payload);
  }
}

function sendRealtimeAck(cursor = state.sync.lastUserCursor) {
  if (!realtimeSocket || realtimeSocket.readyState !== window.WebSocket.OPEN || !cursor) return;
  realtimeSocket.send(JSON.stringify({
    event: "ack",
    data: {
      through_user_event_id: Number(cursor),
      device_id: state.sync.deviceId,
    },
  }));
}

async function syncFromCursor() {
  if (!state.auth || liveSyncInFlight) return;
  liveSyncInFlight = true;
  try {
    let cursor = Number(state.sync.lastUserCursor || 0);
    while (true) {
      const payload = await apiRequest(`/v2/sync?cursor=${encodeURIComponent(String(cursor))}&limit=200`, { method: "GET" });
      const events = Array.isArray(payload?.events) ? payload.events : [];
      for (const event of events) {
        await applyUserEvent(event);
        const nextCursor = Number(event?.user_event_id || cursor);
        if (nextCursor > state.sync.lastUserCursor) {
          state.sync.lastUserCursor = nextCursor;
        }
      }
      saveConversationStore();
      saveSyncCursor();
      const nextCursor = Number(payload?.next_cursor || cursor);
      if (nextCursor > state.sync.lastUserCursor) {
        state.sync.lastUserCursor = nextCursor;
        saveSyncCursor();
      }
      cursor = Number(state.sync.lastUserCursor || cursor);
      if (!payload?.has_more || events.length === 0) break;
    }
    sendRealtimeAck();
    renderAll();
  } catch (error) {
    console.error(error);
  } finally {
    liveSyncInFlight = false;
  }
}

function stopEventStream() {
  if (eventStreamReconnectTimer) {
    window.clearTimeout(eventStreamReconnectTimer);
    eventStreamReconnectTimer = 0;
  }
  if (eventStreamAbort) {
    eventStreamAbort.abort();
    eventStreamAbort = null;
  }
}

function scheduleEventStreamReconnect(delayMs = 1500) {
  if (!state.auth || eventStreamReconnectTimer || eventStreamDisabled) return;
  eventStreamReconnectTimer = window.setTimeout(() => {
    eventStreamReconnectTimer = 0;
    void startEventStream();
  }, delayMs);
}

function handleSSEEvent(name, rawData) {
  if (name === "message_created" || name === "delivery_update") {
    try {
      handleRealtimeEvent(name, rawData ? JSON.parse(rawData) : null);
    } catch (error) {
      console.error(error);
    }
    return;
  }
  if (name === "sync_required") {
    void refreshLiveState();
  }
}

function stopLiveRefreshLoop() {
  if (liveRefreshTimer) {
    window.clearTimeout(liveRefreshTimer);
    liveRefreshTimer = 0;
  }
}

function scheduleLiveRefreshLoop(delayMs = LIVE_SYNC_INTERVAL_MS) {
  if (!state.auth || liveRefreshTimer) return;
  liveRefreshTimer = window.setTimeout(async () => {
    liveRefreshTimer = 0;
    if (!state.auth) return;
    if (!document.hidden) {
      await refreshLiveState();
    }
    if (state.auth && !realtimeSocket) {
      startRealtimeSocket();
    }
    scheduleLiveRefreshLoop();
  }, delayMs);
}

async function startEventStream() {
  if (!state.auth || eventStreamAbort || eventStreamDisabled) return;
  const controller = new AbortController();
  eventStreamAbort = controller;
  try {
    const response = await fetch(`${API_BASE_URL}/v1/events/stream`, {
      method: "GET",
      headers: {
        Accept: "text/event-stream",
        Authorization: `Bearer ${state.auth.accessToken}`,
      },
      signal: controller.signal,
      credentials: "omit",
      cache: "no-store",
    });
    if (response.status === 401 && state.auth?.refreshToken) {
      const refreshed = await refreshAuthTokens();
      if (refreshed) {
        eventStreamAbort = null;
        scheduleEventStreamReconnect(200);
        return;
      }
    }
    if (response.status >= 500) {
      throw new Error(`stream_http_${response.status}`);
    }
    if (!response.ok || !response.body) {
      throw new Error(`stream_http_${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let eventName = "";
    let dataLines = [];

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line) {
          const payload = dataLines.join("\n");
          handleSSEEvent(eventName || "message", payload);
          eventName = "";
          dataLines = [];
          continue;
        }
        if (line.startsWith(":")) continue;
        if (line.startsWith("event:")) {
          eventName = line.slice(6).trim();
          continue;
        }
        if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trim());
        }
      }
    }
    if (!controller.signal.aborted) {
      scheduleEventStreamReconnect();
    }
  } catch (error) {
    if (!controller.signal.aborted) {
      console.error(error);
      if (!eventStreamDisabled) {
        scheduleEventStreamReconnect();
      }
    }
  } finally {
    if (eventStreamAbort === controller) {
      eventStreamAbort = null;
    }
  }
}

async function ensureMessagesLoaded(threadId) {
  const thread = getThreadById(threadId);
  if (!thread || thread.kind === "draft_phone" || thread.loadedMessages) return;
  await loadMessagesForThread(threadId);
}

function stopRealtimeSocket() {
  if (realtimeReconnectTimer) {
    window.clearTimeout(realtimeReconnectTimer);
    realtimeReconnectTimer = 0;
  }
  if (realtimeSocket) {
    realtimeSocket.close();
    realtimeSocket = null;
  }
  realtimeConnectFailures = 0;
}

function scheduleRealtimeReconnect(delayMs = 0) {
  if (!state.auth || realtimeReconnectTimer || realtimeSocket) return;
  const nextDelay = delayMs > 0 ? delayMs : Math.min(10000, 1200 * (2 ** Math.min(realtimeConnectFailures, 3)));
  realtimeReconnectTimer = window.setTimeout(() => {
    realtimeReconnectTimer = 0;
    startRealtimeSocket();
  }, nextDelay);
}

function applyDeliveryUpdate(payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  const status = normalizeDeliveryStatus(TRANSPORT_OHMF, payload?.status || "");
  if (!conversationId || !status) return;
  const through = Number(payload?.through_server_order || 0);
  if (through > 0 && (status === OHMF_DELIVERY_STATUSES.READ || status === OHMF_DELIVERY_STATUSES.DELIVERED)) {
    applyReceiptCheckpoint(status, conversationId, through, payload?.status_updated_at || nowISO());
    return;
  }

  const messageId = sanitizeText(payload?.message_id, 80);
  if (!messageId) return;
  patchMessage(conversationId, messageId, {
    status,
    transport: TRANSPORT_OHMF,
    deliveredAt: payload?.status_updated_at || nowISO(),
    statusUpdatedAt: payload?.status_updated_at || nowISO(),
  });
  renderAll();
}

function applyTypingEvent(eventName, payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  const userId = sanitizeText(payload?.user_id, 80);
  if (!conversationId || !userId) return;
  setRemoteTyping(conversationId, userId, eventName === "typing.started");
  if (state.activeThreadId === conversationId) {
    renderMessages();
  }
}

function handleRealtimeEvent(eventName, payload) {
  if (eventName === "message_created") {
    void applyIncomingMessage(payload);
    return;
  }
  if (eventName === "delivery_update" || eventName === "read_receipt") {
    applyDeliveryUpdate(payload);
    return;
  }
  if (eventName === "typing.started" || eventName === "typing.stopped") {
    applyTypingEvent(eventName, payload);
  }
}

async function applyIncomingMessage(payload) {
  const conversationId = sanitizeText(payload?.conversation_id, 80);
  if (!conversationId) return;
  const existingThread = getThreadById(conversationId);
  const existingMessage = existingThread?.messages?.find((message) => message.id === sanitizeText(payload?.message_id, 80)) || null;
  const nextMessage = await materializeMessage(payload, existingMessage);
  if (nextMessage.direction === "in") {
    setRemoteTyping(conversationId, sanitizeText(payload?.sender_user_id, 80), false);
  }
  if (existingThread) {
    upsertThreadMessage(conversationId, nextMessage);
    if (nextMessage.direction === "in") {
      queueConversationDelivered(conversationId, nextMessage.serverOrder);
    }
    if (isForegroundThread(conversationId) && nextMessage.direction === "in") {
      void markConversationRead(getThreadById(conversationId) || existingThread);
    }
    if (state.activeThreadId === conversationId) {
      renderAll();
      void loadMessagesForThread(conversationId).catch((error) => {
        console.error(error);
      });
    } else {
      renderAll();
    }
    return;
  }
  void (async () => {
    await loadConversationsFromApi();
    await loadMessagesForThread(conversationId);
    renderAll();
  })();
}

async function startRealtimeSocket() {
  if (!state.auth || realtimeSocket) return;
  const forceRefresh = realtimeConnectFailures > 0;
  const wasExpiring = accessTokenExpiresSoon();
  const refreshed = forceRefresh ? await forceRefreshAccessToken() : await ensureFreshAccessToken();
  if (wasExpiring && !refreshed) return;
  const wsURL = new URL(API_BASE_URL.replace(/^http/i, "ws") + "/v2/ws");
  wsURL.searchParams.set("access_token", state.auth.accessToken);
  const socket = new window.WebSocket(wsURL.toString());
  let opened = false;
  realtimeSocket = socket;

  socket.addEventListener("open", () => {
    opened = true;
    realtimeConnectFailures = 0;
    socket.send(JSON.stringify({
      event: "hello",
      data: {
        device_id: state.sync.deviceId,
        last_user_cursor: Number(state.sync.lastUserCursor || 0),
      },
    }));
  });

  socket.addEventListener("message", async (event) => {
    try {
      const message = JSON.parse(event.data);
      const eventName = sanitizeText(message?.event, 80);
      if (eventName === "event") {
        await applyUserEvent(message?.data);
        const nextCursor = Number(message?.data?.user_event_id || 0);
        if (nextCursor > state.sync.lastUserCursor) {
          state.sync.lastUserCursor = nextCursor;
          saveSyncCursor();
          saveConversationStore();
          sendRealtimeAck(nextCursor);
        }
        renderAll();
        return;
      }
      if (eventName === "hello_ack") {
        sendRealtimeAck();
        return;
      }
      if (eventName === "resync_required") {
        void syncFromCursor();
        return;
      }
      handleRealtimeEvent(eventName, message?.data);
    } catch (error) {
      console.error(error);
    }
  });

  socket.addEventListener("close", async () => {
    if (realtimeSocket === socket) {
      realtimeSocket = null;
      state.remoteTypingByThread = {};
      stopLocalTyping();
      if (!opened) {
        realtimeConnectFailures += 1;
        await forceRefreshAccessToken();
      } else if (accessTokenExpiresSoon(10)) {
        await forceRefreshAccessToken();
      }
      scheduleRealtimeReconnect();
    }
  });

  socket.addEventListener("error", () => {
    socket.close();
  });
}

async function markConversationRead(thread) {
  if (!isForegroundThread(thread?.id)) return;
  const lastIncoming = [...(thread.messages || [])].reverse().find((msg) => msg.direction === "in" && msg.serverOrder > 0);
  if (!lastIncoming) return;
  try {
    await apiRequest(`/v2/conversations/${encodeURIComponent(thread.id)}/read`, {
      method: "POST",
      body: JSON.stringify({ through_server_order: lastIncoming.serverOrder }),
    });
    upsertThread({
      ...thread,
      unreadCount: 0,
      readThroughServerOrder: Math.max(Number(thread.readThroughServerOrder || 0), Number(lastIncoming.serverOrder || 0)),
      readStatusUpdatedAt: nowISO(),
    });
    saveConversationStore();
  } catch {} // removed: descriptive catch comment
}

async function markConversationDelivered(thread) {
  const lastIncoming = [...(thread.messages || [])].reverse().find((msg) => msg.direction === "in" && msg.serverOrder > 0);
  if (!lastIncoming) return;
  queueConversationDelivered(thread.id, lastIncoming.serverOrder);
  await flushConversationDelivered(thread.id);
}

function buildThreadItem(thread) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `thread-item${thread.id === state.activeThreadId ? " active" : ""}`;

  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.textContent = initials(thread.title);

  const body = document.createElement("div");
  const meta = document.createElement("div");
  meta.className = "thread-meta";

  const name = document.createElement("p");
  name.className = "thread-name";
  name.textContent = thread.title;

  const time = document.createElement("p");
  time.className = "thread-time";
  time.textContent = formatShortTime(thread.updatedAt);

  const preview = document.createElement("p");
  preview.className = "thread-preview";
  const last = thread.messages?.[thread.messages.length - 1];
  preview.textContent = last ? (last.deleted ? "Message deleted" : last.text) : (thread.previewText || "No messages yet");

  meta.append(name, time);
  body.append(meta, preview);
  button.append(avatar, body);
  button.addEventListener("click", async () => {
    if (state.activeThreadId && state.activeThreadId !== thread.id) {
      stopLocalTyping();
    }
    clearMessageMenuWithoutRender();
    state.activeThreadId = thread.id;
    renderAll();
    openMobileThread();
    try {
      await ensureMessagesLoaded(thread.id);
      await syncThreadReceipts(thread.id);
      renderAll();
    } catch (error) {
      console.error(error);
    }
  });
  return button;
}

function renderThreadList() {
  el.threadList.replaceChildren();
  for (const thread of visibleThreads()) {
    const li = document.createElement("li");
    li.appendChild(buildThreadItem(thread));
    el.threadList.appendChild(li);
  }
}

function isNonSMSMessage(message) {
  return normalizeTransport(message.transport) !== TRANSPORT_SMS;
}

function addReaction(threadId, messageId) {
  const emoji = sanitizeText(window.prompt("Reaction emoji", "👍"), 4);
  if (!emoji) return;
  void apiRequest(`/v1/messages/${encodeURIComponent(messageId)}/reactions`, {
    method: "POST",
    body: JSON.stringify({ emoji }),
  }).then((payload) => {
    applyMessageReactions({
      conversation_id: threadId,
      message_id: messageId,
      reactions: payload?.reactions,
      acted_at: nowISO(),
    });
    renderAll();
  }).catch((error) => {
    console.error(error);
  });
}

async function editMessage(threadId, messageId) {
  const thread = getThreadById(threadId);
  const msg = thread?.messages?.find((m) => m.id === messageId);
  if (!thread || !msg || msg.deleted) return;
  const nextText = sanitizeText(window.prompt("Edit message", msg.text), 1000);
  if (!nextText) return;
  const nextContent = { text: nextText };
  const replyReference = normalizeReplyReference(msg.content?.reply_to);
  if (replyReference) {
    nextContent.reply_to = {
      message_id: replyReference.messageId,
      sender_user_id: replyReference.senderUserId,
      content_type: replyReference.contentType,
      text: replyReference.text,
    };
  }
  let payloadContent = nextContent;
  let payloadContentType = CONTENT_TYPE_TEXT;
  if (msg.isEncrypted) {
    const encryptedEnvelope = await encryptConversationContent(thread, nextContent, CONTENT_TYPE_TEXT);
    if (!encryptedEnvelope) {
      throw new Error("Unable to encrypt edited message");
    }
    payloadContent = encryptedEnvelope;
    payloadContentType = CONTENT_TYPE_ENCRYPTED;
  }
  await apiRequest(`/v1/messages/${encodeURIComponent(messageId)}`, {
    method: "PATCH",
    body: JSON.stringify({ content: payloadContent }),
  });
  void applyMessageEdited({
    conversation_id: threadId,
    preview: thread.messages?.[thread.messages.length - 1]?.id === messageId ? nextText : thread.previewText,
    message: {
      message_id: messageId,
      conversation_id: threadId,
      sender_user_id: state.auth?.userId || "",
      sender_device_id: state.auth?.deviceId || msg.senderDeviceId || "",
      content_type: payloadContentType,
      content: payloadContent,
      transport: msg.transport,
      server_order: msg.serverOrder,
      created_at: msg.createdAt,
      sent_at: msg.sentAt,
      edited_at: nowISO(),
    },
  });
  renderAll();
}

async function deleteMessage(threadId, messageId) {
  const thread = getThreadById(threadId);
  if (!thread) return;
  const target = thread.messages?.find((message) => message.id === messageId);
  if (!target || target.deleted) return;
  await apiRequest(`/v1/messages/${encodeURIComponent(messageId)}`, { method: "DELETE" });
  applyMessageDeleted({
    conversation_id: threadId,
    preview: thread.messages?.[thread.messages.length - 1]?.id === messageId ? "Message deleted" : thread.previewText,
    message: {
      message_id: messageId,
      conversation_id: threadId,
      deleted: true,
      deleted_at: nowISO(),
    },
  });
  renderAll();
}

function buildReactionBadge(message) {
  const reactionKeys = Object.keys(message.reactions || {});
  if (!reactionKeys.length) return null;
  const badge = document.createElement("div");
  badge.className = `message-reactions ${message.direction}`;
  for (const emoji of reactionKeys) {
    const chip = document.createElement("span");
    chip.className = "message-reaction-chip";
    chip.textContent = `${emoji} ${message.reactions[emoji]}`;
    badge.appendChild(chip);
  }
  return badge;
}

function appendMessageMenuButton(menu, label, onClick, extraClass = "") {
  const button = document.createElement("button");
  button.type = "button";
  button.className = extraClass ? `message-action-item ${extraClass}` : "message-action-item";
  button.textContent = label;
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    clearMessageMenuWithoutRender();
    onClick();
  });
  menu.appendChild(button);
}

function buildMessageActionAnchor(thread, message) {
  const allowReply = !message.deleted;
  const allowReact = !message.deleted;
  const allowEdit = message.direction === "out" && !message.deleted && !isAppCardMessage(message);
  const allowDelete = message.direction === "out" && !message.deleted && !isAppCardMessage(message);
  const allowDetails = true;
  if (!allowReply && !allowReact && !allowEdit && !allowDelete && !allowDetails) return null;

  const anchor = document.createElement("div");
  const menuOpen = Boolean(
    state.openMessageMenu &&
    state.openMessageMenu.threadId === thread.id &&
    state.openMessageMenu.messageId === message.id
  );
  anchor.className = `message-action-anchor ${message.direction}${menuOpen ? " menu-open" : ""}`;

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = `message-action-trigger ${message.direction}`;
  trigger.setAttribute("aria-label", "Message actions");
  trigger.setAttribute("aria-haspopup", "menu");
  trigger.setAttribute("aria-expanded", menuOpen ? "true" : "false");
  for (let i = 0; i < 3; i += 1) {
    const dot = document.createElement("span");
    dot.className = "message-action-trigger-dot";
    trigger.appendChild(dot);
  }
  trigger.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleMessageMenu(thread.id, message.id);
  });
  anchor.appendChild(trigger);

  if (!menuOpen) return anchor;

  const menu = document.createElement("div");
  menu.className = `message-action-menu ${message.direction}`;
  menu.setAttribute("role", "menu");

  if (allowReply) {
    appendMessageMenuButton(menu, "Reply", () => setReplyTarget(thread, message));
  }
  if (allowReact) {
    appendMessageMenuButton(menu, "React", () => addReaction(thread.id, message.id));
  }
  if (allowDetails) {
    appendMessageMenuButton(menu, "Details", () => {
      void openMessageMetadata(thread.id, message.id);
    });
  }

  if (allowEdit) {
    appendMessageMenuButton(menu, "Edit", () => {
      void editMessage(thread.id, message.id).catch((error) => {
        console.error(error);
        window.alert(sanitizeText(error.message || "Unable to edit message.", 160));
      });
    });
  }
  if (allowDelete) {
    appendMessageMenuButton(menu, "Delete", () => {
      void deleteMessage(thread.id, message.id).catch((error) => {
        console.error(error);
        window.alert(sanitizeText(error.message || "Unable to delete message.", 160));
      });
    }, "destructive");
  }

  anchor.appendChild(menu);
  return anchor;
}

function scrollMessageListToBottom() {
  el.messageList.scrollTop = el.messageList.scrollHeight;
}

function closeMiniappLauncher() {
  state.miniapp.drawerOpen = false;
  state.miniapp.lastShareError = "";
}

async function closeMiniappWindow() {
  if (state.miniapp.launchContext?.app_session_id) {
    try {
      const currentVersion = Number(state.miniapp.sessionState?.stateVersion || state.miniapp.launchContext?.state_version || 1);
      const persistedVersion = await persistMiniappSession(currentVersion, "WINDOW_CLOSED", { source: "popup_close" });
      if (state.miniapp.sessionState) {
        state.miniapp.sessionState.stateVersion = persistedVersion;
      }
      if (state.miniapp.launchContext) {
        state.miniapp.launchContext.state_version = persistedVersion;
      }
    } catch (error) {
      console.error(error);
      state.miniapp.lastShareError = sanitizeText(error.message || "Unable to save app state.", 180);
    }
  }
  clearMiniappLoadTimeout();
  state.miniapp.popupOpen = false;
  state.miniapp.frameWindow = null;
  el.miniappFrame.src = "about:blank";
}

function renderMiniappWindow() {
  const thread = getActiveThread();
  el.miniappWindow.classList.toggle("hidden", !state.miniapp.popupOpen);
  el.miniappWindowTitle.textContent = state.miniapp.manifest?.name || "Mini-App";
  el.miniappWindowSubtitle.textContent = state.miniapp.loading
    ? "Loading mini-app runtime…"
    : (thread
      ? `Running in ${thread.title}. Close the window to save the latest state.`
      : "Close the window to save the latest state.");
}

function renderMiniappCatalog() {
  el.miniappCatalogCards.replaceChildren();
  for (const entry of state.miniapp.catalog) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "miniapp-card";
    button.dataset.appId = entry.appId;
    if (state.miniapp.selectedAppId === entry.appId) {
      button.classList.add("active");
    }

    const icon = document.createElement("span");
    icon.className = "miniapp-card-icon";
    icon.textContent = sanitizeText(entry.title.slice(0, 1), 1).toUpperCase() || "?";

    const copy = document.createElement("span");
    copy.className = "miniapp-card-copy";

    const title = document.createElement("strong");
    title.textContent = entry.title || entry.appId;

    const summary = document.createElement("span");
    const tags = [];
    if (entry.install?.installed) {
      tags.push(`Installed ${entry.install.installedVersion || entry.version || ""}`.trim());
    }
    if (entry.reviewStatus && entry.reviewStatus !== "approved") {
      tags.push(`Review: ${entry.reviewStatus}`);
    }
    tags.push(entry.sourceType === "dev" ? "Developer app" : "Published app");
    if (entry.updateAvailable && entry.latestApprovedVersion) {
      tags.push(`Update ${entry.latestApprovedVersion}`);
    }
    if (entry.updateRequiresConsent) {
      tags.push("Consent needed");
    }
    summary.textContent = [sanitizeText(entry.summary, 120), ...tags].filter(Boolean).join(" | ");

    copy.append(title, summary);
    button.append(icon, copy);
    el.miniappCatalogCards.append(button);
  }
}

function renderMiniappLauncher() {
  const thread = getActiveThread();
  const supportReason = miniappSupportReason(thread);
  const supported = !supportReason;
  const joinable = state.miniapp.launchContext?.joinable !== false;
  const entry = getMiniappCatalogEntry();
  const installable = !entry || entry.reviewStatus === "approved" || entry.sourceType === "dev";
  const installed = Boolean(entry?.install?.installed);
  el.attachBtn.disabled = false;
  el.attachBtn.title = supported ? "Open apps and attachments" : supportReason;
  el.miniappLauncher.classList.toggle("hidden", !state.miniapp.drawerOpen);
  el.miniappStage.classList.toggle("hidden", !state.miniapp.selectedAppId);
  renderMiniappCatalog();

  if (!state.miniapp.drawerOpen) {
    el.miniappContextCopy.textContent = supported
      ? "Open the attachment menu to choose an app."
      : supportReason;
    el.miniappLaunchMode.textContent = "Ready";
    el.miniappShareBtn.disabled = true;
    el.miniappOpenBtn.disabled = true;
    el.miniappResetBtn.disabled = true;
    return;
  }

  const manifest = state.miniapp.manifest;
  el.miniappPreviewTitle.textContent = manifest?.name || "App Preview";
  el.miniappPreviewSubtitle.textContent = thread
    ? `Prepare ${manifest?.name || "the app"} for ${thread.title}.`
    : "Choose launch settings, then open the app.";
  el.miniappLaunchMode.textContent = state.miniapp.launchContext?.app_session_id
    ? (!joinable ? "Session ended" : (state.miniapp.consentRequired ? "Consent required" : "Attached to thread"))
    : (!installable ? `Blocked: ${entry?.reviewStatus || "unavailable"}` : (entry?.updateRequiresConsent ? "Update needs consent" : (entry?.updateAvailable ? "Update available" : "Needs launch")));
  el.miniappContextCopy.textContent = state.miniapp.lastShareError || (supported
    ? (!installable
        ? `This app is ${entry?.reviewStatus || "unavailable"} and cannot be launched for normal users yet.`
        : (entry?.updateRequiresConsent
            ? `This update expands the app's permissions. Review and approve the new access before opening it in ${thread.title}.`
            : `App sessions attach to ${thread.title}. Permissions can be adjusted before launch.`))
    : supportReason);

  el.miniappPermissions.replaceChildren();
  const permissions = Array.isArray(manifest?.permissions) ? manifest.permissions : [];
  if (!permissions.length) {
    const empty = document.createElement("p");
    empty.className = "miniapp-config-copy";
    empty.textContent = "This app does not request host permissions.";
    el.miniappPermissions.append(empty);
  }
  for (const permission of permissions) {
    const item = document.createElement("label");
    item.className = "miniapp-permission";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = state.miniapp.grantedPermissions.has(permission);
    input.disabled = !supported || !joinable || !installable;
    input.addEventListener("change", () => {
      if (input.checked) state.miniapp.grantedPermissions.add(permission);
      else state.miniapp.grantedPermissions.delete(permission);
    });
    const copy = document.createElement("span");
    copy.textContent = permission;
    item.append(input, copy);
    el.miniappPermissions.append(item);
  }
  el.miniappShareBtn.disabled = !supported || !manifest || !joinable || !installable;
  el.miniappShareBtn.textContent = installed ? (entry?.updateRequiresConsent ? "Review & Send" : "Send") : "Install & Send";
  el.miniappOpenBtn.disabled = !supported || !manifest || !joinable || !installable;
  el.miniappOpenBtn.textContent = !installable
    ? "Pending Review"
    : (!joinable ? "Session Ended" : (installed ? (entry?.updateRequiresConsent ? "Review & Open" : (state.miniapp.consentRequired ? "Join & Open" : "Open App")) : "Install & Open"));
  el.miniappResetBtn.disabled = !state.miniapp.launchContext?.app_session_id;
}

async function openAppCardMessage(message) {
  try {
    await openMiniappCard(message);
  } catch (error) {
    console.error(error);
    state.miniapp.lastShareError = sanitizeText(error.message || "Unable to open app card.", 180);
    renderMiniappLauncher();
  }
}

function buildCompactAppCardBubble(message, joinable, label) {
  const bubble = document.createElement("article");
  bubble.className = `bubble ${message.direction} app-card-bubble is-compact`;
  if (joinable) {
    bubble.tabIndex = 0;
    bubble.setAttribute("role", "button");
    bubble.setAttribute("aria-label", `Open ${label}`);
    bubble.addEventListener("click", () => {
      void openAppCardMessage(message);
    });
    bubble.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      void openAppCardMessage(message);
    });
  } else {
    bubble.setAttribute("aria-disabled", "true");
  }

  const icon = document.createElement("span");
  icon.className = "app-card-compact-icon";
  const iconURL = normalizePreviewURL(message.content?.icon_url);
  if (iconURL) {
    const image = document.createElement("img");
    image.className = "app-card-compact-icon-image";
    image.src = iconURL;
    image.alt = "";
    image.loading = "lazy";
    icon.appendChild(image);
  } else {
    const fallback = document.createElement("span");
    fallback.className = "app-card-compact-icon-fallback";
    fallback.textContent = initials(appCardDisplayName(message.content, 40)).slice(0, 1) || "#";
    icon.appendChild(fallback);
  }

  const text = document.createElement("span");
  text.className = "app-card-compact-label";
  text.textContent = label;

  bubble.append(icon, text);
  return bubble;
}

function buildExpandedAppCardBubble(message, joinable, previewLabel) {
  const bubble = document.createElement("article");
  bubble.className = `bubble ${message.direction} app-card-bubble`;
  const preview = normalizeMiniappMessagePreview(message.content?.message_preview);
  const previewCounterValue = Number(message.content?.preview_state?.counter);
  const previewCounter = Number.isFinite(previewCounterValue) ? Math.trunc(previewCounterValue) : null;
  const previewFrame = document.createElement("div");
  previewFrame.className = `app-card-preview ${preview ? `is-fit-${preview.fitMode} is-type-${preview.type === "live" ? "live" : "image"}` : "is-fallback"}`;

  if (preview) {
    if (preview.type === "live") {
      const iframe = document.createElement("iframe");
      iframe.className = "app-card-preview-frame";
      iframe.src = preview.url;
      iframe.loading = "lazy";
      iframe.referrerPolicy = "no-referrer";
      iframe.title = preview.altText || previewLabel;
      iframe.setAttribute("sandbox", "allow-scripts");
      iframe.setAttribute("tabindex", "-1");
      previewFrame.appendChild(iframe);
    } else {
      const image = document.createElement("img");
      image.className = "app-card-preview-image";
      image.src = preview.url;
      image.alt = preview.altText || previewLabel;
      image.loading = "lazy";
      previewFrame.appendChild(image);
    }
  } else {
    const fallback = document.createElement("div");
    fallback.className = "app-card-preview-fallback";
    const fallbackMark = document.createElement("span");
    fallbackMark.className = "app-card-preview-mark";
    fallbackMark.textContent = previewCounter === null ? "#" : String(previewCounter);
    const fallbackText = document.createElement("span");
    fallbackText.className = "app-card-preview-text";
    fallbackText.textContent = previewCounter === null ? previewLabel : "shared counter";
    fallback.append(fallbackMark, fallbackText);
    previewFrame.appendChild(fallback);
  }
  bubble.appendChild(previewFrame);

  const overlay = document.createElement("div");
  overlay.className = "app-card-overlay";

  const title = document.createElement("strong");
  title.className = "app-card-title";
  title.textContent = previewLabel || "Shared app";

  const summary = document.createElement("p");
  summary.className = "app-card-summary";
  summary.textContent = sanitizeText(message.content?.summary || "Open this shared app in the conversation.", 180);

  overlay.append(title, summary);

  const cta = document.createElement("button");
  cta.type = "button";
  cta.className = "secondary-btn compact app-card-open-btn";
  cta.textContent = joinable ? sanitizeText(message.content?.cta_label || "Open", 32) : "Ended";
  cta.disabled = !joinable;
  cta.addEventListener("click", async () => {
    await openAppCardMessage(message);
  });
  overlay.appendChild(cta);
  bubble.appendChild(overlay);

  return bubble;
}

function buildAppCardBubble(message, presentation = "expanded") {
  const joinable = message.content?.joinable !== false;
  const previewLabel = appCardDisplayName(message.content);
  if (presentation === "compact") return buildCompactAppCardBubble(message, joinable, previewLabel);
  return buildExpandedAppCardBubble(message, joinable, previewLabel);
}

function replyReferenceLabel(thread, replyReference, targetMessage) {
  if (targetMessage) return replyAuthorLabel(thread, targetMessage);
  if (replyReference?.senderUserId && replyReference.senderUserId === state.auth?.userId) return "you";
  return sanitizeText(thread?.title || "this message", 60);
}

function jumpToMessage(threadId, messageId) {
  if (!threadId || !messageId || state.activeThreadId !== threadId) return;
  const selector = window.CSS?.escape
    ? `[data-message-id="${window.CSS.escape(messageId)}"]`
    : `[data-message-id="${messageId.replace(/"/g, '\\"')}"]`;
  const target = el.messageList.querySelector(selector);
  if (!(target instanceof HTMLElement)) return;
  target.scrollIntoView({ behavior: "smooth", block: "center" });
  target.classList.remove("reply-jump-highlight");
  void target.offsetWidth;
  target.classList.add("reply-jump-highlight");
  window.setTimeout(() => {
    target.classList.remove("reply-jump-highlight");
  }, 1600);
}

function buildReplyPreview(thread, message) {
  const replyReference = normalizeReplyReference(message.content?.reply_to);
  if (!replyReference) return null;
  const targetMessage = getMessageById(thread.id, replyReference.messageId);
  const preview = document.createElement(targetMessage ? "button" : "div");
  if (preview instanceof HTMLButtonElement) preview.type = "button";
  preview.className = `reply-preview ${message.direction}${targetMessage ? " is-link" : ""}`;

  const label = document.createElement("span");
  label.className = "reply-preview-label";
  label.textContent = replyReferenceLabel(thread, replyReference, targetMessage);

  const text = document.createElement("span");
  text.className = "reply-preview-text";
  text.textContent = sanitizeText(targetMessage?.text || replyReference.text || "Original message", 180);

  preview.append(label, text);

  if (targetMessage && preview instanceof HTMLButtonElement) {
    preview.addEventListener("click", (event) => {
      event.stopPropagation();
      jumpToMessage(thread.id, replyReference.messageId);
    });
  }
  return preview;
}

function buildReplyBacklink(thread, message, replies) {
  if (!Array.isArray(replies) || replies.length === 0) return null;
  const latestReply = replies[replies.length - 1];
  if (!latestReply?.id) return null;

  const badge = document.createElement("button");
  badge.type = "button";
  badge.className = `reply-thread-badge ${message.direction}`;
  badge.setAttribute(
    "aria-label",
    replies.length === 1 ? "Jump to reply" : `Jump to latest reply (${replies.length} replies)`
  );
  badge.title = replies.length === 1 ? "Jump to reply" : `Jump to latest reply (${replies.length})`;

  const arrow = document.createElement("span");
  arrow.className = "reply-thread-badge-arrow";
  arrow.textContent = "↩";

  const count = document.createElement("span");
  count.className = "reply-thread-badge-count";
  count.textContent = replies.length > 1 ? String(replies.length) : replyAuthorLabel(thread, latestReply);

  badge.append(arrow, count);
  badge.addEventListener("click", (event) => {
    event.stopPropagation();
    jumpToMessage(thread.id, latestReply.id);
  });
  return badge;
}

async function downloadAttachmentMessage(message) {
  const attachmentId = sanitizeText(message?.content?.attachment_id, 80);
  if (!attachmentId) throw new Error("Missing attachment id.");
  const payload = await apiRequest(`/v1/media/attachments/${encodeURIComponent(attachmentId)}/download`);
  const url = sanitizeText(payload?.download_url, 2000);
  if (!url) throw new Error("Missing download URL.");
  const descriptor = message?.content && typeof message.content === "object" ? message.content : {};
  if (sanitizeText(descriptor.file_key, 4000) && sanitizeText(descriptor.file_nonce, 4000)) {
    const response = await fetch(url);
    if (!response.ok) throw new Error("Unable to download attachment.");
    const encryptedBuffer = await response.arrayBuffer();
    const decryptedBuffer = await decryptAttachmentBytes(encryptedBuffer, descriptor);
    const blob = new Blob([decryptedBuffer], { type: sanitizeText(descriptor.mime_type, 200) || "application/octet-stream" });
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = sanitizeText(descriptor.file_name || payload?.file_name || "attachment", 140);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 30_000);
    return;
  }
  window.open(url, "_blank", "noopener");
}

function buildTextBubble(thread, message) {
  const bubble = document.createElement("article");
  bubble.className = `bubble ${message.direction}`;
  if (message.deleted) {
    bubble.textContent = "Message deleted";
    return bubble;
  }

  if (isAttachmentMessage(message)) {
    bubble.classList.add("has-reply");
    const replyPreview = buildReplyPreview(thread, message);
    const action = document.createElement("button");
    action.type = "button";
    action.className = "reply-preview";
    const label = document.createElement("strong");
    label.textContent = "Attachment";
    const body = document.createElement("span");
    body.textContent = sanitizeText(message.content?.file_name || message.text || "Download attachment", 140);
    action.append(label, body);
    action.addEventListener("click", async (event) => {
      event.stopPropagation();
      try {
        await downloadAttachmentMessage(message);
      } catch (error) {
        console.error(error);
        window.alert(sanitizeText(error.message || "Unable to download attachment.", 180));
      }
    });
    if (replyPreview) bubble.append(replyPreview, action);
    else bubble.append(action);
    return bubble;
  }

  const replyPreview = buildReplyPreview(thread, message);
  if (!replyPreview) {
    bubble.textContent = message.text;
    return bubble;
  }

  bubble.classList.add("has-reply");
  const body = document.createElement("p");
  body.className = "bubble-body-text";
  body.textContent = message.text;
  bubble.append(replyPreview, body);
  return bubble;
}

function renderMessages() {
  const thread = getActiveThread();
  const blocked = Boolean(thread?.blocked);
  const encryptedGroupPending = Boolean(
    thread
    && thread.kind === "group"
    && sanitizeText(thread.encryptionState, 40) === "ENCRYPTED"
    && !thread.e2eeReady
  );
  const targetUserId = otherUserIdForThread(thread);
  if (thread && state.openMessageMenu && state.openMessageMenu.threadId !== thread.id) {
    state.openMessageMenu = null;
  }
  el.nicknameBtn.disabled = !thread || thread?.kind === "draft_phone";
  el.groupEncryptionBtn.disabled = !thread || thread.kind !== "group" || sanitizeText(thread.encryptionState, 40) === "ENCRYPTED";
  el.groupEncryptionBtn.textContent = thread?.kind === "group"
    ? (sanitizeText(thread?.encryptionState, 40) === "ENCRYPTED" ? "On" : "Lock")
    : "Lock";
  el.blockBtn.disabled = !thread || !targetUserId;
  el.closeThreadBtn.disabled = !thread || thread?.kind === "draft_phone";
  el.attachBtn.disabled = !thread || blocked || encryptedGroupPending;
  el.blockBtn.textContent = thread?.blockedByViewer ? "Unblock" : "Block";
  renderComposerReply(thread);

  if (!thread) {
    el.title.textContent = "Select a conversation";
    el.subtitle.textContent = "No active thread";
    el.composerInput.disabled = true;
    clearMessageMenuWithoutRender();
    closeMiniappLauncher();
    state.miniapp.popupOpen = false;
    el.miniappFrame.src = "about:blank";
    renderMiniappLauncher();
    el.messageList.replaceChildren(el.emptyState);
    return;
  }

  el.title.textContent = thread.title;
  el.subtitle.textContent = thread.subtitle;
  el.composerInput.disabled = blocked || encryptedGroupPending;
  el.composerInput.placeholder = thread.blockedByOther
    ? "Messaging unavailable in this conversation"
    : blocked
    ? "Unblock this user to send messages"
    : encryptedGroupPending
    ? "Waiting for all members to publish secure messaging keys"
    : "Message";
  renderMiniappLauncher();
  el.messageList.replaceChildren();
  const appCardModes = appCardPresentationModes(thread);
  const replyIndex = replyIndexByTarget(thread);

  for (const message of thread.messages || []) {
    const wrap = document.createElement("div");
    wrap.className = `bubble-wrap ${message.direction}`;
    wrap.dataset.messageId = message.id;

    const bubble = isAppCardMessage(message) && !message.deleted
      ? buildAppCardBubble(message, appCardModes[message.id] || "expanded")
      : buildTextBubble(thread, message);

    const reactionBadge = isNonSMSMessage(message) ? buildReactionBadge(message) : null;
    const replyBacklink = buildReplyBacklink(thread, message, replyIndex[message.id]);
    if (reactionBadge || replyBacklink) wrap.classList.add("has-reactions");

    const meta = document.createElement("p");
    meta.className = `bubble-meta ${message.direction}`;
    const stamp = formatShortTime(message.createdAt);
    const edited = message.editedAt ? "edited" : "";
    const leftMeta = document.createElement("span");
    leftMeta.textContent = stamp;
    meta.appendChild(leftMeta);

    if (message.direction === "out") {
      const normalizedStatus = normalizeDeliveryStatus(message.transport, message.status);
      if (normalizedStatus) {
        const statusNode = document.createElement("span");
        statusNode.className = `delivery-status status-${normalizedStatus.toLowerCase().replace(/_/g, "-")}`;
        const ohmfLabel = deliveryIndicatorLabel(message);
        statusNode.textContent = ohmfLabel || normalizedStatus;
        meta.appendChild(statusNode);
      }
    }

    if (edited) {
      const editedNode = document.createElement("span");
      editedNode.textContent = edited;
      meta.appendChild(editedNode);
    }

    wrap.append(bubble, meta);

    if (reactionBadge) wrap.appendChild(reactionBadge);
    if (replyBacklink) wrap.appendChild(replyBacklink);
    if (isNonSMSMessage(message)) {
      const actionAnchor = buildMessageActionAnchor(thread, message);
      if (actionAnchor) wrap.appendChild(actionAnchor);
    }

    el.messageList.appendChild(wrap);
  }

  const typingText = typingIndicatorText(thread.id);
  if (typingText) {
    const typingWrap = document.createElement("div");
    typingWrap.className = "bubble-wrap in";
    const typingBubble = document.createElement("article");
    typingBubble.className = "bubble in typing";
    typingBubble.textContent = typingText;
    typingWrap.appendChild(typingBubble);
    el.messageList.appendChild(typingWrap);
  }

  requestAnimationFrame(scrollMessageListToBottom);
}

function renderAll() {
  renderThreadList();
  renderMessages();
  renderMiniappWindow();
  renderMessageMetadataWindow();
}

function openMobileThread() {
  if (window.matchMedia("(max-width: 880px)").matches) el.appShell.classList.add("mobile-chat-open");
}

function closeMobileThread() {
  el.appShell.classList.remove("mobile-chat-open");
}

function showAppShell() {
  el.authShell.classList.add("hidden");
  el.appShell.classList.remove("hidden");
}

function showAuthShell() {
  el.appShell.classList.add("hidden");
  el.authShell.classList.remove("hidden");
}

function pushPendingMessage(threadId, content, transport = TRANSPORT_OHMF, contentType = CONTENT_TYPE_TEXT) {
  const thread = getThreadById(threadId);
  if (!thread) return null;
  const normalizedTransport = normalizeTransport(transport);
  const status = normalizedTransport === TRANSPORT_SMS ? SMS_DELIVERY_STATUSES.SENT : OHMF_DELIVERY_STATUSES.SENT;
  const pendingContent = content && typeof content === "object" ? cloneJson(content) : { text: sanitizeText(content, 1000) };
  const pending = {
    id: `tmp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    senderUserId: state.auth?.userId || "",
    direction: "out",
    text: sanitizeText(pendingContent?.text || pendingContent?.file_name || "Attachment", 1000),
    createdAt: nowISO(),
    status,
    statusUpdatedAt: nowISO(),
    transport: normalizedTransport,
    serverOrder: 0,
    reactions: {},
    editedAt: "",
    deleted: false,
    contentType,
    content: pendingContent,
  };
  upsertThread({ ...thread, messages: [...(thread.messages || []), pending], updatedAt: pending.createdAt });
  saveConversationStore();
  return pending.id;
}

function patchMessage(threadId, messageId, patch) {
  const thread = getThreadById(threadId);
  if (!thread) return;
  const nextMessages = (thread.messages || []).map((message) => {
    if (message.id !== messageId) return message;
    const merged = { ...message, ...patch };
    const transport = normalizeTransport(merged.transport);
    const encryptedEnvelopeFingerprintValue = sanitizeText(merged.contentType, 40) === CONTENT_TYPE_ENCRYPTED
      ? encryptedEnvelopeFingerprint(merged)
      : "";
    return applyStoredReceiptCheckpoint(thread, {
      ...merged,
      transport,
      status: normalizeDeliveryStatus(transport, merged.status),
      statusUpdatedAt: patch.status ? (patch.statusUpdatedAt || nowISO()) : merged.statusUpdatedAt,
      encryptedEnvelopeFingerprint: encryptedEnvelopeFingerprintValue,
    });
  });
  const dedupedMessages = patch.id
    ? nextMessages.filter((message, index, items) => index === items.findIndex((candidate) => candidate.id === message.id))
    : nextMessages;
  upsertThread({ ...thread, messages: dedupedMessages, updatedAt: patch.createdAt || thread.updatedAt });
  saveConversationStore();
}

async function createOrGetPhoneConversation(phone) {
  const payload = await apiRequest("/v1/conversations/phone", {
    method: "POST",
    body: JSON.stringify({ phone_e164: phone }),
  });
  const mapped = mapConversation(payload);
  const existing = state.threads.find((t) => t.id === mapped.id);
  if (existing) {
    upsertThread({ ...existing, ...mapped });
  } else {
    upsertThread(mapped);
  }
  return mapped.id;
}

async function createGroupConversation(title, participantPhones) {
  const payload = await apiRequest("/v1/conversations", {
    method: "POST",
    body: JSON.stringify({
      type: "GROUP",
      title: sanitizeText(title, 80),
      participant_phones: participantPhones,
      encryption_state: "ENCRYPTED",
    }),
  });
  await resolveProfilesForUsers(payload?.participants || []);
  const mapped = mapConversation(payload || {});
  upsertThread({
    ...mapped,
    messages: [],
    loadedMessages: true,
  });
  state.activeThreadId = mapped.id;
  saveConversationStore();
  renderAll();
  return mapped;
}

function otherUserIdForThread(thread) {
  if (!thread) return "";
  return sanitizeText((thread.participants || []).find((id) => id && id !== state.auth?.userId), 80);
}

async function updateConversationPreferences(threadId, patch) {
  const payload = await apiRequest(`/v1/conversations/${encodeURIComponent(threadId)}/preferences`, {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
  const mapped = mapConversation(payload || {});
  const existing = getThreadById(threadId);
  if (existing) {
    upsertThread({
      ...existing,
      ...mapped,
      messages: existing.messages,
      loadedMessages: existing.loadedMessages,
    });
  } else if (mapped.id) {
    upsertThread(mapped);
  }
  saveConversationStore();
  return getThreadById(threadId) || mapped;
}

async function refreshConversationThread(threadId) {
  const payload = await apiRequest(`/v1/conversations/${encodeURIComponent(threadId)}`, { method: "GET" });
  const mapped = mapConversation(payload || {});
  const existing = getThreadById(threadId);
  if (existing) {
    upsertThread({
      ...existing,
      ...mapped,
      messages: existing.messages,
      loadedMessages: existing.loadedMessages,
    });
  } else if (mapped.id) {
    upsertThread(mapped);
  }
  saveConversationStore();
  return getThreadById(threadId) || mapped;
}

async function updateConversationMetadata(threadId, patch) {
  const payload = await apiRequest(`/v1/conversations/${encodeURIComponent(threadId)}/metadata`, {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
  const mapped = mapConversation(payload || {});
  const existing = getThreadById(threadId);
  if (existing) {
    upsertThread({
      ...existing,
      ...mapped,
      messages: existing.messages,
      loadedMessages: existing.loadedMessages,
    });
  } else if (mapped.id) {
    upsertThread(mapped);
  }
  saveConversationStore();
  return getThreadById(threadId) || mapped;
}

async function enableGroupEncryption(thread) {
  if (!thread || thread.kind !== "group") return thread;
  if (sanitizeText(thread.encryptionState, 40) === "ENCRYPTED") return thread;
  if (!thread.e2eeReady) {
    throw buildEncryptedGroupNotReadyError(thread);
  }
  return updateConversationMetadata(thread.id, { encryption_state: "ENCRYPTED" });
}

function moveDraftToConversation(draftId, conversationId, phone) {
  const draft = getThreadById(draftId);
  const existing = getThreadById(conversationId);
  const carried = draft?.messages || [];
  const merged = [...(existing?.messages || []), ...carried];
  state.threads = state.threads.filter((t) => t.id !== draftId);
  upsertThread({
    ...(existing || {}),
    id: conversationId,
    kind: "phone",
    title: existing?.title || phone,
    subtitle: existing?.subtitle || "Phone conversation (OTT preferred)",
    externalPhones: [phone],
    participants: existing?.participants || [state.auth.userId],
    blockedByViewer: Boolean(existing?.blockedByViewer),
    blockedByOther: Boolean(existing?.blockedByOther),
    messages: merged,
    loadedMessages: Boolean(existing?.loadedMessages),
    updatedAt: nowISO(),
  });
  state.activeThreadId = conversationId;
}

async function sendOTT(conversationId, content, contentType = CONTENT_TYPE_TEXT) {
  return apiRequest("/v1/messages", {
    method: "POST",
    body: JSON.stringify({
      conversation_id: conversationId,
      idempotency_key: makeIdempotencyKey("conv"),
      content_type: contentType,
      content,
    }),
  });
}

async function sendSMS(phone, content, contentType = CONTENT_TYPE_TEXT) {
  return apiRequest("/v1/messages/phone", {
    method: "POST",
    body: JSON.stringify({
      phone_e164: phone,
      idempotency_key: makeIdempotencyKey("phone"),
      content_type: contentType,
      content,
    }),
  });
}

async function sendEncryptedConversationPayload(thread, plainContent, innerContentType, encryptedEnvelope) {
  let envelope = encryptedEnvelope;
  let workingThread = getThreadById(thread.id) || thread;
  try {
    const payload = await sendOTT(workingThread.id, envelope, CONTENT_TYPE_ENCRYPTED);
    return { payload, envelope, thread: workingThread };
  } catch (error) {
    if (error?.code !== "encrypted_conversation_state_changed") throw error;
    workingThread = await refreshConversationThread(thread.id);
    envelope = await encryptConversationContent(workingThread, plainContent, innerContentType);
    if (!envelope) throw error;
    const payload = await sendOTT(workingThread.id, envelope, CONTENT_TYPE_ENCRYPTED);
    return { payload, envelope, thread: workingThread };
  }
}

async function sendInConversation(thread, content) {
  const shouldEncrypt = thread.kind === "dm" || (thread.kind === "group" && sanitizeText(thread.encryptionState, 40) === "ENCRYPTED");
  let encryptedEnvelope = null;
  if (shouldEncrypt) {
    try {
      encryptedEnvelope = await encryptConversationContent(thread, content, CONTENT_TYPE_TEXT);
    } catch (error) {
      if (thread.kind === "dm") {
        console.error(error);
        encryptedEnvelope = null;
      } else {
        throw error;
      }
    }
  }
  const effectiveContentType = encryptedEnvelope ? CONTENT_TYPE_ENCRYPTED : CONTENT_TYPE_TEXT;
  const pendingId = pushPendingMessage(thread.id, content, TRANSPORT_OHMF, effectiveContentType);
  renderAll();
  try {
    let payload;
    if (encryptedEnvelope) {
      const encryptedSend = await sendEncryptedConversationPayload(thread, content, CONTENT_TYPE_TEXT, encryptedEnvelope);
      encryptedEnvelope = encryptedSend.envelope;
      payload = encryptedSend.payload;
    } else {
      payload = await sendOTT(thread.id, content, effectiveContentType);
    }
    const finalMessageId = sanitizeText(payload.message_id, 80) || pendingId;
    if (encryptedEnvelope) {
      cacheEncryptedMessagePlaintext(finalMessageId, encryptedEnvelope, CONTENT_TYPE_TEXT, content, sanitizeText(content?.text, 1000));
    }
    patchMessage(thread.id, pendingId, {
      id: finalMessageId,
      serverOrder: Number(payload.server_order || 0),
      status: OHMF_DELIVERY_STATUSES.SENT,
      transport: TRANSPORT_OHMF,
      createdAt: nowISO(),
      statusUpdatedAt: nowISO(),
      contentType: effectiveContentType,
      content: encryptedEnvelope || content,
    });
  } catch (err) {
    const phone = thread.externalPhones?.[0];
    if (thread.kind !== "phone" || !phone) {
      patchMessage(thread.id, pendingId, { status: OHMF_DELIVERY_STATUSES.FAIL_SEND });
      throw err;
    }

    try {
      const smsPayload = await sendSMS(phone, content, CONTENT_TYPE_TEXT);
      patchMessage(thread.id, pendingId, {
        id: sanitizeText(smsPayload.message_id, 80) || pendingId,
        serverOrder: Number(smsPayload.server_order || 0),
        status: SMS_DELIVERY_STATUSES.SENT,
        transport: TRANSPORT_SMS,
        createdAt: nowISO(),
        statusUpdatedAt: nowISO(),
      });
      await loadMessagesForThread(thread.id);
    } catch (smsErr) {
      patchMessage(thread.id, pendingId, { status: SMS_DELIVERY_STATUSES.FAIL_SEND, transport: TRANSPORT_SMS });
      throw smsErr;
    }
  }
}

async function sendAttachmentInConversation(thread, attachment) {
  if (!thread || thread.kind === "draft_phone") {
    throw new Error("Attachments require a saved OHMF conversation.");
  }
  const content = {
    attachment_id: attachment.attachment_id,
    file_name: attachment.file_name,
    mime_type: attachment.mime_type,
    size_bytes: attachment.size_bytes,
    object_sha256: attachment.checksum_sha256,
    file_key: sanitizeText(attachment.file_key, 4000),
    file_nonce: sanitizeText(attachment.file_nonce, 4000),
    encryption_scheme: sanitizeText(attachment.encryption_scheme, 120),
    stored_mime_type: sanitizeText(attachment.stored_mime_type, 200),
    stored_size_bytes: Number(attachment.stored_size_bytes || 0),
    encrypted: Boolean(attachment.encrypted),
  };
  const pendingId = pushPendingMessage(thread.id, content, TRANSPORT_OHMF, CONTENT_TYPE_ATTACHMENT);
  renderAll();
  try {
    const shouldEncrypt = content.encrypted && (thread.kind === "dm" || (thread.kind === "group" && sanitizeText(thread.encryptionState, 40) === "ENCRYPTED"));
    let encryptedEnvelope = shouldEncrypt
      ? await encryptConversationContent(thread, content, CONTENT_TYPE_ATTACHMENT)
      : null;
    let payload;
    if (encryptedEnvelope) {
      const encryptedSend = await sendEncryptedConversationPayload(thread, content, CONTENT_TYPE_ATTACHMENT, encryptedEnvelope);
      encryptedEnvelope = encryptedSend.envelope;
      payload = encryptedSend.payload;
    } else {
      payload = await sendOTT(thread.id, content, CONTENT_TYPE_ATTACHMENT);
    }
    if (encryptedEnvelope) {
      cacheEncryptedMessagePlaintext(
        sanitizeText(payload.message_id, 80) || pendingId,
        encryptedEnvelope,
        CONTENT_TYPE_ATTACHMENT,
        content,
        sanitizeText(attachment.file_name || "Attachment", 140)
      );
    }
    await apiRequest("/v1/media/attachments", {
      method: "POST",
      body: JSON.stringify({
        attachment_id: attachment.attachment_id,
        message_id: payload.message_id,
        file_name: content.encrypted ? "" : attachment.file_name,
      }),
    });
    patchMessage(thread.id, pendingId, {
      id: sanitizeText(payload.message_id, 80) || pendingId,
      serverOrder: Number(payload.server_order || 0),
      status: OHMF_DELIVERY_STATUSES.SENT,
      transport: TRANSPORT_OHMF,
      createdAt: nowISO(),
      statusUpdatedAt: nowISO(),
      contentType: CONTENT_TYPE_ATTACHMENT,
      content,
      text: sanitizeText(attachment.file_name || "Attachment", 140),
    });
  } catch (error) {
    patchMessage(thread.id, pendingId, { status: OHMF_DELIVERY_STATUSES.FAIL_SEND });
    throw error;
  }
}

function ensureDraftPhoneThread(phone) {
  const existing = state.threads.find((thread) => thread.kind === "draft_phone" && thread.externalPhones?.[0] === phone);
  if (existing) {
    state.activeThreadId = existing.id;
    return existing;
  }
  const draft = {
    id: `draft:${phone}`,
    kind: "draft_phone",
    title: phone,
    subtitle: "New phone conversation",
    nickname: "",
    blockedByViewer: false,
    blockedByOther: false,
    blocked: false,
    closed: false,
    updatedAt: nowISO(),
    participants: [state.auth?.userId || ""],
    externalPhones: [phone],
    messages: [],
    loadedMessages: true,
  };
  upsertThread(draft);
  state.activeThreadId = draft.id;
  saveConversationStore();
  return draft;
}

async function sendInDraftPhoneConversation(thread, content) {
  const phone = thread.externalPhones?.[0] || "";
    const pendingId = pushPendingMessage(thread.id, content, TRANSPORT_OHMF);
  renderAll();
  try {
    const conversationId = await createOrGetPhoneConversation(phone);
    moveDraftToConversation(thread.id, conversationId, phone);
    const payload = await sendOTT(conversationId, content, CONTENT_TYPE_TEXT);
    const finalMessageId = sanitizeText(payload.message_id, 80) || pendingId;
    patchMessage(conversationId, pendingId, {
      id: finalMessageId,
      serverOrder: Number(payload.server_order || 0),
      status: OHMF_DELIVERY_STATUSES.SENT,
      transport: TRANSPORT_OHMF,
      createdAt: nowISO(),
      statusUpdatedAt: nowISO(),
    });
  } catch {
    try {
      const smsPayload = await sendSMS(phone, content, CONTENT_TYPE_TEXT);
      const conversationId = sanitizeText(smsPayload.conversation_id, 80);
      if (conversationId) moveDraftToConversation(thread.id, conversationId, phone);
      const targetThreadId = conversationId || thread.id;
      patchMessage(targetThreadId, pendingId, {
        id: sanitizeText(smsPayload.message_id, 80) || pendingId,
        serverOrder: Number(smsPayload.server_order || 0),
        status: SMS_DELIVERY_STATUSES.SENT,
        transport: TRANSPORT_SMS,
        createdAt: nowISO(),
        statusUpdatedAt: nowISO(),
      });
      if (conversationId) await loadMessagesForThread(conversationId);
    } catch (error) {
      patchMessage(thread.id, pendingId, { status: SMS_DELIVERY_STATUSES.FAIL_SEND, transport: TRANSPORT_SMS });
      throw error;
    }
  }
}

function updatePhonePreview() {
  el.phoneInput.value = formatPhoneLocal(el.phoneInput.value);
  const preview = toE164(el.countryCodeSelect.value, el.phoneInput.value);
  el.phoneE164Preview.textContent = preview ? `Will send OTP to ${preview}` : "Enter at least 8 digits including country code";
}

async function startPhoneAuth(event) {
  event.preventDefault();
  const phone = toE164(el.countryCodeSelect.value, el.phoneInput.value);
  if (!phone) {
    setAuthError("Enter a valid phone number.");
    return;
  }
  setAuthMessage("Requesting OTP...");
  try {
    const payload = await apiRequest("/v1/auth/phone/start", {
      method: "POST",
      body: JSON.stringify({ phone_e164: phone, channel: "SMS" }),
    });
    state.challengeId = sanitizeText(payload.challenge_id, 80);
    el.phoneVerifyForm.classList.remove("hidden");
    setAuthMessage("OTP sent. Enter code to continue.");
  } catch (error) {
    setAuthError(`OTP start failed: ${error.message}`);
  }
}

async function verifyPhoneAuth(event) {
  event.preventDefault();
  const otp = sanitizeText(el.otpInput.value, 8);
  if (!state.challengeId || otp.length < 4) {
    setAuthError("Challenge and OTP are required.");
    return;
  }
  setAuthMessage("Verifying...");
  try {
    const payload = await apiRequest("/v1/auth/phone/verify", {
      method: "POST",
      body: JSON.stringify({
        challenge_id: state.challengeId,
        otp_code: otp,
        device: { platform: "WEB", device_name: "OHMF Web", capabilities: ["MINI_APPS", "E2EE_OTT_V2", "WEB_PUSH_V1"] },
      }),
    });
    const user = payload?.user || {};
    const tokens = payload?.tokens || {};
    if (!tokens.access_token || !tokens.refresh_token || !user.user_id) throw new Error("invalid_auth_response");
    authStoreSet({
      userId: sanitizeText(user.user_id, 80),
      phoneE164: sanitizeText(user.primary_phone_e164, 32),
      deviceId: sanitizeText(payload?.device?.device_id, 80),
      accessToken: sanitizeText(tokens.access_token, 3000),
      refreshToken: sanitizeText(tokens.refresh_token, 3000),
    });
    state.challengeId = "";
    el.phoneVerifyForm.classList.add("hidden");
    el.phoneStartForm.reset();
    el.phoneVerifyForm.reset();
    updatePhonePreview();
    await bootAfterAuth();
  } catch (error) {
    setAuthError(`Verify failed: ${error.message}`);
  }
}

async function bootAfterAuth() {
  state.query = "";
  state.activeThreadId = null;
  state.threads = [];
  state.profiles = {};
  state.selfProfile = null;
  state.miniapp.catalog = [];
  state.miniapp.catalogLoaded = false;
  state.crypto = { device: null, published: false, bundleCache: {}, decryptedMessageCache: {} };
  eventStreamDisabled = false;
  ensureSyncDeviceId();
  await hydrateCryptoClientState();
  loadConversationStore();
  loadSyncCursor();
  showAppShell();
  renderAll();
  try {
    await loadSelfProfile();
    await loadMiniappCatalog().catch((error) => {
      console.error(error);
    });
    await publishCryptoBundle().catch((error) => {
      console.error(error);
    });
    await loadConversationsFromApi();
    const requestedConversationId = sanitizeText(new URLSearchParams(window.location.search).get("conversation_id"), 80);
    if (requestedConversationId && getThreadById(requestedConversationId)) {
      state.activeThreadId = requestedConversationId;
      window.history.replaceState({}, document.title, window.location.pathname);
    }
    if (state.activeThreadId) await ensureMessagesLoaded(state.activeThreadId);
    await syncFromCursor();
    await registerServiceWorkerAndPush().catch((error) => {
      console.error(error);
    });
    stopEventStream();
    stopRealtimeSocket();
    stopLiveRefreshLoop();
    startRealtimeSocket();
    scheduleLiveRefreshLoop();
    renderAll();
  } catch (error) {
    console.error(error);
  }
}

async function selectMiniapp(appId) {
  const entry = getMiniappCatalogEntry(appId);
  if (!entry) return;
  state.miniapp.selectedAppId = appId;
  state.miniapp.popupOpen = false;
  state.miniapp.launchContext = null;
  state.miniapp.sessionState = normalizeMiniappSessionState(null);
  state.miniapp.frameWindow = null;
  state.miniapp.consentRequired = false;
  state.miniapp.lastShareError = "";
  clearMiniappLoadTimeout();
  el.miniappFrame.src = "about:blank";
  try {
    const manifest = entry.manifest || await loadMiniappManifestByAppId(entry.appId);
    state.miniapp.manifest = manifest;
    state.miniapp.grantedPermissions = new Set(Array.isArray(manifest.permissions) ? manifest.permissions : []);
  } catch (error) {
    console.error(error);
  }
  renderMiniappLauncher();
}

async function openEmbeddedMiniapp() {
  try {
    if (state.miniapp.launchContext?.joinable === false) {
      throw new Error("This shared app session has ended.");
    }
    if (state.miniapp.launchContext?.app_session_id) {
      if (state.miniapp.consentRequired) {
        await joinMiniappSession(state.miniapp.launchContext.app_session_id);
      } else {
        await fetchMiniappSession(state.miniapp.launchContext.app_session_id);
      }
    } else {
      await ensureMiniappSession();
    }
    state.miniapp.popupOpen = true;
    startMiniappLoadTimeout();
    el.miniappFrame.setAttribute("sandbox", "allow-scripts");
    el.miniappFrame.src = buildMiniappFrameURL();
    renderAll();
  } catch (error) {
    console.error(error);
    clearMiniappLoadTimeout();
    state.miniapp.lastShareError = sanitizeText(error.message || "Unable to open app.", 180);
    renderAll();
  }
}

async function resetEmbeddedMiniappSession() {
  if (!state.miniapp.launchContext?.app_session_id) return;
  try {
    await apiRequest(`/v1/apps/sessions/${encodeURIComponent(state.miniapp.launchContext.app_session_id)}`, { method: "DELETE" });
  } catch (error) {
    console.error(error);
  }
  clearMiniappLoadTimeout();
  state.miniapp.popupOpen = false;
  state.miniapp.launchContext = null;
  state.miniapp.sessionState = normalizeMiniappSessionState(null);
  state.miniapp.frameWindow = null;
  state.miniapp.consentRequired = false;
  state.miniapp.lastShareError = "";
  el.miniappFrame.src = "about:blank";
  renderAll();
}

function logout() {
  stopLocalTyping();
  stopEventStream();
  stopRealtimeSocket();
  stopLiveRefreshLoop();
  authStoreClear();
  state.threads = [];
  state.activeThreadId = null;
  state.query = "";
  state.typingDraft = "";
  state.remoteTypingByThread = {};
  state.replyTarget = null;
  state.openMessageMenu = null;
  resetMessageMetadataState();
  state.miniapp.drawerOpen = false;
  state.miniapp.popupOpen = false;
  state.miniapp.selectedAppId = "";
  state.miniapp.catalog = [];
  state.miniapp.catalogLoaded = false;
  state.miniapp.manifest = null;
  state.miniapp.launchContext = null;
  state.miniapp.sessionState = null;
  state.miniapp.frameWindow = null;
  state.miniapp.consentRequired = false;
  state.miniapp.lastShareError = "";
  clearMiniappLoadTimeout();
  state.sync.lastUserCursor = 0;
  state.profiles = {};
  state.selfProfile = null;
  state.crypto = { device: null, published: false, bundleCache: {}, decryptedMessageCache: {} };
  showAuthShell();
  setAuthMessage("Signed out.");
  renderAll();
}

document.addEventListener("visibilitychange", () => {
  if (!state.auth || document.hidden) return;
  void syncFromCursor();
  if (!realtimeSocket) {
    startRealtimeSocket();
  }
  if (state.activeThreadId) {
    void syncThreadReceipts(state.activeThreadId);
  }
});

window.addEventListener("focus", () => {
  if (!state.auth) return;
  if (state.activeThreadId) {
    void syncThreadReceipts(state.activeThreadId);
  }
});

window.addEventListener("online", () => {
  if (!state.auth) return;
  void syncFromCursor();
  if (!realtimeSocket) {
    startRealtimeSocket();
  }
});

el.searchInput.addEventListener("input", (event) => {
  state.query = sanitizeText(event.target.value, 120);
  renderThreadList();
});

el.attachBtn.addEventListener("click", async () => {
  if (el.attachBtn.disabled) return;
  state.miniapp.drawerOpen = !state.miniapp.drawerOpen;
  if (state.miniapp.drawerOpen && !state.miniapp.catalogLoaded) {
    await loadMiniappCatalog().catch((error) => {
      console.error(error);
    });
  }
  if (state.miniapp.drawerOpen && !state.miniapp.selectedAppId && state.miniapp.catalog[0]?.appId) {
    await selectMiniapp(state.miniapp.catalog[0].appId);
  }
  renderMiniappLauncher();
});

el.miniappCloseBtn.addEventListener("click", () => {
  closeMiniappLauncher();
  renderMiniappLauncher();
});

el.miniappBackdrop.addEventListener("click", async () => {
  await closeMiniappWindow();
  renderAll();
});

el.miniappWindowCloseBtn.addEventListener("click", async () => {
  await closeMiniappWindow();
  renderAll();
});

el.miniappCatalogCards.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const card = target.closest("[data-app-id]");
  if (!card) return;
  await selectMiniapp(sanitizeText(card.getAttribute("data-app-id"), 120));
});

el.miniappUploadCard.addEventListener("click", () => {
  closeMiniappLauncher();
  el.attachmentInput.click();
});

el.attachmentInput.addEventListener("change", async () => {
  const [file] = Array.from(el.attachmentInput.files || []);
  el.attachmentInput.value = "";
  const thread = getActiveThread();
  if (!file || !thread) return;
  try {
    const uploaded = await uploadEncryptedMediaFile(thread, file);
    await sendAttachmentInConversation(thread, uploaded);
    renderAll();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to send attachment.", 180));
  }
});

el.miniappShareBtn.addEventListener("click", async () => {
  try {
    await shareMiniappToConversation();
  } catch (error) {
    console.error(error);
    state.miniapp.lastShareError = sanitizeText(error.message || "Unable to share app.", 180);
    renderMiniappLauncher();
  }
});

el.miniappOpenBtn.addEventListener("click", async () => {
  try {
    await openEmbeddedMiniapp();
  } catch (error) {
    console.error(error);
  }
});

el.miniappResetBtn.addEventListener("click", async () => {
  await resetEmbeddedMiniappSession();
});

el.composerInput.addEventListener("input", () => {
  state.typingDraft = sanitizeText(el.composerInput.value, 1000);
  syncLocalTypingSignal();
  renderMessages();
});

el.composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  const content = composerMessageContent(el.composerInput.value);
  if (!content) return;
  el.composerInput.value = "";
  state.typingDraft = "";
  stopLocalTyping();
  renderMessages();
  try {
    const thread = getActiveThread();
    if (thread && !thread.blocked) {
      if (thread.kind === "draft_phone") await sendInDraftPhoneConversation(thread, content);
      else await sendInConversation(thread, content);
    }
    clearReplyTargetWithoutRender();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to send message.", 180));
  }
  renderAll();
}); // removed: single-use composer send helper inlined into submit handler

el.composerReplyCancel.addEventListener("click", () => {
  clearReplyTarget();
  el.composerInput.focus();
});

el.backBtn.addEventListener("click", () => closeMobileThread());

el.newChatBtn.addEventListener("click", () => {
  el.newChatForm.classList.toggle("hidden");
  if (!el.newChatForm.classList.contains("hidden")) el.newPhoneInput.focus();
});

el.newGroupBtn.addEventListener("click", async () => {
  const title = window.prompt("Group title", "");
  if (title === null) return;
  const members = window.prompt("Participant phone numbers in E.164 format (comma separated)", "");
  if (members === null) return;
  const participantPhones = members
    .split(",")
    .map((item) => sanitizeText(item, 32))
    .filter(Boolean);
  if (!participantPhones.length) {
    window.alert("Enter at least one participant phone number.");
    return;
  }
  try {
    await createGroupConversation(title, participantPhones);
    openMobileThread();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to create group.", 180));
  }
});

el.newChatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const phone = toE164(el.newCountryCodeSelect.value, el.newPhoneInput.value);
  if (!phone) {
    el.newPhoneInput.focus();
    return;
  }
  ensureDraftPhoneThread(phone);
  el.newChatForm.classList.add("hidden");
  el.newPhoneInput.value = "";
  renderAll();
  openMobileThread();
});

el.logoutBtn.addEventListener("click", async () => {
  try {
    await apiRequest("/v1/auth/logout", { method: "POST", body: JSON.stringify({}) });
  } catch {} // removed: descriptive logout comment
  logout();
});

el.nicknameBtn.addEventListener("click", async () => {
  const thread = getActiveThread();
  if (!thread || thread.kind === "draft_phone") return;
  const nextValue = window.prompt("Set a nickname for this conversation", thread.nickname || "");
  if (nextValue === null) return;
  try {
    await updateConversationPreferences(thread.id, { nickname: sanitizeText(nextValue, 80) });
    renderAll();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to update nickname.", 160));
  }
});

el.groupEncryptionBtn.addEventListener("click", async () => {
  const thread = getActiveThread();
  if (!thread || thread.kind !== "group" || sanitizeText(thread.encryptionState, 40) === "ENCRYPTED") return;
  const confirmed = window.confirm("Enable end-to-end encryption for this group? All members must have secure messaging keys published.");
  if (!confirmed) return;
  try {
    await enableGroupEncryption(thread);
    renderAll();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to enable group encryption.", 180));
  }
});

el.blockBtn.addEventListener("click", async () => {
  const thread = getActiveThread();
  const targetUserId = otherUserIdForThread(thread);
  if (!thread || !targetUserId) return;
  try {
    if (thread.blockedByViewer) {
      await apiRequest(`/v1/blocks/${encodeURIComponent(targetUserId)}`, { method: "DELETE" });
      applyConversationStateUpdate({
        conversation_id: thread.id,
        blocked: Boolean(thread.blockedByOther),
        blocked_by_viewer: false,
        blocked_by_other: Boolean(thread.blockedByOther),
        updated_at: nowISO(),
      });
    } else {
      await apiRequest(`/v1/blocks/${encodeURIComponent(targetUserId)}`, { method: "POST", body: JSON.stringify({}) });
      applyConversationStateUpdate({
        conversation_id: thread.id,
        blocked: true,
        blocked_by_viewer: true,
        blocked_by_other: Boolean(thread.blockedByOther),
        updated_at: nowISO(),
      });
    }
    renderAll();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to update block state.", 160));
  }
});

el.closeThreadBtn.addEventListener("click", async () => {
  const thread = getActiveThread();
  if (!thread || thread.kind === "draft_phone") return;
  try {
    await updateConversationPreferences(thread.id, { closed: true });
    const remaining = visibleThreads();
    state.activeThreadId = remaining.length ? remaining[0].id : null;
    renderAll();
  } catch (error) {
    console.error(error);
    window.alert(sanitizeText(error.message || "Unable to close conversation.", 160));
  }
});

el.messageMetadataBackdrop.addEventListener("click", closeMessageMetadata);
el.messageMetadataCloseBtn.addEventListener("click", closeMessageMetadata);

el.phoneStartForm.addEventListener("submit", startPhoneAuth);
el.phoneVerifyForm.addEventListener("submit", verifyPhoneAuth);
el.phoneInput.addEventListener("input", updatePhonePreview);
el.countryCodeSelect.addEventListener("change", updatePhonePreview);
el.newPhoneInput.addEventListener("input", () => {
  el.newPhoneInput.value = formatPhoneLocal(el.newPhoneInput.value);
}); // removed: single-use phone formatting helper inlined into listener

window.addEventListener("resize", () => {
  if (!window.matchMedia("(max-width: 880px)").matches) closeMobileThread();
});

document.addEventListener("click", (event) => {
  if (!state.openMessageMenu) return;
  const target = event.target;
  if (target instanceof Element && target.closest(".message-action-anchor")) return;
  clearMessageMenu();
});

window.addEventListener("message", async (event) => {
  if (event.source !== el.miniappFrame.contentWindow) return;
  if (!state.miniapp.manifest?.entrypoint?.url) return;
  const expectedOrigin = new URL(state.miniapp.manifest.entrypoint.url, window.location.href).origin;
  if (event.origin !== expectedOrigin) return;

  const message = event.data;
  if (!message || typeof message !== "object" || message.channel !== state.miniapp.channelId) return;
  clearMiniappLoadTimeout();
  state.miniapp.frameWindow = event.source;
  const requestId = sanitizeText(message.request_id, 80);
  if (!requestId) return;

  try {
    const result = await handleMiniappBridgeCall(message);
    sendMiniappBridgeResponse(event.source, requestId, true, result);
  } catch (error) {
    sendMiniappBridgeResponse(event.source, requestId, false, null, {
      code: sanitizeText(error.code || "bridge_error", 80),
      message: sanitizeText(error.message || "Bridge call failed", 220),
    });
  }
});

el.miniappFrame.addEventListener("load", () => {
  if (!state.miniapp.popupOpen) return;
  if (el.miniappFrame.src === "about:blank") return;
  clearMiniappLoadTimeout();
  renderMiniappWindow();
});

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type !== "notification.open") return;
    const conversationId = sanitizeText(event.data?.conversation_id, 80);
    if (conversationId) {
      state.activeThreadId = conversationId;
      renderAll();
    }
    void syncFromCursor();
  });
}

document.addEventListener("keydown", async (event) => {
  if (event.key !== "Escape") return;
  if (state.messageMetadata.open) {
    closeMessageMetadata();
    return;
  }
  if (state.miniapp.popupOpen) {
    await closeMiniappWindow();
    renderAll();
    return;
  }
  if (!state.miniapp.drawerOpen) return;
  closeMiniappLauncher();
  renderMiniappLauncher();
});

async function init() {
  syncAuthHint();
  updatePhonePreview();
  const session = authStoreLoad();
  if (session) {
    state.auth = session;
    await bootAfterAuth();
    return;
  }
  showAuthShell();
  setAuthMessage("");
  renderAll();
}

init();
