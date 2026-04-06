"use strict";

function buildRunID() {
  return String(Date.now());
}

function buildPhone(runID, suffix) {
  const core = String(runID).slice(-5);
  return `+1555${core}${suffix}`;
}

async function parseResponse(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

async function requestJSON(baseURL, path, options = {}) {
  const response = await fetch(`${baseURL}${path}`, options);
  const body = await parseResponse(response);
  if (!response.ok) {
    throw new Error(`request_failed ${response.status} ${path} ${JSON.stringify(body)}`);
  }
  return body;
}

async function postJSON(baseURL, path, body, bearerToken = "") {
  const headers = { "Content-Type": "application/json" };
  if (bearerToken) headers.Authorization = `Bearer ${bearerToken}`;
  return requestJSON(baseURL, path, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
}

async function createVerifiedUser(baseURL, phoneE164, deviceName) {
  const start = await postJSON(baseURL, "/v1/auth/phone/start", {
    phone_e164: phoneE164,
    channel: "SMS",
  });
  const verify = await postJSON(baseURL, "/v1/auth/phone/verify", {
    challenge_id: start.challenge_id,
    otp_code: "123456",
    device: {
      platform: "WEB",
      device_name: deviceName,
      capabilities: ["MINI_APPS", "WEB_PUSH_V1"],
    },
  });
  return {
    userId: verify?.user?.user_id || "",
    accessToken: verify?.tokens?.access_token || "",
    refreshToken: verify?.tokens?.refresh_token || "",
    deviceId: verify?.device?.device_id || "",
    phoneE164,
    raw: verify,
  };
}

async function createDirectConversation(baseURL, accessToken, participantUserID) {
  const response = await postJSON(baseURL, "/v1/conversations", {
    type: "DM",
    participants: [participantUserID],
  }, accessToken);
  return {
    conversationId: response.conversation_id,
    raw: response,
  };
}

async function sendTextMessage(baseURL, accessToken, conversationID, text, idempotencyKey) {
  return postJSON(baseURL, "/v1/messages", {
    conversation_id: conversationID,
    idempotency_key: idempotencyKey,
    content_type: "text",
    content: { text },
  }, accessToken);
}

function normalizeNotificationPreferences(raw = {}) {
  return {
    send_read_receipts: raw.send_read_receipts !== false,
    share_presence: raw.share_presence !== false,
    share_typing: raw.share_typing !== false,
  };
}

async function getNotificationPreferences(baseURL, accessToken) {
  const headers = {};
  if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
  const payload = await requestJSON(baseURL, "/v1/notifications/preferences", {
    method: "GET",
    headers,
  });
  return normalizeNotificationPreferences(payload);
}

async function updateNotificationPreferences(baseURL, accessToken, patch = {}) {
  const headers = {};
  if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
  const current = await requestJSON(baseURL, "/v1/notifications/preferences", {
    method: "GET",
    headers,
  });
  const payload = await requestJSON(baseURL, "/v1/notifications/preferences", {
    method: "PUT",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      push_enabled: current?.push_enabled !== false,
      mute_unknown_senders: Boolean(current?.mute_unknown_senders),
      show_previews: current?.show_previews !== false,
      muted_conversation_notifications: Boolean(current?.muted_conversation_notifications),
      send_read_receipts: patch.send_read_receipts !== undefined ? Boolean(patch.send_read_receipts) : current?.send_read_receipts !== false,
      share_presence: patch.share_presence !== undefined ? Boolean(patch.share_presence) : current?.share_presence !== false,
      share_typing: patch.share_typing !== undefined ? Boolean(patch.share_typing) : current?.share_typing !== false,
    }),
  });
  return normalizeNotificationPreferences(payload);
}

module.exports = {
  buildPhone,
  buildRunID,
  createDirectConversation,
  createVerifiedUser,
  getNotificationPreferences,
  normalizeNotificationPreferences,
  sendTextMessage,
  updateNotificationPreferences,
};
