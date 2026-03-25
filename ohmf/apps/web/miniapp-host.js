(function attachMiniappHostHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFMiniappHost = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createMiniappHostHelpers() {
  "use strict";

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/[\u0000-\u001f\u007f]/g, "").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function buildRestoreState(input) {
    const threadId = sanitizeText(input && input.threadId, 120);
    const appId = sanitizeText(input && input.appId, 120);
    const appSessionId = sanitizeText(input && input.appSessionId, 120);
    if (!threadId || !appId || !appSessionId || input?.popupOpen !== true) {
      return null;
    }
    return {
      thread_id: threadId,
      app_id: appId,
      app_session_id: appSessionId,
      popup_open: true,
    };
  }

  function parseRestoreState(raw) {
    if (!raw) return null;
    try {
      const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
      if (!parsed || typeof parsed !== "object") return null;
      const threadId = sanitizeText(parsed.thread_id, 120);
      const appId = sanitizeText(parsed.app_id, 120);
      const appSessionId = sanitizeText(parsed.app_session_id, 120);
      if (!threadId || !appId || !appSessionId || parsed.popup_open !== true) {
        return null;
      }
      return {
        threadId,
        appId,
        appSessionId,
        popupOpen: true,
      };
    } catch {
      return null;
    }
  }

  function normalizeSessionEvent(raw) {
    if (!raw || typeof raw !== "object") return null;
    const sessionId = sanitizeText(raw.session_id, 120);
    const eventType = sanitizeText(raw.event_type, 80);
    if (!sessionId || !eventType) return null;
    return {
      sessionId,
      eventType,
      eventSeq: Number(raw.event_seq || 0) || 0,
      actorId: sanitizeText(raw.actor_id, 120),
      body: raw.body && typeof raw.body === "object" ? raw.body : {},
      createdAt: sanitizeText(raw.created_at, 80),
    };
  }

  function shouldRefreshSession(raw, activeSessionId) {
    const event = normalizeSessionEvent(raw);
    const current = sanitizeText(activeSessionId, 120);
    if (!event || !current) return false;
    return event.sessionId === current;
  }

  return {
    buildRestoreState,
    normalizeSessionEvent,
    parseRestoreState,
    shouldRefreshSession,
  };
}));
