(function attachEightballPermissions(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFEightballPermissions = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createEightballPermissions() {
  "use strict";

  const CAPABILITY_ACTIONS = Object.freeze({
    "realtime.session": "recording shots",
    "conversation.send_message": "projecting the latest summary",
    "storage.session": "saving local notes",
    "conversation.read_context": "refreshing conversation context",
  });

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/[\u0000-\u001f\u007f]/g, "").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function missingCapabilities(granted, required) {
    const grantedSet = new Set(Array.isArray(granted) ? granted.map((value) => sanitizeText(value, 120)).filter(Boolean) : []);
    return (Array.isArray(required) ? required : [])
      .map((value) => sanitizeText(value, 120))
      .filter((value) => value && !grantedSet.has(value));
  }

  function describeBlockedActions(granted) {
    const missingWrite = missingCapabilities(granted, ["realtime.session"]);
    const missingProject = missingCapabilities(granted, ["conversation.send_message"]);
    const missingDraft = missingCapabilities(granted, ["storage.session"]);
    const missingRefresh = missingCapabilities(granted, ["conversation.read_context"]);
    const missing = Array.from(new Set([
      ...missingWrite,
      ...missingProject,
      ...missingDraft,
      ...missingRefresh,
    ])).sort();
    const notices = missing.map((capability) => CAPABILITY_ACTIONS[capability] || capability);
    return {
      writeDisabled: missingWrite.length > 0,
      projectDisabled: missingProject.length > 0,
      draftDisabled: missingDraft.length > 0,
      refreshDisabled: missingRefresh.length > 0,
      missing,
      blockedSummary: notices.length ? `Blocked: host denied ${notices.join(", ")}.` : "",
    };
  }

  function permissionErrorMessage(error) {
    const message = sanitizeText(error && error.message, 180);
    const capability = sanitizeText(error && error.details && error.details.required_capability, 120);
    if (capability) {
      return `Blocked: host denied ${capability}.`;
    }
    if (message) {
      return message.startsWith("Blocked:") ? message : `Blocked: ${message}`;
    }
    return "Blocked: required permission was denied by the host.";
  }

  return {
    describeBlockedActions,
    missingCapabilities,
    permissionErrorMessage,
  };
}));
