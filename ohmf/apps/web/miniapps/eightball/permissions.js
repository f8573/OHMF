(function attachEightballPermissions(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFEightballPermissions = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createEightballPermissions() {
  "use strict";

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
    const missingAsk = missingCapabilities(granted, ["realtime.session"]);
    const missingSend = missingCapabilities(granted, ["conversation.send_message"]);
    const missingDraft = missingCapabilities(granted, ["storage.session"]);
    const missingRefresh = missingCapabilities(granted, ["conversation.read_context"]);
    const notices = [];
    if (missingAsk.length) notices.push("asking new questions");
    if (missingSend.length) notices.push("sending answers to the thread");
    if (missingDraft.length) notices.push("saving draft questions");
    if (missingRefresh.length) notices.push("refreshing conversation context");
    return {
      askDisabled: missingAsk.length > 0,
      sendDisabled: missingSend.length > 0,
      draftDisabled: missingDraft.length > 0,
      refreshDisabled: missingRefresh.length > 0,
      missing: Array.from(new Set([
        ...missingAsk,
        ...missingSend,
        ...missingDraft,
        ...missingRefresh,
      ])).sort(),
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
