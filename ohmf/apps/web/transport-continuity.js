(function attachTransportContinuityHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFTransportUI = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createTransportContinuityHelpers() {
  "use strict";

  const TRANSPORT_SMS = "SMS";
  const TRANSPORT_OHMF = "OHMF";

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/\s+/g, " ").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function normalizeTransport(value) {
    return sanitizeText(value, 24).toUpperCase() === TRANSPORT_SMS ? TRANSPORT_SMS : TRANSPORT_OHMF;
  }

  function hasTransportHistory(messages, transport) {
    const target = normalizeTransport(transport);
    return Array.isArray(messages) && messages.some((message) => normalizeTransport(message && message.transport) === target);
  }

  function threadTransportSummary(thread) {
    const kind = sanitizeText(thread && thread.kind, 24).toLowerCase();
    const e2eeReady = Boolean(thread && thread.e2eeReady);
    const hasPhoneHistory = hasTransportHistory(thread && thread.messages, TRANSPORT_SMS);
    const hasSecureHistory = hasTransportHistory(thread && thread.messages, TRANSPORT_OHMF);

    if (kind === "draft_phone" || kind === "phone") {
      return {
        label: "Phone delivery",
        subtitle: e2eeReady ? "Phone delivery while promotion finishes" : "Phone delivery",
        tone: "phone",
        promoted: false,
        hasPhoneHistory,
        hasSecureHistory,
      };
    }

    if (kind === "dm") {
      if (!e2eeReady) {
        return {
          label: "OHMF setup pending",
          subtitle: "Waiting for OHMF setup",
          tone: "pending",
          promoted: false,
          hasPhoneHistory,
          hasSecureHistory,
        };
      }
      return {
        label: "Secure OHMF",
        subtitle: hasPhoneHistory ? "Promoted from phone delivery" : "Secure OHMF delivery",
        tone: "secure",
        promoted: hasPhoneHistory && hasSecureHistory,
        hasPhoneHistory,
        hasSecureHistory,
      };
    }

    return null;
  }

  function messageTransportBadge(message) {
    const transport = normalizeTransport(message && message.transport);
    return {
      label: transport === TRANSPORT_SMS ? "Phone delivery" : "Secure OHMF",
      tone: transport === TRANSPORT_SMS ? "phone" : "secure",
      transport,
    };
  }

  function formatOutgoingStatusLabel(message, fallbackLabel) {
    const transport = normalizeTransport(message && message.transport);
    const status = sanitizeText(message && message.status, 40).toUpperCase();
    const fallback = sanitizeText(fallbackLabel, 160);

    if (transport === TRANSPORT_SMS) {
      if (status === "FAIL_SEND") return "Phone delivery failed. Retry to resend.";
      return fallback || "Sent";
    }

    if (status === "FAIL_SEND") return "Secure OHMF failed. Retry to resend.";
    if (status === "FAIL_DELIVERY") return "Secure OHMF delivery failed.";
    return fallback;
  }

  function messageFailureDetail(message) {
    const transport = normalizeTransport(message && message.transport);
    const decryptStatus = sanitizeText(message && message.decryptStatus, 40).toLowerCase();
    if (transport !== TRANSPORT_OHMF) return null;
    if (decryptStatus === "other_device") {
      return {
        label: "This secure message is for another linked device.",
        tone: "warning",
      };
    }
    if (decryptStatus === "error") {
      return {
        label: "This secure message could not be decrypted on this device.",
        tone: "warning",
      };
    }
    return null;
  }

  return {
    formatOutgoingStatusLabel,
    messageFailureDetail,
    messageTransportBadge,
    normalizeTransport,
    threadTransportSummary,
  };
}));
