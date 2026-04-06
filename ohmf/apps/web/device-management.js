(function attachDeviceManagementHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFDeviceUI = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createDeviceManagementHelpers() {
  "use strict";

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/\s+/g, " ").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function normalizeCapabilities(raw) {
    if (!Array.isArray(raw)) return [];
    const seen = new Set();
    const out = [];
    for (const value of raw) {
      const item = sanitizeText(value, 40).toUpperCase();
      if (!item || seen.has(item)) continue;
      seen.add(item);
      out.push(item);
    }
    return out;
  }

  function safeDateValue(raw) {
    const value = sanitizeText(raw, 80);
    if (!value) return "";
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
  }

  function normalizeLinkedDevices(rawDevices, currentDeviceId) {
    const current = sanitizeText(currentDeviceId, 80);
    const devices = Array.isArray(rawDevices) ? rawDevices : [];
    return devices
      .map((raw) => {
        const id = sanitizeText(raw && (raw.id || raw.device_id), 80);
        if (!id) return null;
        const platform = sanitizeText(raw && raw.platform, 24).toUpperCase() || "UNKNOWN";
        const deviceName = sanitizeText(raw && raw.device_name, 80) || `${platform} device`;
        const clientVersion = sanitizeText(raw && raw.client_version, 40);
        const lastSeenAt = safeDateValue(raw && raw.last_seen_at);
        const attestationType = sanitizeText(raw && raw.attestation_type, 40).toUpperCase();
        const attestationState = sanitizeText(raw && raw.attestation_state, 24).toUpperCase();
        const attestedAt = safeDateValue(raw && raw.attested_at);
        const attestationExpiresAt = safeDateValue(raw && raw.attestation_expires_at);
        const attestationLastError = sanitizeText(raw && raw.attestation_last_error, 200);
        const capabilities = normalizeCapabilities(raw && raw.capabilities);
        return {
          id,
          deviceName,
          platform,
          clientVersion,
          lastSeenAt,
          attestationType,
          attestationState,
          attestedAt,
          attestationExpiresAt,
          attestationLastError,
          capabilities,
          isCurrent: id === current,
        };
      })
      .filter(Boolean)
      .sort((left, right) => {
        if (left.isCurrent !== right.isCurrent) return left.isCurrent ? -1 : 1;
        if (left.lastSeenAt && right.lastSeenAt && left.lastSeenAt !== right.lastSeenAt) {
          return right.lastSeenAt.localeCompare(left.lastSeenAt);
        }
        if (left.lastSeenAt !== right.lastSeenAt) return left.lastSeenAt ? -1 : 1;
        return left.deviceName.localeCompare(right.deviceName);
      });
  }

  function describePairingError(error) {
    const code = sanitizeText(error && error.code, 80).toLowerCase();
    if (code === "invalid_pairing_code") {
      return {
        kind: "invalid",
        title: "Invalid pairing code.",
        message: "That pairing code was not recognized. Start a new code from one of your linked devices and try again.",
      };
    }
    if (code === "pairing_expired") {
      return {
        kind: "expired",
        title: "Pairing code expired.",
        message: "That pairing code has expired. Generate a fresh code from one of your linked devices to continue.",
      };
    }
    return {
      kind: "error",
      title: "Pairing failed.",
      message: sanitizeText(error && error.message, 200) || "Unable to pair this device right now.",
    };
  }

  return {
    describePairingError,
    normalizeLinkedDevices,
  };
}));
