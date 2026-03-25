(function attachTrustVerificationHelpers(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFTrustUI = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createTrustVerificationHelpers() {
  "use strict";

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/\s+/g, " ").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function normalizeTrustState(value) {
    const normalized = sanitizeText(value, 24).toUpperCase();
    if (!normalized || normalized === "UNKNOWN" || normalized === "TOFU") return "UNVERIFIED";
    if (normalized === "BLOCKED") return "REVOKED";
    if (normalized === "VERIFIED" || normalized === "REVOKED" || normalized === "MISMATCH" || normalized === "UNVERIFIED") {
      return normalized;
    }
    return "UNVERIFIED";
  }

  function trustStateBadgeLabel(state) {
    const normalized = normalizeTrustState(state);
    if (normalized === "VERIFIED") return "Verified";
    if (normalized === "REVOKED") return "Revoked";
    if (normalized === "MISMATCH") return "Mismatch";
    return "Unverified";
  }

  function trustStateWarning(state) {
    const normalized = normalizeTrustState(state);
    if (normalized === "MISMATCH") {
      return "Fingerprint changed since the last trusted state. Review this device before trusting it again.";
    }
    if (normalized === "REVOKED") {
      return "Verification was revoked. This device should not be treated as trusted.";
    }
    return "";
  }

  function deviceLabel(bundle) {
    const explicit = sanitizeText(bundle && bundle.device_name, 80);
    if (explicit) return explicit;
    const deviceId = sanitizeText(bundle && bundle.device_id, 80);
    if (!deviceId) return "Unknown device";
    return `Device ${deviceId.slice(0, 8)}`;
  }

  function trustStateRank(state) {
    const normalized = normalizeTrustState(state);
    if (normalized === "MISMATCH") return 0;
    if (normalized === "REVOKED") return 1;
    if (normalized === "UNVERIFIED") return 2;
    return 3;
  }

  function normalizeTrustDevice(bundle, rawTrust) {
    const deviceId = sanitizeText(bundle && bundle.device_id, 80);
    if (!deviceId) return null;
    const currentFingerprint = sanitizeText((rawTrust && rawTrust.current_fingerprint) || (bundle && bundle.fingerprint), 128);
    const recordedFingerprint = sanitizeText((rawTrust && rawTrust.recorded_fingerprint) || (rawTrust && rawTrust.fingerprint), 128);
    const state = normalizeTrustState((rawTrust && rawTrust.effective_trust_state) || (rawTrust && rawTrust.trust_state));
    const warning = sanitizeText(rawTrust && rawTrust.warning, 220) || trustStateWarning(state);
    return {
      deviceId,
      deviceLabel: deviceLabel(bundle),
      currentFingerprint,
      recordedFingerprint,
      trustState: state,
      trustStateLabel: trustStateBadgeLabel(state),
      warning,
      verifiedAt: sanitizeText(rawTrust && rawTrust.verified_at, 80),
      canVerify: Boolean(currentFingerprint) && state !== "VERIFIED",
      canRevoke: Boolean(currentFingerprint) && (state === "VERIFIED" || state === "MISMATCH"),
    };
  }

  function normalizeTrustDevices(rawBundles, rawTrustByDevice) {
    const bundles = Array.isArray(rawBundles) ? rawBundles : [];
    const trustByDevice = rawTrustByDevice && typeof rawTrustByDevice === "object" ? rawTrustByDevice : {};
    return bundles
      .map((bundle) => normalizeTrustDevice(bundle, trustByDevice[sanitizeText(bundle && bundle.device_id, 80)]))
      .filter(Boolean)
      .sort((left, right) => {
        const rankDiff = trustStateRank(left.trustState) - trustStateRank(right.trustState);
        if (rankDiff !== 0) return rankDiff;
        return left.deviceLabel.localeCompare(right.deviceLabel);
      });
  }

  function summarizeTrustDevices(devices) {
    const items = Array.isArray(devices) ? devices : [];
    if (!items.length) return "No secure devices are published for this contact yet.";
    const mismatchCount = items.filter((item) => item.trustState === "MISMATCH").length;
    if (mismatchCount) return `${mismatchCount} device${mismatchCount === 1 ? "" : "s"} need trust review.`;
    const revokedCount = items.filter((item) => item.trustState === "REVOKED").length;
    if (revokedCount) return `${revokedCount} device${revokedCount === 1 ? "" : "s"} remain revoked.`;
    const verifiedCount = items.filter((item) => item.trustState === "VERIFIED").length;
    if (verifiedCount === items.length) return `All ${items.length} device${items.length === 1 ? "" : "s"} verified.`;
    return `${verifiedCount} of ${items.length} device${items.length === 1 ? "" : "s"} verified.`;
  }

  return {
    normalizeTrustDevice,
    normalizeTrustDevices,
    summarizeTrustDevices,
    trustStateBadgeLabel,
    trustStateWarning,
  };
}));
