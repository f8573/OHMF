function base64UrlEncode(value) {
  return Buffer.from(JSON.stringify(value))
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function buildFakeJwt(claims = {}) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    sub: "user-1",
    exp: now + 60 * 60,
    iat: now,
    ...claims,
  };
  return `${base64UrlEncode({ alg: "none", typ: "JWT" })}.${base64UrlEncode(payload)}.signature`;
}

function buildAuthSession(overrides = {}) {
  return {
    accessToken: buildFakeJwt({ sub: overrides.userId || "user-1" }),
    refreshToken: overrides.refreshToken || "refresh-token",
    userId: overrides.userId || "user-1",
    deviceId: overrides.deviceId || "device-web-1",
    phoneE164: overrides.phoneE164 || "+15550001111",
    ...overrides,
  };
}

module.exports = {
  buildAuthSession,
  buildFakeJwt,
};
