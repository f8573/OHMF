"use strict";

async function seedApiBaseURL(page, apiBaseURL) {
  const parsed = new URL(apiBaseURL);
  const apiBase = parsed.toString().replace(/\/+$/, "");
  const apiPort = parsed.port || (parsed.protocol === "https:" ? "443" : "80");
  const frontendBase = process.env.OHMF_E2E_BASE_URL || "http://127.0.0.1:5173";
  const frontendURL = new URL(frontendBase);
  const frontendPort = frontendURL.port || (frontendURL.protocol === "https:" ? "443" : "80");
  const frontendOrigin = `${frontendURL.protocol}//${frontendURL.hostname}:${frontendPort}`;

  await page.route("**/runtime-config.js", async (route) => {
    await route.fulfill({
      contentType: "application/javascript",
      body: `window.OHMF_RUNTIME_CONFIG = Object.freeze({
  frontend_port: "${frontendPort}",
  api_host_port: "${apiPort}",
  api_base_url: "${apiBase}",
  developer_mode: true,
  miniapp_sandbox_port: "${frontendPort}",
  miniapp_sandbox_url: "${frontendOrigin}",
  asset_version: "playwright-live"
});`,
    });
  });

  await page.addInitScript(({ seededApiBaseURL }) => {
    try {
      if (window.top === window) {
        let seededConfig = {};
        try {
          const parsed = new URL(seededApiBaseURL);
          const inferredPort = parsed.port || (parsed.protocol === "https:" ? "443" : "80");
          seededConfig = {
            ...(window.OHMF_WEB_CONFIG || {}),
            api_base_url: parsed.toString().replace(/\/+$/, ""),
            api_host_port: inferredPort,
          };
        } catch {}
        window.OHMF_WEB_CONFIG = seededConfig;
        window.localStorage.setItem("ohmf.api_host_port", seededConfig.api_host_port || "");
        window.localStorage.setItem("ohmf.apiBaseUrl", seededApiBaseURL);
      }
    } catch {}
  }, {
    seededApiBaseURL: apiBase,
  });
}

function contextOptionsForProject(projectName = "") {
  return /mobile/i.test(projectName)
    ? {
        viewport: { width: 390, height: 844 },
        isMobile: true,
        hasTouch: true,
      }
    : {
        viewport: { width: 1600, height: 900 },
      };
}

async function bootstrapAuthenticatedPage(page, session, apiBaseURL) {
  await page.addInitScript(({ seededSession }) => {
    try {
      if (window.top === window) {
        window.sessionStorage.setItem("ohmf.auth.session.v1", JSON.stringify(seededSession));
        window.localStorage.removeItem("ohmf.dev_apps");
      }
    } catch {}
  }, {
    seededSession: session,
  });
  await seedApiBaseURL(page, apiBaseURL);
  await page.goto("/");
}

async function signInThroughOtp(page, phoneE164, apiBaseURL) {
  await seedApiBaseURL(page, apiBaseURL);
  await page.goto("/");
  await page.locator("#phone-input").fill(String(phoneE164 || "").replace(/^\+1/, ""));
  await page.locator("#phone-start-form").getByRole("button", { name: "Send OTP" }).click();
  await page.locator("#otp-input").fill("123456");
  await page.locator("#phone-verify-form").getByRole("button", { name: "Verify and Continue" }).click();
  await page.locator("#app-shell").waitFor({ state: "visible" });
  return page.evaluate(() => {
    const raw = window.sessionStorage.getItem("ohmf.auth.session.v1");
    return raw ? JSON.parse(raw) : null;
  });
}

module.exports = {
  bootstrapAuthenticatedPage,
  contextOptionsForProject,
  seedApiBaseURL,
  signInThroughOtp,
};
