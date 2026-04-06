function runtimePorts() {
  const baseURL = new URL(process.env.OHMF_E2E_BASE_URL || "http://127.0.0.1:5173");
  return {
    frontendPort: baseURL.port || (baseURL.protocol === "https:" ? "443" : "80"),
    sandboxPort: String(process.env.MINIAPP_SANDBOX_PORT || "5174"),
    apiBaseUrl: (process.env.OHMF_API_BASE_URL || "http://127.0.0.1:18080").replace(/\/+$/, ""),
  };
}

async function seedRuntimeConfig(page) {
  const { frontendPort, sandboxPort, apiBaseUrl } = runtimePorts();
  await page.route("**/runtime-config.js", async (route) => {
    await route.fulfill({
      contentType: "application/javascript",
      body: `window.OHMF_RUNTIME_CONFIG = Object.freeze({
  frontend_port: "${frontendPort}",
  api_host_port: "${new URL(apiBaseUrl).port || "18080"}",
  api_base_url: "${apiBaseUrl}",
  miniapp_sandbox_port: "${sandboxPort}",
  miniapp_sandbox_url: "http://127.0.0.1:${sandboxPort}",
  asset_version: "playwright-runtime"
});`,
    });
  });
}

async function resetMiniAppRuntimeStorage(page) {
  await seedRuntimeConfig(page);
  await page.goto("/");
  await page.evaluate(() => {
    try {
      if (window.top === window) {
        window.localStorage.clear();
        window.sessionStorage.clear();
      }
    } catch {}
  });
}

async function loadMiniAppManifest(page, manifestUrl, expectedAppId = "") {
  const inferredAppId = manifestUrl.includes("eightball")
    ? "app.ohmf.eightball"
    : manifestUrl.includes("counter")
    ? "app.ohmf.counter-lab"
    : "";
  const targetAppId = expectedAppId || inferredAppId;
  const runtimeUrl = new URL("/miniapp-runtime.html", process.env.OHMF_E2E_BASE_URL || "http://127.0.0.1:5173");
  runtimeUrl.searchParams.set("manifest", manifestUrl);
  await page.goto(runtimeUrl.toString());
  await page.locator("#manifest-url").waitFor({ state: "visible" });
  await page.waitForFunction((expected) => {
    const input = document.querySelector("#manifest-url");
    return Boolean(input) && input.value === expected;
  }, manifestUrl);
  await page.locator("#manifest-json").waitFor();
  if (targetAppId) {
    await page.locator("#manifest-json").waitFor({ state: "visible" });
    await page.locator("#manifest-json").filter({ hasText: targetAppId }).waitFor();
  } else {
    await page.locator(".permission-item").first().waitFor();
  }
}

function eightballFrame(page) {
  return page.frameLocator("#app-frame");
}

module.exports = {
  eightballFrame,
  loadMiniAppManifest,
  resetMiniAppRuntimeStorage,
  seedRuntimeConfig,
};
