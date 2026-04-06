const { defineConfig, devices } = require("@playwright/test");

const BASE_URL = process.env.OHMF_E2E_BASE_URL || "http://127.0.0.1:5173";
const API_BASE_URL = process.env.OHMF_API_BASE_URL || "http://127.0.0.1:18080";
const FRONTEND_PORT = String(process.env.CLIENT_PORT || new URL(BASE_URL).port || "5173");
const SANDBOX_PORT = String(process.env.MINIAPP_SANDBOX_PORT || "5174");

module.exports = defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 2 : 1,
  reporter: [["list"], ["html", { open: "never", outputFolder: "playwright-report" }]],
  timeout: 45_000,
  expect: {
    timeout: 10_000,
    toHaveScreenshot: {
      maxDiffPixelRatio: 0.02,
    },
  },
  use: {
    baseURL: BASE_URL,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    video: "retain-on-failure",
    viewport: { width: 1600, height: 900 },
    ignoreHTTPSErrors: true,
  },
  webServer: {
    command: "py dev_server.py",
    cwd: __dirname,
    url: BASE_URL,
    reuseExistingServer: true,
    timeout: 30_000,
    env: {
      CLIENT_PORT: FRONTEND_PORT,
      MINIAPP_SANDBOX_PORT: SANDBOX_PORT,
    },
  },
  metadata: {
    ohmfBaseUrl: BASE_URL,
    ohmfApiBaseUrl: API_BASE_URL,
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        baseURL: BASE_URL,
      },
    },
    {
      name: "firefox",
      use: {
        ...devices["Desktop Firefox"],
        baseURL: BASE_URL,
      },
    },
    {
      name: "webkit",
      use: {
        ...devices["Desktop Safari"],
        baseURL: BASE_URL,
      },
    },
    {
      name: "mobile-chromium",
      use: {
        ...devices["Pixel 7"],
        baseURL: BASE_URL,
      },
    },
  ],
});
