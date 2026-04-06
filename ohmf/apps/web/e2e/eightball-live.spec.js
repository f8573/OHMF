const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const { buildPhone, buildRunID } = require("./helpers/live-api");
const { eightballFrame, loadMiniAppManifest, resetMiniAppRuntimeStorage } = require("./helpers/miniapp-runtime");
const { signInThroughOtp } = require("./helpers/live-ui");

const LIVE_ENABLED = process.env.OHMF_E2E_LIVE === "1";
const API_BASE_URL = process.env.OHMF_API_BASE_URL || "http://127.0.0.1:18080";

test.describe("8 Ball Pool live bridge", () => {
  test.skip(!LIVE_ENABLED, "Set OHMF_E2E_LIVE=1 to run real-stack browser coverage.");

  test("@live launches through the runtime bridge, uses a gateway session, and survives relaunch", async ({ page }, testInfo) => {
    const runID = buildRunID();
    const phone = buildPhone(runID, "31");
    const liveAppId = `app.ohmf.eightball.live.${runID}`;

    await resetMiniAppRuntimeStorage(page);
    const session = await signInThroughOtp(page, phone, API_BASE_URL);
    await page.route("**/miniapps/eightball/manifest.json", async (route) => {
      const response = await route.fetch();
      const manifest = await response.json();
      manifest.app_id = liveAppId;
      await route.fulfill({
        status: response.status(),
        contentType: "application/json",
        body: JSON.stringify(manifest),
      });
    });
    await loadMiniAppManifest(page, "./miniapps/eightball/manifest.json", liveAppId);

    await expect(page.locator("#log-list")).toContainText("runtime.gateway_session");

    const frame = eightballFrame(page);
    await expect(frame.locator("#status-pill")).toHaveText("Mini-app ready.");

    await frame.getByRole("button", { name: "Start Match" }).click();
    await frame.getByRole("button", { name: "Pocket Ball" }).click();
    await frame.getByRole("button", { name: "Pocket Ball" }).click();

    const viewerLabel = session?.phoneE164 || phone;
    const summary = `${viewerLabel} claims solids and keeps shooting. 6 object balls and the 8-ball remain.`;
    await expect(frame.locator("#table-summary")).toHaveText(summary);
    await expect(page.locator("#transcript-list")).toContainText(summary);

    await page.getByRole("button", { name: "Relaunch" }).click();

    const relaunchedFrame = eightballFrame(page);
    await expect(relaunchedFrame.locator("#table-summary")).toHaveText(summary);
    await expect(relaunchedFrame.locator("#shared-line")).toHaveText(summary);
    await captureEvidence(page, testInfo, "eightball-live-runtime");
  });
});
