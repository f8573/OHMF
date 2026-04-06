const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const {
  eightballFrame,
  loadMiniAppManifest,
  resetMiniAppRuntimeStorage,
  seedRuntimeConfig,
} = require("./helpers/miniapp-runtime");

test.describe("8 Ball Pool runtime", () => {
  test("launches in the runtime, keeps exact copy, and projects only the latest summary", async ({ page }, testInfo) => {
    await resetMiniAppRuntimeStorage(page);
    await loadMiniAppManifest(page, "./miniapps/eightball/manifest.json");

    const frame = eightballFrame(page);
    await expect(frame.getByRole("heading", { name: "8 Ball Pool" })).toBeVisible();
    await expect(frame.locator("#status-pill")).toHaveText("Mini-app ready.");
    await expect(frame.locator(".subcopy")).toHaveText(
      "Shared rack state, pocket calling, turn flow, and thread projection in a player-facing match UI."
    );
    const clickOptions = /mobile/i.test(testInfo.project.name) ? { force: true } : {};
    await expect(frame.locator("#playfield-canvas")).toBeVisible();

    await frame.locator("#start-btn").click(clickOptions);
    await expect(frame.locator("#table-summary")).toHaveText("Avery breaks.");
    await expect(frame.locator("#turn-pill")).toHaveText("Avery to shoot");

    await expect(frame.locator("#submit-shot-btn")).toHaveText("Record Break");
    await frame.locator("#submit-shot-btn").click(clickOptions);
    await expect(frame.locator("#table-summary")).toHaveText("Avery pockets a ball on the break and keeps shooting.");

    await expect(frame.locator("#submit-shot-btn")).toHaveText("Submit Shot");
    await frame.locator("#submit-shot-btn").click(clickOptions);
    await expect(frame.locator("#table-summary")).toHaveText(
      "Avery claims solids and keeps shooting. 6 object balls and the 8-ball remain."
    );
    await expect(frame.locator("#scoreboard")).toContainText("Solids · 7 balls left · 0 fouls");

    await frame.locator("#project-btn").click(clickOptions);

    await expect(page.locator("#transcript-list")).toContainText(
      "Avery claims solids and keeps shooting. 6 object balls and the 8-ball remain."
    );
    await captureEvidence(page, testInfo, "eightball-runtime");
  });

  test("disables mandatory actions when the host denies capabilities", async ({ page }) => {
    await resetMiniAppRuntimeStorage(page);
    await loadMiniAppManifest(page, "./miniapps/eightball/manifest.json");

    const permissions = page.locator(".permission-item");
    for (const capability of [
      "conversation.read_context",
      "conversation.send_message",
      "realtime.session",
      "storage.session",
    ]) {
      await permissions
        .filter({ hasText: capability })
        .locator("input")
        .evaluate((input) => {
          input.checked = false;
          input.dispatchEvent(new Event("change", { bubbles: true }));
        });
    }
    await page.getByRole("button", { name: "Relaunch" }).click();

    const frame = eightballFrame(page);
    await expect(frame.locator("#status-pill")).toHaveText(
      "Blocked: host denied refreshing conversation context, projecting the latest summary, recording shots, saving local notes."
    );
    await expect(frame.getByRole("button", { name: "Rack Match" })).toBeDisabled();
    await expect(frame.getByRole("button", { name: "Submit Shot" })).toBeDisabled();
    await expect(frame.getByRole("button", { name: "Project Status" })).toBeDisabled();
    await expect(frame.getByRole("button", { name: "Refresh" })).toBeDisabled();
  });

  test("supports preview mode with exact copy and stable visuals", async ({ page }) => {
    await seedRuntimeConfig(page);
    await page.goto("/miniapps/eightball/index.html?preview=1");
    await expect(page.locator("#preview-shell")).toBeVisible();
    await expect(page.locator("#preview-answer-text")).toHaveText("Open the table.");
    await expect(page.locator("#preview-caption")).toHaveText("Shared match state ready for the thread.");
    await expect(page.locator("#preview-shell .preview-card")).toHaveScreenshot("eightball-preview-card.png");
  });
});
