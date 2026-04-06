const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const { installAuthenticatedAppMocks } = require("./helpers/mocks");

test.describe("device management and privacy", () => {
  test("renders linked devices, activity, and account privacy toggles", async ({ page }, testInfo) => {
    await installAuthenticatedAppMocks(page);

    await page.goto("/");
    await page.locator("#linked-devices-btn").click();

    await expect(page.locator("#linked-device-list")).toContainText("This browser");
    await expect(page.locator("#device-activity-list")).toContainText("Linked a new device.");

    const readReceipts = page.locator("#privacy-read-receipts-toggle");
    await expect(readReceipts).toBeChecked();
    await readReceipts.evaluate((input) => {
      input.checked = false;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    });
    await expect(readReceipts).not.toBeChecked();

    const typing = page.locator("#privacy-typing-toggle");
    await typing.evaluate((input) => {
      input.checked = false;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    });
    await expect(typing).not.toBeChecked();

    await captureEvidence(page, testInfo, "device-manager-privacy");
  });
});
