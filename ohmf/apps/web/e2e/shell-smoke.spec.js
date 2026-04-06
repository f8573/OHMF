const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const { installAuthenticatedAppMocks } = require("./helpers/mocks");

test.describe("authenticated shell", () => {
  test("@smoke renders the main messaging shell with seeded conversations", async ({ page }, testInfo) => {
    await installAuthenticatedAppMocks(page);

    await page.goto("/");

    const groupThread = page.locator(".thread-item").filter({
      has: page.locator(".thread-name", { hasText: "Project Nightfall" })
    });
    const dmThread = page.locator(".thread-item").filter({
      has: page.locator(".thread-name", { hasText: "Morgan" })
    });

    await expect(groupThread).toHaveCount(1);
    await expect(dmThread).toHaveCount(1);

    await groupThread.click();

    await expect(page.locator("#chat")).toBeVisible();
    await expect(page.locator("#chat-title")).toContainText("Project Nightfall");
    await expect(page.locator("#group-details-btn")).toBeEnabled();

    await captureEvidence(page, testInfo, "authenticated-shell");
  });
});
