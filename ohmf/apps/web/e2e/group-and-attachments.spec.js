const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const { installAuthenticatedAppMocks } = require("./helpers/mocks");

test.describe("group details and attachment draft", () => {
  test("opens group details and stages an attachment draft before send", async ({ page }, testInfo) => {
    await installAuthenticatedAppMocks(page);

    await page.goto("/");
    await page.locator(".thread-item").filter({
      has: page.locator(".thread-name", { hasText: "Project Nightfall" })
    }).click();
    await page.locator("#group-details-btn").click();

    await expect(page.locator("#group-manager-name")).toContainText("Project Nightfall");
    await expect(page.locator("#group-member-list")).toContainText("Casey");
    await expect(page.locator("#group-member-list")).toContainText("Riley");

    await page.setInputFiles("#attachment-input", {
      name: "table-layout.png",
      mimeType: "image/png",
      buffer: Buffer.from("fake-image-content")
    });

    await expect(page.locator("#composer-attachment")).toBeVisible();
    await expect(page.locator("#composer-attachment-label")).toContainText(/Attachment/i);
    await expect(page.locator("#composer-attachment-text")).toContainText("table-layout.png");

    await captureEvidence(page, testInfo, "group-manager-and-attachment");
  });
});
