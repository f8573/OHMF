const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const {
  buildPhone,
  buildRunID,
  createDirectConversation,
  createVerifiedUser,
  sendTextMessage,
} = require("./helpers/live-api");
const { signInThroughOtp } = require("./helpers/live-ui");

const LIVE_ENABLED = process.env.OHMF_E2E_LIVE === "1";
const API_BASE_URL = process.env.OHMF_API_BASE_URL || "http://127.0.0.1:18080";

test.describe("live auth and shell", () => {
  test.skip(!LIVE_ENABLED, "Set OHMF_E2E_LIVE=1 to run real-stack browser coverage.");

  test("@live signs in through OTP and renders a live seeded conversation", async ({ page }, testInfo) => {
    const isMobile = /mobile/i.test(testInfo.project.name);
    const runID = buildRunID();
    const primaryPhone = buildPhone(runID, "01");
    const peer = await createVerifiedUser(API_BASE_URL, buildPhone(runID, "02"), "Playwright Live Peer");

    const session = await signInThroughOtp(page, primaryPhone, API_BASE_URL);
    await expect(page.locator("#app-shell")).toBeVisible();
    await expect(page.locator("#linked-devices-btn")).toBeVisible();
    expect(session?.accessToken).toBeTruthy();

    const conversation = await createDirectConversation(API_BASE_URL, session.accessToken, peer.userId);
    const messageText = `Live stack hello ${runID}`;
    await sendTextMessage(
      API_BASE_URL,
      session.accessToken,
      conversation.conversationId,
      messageText,
      `pw-live-${runID}`
    );

    await page.reload({ waitUntil: "domcontentloaded" });
    await expect(page.locator(".thread-item")).toHaveCount(1);
    await expect(page.locator("#thread-list")).toContainText(messageText);

    await page.locator(".thread-item").first().click();
    await expect(page.locator("#message-list")).toContainText(messageText);

    if (isMobile) {
      await page.locator("#back-btn").click();
    }
    await page.locator("#linked-devices-btn").click();
    await expect(page.locator("#linked-device-list")).toContainText("OHMF Web");

    await captureEvidence(page, testInfo, "live-auth-shell");

    await page.keyboard.press("Escape");
    await page.locator("#logout-btn").click();
    await expect(page.locator("#auth-shell")).toBeVisible();
  });
});
