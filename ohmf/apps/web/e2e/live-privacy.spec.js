const { test, expect } = require("@playwright/test");

const { captureEvidence } = require("./helpers/evidence");
const {
  buildPhone,
  buildRunID,
  createDirectConversation,
  getNotificationPreferences,
  sendTextMessage,
  updateNotificationPreferences,
} = require("./helpers/live-api");
const {
  contextOptionsForProject,
  signInThroughOtp,
} = require("./helpers/live-ui");

const LIVE_ENABLED = process.env.OHMF_E2E_LIVE === "1";
const API_BASE_URL = process.env.OHMF_API_BASE_URL || "http://127.0.0.1:18080";

function isMobileProject(projectName = "") {
  return /mobile/i.test(projectName);
}

async function ensureSingleConversationOpen(page, mobile) {
  if (mobile) {
    const activeTitle = (await page.locator("#chat-title").textContent() || "").trim();
    if (activeTitle && activeTitle !== "Select a conversation" && !await page.locator(".thread-item").first().isVisible()) {
      return;
    }
  }
  await expect(page.locator(".thread-item")).toHaveCount(1);
  await page.locator(".thread-item").first().click();
  await expect(page.locator("#chat-title")).not.toHaveText("Select a conversation");
}

async function ensureSidebarVisible(page, mobile) {
  if (!mobile) return;
  if (await page.locator("#linked-devices-btn").isVisible()) return;
  await page.locator("#back-btn").click();
}

async function openDeviceManager(page, mobile) {
  await ensureSidebarVisible(page, mobile);
  await page.locator("#linked-devices-btn").click();
  await expect(page.locator("#privacy-settings-status")).toContainText(/privacy/i);
  await expect(page.locator("#privacy-read-receipts-toggle")).toBeEnabled();
  await expect(page.locator("#privacy-typing-toggle")).toBeEnabled();
}

async function closeDeviceManager(page) {
  await page.keyboard.press("Escape");
  await expect(page.locator("#device-manager-window")).toHaveClass(/hidden/);
}

async function waitForPrefs(accessToken, expected) {
  await expect.poll(async () => {
    const prefs = await getNotificationPreferences(API_BASE_URL, accessToken);
    return JSON.stringify([prefs.send_read_receipts, prefs.share_typing]);
  }, { timeout: 10000 }).toBe(JSON.stringify([expected.send_read_receipts, expected.share_typing]));
}

async function clearComposer(page) {
  const composer = page.locator("#composer-input");
  await composer.fill("");
}

async function lastOutgoingStatusText(page) {
  const statuses = page.locator("#message-list .bubble-wrap.out .delivery-status");
  const count = await statuses.count();
  if (!count) return "";
  return (await statuses.nth(count - 1).textContent() || "").trim();
}

function realtimeWebSocketURL(apiBaseURL, accessToken) {
  const wsURL = new URL(apiBaseURL.replace(/^http/i, "ws") + "/v2/ws");
  wsURL.searchParams.set("access_token", accessToken);
  return wsURL.toString();
}

async function openRealtimeClient(session) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(realtimeWebSocketURL(API_BASE_URL, session.accessToken));
    const client = { socket, messages: [], closed: false };
    let settled = false;

    const finish = (error) => {
      if (settled) return;
      settled = true;
      if (error) reject(error);
      else resolve(client);
    };

    socket.addEventListener("open", () => {
      socket.send(JSON.stringify({
        event: "hello",
        data: {
          device_id: session.deviceId,
          last_user_cursor: 0,
        },
      }));
    });

    socket.addEventListener("message", (event) => {
      let parsed;
      try {
        parsed = JSON.parse(event.data);
      } catch {
        return;
      }
      client.messages.push(parsed);
      if (parsed?.event === "hello_ack") {
        finish();
      }
    });

    socket.addEventListener("error", () => {
      finish(new Error("realtime socket error"));
    });

    socket.addEventListener("close", () => {
      client.closed = true;
      if (!settled) {
        finish(new Error("realtime socket closed before hello_ack"));
      }
    });
  });
}

function clearRealtimeMessages(client) {
  if (client?.messages) client.messages.length = 0;
}

async function waitForRealtimeEvent(client, eventName, predicate = () => true, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const index = client.messages.findIndex((message) => (
      message?.event === eventName
      && predicate(message?.data || {})
    ));
    if (index >= 0) {
      return client.messages.splice(index, 1)[0];
    }
    if (client.closed) {
      throw new Error(`realtime socket closed while waiting for ${eventName}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for realtime event ${eventName}`);
}

async function expectNoRealtimeEvent(client, eventName, predicate = () => true, timeout = 2000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const index = client.messages.findIndex((message) => (
      message?.event === eventName
      && predicate(message?.data || {})
    ));
    if (index >= 0) {
      const matched = client.messages.splice(index, 1)[0];
      throw new Error(`unexpected realtime event ${eventName}: ${JSON.stringify(matched?.data || {})}`);
    }
    if (client.closed) {
      throw new Error(`realtime socket closed while verifying absence of ${eventName}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

test.describe("live privacy enforcement", () => {
  test.skip(!LIVE_ENABLED, "Set OHMF_E2E_LIVE=1 to run real-stack browser coverage.");

  test("@live suppresses and restores typing and read receipts between two users", async ({ browser }, testInfo) => {
    test.setTimeout(90000);
    const mobile = isMobileProject(testInfo.project.name);
    const runID = buildRunID();
    const alicePhone = buildPhone(runID, "31");
    const bobPhone = buildPhone(runID, "32");

    const aliceContext = await browser.newContext(contextOptionsForProject(testInfo.project.name));
    const bobContext = await browser.newContext(contextOptionsForProject(testInfo.project.name));
    const alicePage = await aliceContext.newPage();
    const bobPage = await bobContext.newPage();
    let aliceRealtime = null;
    let bobRealtime = null;

    try {
      const alice = await signInThroughOtp(alicePage, alicePhone, API_BASE_URL);
      const bob = await signInThroughOtp(bobPage, bobPhone, API_BASE_URL);
      const conversation = await createDirectConversation(API_BASE_URL, alice.accessToken, bob.userId);
      await sendTextMessage(
        API_BASE_URL,
        alice.accessToken,
        conversation.conversationId,
        `privacy seed ${runID}`,
        `pw-live-privacy-seed-${runID}`
      );
      await alicePage.reload({ waitUntil: "domcontentloaded" });
      await bobPage.reload({ waitUntil: "domcontentloaded" });
      aliceRealtime = await openRealtimeClient(alice);
      bobRealtime = await openRealtimeClient(bob);

      await ensureSingleConversationOpen(alicePage, mobile);
      await ensureSingleConversationOpen(bobPage, mobile);

      await updateNotificationPreferences(API_BASE_URL, bob.accessToken, {
        send_read_receipts: false,
        share_typing: false,
      });
      await waitForPrefs(bob.accessToken, { send_read_receipts: false, share_typing: false });
      await openDeviceManager(bobPage, mobile);
      await expect(bobPage.locator("#privacy-read-receipts-toggle")).not.toBeChecked();
      await expect(bobPage.locator("#privacy-typing-toggle")).not.toBeChecked();
      await closeDeviceManager(bobPage);
      await ensureSingleConversationOpen(alicePage, mobile);
      await ensureSingleConversationOpen(bobPage, mobile);

      await bobPage.bringToFront();
      await clearComposer(bobPage);
      await bobPage.locator("#composer-input").type("typing hidden", { delay: 50 });
      await alicePage.bringToFront();
      await alicePage.waitForTimeout(1500);
      await expect(alicePage.locator("#message-list .bubble.typing")).toHaveCount(0);
      clearRealtimeMessages(aliceRealtime);
      bobRealtime.socket.send(JSON.stringify({
        event: "typing.started",
        data: { conversation_id: conversation.conversationId },
      }));
      await expectNoRealtimeEvent(
        aliceRealtime,
        "typing.started",
        (payload) => payload?.conversation_id === conversation.conversationId && payload?.user_id === bob.userId,
        2000
      );

      await clearComposer(bobPage);
      await bobPage.waitForTimeout(250);

      const hiddenReadText = `privacy hidden read ${runID}`;
      await alicePage.bringToFront();
      await alicePage.locator("#composer-input").fill(hiddenReadText);
      await alicePage.locator("#composer-input").press("Enter");
      await expect(alicePage.locator("#message-list")).toContainText(hiddenReadText);

      await bobPage.bringToFront();
      await expect(bobPage.locator("#message-list")).toContainText(hiddenReadText);
      await alicePage.waitForTimeout(2000);
      await expect.poll(async () => await lastOutgoingStatusText(alicePage), { timeout: 5000 }).not.toMatch(/read/i);

      await updateNotificationPreferences(API_BASE_URL, bob.accessToken, {
        send_read_receipts: true,
        share_typing: true,
      });
      await waitForPrefs(bob.accessToken, { send_read_receipts: true, share_typing: true });
      await openDeviceManager(bobPage, mobile);
      await expect(bobPage.locator("#privacy-read-receipts-toggle")).toBeChecked();
      await expect(bobPage.locator("#privacy-typing-toggle")).toBeChecked();
      await closeDeviceManager(bobPage);
      await ensureSingleConversationOpen(alicePage, mobile);
      await ensureSingleConversationOpen(bobPage, mobile);

      clearRealtimeMessages(aliceRealtime);
      bobRealtime.socket.send(JSON.stringify({
        event: "typing.started",
        data: { conversation_id: conversation.conversationId },
      }));
      await waitForRealtimeEvent(
        aliceRealtime,
        "typing.started",
        (payload) => payload?.conversation_id === conversation.conversationId && payload?.user_id === bob.userId,
        5000
      );
      bobRealtime.socket.send(JSON.stringify({
        event: "typing.stopped",
        data: { conversation_id: conversation.conversationId },
      }));
      await waitForRealtimeEvent(
        aliceRealtime,
        "typing.stopped",
        (payload) => payload?.conversation_id === conversation.conversationId && payload?.user_id === bob.userId,
        5000
      );

      const visibleReadText = `privacy visible read ${runID}`;
      await alicePage.bringToFront();
      await alicePage.locator("#composer-input").fill(visibleReadText);
      await alicePage.locator("#composer-input").press("Enter");
      await expect(alicePage.locator("#message-list")).toContainText(visibleReadText);

      await bobPage.bringToFront();
      await expect(bobPage.locator("#message-list")).toContainText(visibleReadText);
      await expect.poll(async () => await lastOutgoingStatusText(alicePage), { timeout: 10000 }).toMatch(/read/i);

      await captureEvidence(alicePage, testInfo, "live-privacy-alice");
      await captureEvidence(bobPage, testInfo, "live-privacy-bob");
    } finally {
      if (aliceRealtime?.socket && aliceRealtime.socket.readyState === WebSocket.OPEN) {
        aliceRealtime.socket.close();
      }
      if (bobRealtime?.socket && bobRealtime.socket.readyState === WebSocket.OPEN) {
        bobRealtime.socket.close();
      }
      await aliceContext.close();
      await bobContext.close();
    }
  });
});
