const { buildAuthSession } = require("./auth");

function isoAt(minutesFromNow) {
  return new Date(Date.now() + minutesFromNow * 60 * 1000).toISOString();
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function defaultFixture() {
  const groupConversation = {
    conversation_id: "conv-group-1",
    type: "GROUP",
    title: "Project Nightfall",
    avatar_url: "https://images.example.test/group-nightfall.png",
    description: "Turn coordination, attachments, and launch demos.",
    viewer_role: "OWNER",
    participants: ["user-1", "user-2", "user-3"],
    updated_at: isoAt(-5),
    last_message_preview: "@Casey nice shot",
    unread_count: 0,
    encryption_state: "ENCRYPTED",
    encryption_epoch: 3,
    e2ee_ready: true,
    blocked: false,
    blocked_by_viewer: false,
    blocked_by_other: false,
    closed: false,
    archived: false,
    nickname: ""
  };
  const dmConversation = {
    conversation_id: "conv-dm-1",
    type: "DM",
    title: "",
    avatar_url: "",
    description: "",
    viewer_role: "MEMBER",
    participants: ["user-1", "user-4"],
    updated_at: isoAt(-8),
    last_message_preview: "Need the audit log screenshots.",
    unread_count: 1,
    encryption_state: "ENCRYPTED",
    encryption_epoch: 1,
    e2ee_ready: true,
    blocked: false,
    blocked_by_viewer: false,
    blocked_by_other: false,
    closed: false,
    archived: false,
    nickname: ""
  };

  return {
    session: buildAuthSession(),
    profiles: [
      { user_id: "user-1", display_name: "James", primary_phone_e164: "+15550001111" },
      { user_id: "user-2", display_name: "Casey", primary_phone_e164: "+15550002222" },
      { user_id: "user-3", display_name: "Riley", primary_phone_e164: "+15550003333" },
      { user_id: "user-4", display_name: "Morgan", primary_phone_e164: "+15550004444" }
    ],
    conversations: [groupConversation, dmConversation],
    messages: {
      "conv-group-1": [
        {
          message_id: "msg-group-1",
          conversation_id: "conv-group-1",
          sender_user_id: "user-2",
          sender_device_id: "device-ios-2",
          content_type: "text",
          content: {
            text: "@James take the next shot",
            mentions: [
              { user_id: "user-1", display: "James", start: 0, end: 6 }
            ]
          },
          transport: "OHMF",
          server_order: 11,
          status: "READ",
          created_at: isoAt(-20)
        }
      ],
      "conv-dm-1": [
        {
          message_id: "msg-dm-1",
          conversation_id: "conv-dm-1",
          sender_user_id: "user-4",
          sender_device_id: "device-ios-4",
          content_type: "text",
          content: { text: "Need the audit log screenshots." },
          transport: "OHMF",
          server_order: 4,
          status: "DELIVERED",
          created_at: isoAt(-15)
        }
      ]
    },
    privacyPrefs: {
      push_enabled: true,
      mute_unknown_senders: false,
      show_previews: true,
      muted_conversation_notifications: false,
      send_read_receipts: true,
      share_presence: true,
      share_typing: true
    },
    devices: [
      {
        id: "device-web-1",
        platform: "web",
        device_name: "This browser",
        client_version: "0.1.0",
        capabilities: ["web_push_v1", "miniapp_runtime_v1"],
        attestation_state: "verified",
        attestation_type: "passkey",
        attested_at: isoAt(-120),
        attestation_expires_at: isoAt(1440),
        last_seen_at: isoAt(-1)
      },
      {
        id: "device-ios-2",
        platform: "ios",
        device_name: "James iPhone",
        client_version: "0.1.0",
        capabilities: ["e2ee_ott_v2", "device_pairing_v1"],
        attestation_state: "verified",
        attestation_type: "devicecheck",
        attested_at: isoAt(-240),
        attestation_expires_at: isoAt(720),
        last_seen_at: isoAt(-12)
      }
    ],
    deviceActivity: [
      {
        id: 41,
        event_type: "device_pairing_completed",
        created_at: isoAt(-180),
        device_id: "device-ios-2",
        summary: "Linked a new device."
      },
      {
        id: 42,
        event_type: "device_trust_verified",
        created_at: isoAt(-60),
        device_id: "device-ios-2",
        summary: "Verified a contact device."
      }
    ],
    apps: [
      {
        app_id: "app.ohmf.eightball",
        title: "8 Ball Pool",
        summary: "Turn-based pool demo with shared table state.",
        source_type: "builtin",
        update_available: false,
        install: {
          installed: true,
          supported: true,
          update_requires_consent: false
        },
        manifest: {
          app_id: "app.ohmf.eightball",
          name: "8 Ball Pool",
          permissions: ["conversation.read_context", "storage.session.write"],
          metadata: {
            summary: "Turn-based pool demo with shared table state."
          },
          entrypoint: {
            url: "http://127.0.0.1:5174/eightball/index.html"
          }
        }
      }
    ]
  };
}

function mergeFixture(overrides = {}) {
  const base = defaultFixture();
  const merged = { ...base, ...clone(overrides) };
  merged.session = { ...base.session, ...(overrides.session || {}) };
  merged.privacyPrefs = { ...base.privacyPrefs, ...(overrides.privacyPrefs || {}) };
  if (!overrides.conversations) merged.conversations = base.conversations;
  if (!overrides.messages) merged.messages = base.messages;
  if (!overrides.profiles) merged.profiles = base.profiles;
  if (!overrides.devices) merged.devices = base.devices;
  if (!overrides.deviceActivity) merged.deviceActivity = base.deviceActivity;
  if (!overrides.apps) merged.apps = base.apps;
  return merged;
}

function buildUsersResolvePayload(profiles, requestedUserIds) {
  const requested = new Set((requestedUserIds || []).map((item) => String(item)));
  return {
    items: profiles.filter((item) => requested.has(item.user_id))
  };
}

async function installAuthenticatedAppMocks(page, overrides = {}) {
  const fixture = mergeFixture(overrides);
  const conversationsById = new Map(fixture.conversations.map((item) => [item.conversation_id, clone(item)]));
  let privacyPrefs = { ...fixture.privacyPrefs };

  await page.addInitScript(({ session }) => {
    try {
      if (window.top === window) {
        window.sessionStorage.setItem("ohmf.auth.session.v1", JSON.stringify(session));
        window.localStorage.setItem("ohmf.apiBaseUrl", "http://127.0.0.1:18080");
        window.localStorage.removeItem("ohmf.dev_apps");
      }
    } catch {}

    class MockWebSocket {
      constructor(url) {
        this.url = url;
        this.readyState = 1;
        this.listeners = new Map();
        setTimeout(() => this.#emit("open", {}), 0);
      }

      addEventListener(type, handler) {
        const handlers = this.listeners.get(type) || [];
        handlers.push(handler);
        this.listeners.set(type, handlers);
      }

      removeEventListener(type, handler) {
        const handlers = (this.listeners.get(type) || []).filter((item) => item !== handler);
        this.listeners.set(type, handlers);
      }

      send() {}

      close() {
        this.readyState = 3;
        this.#emit("close", {});
      }

      #emit(type, event) {
        const handlers = this.listeners.get(type) || [];
        for (const handler of handlers) handler(event);
        const direct = this[`on${type}`];
        if (typeof direct === "function") direct(event);
      }
    }

    window.WebSocket = MockWebSocket;
  }, { session: fixture.session });

  await page.route(/http:\/\/(127\.0\.0\.1|localhost):18080\/.*/, async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const path = url.pathname;

    const json = async (status, body) => route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(body)
    });

    if ((path === "/v1/conversations" || path === "/v2/conversations") && method === "GET") {
      return json(200, { items: Array.from(conversationsById.values()) });
    }

    if (path === "/v2/sync" && method === "GET") {
      return json(200, { events: [], next_cursor: 0, has_more: false });
    }

    if (/^\/v1\/conversations\/[^/]+\/messages$/.test(path) && method === "GET") {
      const conversationId = decodeURIComponent(path.split("/")[3]);
      return json(200, { items: fixture.messages[conversationId] || [] });
    }

    if (/^\/v1\/conversations\/[^/]+\/read-status$/.test(path) && method === "GET") {
      return json(200, {
        members: [
          {
            user_id: fixture.session.userId,
            last_read_server_order: 11,
            last_delivered_server_order: 11,
            read_at: isoAt(-3),
            delivery_at: isoAt(-4)
          }
        ]
      });
    }

    if (/^\/v1\/conversations\/[^/]+$/.test(path) && method === "GET") {
      const conversationId = decodeURIComponent(path.split("/")[3]);
      return json(200, conversationsById.get(conversationId) || {});
    }

    if (/^\/v1\/conversations\/[^/]+\/metadata$/.test(path) && method === "PATCH") {
      const conversationId = decodeURIComponent(path.split("/")[3]);
      const patch = request.postDataJSON();
      const current = conversationsById.get(conversationId);
      if (!current) return json(404, { message: "not found" });
      const next = {
        ...current,
        title: patch.title !== undefined ? patch.title : current.title,
        avatar_url: patch.avatar_url !== undefined ? patch.avatar_url : current.avatar_url,
        encryption_state: patch.encryption_state !== undefined ? patch.encryption_state : current.encryption_state
      };
      conversationsById.set(conversationId, next);
      return json(200, next);
    }

    if (/^\/v1\/conversations\/[^/]+\/members$/.test(path) && method === "POST") {
      const conversationId = decodeURIComponent(path.split("/")[3]);
      const body = request.postDataJSON();
      const current = conversationsById.get(conversationId);
      if (!current) return json(404, { message: "not found" });
      const nextParticipants = [...new Set([...(current.participants || []), ...((body && body.user_ids) || [])])];
      const next = { ...current, participants: nextParticipants };
      conversationsById.set(conversationId, next);
      return json(200, next);
    }

    if (/^\/v1\/conversations\/[^/]+\/members\/[^/]+$/.test(path) && method === "DELETE") {
      const [, , , conversationId, , memberUserId] = path.split("/");
      const current = conversationsById.get(decodeURIComponent(conversationId));
      if (!current) return json(404, { message: "not found" });
      const next = {
        ...current,
        participants: (current.participants || []).filter((item) => item !== decodeURIComponent(memberUserId))
      };
      conversationsById.set(decodeURIComponent(conversationId), next);
      return json(200, next);
    }

    if (path === "/v1/users/resolve" && method === "POST") {
      const body = request.postDataJSON();
      return json(200, buildUsersResolvePayload(fixture.profiles, body?.user_ids || []));
    }

    if (path === "/v1/notifications/preferences" && method === "GET") {
      return json(200, privacyPrefs);
    }

    if (path === "/v1/notifications/preferences" && method === "PUT") {
      privacyPrefs = { ...privacyPrefs, ...(request.postDataJSON() || {}) };
      return json(200, privacyPrefs);
    }

    if (path === "/v1/devices" && method === "GET") {
      return json(200, { devices: fixture.devices });
    }

    if (path === "/v1/devices/activity" && method === "GET") {
      return json(200, { items: fixture.deviceActivity });
    }

    if (path.startsWith("/v1/apps/register")) {
      return json(200, {});
    }

    if (path === "/v1/apps" && method === "GET") {
      return json(200, { items: fixture.apps });
    }

    if (/^\/v1\/apps\/[^/]+$/.test(path) && method === "GET") {
      const appId = decodeURIComponent(path.split("/")[3]);
      const app = fixture.apps.find((item) => item.app_id === appId);
      return json(200, app || {});
    }

    if (/^\/v1\/auth\/refresh$/.test(path) && method === "POST") {
      return json(200, {
        access_token: fixture.session.accessToken,
        refresh_token: fixture.session.refreshToken
      });
    }

    if (/^\/v1\/messages/.test(path) || /^\/v2\/conversations\/[^/]+\/read$/.test(path)) {
      return json(200, {});
    }

    return json(200, {});
  });

  return fixture;
}

module.exports = {
  installAuthenticatedAppMocks,
};
