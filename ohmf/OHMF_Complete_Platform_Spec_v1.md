# OHMF Complete Platform Spec v1

Prepared: 2026-03-24  
Audience: product, engineering, design, QA, operations  
Purpose: single-document reference for the current OHMF platform, feature inventory, implementation status, and near-term delivery direction

## 1. Scope

This document is the authoritative platform inventory for OHMF as it exists in this repository on 2026-03-24.

It is meant to answer four questions in one place:

1. What is OHMF trying to be?
2. What has already been built?
3. What is partially built but not yet complete?
4. What still needs to be built?

This document intentionally covers more than messaging UX. It includes:

- client applications
- backend services
- transport modes
- end-to-end encryption
- mini-apps and SDK surface
- operational architecture
- developer tooling
- safety and trust systems
- platform gaps

## 2. Status Legend

Use only these statuses in this document:

- `Implemented`: shipped in the repo with runtime code and/or tests
- `In Progress`: partially present, scaffolded, or implemented on one surface but not the full intended product
- `To-do`: not yet built, or only described conceptually

## 3. Product Definition

OHMF is a messaging platform that combines:

- secure over-the-top messaging between OHMF users
- phone-number-first onboarding
- SMS/MMS bridge behavior for non-onboarded contacts
- linked-device support
- realtime sync and event replay
- mini-app runtime support inside conversations
- registry-backed app distribution

The intended product is not just "another chat app." It is a communication and application platform where:

- phone contacts can be reached before they fully onboard
- encrypted messaging becomes available once a secure bundle exists
- multi-device behavior is treated as a first-class requirement
- mini-apps can participate in a conversation with explicit permissions

## 4. High-Level Architecture

### 4.1 Main Components

| Area | Status | Notes |
| --- | --- | --- |
| Gateway service | `Implemented` | Primary client-facing HTTP and WebSocket boundary |
| PostgreSQL primary datastore | `Implemented` | System of record for conversations, messages, auth, device state, and mini-app runtime state |
| Redis | `Implemented` | Rate limiting, presence, typing freshness, some realtime/session state |
| Kafka pipeline | `Implemented` | Used by processor services in compose stack |
| Cassandra optional read path | `In Progress` | Present in architecture and compose, not the default read path for all features |
| Mini-app registry service | `Implemented` | Separate control plane for app catalog, releases, installs, and review workflow |
| Web client | `Implemented` | Main usable product surface in this repo |
| Android mini-app host scaffold | `In Progress` | Host shell exists, full messaging client does not |
| Native iOS/macOS clients | `To-do` | Not present in repo |

### 4.2 Service Ownership

| Responsibility | Owner | Status | Notes |
| --- | --- | --- | --- |
| Auth/session/device lifecycle | Gateway | `Implemented` | OTP auth, refresh rotation, recovery, pairing hooks |
| Messaging and conversation persistence | Gateway | `Implemented` | Core runtime state lives here |
| Realtime websocket fanout | Gateway | `Implemented` | Includes sync/resume surfaces |
| Mini-app runtime sessions | Gateway | `Implemented` | Session state, events, snapshots, joins |
| Mini-app app catalog and releases | Apps service | `Implemented` | Registry and review workflow live outside gateway |
| Carrier/SMS execution | Processor stack + relay path | `Implemented` | Present in architecture and route surface |

## 5. Client Surfaces

### 5.1 Web Client

| Feature | Status | Notes |
| --- | --- | --- |
| Phone OTP login | `Implemented` | Primary login surface |
| Token refresh/logout | `Implemented` | Session lifecycle wired |
| Conversation list and active thread | `Implemented` | Core two-pane shell exists |
| Mobile responsive behavior | `Implemented` | Thread-only view on narrow screens |
| DM and group viewing | `Implemented` | Main thread rendering exists |
| Send text messages | `Implemented` | Includes phone draft flow |
| Attachments | `Implemented` | Attachment send path exists; exact feature breadth is narrower than mature consumer apps |
| Reactions, replies, edits, deletes | `Implemented` | UI and backend behaviors are present |
| Receipt display improvements | `Implemented` | UI now resolves labels like `Sent to Alice`, `Delivered to Alice`, `Read by Alice` |
| Named typing indicators | `Implemented` | Replaces generic "somebody is typing" copy |
| Encrypted message fallback handling | `Implemented` | Prevents misrendering on wrong-device envelopes |
| New-device crypto restore/backfill | `Implemented` | Client-side backup/restore path added |
| Full polished production UX | `In Progress` | Functional but still intentionally barebones in presentation and QA depth |
| Visible device-pairing UI | `To-do` | Pairing endpoints exist but the web client is still OTP-first |

### 5.2 Android

| Feature | Status | Notes |
| --- | --- | --- |
| Mini-app host app | `Implemented` | Source scaffold exists |
| Catalog view | `Implemented` | Activity and registry client exist |
| WebView runtime | `Implemented` | Bridge shell mirrors web host contract |
| Messaging client | `To-do` | No full Android messaging app in this repo |
| Hardened production host isolation | `In Progress` | Explicitly called out as not production-hardened yet |

### 5.3 Other Surfaces

| Feature | Status | Notes |
| --- | --- | --- |
| Web mini-app runtime page | `Implemented` | Local test/runtime shell exists |
| Standalone developer mini-app test harness | `Implemented` | Mock host and package tooling exist |
| Native desktop client | `To-do` | Not present |
| iOS client | `To-do` | Not present |

## 6. Identity, Auth, and Account

### 6.1 User Identity Model

OHMF is currently phone-number-centric.

Core account identity properties:

- primary phone number
- user ID
- one or more devices
- access and refresh token state
- optional profile data

### 6.2 Auth and Session Features

| Feature | Status | Notes |
| --- | --- | --- |
| Phone OTP start/verify | `Implemented` | Main auth flow |
| Refresh token rotation | `Implemented` | Supported in gateway |
| Logout | `Implemented` | Explicit route exists |
| Recovery codes | `Implemented` | Route and backend support present |
| 2FA support | `Implemented` | Documented by gateway README as implemented capability |
| Multi-device session issuance | `Implemented` | Each verified or paired device gets its own device/session state |
| Pair new device by pairing code | `Implemented` | Backend flow present |
| Pairing completion user event | `Implemented` | Linked-device event emitted for sync |
| Device push token management | `Implemented` | Part of device lifecycle |
| Account export | `Implemented` | Gateway capability |
| Account deletion | `Implemented` | Gateway capability with audit |

### 6.3 Account and Device Trust

| Feature | Status | Notes |
| --- | --- | --- |
| Device records per user | `Implemented` | Device-scoped state is first-class |
| Device capability modeling | `Implemented` | Used for E2EE readiness gating |
| Device bundle publication | `Implemented` | Needed for encrypted OHMF delivery |
| Device attestation challenge/verify | `Implemented` | Gateway README marks attestation as implemented |
| Contact/device trust pins | `In Progress` | Data model and client crypto state indicate trust support, but user-facing trust UX is incomplete |
| Human-verifiable safety-number workflow | `To-do` | No finished end-user verification ceremony comparable to Signal or Apple CKV |

## 7. Transport and Conversation Model

### 7.1 Supported Conversation Types

| Conversation Type | Status | Notes |
| --- | --- | --- |
| OHMF direct message (`DM`) | `Implemented` | Main secure OTT path |
| Phone direct message (`PHONE_DM`) | `Implemented` | Transitional thread for a phone contact before secure onboarding |
| Group conversation | `Implemented` | Supports encryption state and lifecycle behaviors |
| Carrier plaintext transport state | `Implemented` | Used for phone/SMS path |
| Hybrid pre-onboarding conversation behavior | `Implemented` | Secure delivery is now gated on recipient bundle availability |

### 7.2 Transport Policy Principles

Current OHMF transport behavior is:

- If the recipient is just a phone contact or has not yet published secure keys, conversation creation stays on the `PHONE_DM` path.
- Once the recipient publishes a supported device bundle, that phone thread can be promoted to an OHMF `DM`.
- The web UI now refuses to treat such a thread as secure before the keys exist.

This closes a major integrity gap: previously, a user could appear onboarded before secure delivery was actually possible.

### 7.3 Conversation Features

| Feature | Status | Notes |
| --- | --- | --- |
| Create or resolve conversation by phone | `Implemented` | First-send phone draft flow exists |
| Create/list/get conversations | `Implemented` | Core API surface exists |
| Group membership persistence | `Implemented` | Conversation members stored server-side |
| External conversation members | `Implemented` | Used for phone-number recipients before full user onboarding |
| Blocking-aware conversation send checks | `Implemented` | Recipient block checks run in message send path |
| Group naming/photo UX parity with consumer apps | `In Progress` | Core group model exists; not fully surfaced/polished everywhere |

## 8. Messaging Core

### 8.1 Basic Message Lifecycle

| Feature | Status | Notes |
| --- | --- | --- |
| Send message | `Implemented` | Includes idempotency support |
| Load timeline | `Implemented` | Conversation message listing exists |
| Ordered server timeline | `Implemented` | Backed by conversation counters/server order |
| Idempotent send semantics | `Implemented` | Explicitly called out in gateway README |
| Rich message content object | `Implemented` | Messages can hold structured content |
| Phone send endpoint | `Implemented` | Supports draft-phone onboarding path |
| Sync-device fanout | `Implemented` | Sender device sync and recipient realtime paths exist |

### 8.2 Delivery State and Receipts

| Feature | Status | Notes |
| --- | --- | --- |
| Sent status | `Implemented` | Persisted and surfaced |
| Delivered status | `Implemented` | Persisted and surfaced |
| Read status | `Implemented` | Persisted and surfaced |
| Conversation read checkpoints | `Implemented` | Member-level checkpoint model exists |
| Delivery timestamps | `Implemented` | Verified in message lifecycle tests |
| Read timestamps | `Implemented` | Verified in message lifecycle tests |
| Named receipt labels in UI | `Implemented` | `Sent to`, `Delivered to`, `Read by` |
| Group member receipt projection | `Implemented` | Read-status member data feeds UI where available |

### 8.3 Message Editing, Replies, Reactions, and Deletion

| Feature | Status | Notes |
| --- | --- | --- |
| Edit plaintext message | `Implemented` | Server and UI support exist |
| Edit encrypted message metadata/history handling | `Implemented` | Edit history preserves ciphertext, not plaintext placeholders |
| Reply to message | `Implemented` | Reply threading and counts supported |
| Reaction add/remove | `Implemented` | History and projection supported |
| Reaction history | `Implemented` | Persisted and test-covered |
| Soft delete/tombstone | `Implemented` | Deleted replies remain as tombstones where appropriate |
| Forward message | `Implemented` | Mentioned in gateway README and handler paths |
| Pin message | `Implemented` | Mentioned in gateway README |
| Undo send with strict short window | `To-do` | OHMF supports delete/tombstone, not an iMessage-style 2-minute unsend contract |

### 8.4 Search and Retrieval

| Feature | Status | Notes |
| --- | --- | --- |
| Conversation search endpoint | `Implemented` | Route exists |
| Search normalization and typo heuristics | `Implemented` | Recent fixes aligned tests and normalization behavior |
| Per-conversation search filtering UX | `In Progress` | Backend exists; end-user UX can still mature |

### 8.5 Typing and Presence

| Feature | Status | Notes |
| --- | --- | --- |
| Typing indicators | `Implemented` | Presence/typing freshness stored in Redis |
| Named typing indicators in web UI | `Implemented` | Now resolves participant names |
| Presence endpoints | `Implemented` | Route surface exists |
| Fine-grained availability controls | `To-do` | No fully developed user privacy controls yet |

## 9. Media and Rich Content

| Feature | Status | Notes |
| --- | --- | --- |
| Media service boundary | `Implemented` | Separate service directory/readme exists |
| File attachment send path | `Implemented` | Supported in web send flow |
| Media picker in mini-app runtime | `Implemented` | `media.pick_user` capability exists |
| Rich previews and polished media gallery UX | `In Progress` | Not yet a fully mature consumer-media experience |
| Voice notes / audio message UX | `To-do` | Not documented as complete in this repo |
| Live photo / advanced camera effects | `To-do` | Not an OHMF capability today |

## 10. Security and E2EE

### 10.1 Direct-Message E2EE

| Feature | Status | Notes |
| --- | --- | --- |
| Device bundle publication | `Implemented` | Signal-style bundle support present |
| Per-device recipient encryption | `Implemented` | Messages target device entries, not just users |
| Sender self-copy / multi-device decryption path | `Implemented` | Client stores/imports owned device states |
| Wrong-device envelope detection | `Implemented` | Client renders explicit mismatch when needed |
| New-device crypto restore | `Implemented` | Encrypted backup sync/restore path present in web client |
| Pre-registration secure send gating | `Implemented` | Recipient must publish secure keys before OHMF secure delivery is allowed |

### 10.2 Group E2EE / MLS

| Feature | Status | Notes |
| --- | --- | --- |
| Group encrypted state model | `Implemented` | Group conversations can be encrypted |
| MLS session store and epoch model | `Implemented` | Backend store and lifecycle logic exist |
| MLS message validation | `Implemented` | Envelope validation exists |
| MLS group lifecycle test coverage | `Implemented` | Send, delivery, read, edit, reply, reaction, delete test added |
| Epoch secret persistence | `Implemented` | Upsert logic fixed to match schema |
| New-device epoch backfill | `Implemented` | Client backup/restore imports epoch secrets for same-epoch history decryption |
| Automatic MLS rekey UX for every edge case | `In Progress` | Core primitives exist; end-user UX can still grow |

### 10.3 Edit History and Encrypted Content Integrity

| Feature | Status | Notes |
| --- | --- | --- |
| Ciphertext preserved in encrypted edit history | `Implemented` | No fake `[Encrypted Message]` body stored in history |
| Placeholder-only preview behavior | `Implemented` | Placeholder text is used for projection/previews, not canonical encrypted content |
| Metadata-only edit restrictions | `Implemented` | Encryption middleware prevents invalid content edits where required |

### 10.4 Trust and Verification

| Feature | Status | Notes |
| --- | --- | --- |
| Device attestation | `Implemented` | Challenge/verify and relay enforcement noted by gateway README |
| Relay enforcement against attestation state | `Implemented` | Explicitly documented |
| Human-readable identity verification UX | `To-do` | Needs a product-level workflow |
| Public verification code model similar to Apple CKV | `To-do` | Not currently present |

## 11. Multi-Device and Sync

| Feature | Status | Notes |
| --- | --- | --- |
| WebSocket realtime channel | `Implemented` | `GET /v1/ws` and `GET /v2/ws` |
| Sync replay/resume | `Implemented` | `/v1/sync`, `/v2/sync`, event stream routes |
| Linked-device session issuance | `Implemented` | Pairing flow completes device creation and token issuance |
| Linked-device user event | `Implemented` | `account_device_linked` event now emitted |
| Crypto backup sync to device-keys backup endpoint | `Implemented` | Web client syncs wrapped state |
| Restore on newly linked device | `Implemented` | Automatic attempt after bundle publication |
| Cross-device history decryption for old DM messages | `Implemented` | Imported sibling-device crypto state can decrypt old copies |
| Cross-device history decryption for old MLS messages | `Implemented` | Imported epoch secrets restore same-epoch readability |
| Pairing UI in main web product | `To-do` | Backend exists; product surface still missing |

## 12. Discovery, Contacts, Blocking, and Abuse

| Feature | Status | Notes |
| --- | --- | --- |
| Contact discovery endpoints | `Implemented` | Gateway route families exist |
| Discovery hashing | `Implemented` | Explicitly listed by gateway README |
| User blocks | `Implemented` | Messaging send path respects block checks |
| Abuse service boundary | `Implemented` | Service and route area exist |
| Group/block moderation UX depth | `In Progress` | Core data path exists, but product surface is not yet fully mature |

## 13. Mini-App Platform

### 13.1 Platform Summary

Mini-apps are a strategic OHMF differentiator. The platform is not a toy iframe embed. It includes:

- manifest schema
- runtime launch context
- permission/capability declarations
- session state snapshots
- event model
- registry/review workflow
- host SDKs
- conversation sharing support

### 13.2 Mini-App Core Features

| Feature | Status | Notes |
| --- | --- | --- |
| Canonical manifest schema | `Implemented` | Present in `packages/miniapp` and protocol schemas |
| Web SDK | `Implemented` | `packages/miniapp/sdk-web` and app-side helper |
| Shared TypeScript SDK types | `Implemented` | `packages/miniapp/sdk-types` |
| Test harness / mock host | `Implemented` | Reusable local harness exists |
| CLI for validate/sign/package/upload/submit | `Implemented` | `miniapp-cli.mjs` |
| Gateway-backed catalog lookup | `Implemented` | Web runtime uses API-backed catalog |
| App install tracking | `Implemented` | Apps service and gateway integration present |
| Session persistence | `Implemented` | Gateway owns runtime session state |
| Session event log | `Implemented` | Gateway owns append-only event model |
| Session state snapshot updates | `Implemented` | Bridge and session service support exist |
| Conversation share integration | `Implemented` | Mini-app shares into conversations are tested in gateway |
| Permission enforcement in host | `Implemented` | Host grants and runtime enforcement exist |
| Release review workflow | `Implemented` | Draft, submitted, under_review, needs_changes, approved, rejected, suspended, revoked |
| Publisher key registration/rotation/revocation | `Implemented` | Apps service supports this in DB-backed mode |
| Android host parity | `In Progress` | Runtime scaffold exists; broader product integration is incomplete |
| Full production marketplace with discovery ranking/editorial tooling | `In Progress` | Registry basics exist, marketplace maturity does not |

### 13.3 SDK / Bridge Capability Inventory

| Capability | Status | Notes |
| --- | --- | --- |
| `host.getLaunchContext` | `Implemented` | Launch metadata and permissions |
| `conversation.read_context` | `Implemented` | Recent conversation context available to app |
| `conversation.send_message` | `Implemented` | App can project content into thread |
| `participants.read_basic` | `Implemented` | Participant list access |
| `storage.session` | `Implemented` | App-private session storage |
| `storage.shared_conversation` | `Implemented` | Shared conversation-scoped state |
| `session.updateState` | `Implemented` | Shared state snapshot updates |
| `media.pick_user` | `Implemented` | Media/user pick support surface exists |
| `notifications.in_app` | `Implemented` | Host-triggered in-app notifications |
| `realtime.session` | `Implemented` | Session realtime support exists |

### 13.4 Demo Mini-Apps

| Mini-App | Status | Notes |
| --- | --- | --- |
| Counter Lab | `Implemented` | Minimal example |
| Mystic 8-Ball | `Implemented` | Current shipped conversation-oriented demo |
| 8 Ball Pool public demo mini-app | `To-do` | Requested showcase app; should become the flagship public SDK/game demo |

### 13.5 8 Ball Pool Demo Requirement

The repository already includes a `Mystic 8-Ball` sample. That is useful as a lightweight example, but it is not the same as a public-facing `8 Ball Pool` game.

The intended `8 Ball Pool` demo should be documented as a dedicated deliverable because it demonstrates things the current example does not:

- turn-based or realtime multiplayer state synchronization
- conversation projection of game events
- participant permissions
- richer session state snapshots
- reconnect/resume behavior
- media or avatar customization hooks
- a demo that feels like a real product, not only a host API smoke test

Recommended scope for the eventual `8 Ball Pool` demo:

| Capability | Status | Notes |
| --- | --- | --- |
| Installable mini-app manifest | `To-do` | Separate app ID and release |
| Conversation launch and invite flow | `To-do` | Must work in active thread context |
| Shared game table state | `To-do` | Session snapshot source of truth |
| Turn or shot event log | `To-do` | Use runtime event model |
| Public-facing art and polish | `To-do` | Should showcase the platform visually |
| Spectator-safe replay/resume | `To-do` | Strong demo for session recovery |
| Web host support | `To-do` | Minimum viable target |
| Android host support | `In Progress` | Depends on host maturity |

## 14. Registry and Developer Platform

| Feature | Status | Notes |
| --- | --- | --- |
| App create endpoint | `Implemented` | Publisher-facing |
| Release create/list/submit | `Implemented` | Publisher-facing |
| Admin review transitions | `Implemented` | Start review, needs changes, approve, reject, suspend |
| Install/uninstall | `Implemented` | User-level install tracking exists |
| Update detection | `Implemented` | Compares installed vs latest approved |
| Permission-expanding update warning | `Implemented` | Update requires consent rather than silent privilege escalation |
| Dev-mode raw/local registry flow | `Implemented` | Helpful for local iteration |
| Production-grade publisher tooling UX | `In Progress` | API exists; complete publisher console does not |

## 15. Operations, Tooling, and Deployment

| Feature | Status | Notes |
| --- | --- | --- |
| Docker compose local stack | `Implemented` | Gateway, Redis, Kafka, Cassandra, processors, and supporting infra |
| Local non-admin toolchain in `.tools` | `Implemented` | Go and auxiliary tools included locally |
| OpenAPI exposure | `Implemented` | Gateway serves `openapi.yaml` |
| Health/readiness routes | `Implemented` | `/healthz` and `/readyz` |
| Observability package/infra | `Implemented` | Observability directories exist |
| Production deployment automation | `In Progress` | Repo has infra pieces, but not a full polished production platform story |

## 16. Current Feature Summary by Domain

### 16.1 Strongest Implemented Areas

- gateway-centered runtime architecture
- phone OTP onboarding
- conversation/message persistence
- message lifecycle controls: receipts, replies, reactions, edits, delete/tombstones
- realtime and sync foundations
- device-scoped E2EE and MLS group support
- multi-device crypto restore/backfill
- mini-app manifest, SDK, registry, and runtime session model

### 16.2 Areas Clearly In Progress

- Android host hardening and full client parity
- richer consumer-grade media UX
- richer trust-verification UX
- complete user-facing device-linking UX
- product polish and marketplace/editorial layers for mini-app discovery

### 16.3 Major To-do Areas

- native iOS/macOS/Android full messaging clients
- public verification UX for contact trust
- flagship `8 Ball Pool` mini-app demo
- stronger moderation/admin surfaces
- higher-fidelity media and calling/social presence layers

## 17. Recommended Near-Term Roadmap

### 17.1 Build Now

| Item | Status | Why |
| --- | --- | --- |
| Pair-device UI in web client | `To-do` | Backend exists; user-facing multi-device story is incomplete without it |
| 8 Ball Pool mini-app demo | `To-do` | Best public proof that OHMF mini-apps are productizable, not only theoretical |
| Contact/device trust verification UX | `To-do` | E2EE maturity needs human trust workflows |
| Media and attachment polish | `In Progress` | Important for baseline messaging competitiveness |

### 17.2 Keep Advancing

| Item | Status | Why |
| --- | --- | --- |
| MLS group reliability and rekey ergonomics | `In Progress` | Core security differentiator |
| Android host maturity | `In Progress` | Needed for mini-app platform credibility |
| Registry and publisher experience | `In Progress` | Needed to make mini-app ecosystem usable |

### 17.3 Do Not Confuse with Priority

These may become important later, but they are not the current highest-leverage items:

- aesthetic-only theming features
- novelty effects that do not improve messaging reliability or platform differentiation
- deep platform-specific integrations before the core device/linking/security flows are polished

## 18. Reference File Map

Important implementation anchors in this repo:

- `services/gateway/README.md`
- `services/apps/README.md`
- `packages/miniapp/README.md`
- `packages/miniapp/sdk-types/index.d.ts`
- `packages/protocol/types/miniapp.go`
- `apps/web/README.md`
- `apps/android/README.md`
- `apps/web/miniapps/eightball/*`

## 19. Final Assessment

OHMF is already more than a prototype. The repository contains a real messaging platform core with:

- account lifecycle
- multi-transport messaging
- secure device-aware messaging
- group encryption support
- realtime and sync infrastructure
- an unusually ambitious mini-app platform

What it lacks is not a foundation. It lacks completion in the areas that convert a strong engineering base into a polished product:

- user-facing device/linking flows
- trust UX
- media polish
- stronger mobile clients
- a flagship demo app that immediately communicates platform value

That is why the highest-leverage public deliverable after core messaging polish should be the `8 Ball Pool` mini-app: it demonstrates conversations, shared state, permissions, runtime events, reconnect, and product ambition in one artifact.
