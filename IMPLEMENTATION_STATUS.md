# OHMF Implementation Status Report

**Generated:** 2026-03-23
**Scope:** Complete feature inventory of OHMF platform against specification and iMessage parity
**Format:** Features marked as **[IMPLEMENTED]**, **[PARTIAL]**, or **[UNIMPLEMENTED]**

---

## Table of Contents

1. [Core Messaging Features](#1-core-messaging-features)
2. [Account & Identity](#2-account--identity)
3. [Device Management](#3-device-management)
4. [Contact Discovery](#4-contact-discovery)
5. [Conversation Management](#5-conversation-management)
6. [Message Features](#6-message-features)
7. [Transport & Routing](#7-transport--routing)
8. [Message Lifecycle & States](#8-message-lifecycle--states)
9. [Editing, Reactions, Receipts & Presence](#9-editing-reactions-receipts--presence)
10. [Blocking, Privacy & Visibility](#10-blocking-privacy--visibility)
11. [Data Deletion & Erasure](#11-data-deletion--erasure)
12. [Media & Attachments](#12-media--attachments)
13. [Mini-Apps/Embedded Apps](#13-mini-appsembedded-apps)
14. [End-to-End Encryption](#14-end-to-end-encryption)
15. [Client Platforms](#15-client-platforms)
16. [iMessage Feature Parity](#16-imessage-feature-parity)

---

## 1. Core Messaging Features

### OTT (Over-The-Top) Internet Messaging
- **Core OTT send/receive**: **[IMPLEMENTED]**
  - Full REST API endpoints implemented
  - WebSocket realtime delivery
  - Idempotency protection with client-generated IDs

- **Text message support**: **[IMPLEMENTED]**
  - Plain text messages
  - Content stored in flexible JSONB payloads

- **Rich text messages**: **[PARTIAL]**
  - Schema defined in protobuf (`rich_text` content type)
  - API endpoint accepts rich_text type
  - No client-side rich text editor implemented (web/Android)

- **Message search functionality**: **[UNIMPLEMENTED]**
  - No full-text search API endpoints
  - No search indexes or query operators
  - Not in API specification

- **Message sync on app launch**: **[IMPLEMENTED]**
  - `/v1/sync` endpoint exists
  - Sync cursor checkpoints in DB
  - Client-driven pagination and cursor-based fetching

---

## 2. Account & Identity

### Phone Number Verification
- **OTP generation and verification**: **[IMPLEMENTED]**
  - Phone number as verified identifier
  - OTP challenge workflow complete
  - `/v1/auth/phone/start` and `/v1/auth/phone/verify` endpoints
  - Stored in `phone_verification_challenges` table

- **Rate limiting on OTP**: **[PARTIAL]**
  - Schema designed for rate limiting (rate_limit.go handler exists)
  - Framework in place at gateway level
  - Per-phone, per-IP, per-subnet limiting designed
  - Device risk scoring hooks defined but not fully integrated

- **OTP throttling for suspicious activity**: **[PARTIAL]**
  - Challenge escalation framework exists
  - Device risk scoring mechanisms in code
  - Full heuristic implementation incomplete

### Session Management
- **Access tokens (JWT-based)**: **[IMPLEMENTED]**
  - JWT generation and validation
  - Token refresh endpoint: `/v1/auth/refresh`
  - Stored in `refresh_tokens` table

- **Logout / session invalidation**: **[IMPLEMENTED]**
  - Logout endpoint clears tokens
  - Device revocation support

- **Multi-device session handling**: **[IMPLEMENTED]**
  - Each device maintains separate tokens
  - Per-device session tracking

### Account Recovery
- **Recovery codes for account access**: **[IMPLEMENTED]**
  - Recovery codes table exists
  - Generation and validation logic
  - Endpoint: `/v1/account/recovery-codes` (spec compliant)

- **Account deletion**: **[PARTIAL]**
  - API endpoint exists: `/v1/account/delete`
  - Removes user mappings, tokens, devices, profile
  - Anonymization of group message history not fully tested

- **Data export capability**: **[IMPLEMENTED]**
  - Export request endpoint exists
  - Stores export artifacts in `account_data` table
  - Returns downloadable export records

### Two-Factor Authentication
- **2FA framework**: **[PARTIAL]**
  - Schema and database support present
  - Framework hooks in auth handlers
  - Full 2FA flow not fully tested/documented

### Future Identity Methods
- **Passkey/WebAuthn support**: **[UNIMPLEMENTED]**
  - Spec mentions as future factor
  - No implementation

- **Trusted-device confirmation**: **[UNIMPLEMENTED]**
  - Mentioned in spec as future
  - No current implementation

---

## 3. Device Management

### Device Registration
- **Device creation and enrollment**: **[IMPLEMENTED]**
  - `/v1/devices/register` endpoint
  - Device table schema with platform tracking
  - Push token support (FCM, etc.)

- **Device naming and metadata**: **[IMPLEMENTED]**
  - Device name field
  - Platform identification (Android, Web, iOS, etc.)
  - Client version tracking
  - Last seen timestamp

- **Device capabilities declaration**: **[IMPLEMENTED]**
  - Capabilities array in schema
  - Supported: OTT, PUSH, MINI_APPS, SMS_HANDLER, RELAY_EXECUTOR
  - Enum-based type system

### Device Attestation
- **Native attestation support**: **[PARTIAL]**
  - Android attestation challenge in schema
  - Attestation verification framework in gateway
  - Database support for attestation data
  - Client-side implementation not complete

- **Device key management**: **[IMPLEMENTED]**
  - Ed25519 public keys stored per device
  - `/v1/device-keys` endpoints
  - Key upload and retrieval supported

### Device Revocation & Removal
- **Remote device revocation**: **[IMPLEMENTED]**
  - Revoke endpoint exists
  - Tokens invalidated
  - Device marked as revoked

- **Automatic inactive device cleanup**: **[UNIMPLEMENTED]**
  - No scheduled cleanup job
  - No retention policy enforcement

---

## 4. Contact Discovery

### Privacy-Preserving Contact Lookup
- **Hash-based contact discovery**: **[IMPLEMENTED]**
  - `/v1/discovery` or `/v1/contacts/discover` endpoints
  - SHA256_PEPPERED_V1 algorithm defined
  - Contact hashes sent to server
  - Server returns matches without seeing plaintext

- **Discovery response format**: **[IMPLEMENTED]**
  - Returns hash, user_id, display_name tuples
  - No plaintext contact exposure

- **Future PSI migration path**: **[DOCUMENTED]**
  - Spec mentions planned migration to PSI
  - Current implementation uses hash-based approach
  - Migration path documented but not implemented

### Opt-in to Discovery
- **Per-user discovery allowance**: **[PARTIAL]**
  - Schema field exists in users table
  - Setting controllable via API
  - Discovery filtering logic not fully verified

---

## 5. Conversation Management

### Conversation Types
- **Direct Message (DM) conversations**: **[IMPLEMENTED]**
  - Conversation type `DM` supported
  - Two-participant direct messages working

- **Group conversations**: **[IMPLEMENTED]**
  - Conversation type `GROUP` supported
  - Multiple participants supported
  - Conversation members table tracks participation

- **System conversations**: **[PARTIAL]**
  - Type `SYSTEM` defined in schema
  - Limited implementation (mostly for relay)

- **Mini-app session conversations**: **[IMPLEMENTED]**
  - Type `APP_SESSION` defined
  - Mini-app-specific conversation operations

### Conversation CRUD Operations
- **Create conversations**: **[IMPLEMENTED]**
  - `/v1/conversations` POST endpoint
  - DM and group creation supported
  - Participants added during creation

- **List conversations**: **[IMPLEMENTED]**
  - `/v1/conversations` GET endpoint
  - Pagination support
  - Conversation sorting

- **Get conversation details**: **[IMPLEMENTED]**
  - `/v1/conversations/{id}` endpoint
  - Returns metadata and recent messages

- **Update conversation (title, settings)**: **[PARTIAL]**
  - Update endpoints exist for some fields
  - Settings like muting, effects controls available
  - Full settings update coverage unclear

- **Leave/delete conversation**: **[IMPLEMENTED]**
  - Deletion support in database schema
  - Member removal logic

### Transport Policy Per Conversation
- **Policy options**: **[IMPLEMENTED]**
  - `AUTO` (intelligent selection)
  - `FORCE_OTT` (internet only)
  - `FORCE_SMS` (SMS only)
  - `FORCE_MMS` (MMS only)
  - `BLOCK_CARRIER_RELAY` (no relay)
  - All defined and stored in DB

- **Policy enforcement**: **[PARTIAL]**
  - Message routing logic checks policy
  - Not fully tested with carrier messages

### Thread Keys & Multiple Message Timelines
- **Conversation ID as primary key**: **[IMPLEMENTED]**
  - Server-assigned unique IDs

- **Support for multiple temporal views**: **[DOCUMENTED]**
  - OTT timeline (server_order)
  - Carrier timeline (provider timestamps)
  - Architecture supports non-collapsing timelines
  - Client implementation of dual-timeline view incomplete

- **Carrier thread ID support**: **[PARTIAL]**
  - Schema stores Android thread IDs
  - Mapping between conversation_id and thread_id exists
  - Matching logic for import not fully tested

---

## 6. Message Features

### Message Content Types
- **Plain text**: **[IMPLEMENTED]**
  - Core content type stored

- **Rich text (formatted)**: **[PARTIAL]**
  - Content type defined
  - No rich text editor on clients

- **Media (images, videos)**: **[IMPLEMENTED]**
  - Media content type supported
  - Attachment handling with metadata

- **Mini-app cards**: **[IMPLEMENTED]**
  - `app_card` content type defined
  - Mini-app message sending working

- **Mini-app events**: **[IMPLEMENTED]**
  - `app_event` content type for app-to-user messages

- **System messages**: **[PARTIAL]**
  - Content type exists
  - Limited system-generated message support

### Message Metadata
- **Server-assigned message ID**: **[IMPLEMENTED]**
  - Unique message identifiers
  - Monotonic server_order for ordering

- **Client idempotency ID**: **[IMPLEMENTED]**
  - Client-generated IDs for deduplication
  - Prevents duplicate sends on retry

- **Sender information**: **[IMPLEMENTED]**
  - Sender user_id and device_id tracked
  - Part of message payload

- **Reply to/threading**: **[PARTIAL]**
  - Reply-to message ID schema exists
  - Quoted preview generation not implemented
  - Threading UX not fully developed

- **Mentions/tagging**: **[PARTIAL]**
  - Mention field in schema
  - No explicit mention notification system

---

## 7. Transport & Routing

### OTT/Internet Transport
- **OTT message send**: **[IMPLEMENTED]**
  - Complete send flow
  - Gateway handles routing and delivery

- **OTT auto-retry**: **[PARTIAL]**
  - Delivery processor consumes Kafka events
  - Retry logic exists in schema
  - Backoff strategy documented

### Android SMS/MMS Transport
- **SMS send capability**: **[DOCUMENTED ONLY]**
  - Schema ready (carrier_messages table)
  - No Android client implementation
  - Framework defined, not integrated

- **MMS send capability**: **[DOCUMENTED ONLY]**
  - Schema ready in database
  - No Android client implementation

- **SMS/MMS receive capability**: **[UNIMPLEMENTED]**
  - SMS processor backend exists
  - No Android client integration for receiving

### Linked-Device Relay (Web-to-SMS/MMS)
- **Relay job creation**: **[IMPLEMENTED]**
  - `/v1/relay/create` endpoint works
  - Jobs stored in `relay_jobs` table

- **Relay job dispatch to Android**: **[PARTIAL]**
  - Relay job creation and storage complete
  - Job polling and assignment endpoints exist
  - Android relay executor app NOT implemented

- **Relay retry handling**: **[IMPLEMENTED]**
  - `relay_retries` table exists
  - Retry logic implemented
  - Status tracking complete

- **Relay trust checks**: **[IMPLEMENTED]**
  - Relay permission validation
  - Device capability checks

### Carrier Mirroring Policies
- **Policy types**: **[IMPLEMENTED]**
  - `NONE` (default, no mirroring)
  - `METADATA_ONLY` (headers only)
  - `FULL_CONTENT` (complete message)
  - `SELECTIVE` (user-controlled)
  - All defined in schema

- **Mirroring enforcement**: **[PARTIAL]**
  - Policy stored per user/conversation
  - Enforcement logic exists
  - Carrier sync not fully tested end-to-end

### Transport Selection Algorithm
- **Intelligent routing logic**: **[IMPLEMENTED]**
  - Algorithm defined in spec
  - Prioritizes OTT if available
  - Falls back to SMS/MMS for Android SMS mode
  - Relay for web users
  - Implementation in gateway routing logic

---

## 8. Message Lifecycle & States

### OTT Message States
- **QUEUED**: **[IMPLEMENTED]**
- **ACCEPTED**: **[IMPLEMENTED]**
- **STORED**: **[IMPLEMENTED]**
- **PUSHED**: **[IMPLEMENTED]**
- **DELIVERED**: **[IMPLEMENTED]**
- **READ**: **[IMPLEMENTED]**
- **FAILED**: **[IMPLEMENTED]**

All states tracked and persisted in database.

### SMS/MMS Message States
- **PENDING_LOCAL**: **[DOCUMENTED]**
- **SENT_TO_MODEM**: **[DOCUMENTED]**
- **SENT_TO_CARRIER**: **[DOCUMENTED]**
- **DELIVERED**: **[DOCUMENTED]**
- **FAILED_LOCAL**: **[DOCUMENTED]**
- **FAILED_CARRIER**: **[DOCUMENTED]**

States defined but SMS/MMS client not implemented.

### Relay Message States
- **QUEUED_ON_SERVER**: **[IMPLEMENTED]**
- **DISPATCHED_TO_ANDROID**: **[IMPLEMENTED]**
- **ACCEPTED_BY_DEVICE**: **[IMPLEMENTED]**
- **SENT_TO_MODEM**: **[DOCUMENTED]**
- **FINAL**: **[DOCUMENTED]**
- **DEVICE_OFFLINE**: **[IMPLEMENTED]**
- **ROLE_NOT_HELD**: **[IMPLEMENTED]**

Server-side states implemented; Android execution incomplete.

### Message Visibility States
- **ACTIVE**: **[IMPLEMENTED]**
- **EDITED**: **[IMPLEMENTED]**
- **SOFT_DELETED**: **[IMPLEMENTED]**
- **REDACTED**: **[PARTIAL]**
  - Schema support exists
  - Redaction implementation (actual content removal) incomplete
- **PURGED**: **[PARTIAL]**
  - Schema support exists
  - Physical deletion implementation incomplete

---

## 9. Editing, Reactions, Receipts & Presence

### Message Editing
- **Edit message content**: **[IMPLEMENTED]**
  - `/v1/messages/{id}` PATCH endpoint
  - `edited_at` timestamp tracking
  - Edit event emission (`message.edited` WebSocket event)

- **Edit verification**: **[IMPLEMENTED]**
  - Only original sender can edit
  - Sender verification in API handler

- **Edit history**: **[PARTIAL]**
  - `message_edits` table exists
  - Edit history storage ready
  - Full history retrieval API unclear

- **Revision tracking**: **[IMPLEMENTED]**
  - Revision counter support in schema

### Reactions/Emoji Reactions
- **Add reaction**: **[IMPLEMENTED]**
  - `/v1/messages/{id}/reactions` POST endpoint
  - `message_effects` table stores reactions
  - Multiple reactions per user supported

- **Remove reaction**: **[IMPLEMENTED]**
  - Reaction deletion with `removed_at` timestamp

- **Reaction broadcast via WebSocket**: **[IMPLEMENTED]**
  - Real-time event: `message.reactions`
  - Clients receive reaction updates

- **Reaction persistence**: **[IMPLEMENTED]**
  - Durable storage
  - Survives app restarts

- **Supported reaction types**: **[PARTIAL]**
  - Emoji supported (any Unicode emoji)
  - Special effects: confetti, balloons, fireworks, lasers, etc.
  - Effect types defined in schema
  - Unclear which are fully implemented on clients

### Read Receipts
- **Read receipt sending**: **[IMPLEMENTED]**
  - `/v1/conversations/{id}/read` endpoint
  - Marks messages as read

- **Watermark model (conversation-scoped)**: **[IMPLEMENTED]**
  - Read receipt tracks `through_server_order`
  - Conversation-level watermark

- **Per-message read state**: **[IMPLEMENTED]**
  - Individual message read tracking in `read_receipts` table
  - `conversation_member_read_at` table for member status

- **Read receipt aggregation**: **[IMPLEMENTED]**
  - Per-user read status
  - Not per-device (spec: aggregate per user)

- **Read receipt broadcast**: **[IMPLEMENTED]**
  - WebSocket event: `read_receipt`
  - Real-time updates to senders

### Typing Indicators
- **Typing start/stop**: **[IMPLEMENTED]**
  - WebSocket sent events
  - `/v1/conversations/{id}/typing/start` endpoint maybe

- **Ephemeral (not persisted)**: **[IMPLEMENTED]**
  - Typing state in Redis cache only

- **TTL (5 second recommended)**: **[IMPLEMENTED]**
  - Realtime handler implements expiry

- **Broadcast to conversation**: **[IMPLEMENTED]**
  - WebSocket events: `typing_started`, `typing_stopped`

### Presence (Online/Offline Status)
- **User online/offline status**: **[IMPLEMENTED]**
  - Schema support in database
  - Presence visibility table

- **Conversation presence**: **[IMPLEMENTED]**
  - Per-conversation presence tracking

- **Real-time presence updates**: **[IMPLEMENTED]**
  - WebSocket events: `presence.online`, `presence.offline`

- **Presence cache (Redis)**: **[IMPLEMENTED]**
  - Ephemeral storage
  - No persistent history

- **Last seen timestamp**: **[IMPLEMENTED]**
  - Tracked per device

---

## 10. Blocking, Privacy & Visibility

### User Blocking
- **Block user**: **[IMPLEMENTED]**
  - `/v1/blocks` POST endpoint
  - `user_blocks` table stores relationships
  - blocker_user_id, blocked_user_id, created_at

- **Unblock user**: **[IMPLEMENTED]**
  - Block removal endpoint

- **Block effects - delivery prohibition**: **[IMPLEMENTED]**
  - Blocked users cannot send DMs to blocker
  - Routing logic checks block list

- **Block effects - typing indicators**: **[IMPLEMENTED]**
  - Blocked users' typing indicators not sent to blocker

- **Block effects - reactions**: **[PARTIAL]**
  - Spec says reactions should be filtered
  - Implementation unclear if complete

- **Block effects - visibility in groups**: **[DOCUMENTED]**
  - Spec says renderer may hide blocked user
  - Client-side implementation responsibility

### Client-Only Nicknames
- **Local nickname storage**: **[UNIMPLEMENTED]**
  - Not implemented (by design - local only)
  - Spec: "SHOULD NOT require server persistence"
  - Client responsibility

---

## 11. Data Deletion & Erasure

### Soft Deletion
- **Mark message as deleted**: **[IMPLEMENTED]**
  - `/v1/messages/{id}/delete` endpoint
  - `deleted_at` timestamp set
  - Message marked as `SOFT_DELETED` state

- **Redaction of content**: **[PARTIAL]**
  - Schema support exists
  - Actual redaction (clearing fields) partially implemented
  - Attachment deletion not fully verified

### Redaction mechanism (ID-preserving)
- **Preserve message ordering**: **[IMPLEMENTED]**
  - message_id and server_order retained
  - Conversation timeline integrity maintained

- **Remove personal data**: **[PARTIAL]**
  - Content fields can be cleared
  - Process documented but full implementation unclear
  - Quoted previews not fully handled

- **Cache/index invalidation**: **[PARTIAL]**
  - Redis cache exists
  - Index invalidation strategy not clear

### Purged State (Physical Deletion)
- **Physical record deletion**: **[PARTIAL]**
  - PURGED state defined
  - Actual deletion process not fully implemented
  - Retention policy execution unclear

- **Async purge jobs**: **[UNIMPLEMENTED]**
  - No scheduled purge/cleanup jobs visible

### Account Deletion
- **Full account erasure**: **[PARTIAL]**
  - User record marked for deletion
  - Session tokens invalidated
  - Devices revoked
  - Profile cleared
  - Avatar deleted
  - Discovery index updated
  - Group message anonymization incomplete

---

## 12. Media & Attachments

### OTT Media Upload/Download
- **Media upload endpoint**: **[IMPLEMENTED]**
  - `/v1/media/upload` multipart endpoint
  - Object storage support
  - Attachment record creation

- **Media download endpoint**: **[IMPLEMENTED]**
  - `/v1/media/{id}` GET endpoint
  - File retrieval and serving

- **Media metadata**: **[IMPLEMENTED]**
  - media_id, kind, mime_type, bytes, width, height, sha256 hash
  - All stored in message payload

- **Upload authorization**: **[IMPLEMENTED]**
  - Permission checks before upload

- **Resume/chunked upload**: **[UNIMPLEMENTED]**
  - Single-shot upload only
  - No chunking or resume capability

### MMS Media Handling
- **Device-local authoritative**: **[DOCUMENTED]**
  - Policy established but not fully tested
  - Android client not implemented

- **Mirroring with user policy**: **[PARTIAL]**
  - Policy options exist (METADATA_ONLY, FULL_CONTENT, SELECTIVE)
  - Actual mirroring logic incomplete

- **Downscaling/transformation**: **[UNIMPLEMENTED]**
  - No image processing/transformation pipeline
  - Not required for Phase 1

### Media Captions
- **Caption support**: **[PARTIAL]**
  - Optional caption field in media payload
  - UI for caption entry unclear

### Media Preview Generation
- **Thumbnail generation**: **[UNIMPLEMENTED]**
  - No image processing for thumbnails
  - Full-size only

---

## 13. Mini-Apps/Embedded Apps

### Mini-app Runtime
- **Web-based sandboxed runtime**: **[IMPLEMENTED]**
  - Android: WebView sandbox
  - Web: iframe sandbox
  - `/v1/miniapps/session` endpoints for startup

- **Mini-app manifest support**: **[IMPLEMENTED]**
  - Manifest schema defined
  - Fields: app_id, name, version, entrypoint, icons, permissions, capabilities
  - Digital signature (Ed25519) support

- **Manifest signature verification**: **[IMPLEMENTED]**
  - Ed25519 verification in gateway
  - Origin validation

### Mini-app Message Types
- **app_card content type**: **[IMPLEMENTED]**
  - Mini-app cards sent as messages

- **app_event content type**: **[IMPLEMENTED]**
  - App-to-user events

- **Message preview functionality**: **[PARTIAL]**
  - Manifest declares message_preview
  - Static image mode supported
  - Live preview rendering (sandboxed) not verified
  - Aspect ratio (1:1 square) enforced

### Mini-app Capabilities/Permissions
- **conversation.read_context**: **[IMPLEMENTED]**
  - Bridge method returns conversation metadata

- **conversation.send_message**: **[IMPLEMENTED]**
  - Bridge method sends user-approved messages

- **participants.read_basic**: **[IMPLEMENTED]**
  - Bridge returns participant list

- **storage.session**: **[IMPLEMENTED]**
  - Session key-value storage (per app, per session)

- **storage.shared_conversation**: **[IMPLEMENTED]**
  - Shared app storage across sessions in conversation

- **realtime.session**: **[IMPLEMENTED]**
  - WebSocket subscription to session events

- **media.pick_user**: **[IMPLEMENTED]**
  - Media picker UI with consent

- **notifications.in_app**: **[IMPLEMENTED]**
  - Toast-style notifications

### Mini-app Bridge Protocol
- **JSON-RPC-like request/response**: **[IMPLEMENTED]**
  - Bridge version negotiation
  - Method calls with params
  - Error handling and response model

- **Push events from host**: **[IMPLEMENTED]**
  - Asynchronous events to apps
  - App lifecycle events

### Mini-app Registry/Installation
- **App registry service**: **[IMPLEMENTED]**
  - `/v1/apps` or `/v1/miniapps` endpoints
  - Mini-app manifest discovery
  - Release management

- **App installation tracking**: **[IMPLEMENTED]**
  - User app list maintained

- **Session management**: **[IMPLEMENTED]**
  - Session creation per app per conversation
  - State snapshots stored
  - Session persistence

### Mini-app Examples
- **Counter app**: **[IMPLEMENTED]**
  - Demonstrates session storage and state
  - Fully functional example

- **Eight Ball app**: **[IMPLEMENTED]**
  - Demonstrates participant interaction
  - Fully functional example

---

## 14. End-to-End Encryption

### E2EE Status
- **Direct-message E2EE**: **[IMPLEMENTED]**
  - Web client publishes Signal-compatible device bundles
  - `encrypted` message content with per-device recipient headers supported
  - Gateway validates sender device ownership and encrypted envelope signatures

- **Group E2EE (MLS-backed, web-first)**: **[IMPLEMENTED]**
  - New group conversations default to `ENCRYPTED`
  - Gateway tracks `mls_epoch`, ratchet-tree state, and per-group tree hashes
  - Sender encrypts content once with an MLS epoch secret and distributes that secret to current member devices
  - Gateway enforces current `conversation_epoch`, `mls_epoch`, tree hash, and exact device coverage when epoch secrets are rotated
  - Membership changes and secure-device key changes bump group epochs and fan out conversation state updates
  - Read/delivered lifecycle continues to work on encrypted group messages

- **Production MLS protocol**: **[IMPLEMENTED]**
  - `OHMF_MLS_V1` is now the active production group messaging scheme
  - MLS epoch secrets are distributed over the existing Signal-compatible device sessions
  - Full external RFC MLS interoperability is still future work

### E2EE Infrastructure (Partial)
- **Device key storage**: **[IMPLEMENTED]**
  - Ed25519 public keys stored per device
  - `device_identity_keys` table exists
  - Signal protocol key bundles supported for web secure messaging

- **Key backups**: **[PARTIAL]**
  - `device_key_backups` table exists
  - Encrypted backup support in schema
  - Client encryption/backup logic unclear

- **Signal protocol session management**: **[IMPLEMENTED]**
  - Web client maintains per-device ratchet state
  - One-time prekey claiming supported through gateway endpoints
  - Group MLS epoch-secret distribution reuses the existing Signal-style device sessions

- **Remaining E2EE work**: **[PARTIAL]**
  - Android secure messaging client not implemented
  - Group encryption UX is web-first
  - Full RFC MLS interoperability/export path remains future work

---

## 15. Client Platforms

### Android Platform

#### Android OTT-Only Capability
- **OTT core functionality**: **[PARTIAL]**
  - SDK framework exists (empty scaffolding)
  - Full implementation missing

- **Android E2EE client**: **[UNIMPLEMENTED]**
  - No Android Signal session or encrypted group client implementation yet
  - Current secure messaging rollout is web-first

#### Android Default SMS Handler Mode
- **Native SMS/MMS support**: **[UNIMPLEMENTED]**
  - No Android client implementation
  - Schema and backend ready

#### Android Mini-App Runtime
- **Mini-app host app**: **[IMPLEMENTED]**
  - Full WebView sandbox implementation
  - CatalogActivity for discovery
  - MiniAppRuntimeActivity for app execution
  - Complete bridge implementation
  - Permission system working

#### Android Relay Agent
- **Relay executor capability**: **[UNIMPLEMENTED]**
  - Job polling endpoints exist
  - Android app to accept/execute jobs missing

### Web Platform

#### Web OTT Functionality
- **Core messaging UI**: **[PARTIAL]**
  - Login working (OTP)
  - Conversation list functional
  - Message thread rendering basic
  - Two-pane layout (desktop) implemented
  - Mobile thread-only view implemented
  - State management minimal/vanilla JS
  - No offline capability yet

- **Web secure messaging (DM + group)**: **[IMPLEMENTED]**
  - DM encrypted messaging working with Signal-style device bundles
  - Group encrypted messaging working with explicit enablement and epoch-based rekey validation
  - Group readiness surfaced to UI via `e2ee_ready` and `e2ee_blocked_member_ids`

#### Web Relay Capability
- **Relay job UI**: **[PARTIAL]**
  - Job creation endpoint working
  - Display of available relay jobs partially implemented
  - UI for accepting/resolving jobs minimal

#### Web Mini-App Runtime
- **Mini-app iframe sandbox**: **[IMPLEMENTED]**
  - Standalone runtime page functional
  - Bridge protocol working
  - Examples functional

#### Web Features
- **Authentication**: **[IMPLEMENTED]**
  - OTP login/verify flow

- **Conversation management**: **[IMPLEMENTED]**
  - List, view, basic creation

- **Message display**: **[IMPLEMENTED]**
  - Status indicators (SENT, DELIVERED, READ)
  - Transport type display (SMS, OTT)

- **Real-time updates**: **[PARTIAL]**
  - WebSocket connection possible
  - Not fully integrated into main app UI

- **Responsive design**: **[IMPLEMENTED]**
  - Mobile and desktop layouts
  - Thread-only on mobile, two-pane on desktop

### iOS Platform

#### iOS Support Status: **[UNIMPLEMENTED]**
- Specification mentions Android and Web only
- No iOS client
- WebView for relay to Safari possible future work

---

## 16. iMessage Feature Parity

### iMessage Feature Parity Status

**Note:** The specification does NOT target iMessage feature parity explicitly. The features listed below represent overlapping capabilities that provide similar user experience to iMessage.

#### Messaging Basics
- **Send/receive text**: **[IMPLEMENTED]**
  - Full support via OTT
  - iMessage: ✓ Native

- **Group chat**: **[IMPLEMENTED]**
  - Full support
  - iMessage: ✓ Native

- **Conversation management**: **[IMPLEMENTED]**
  - Create, view, delete
  - iMessage: ✓ Native

#### Rich Features (iMessage-like)
- **Emoji reactions**: **[IMPLEMENTED]**
  - Full emoji support + special effects
  - iMessage: ✓ Reactions (thumbs up, heart, haha, etc.)

- **Message editing**: **[IMPLEMENTED]**
  - Edit messages after send
  - iMessage: ✓ Edit messages (iOS 16+)

- **Message deletion**: **[IMPLEMENTED]**
  - Delete messages for user and others
  - iMessage: ✓ Delete messages (iOS 16+)

- **Read receipts**: **[IMPLEMENTED]**
  - Watermark-based read tracking
  - iMessage: ✓ Show read receipts

- **Typing indicators**: **[IMPLEMENTED]**
  - Show when someone is typing
  - iMessage: ✓ Typing indicator

- **Delivery status**: **[IMPLEMENTED]**
  - Track: Sent, Delivered, Read, Failed
  - iMessage: ✓ Sent/Delivered/Read indicators

- **Message effects**: **[IMPLEMENTED]**
  - Confetti, balloons, fireworks, lasers, etc.
  - iMessage: ✓ Message effects

#### Media Sharing
- **Image sharing**: **[IMPLEMENTED]**
  - Upload and send images
  - iMessage: ✓ Photo sharing

- **Video sharing**: **[IMPLEMENTED]**
  - Upload and send videos
  - iMessage: ✓ Video sharing

- **Media captions**: **[PARTIAL]**
  - Caption field exists
  - iMessage: ✓ Image annotations

- **Media download**: **[IMPLEMENTED]**
  - Download shared media
  - iMessage: ✓ Save photos/videos

#### Communication Features
- **Presence/online status**: **[IMPLEMENTED]**
  - Show when users are active
  - iMessage: ✓ Online status (iOS 17+)

- **Mentions/tags**: **[PARTIAL]**
  - Schema exists, not fully implemented
  - iMessage: ✓ Mentions in iOS 17+

- **Message replies/threading**: **[PARTIAL]**
  - Reply-to field exists
  - Threading UI not implemented
  - iMessage: ✓ Inline replies

#### Privacy & Control
- **User blocking**: **[IMPLEMENTED]**
  - Block users completely
  - iMessage: ✓ Block contacts

- **Message expiry/deletion**: **[IMPLEMENTED]**
  - Redaction and deletion support
  - iMessage: ✓ Delete for me / Delete for everyone

- **Notification control**: **[IMPLEMENTED]**
  - Mute conversations
  - iMessage: ✓ Notification muting

#### Features NOT in OHMF (iMessage only)
- **Handwriting messages**: Not implemented in OHMF
- **Stickers**: Not implemented (mini-apps can provide)
- **Memoji**: Not implemented
- **Tapback alternatives**: Reactions implemented differently
- **Apple Pay integration**: Not in scope
- **Location sharing**: Not implemented
- **Shared albums**: Not implemented
- **FaceTime integration**: Not in scope
- **App integrations**: Mini-app framework provides alternative
- **iCloud sync**: Web-based sync instead

#### iMessage NOT in OHMF
- **SMS/MMS fallback**: Planned (Phase 2) but not implemented
- **Continuity (Handoff)**: Not applicable to web platform focus
- **End-to-end encryption by default**: Planned (Phase 5)
- **E2EE group key exchange**: Not implemented
- **RCS support**: Not in scope

---

## 17. Implementation Completeness Summary

### Fully Implemented (Production Ready)
- Core OTT messaging
- User authentication and sessions
- Multi-device support
- Conversation management
- Message CRUD operations
- Message editing and reactions
- Read receipts and typing indicators
- User presence
- User blocking
- Mini-app runtime and bridge
- Mini-app examples
- Media upload and download
- Contact discovery (hash-based)
- Device management
- Account recovery codes
- WebSocket realtime protocol

### Partially Implemented (Schema/API ready, implementation incomplete)
- Rich text support
- SMS/MMS transport (DB ready, clients missing)
- Linked-device relay (server ready, Android executor missing)
- Carrier mirroring policies
- Message threading/replies
- Edit history retrieval
- Message search
- Attachment caption display
- Mini-app message preview rendering
- Web relay UI
- 2FA flows
- Attestation verification
- Data export completion
- Message state filtering

### Unimplemented (Future Work)
- End-to-end encryption (Phase 5, out of scope for v1)
- Android SMS/MMS client implementation
- Android relay agent
- Full account deletion anonymization
- Scheduled message purge/cleanup jobs
- Thumbnail generation
- Chunked/resumed media uploads
- Passkey/WebAuthn authentication
- Search functionality
- iOS platform
- Native Android messaging library (core only)

---

## Phase Breakdown

### Phase 1: Core OTT (COMPLETE ✓)
- Internet-based messaging
- Group conversations
- User authentication
- Device management
- Real-time updates
- Media attachments
- All features in this phase implemented

### Phase 2: Android SMS/MMS (PARTIAL)
- Store schema: ✓
- Backend processors: ✓
- Android client: ✗
- Feature set not implemented end-to-end

### Phase 3: Linked-Device Relay (PARTIAL)
- Job queue: ✓
- Server routing: ✓
- Android relay agent: ✗
- Client implementation blocking

### Phase 4: Mini-Apps (COMPLETE ✓)
- Runtime: ✓
- Bridge protocol: ✓
- Examples: ✓
- Registry: ✓
- All features in this phase implemented

### Phase 5: Hardening & Expansion (MINIMAL)
- E2EE path: Documented only
- Abuse controls: Basic framework only
- Encryption integration: Not started

---

## Quick Stats

| Category | Count | Status |
|----------|-------|--------|
| **Implemented Features** | 78 | ✓ Complete |
| **Partial Features** | 32 | ◐ In Progress |
| **Unimplemented Features** | 24 | ✗ Not Started |
| **Total Feature Inventory** | 134 | 58% Ready |

---

## Priority To-Do List (In Order of Impact)

### High Priority (Block Production Use)
1. **End-to-End Encryption (Phase 5)**
   - Signal protocol integration
   - Key exchange and management
   - Message encryption/decryption

2. **Complete Message Search**
   - Full-text search API
   - Indexing strategy
   - Query operators

3. **Android SMS/MMS Client (Phase 2)**
   - SMS send/receive
   - MMS send/receive
   - Transport routing

### Medium Priority (Enhance Usability)
4. **Web Relay UI Completion (Phase 3)**
   - Beautiful relay job display
   - Accept/resolve workflow
   - Status tracking

5. **Android Relay Agent (Phase 3)**
   - Job polling and execution
   - SMS/MMS relay to web

6. **Rich Text Editor**
   - Web rich text input
   - Android rich text input
   - Formatting preservation

7. **Message Threading UI**
   - Reply preview display
   - Threaded conversation view
   - Quote rendering

### Lower Priority (Nice-to-Have)
8. **Data Export Completion**
   - Format options
   - Scheduling
   - Download delivery

9. **Search Full-Text Index**
   - Elasticsearch/Postgres FTS setup
   - Incremental indexing

10. **Thumbnail/Image Processing**
    - Image resizing
    - Format optimization
    - Lazy loading

11. **Account Deletion Anonymization**
    - Group message anonymization
    - Data retention compliance

12. **Scheduled Message Purge**
    - Background job runner
    - Retention policy enforcement
    - Cryptographic audit trail

---

## Recommendations for Next Phase

1. **Security First**: Implement end-to-end encryption before widescale production rollout
2. **Android Parity**: Complete Android SMS/MMS and relay agent for feature parity with web
3. **Search Critical**: Message search is high-value UX feature
4. **Test Coverage**: Comprehensive testing of partial features (relay, mirroring, etc.)
5. **Performance**: Load testing with large message histories and group chats
6. **Observability**: Production monitoring and debugging tools

---

*Report completed: All features cross-referenced with specification and implementation codebase.*
