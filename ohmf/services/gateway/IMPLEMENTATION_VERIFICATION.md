# E2EE and Group E2EE Implementation Verification

## Current Status

The web-first MLS-backed group E2EE rollout is now complete for the gateway and web client.

The shipped production group messaging path is:

- `OHMF_MLS_V1` encrypted envelopes for groups
- one content encryption per message using an MLS epoch secret
- one wrapped epoch-secret package per current member device when a sender establishes or rotates that epoch secret
- gateway validation of sender device, envelope signature, `conversation_epoch`, `mls_epoch`, tree hash, and exact device coverage for epoch-secret rotations

## Completed Features

### Direct-message E2EE
- Device bundle publishing and one-time prekey claiming are implemented.
- The web client maintains per-device Signal ratchet state.
- The gateway accepts and validates `content_type="encrypted"` messages.

### Group E2EE
- New groups are created as `ENCRYPTED` by policy, and legacy plaintext groups can still be upgraded through conversation metadata updates.
- Conversation responses now expose `e2ee_ready`, `e2ee_blocked_member_ids`, `mls_enabled`, `mls_epoch`, and `mls_tree_hash`.
- Encrypted groups require every current member to have at least one compatible secure-messaging device.
- Group member add/remove operations and secure-device key changes bump both `encryption_epoch` and `mls_epoch`.
- The gateway rejects stale encrypted envelopes, stale MLS tree hashes, and MLS epoch-secret rotations whose device package set does not exactly match the current member-device set.
- Conversation state updates are fanned out to members after encrypted-group state changes so clients can refresh and rekey.
- Web encrypted group sends retry once after refreshing conversation state when the server reports an epoch or membership mismatch.

### Delivery and Read State
- Delivered/read checkpoints remain conversation-level and continue to work for encrypted group messages.
- The group E2EE rollout does not require server decryption and does not change receipt semantics.

## Completed Verification

- `go test ./internal/conversations`
- `go test ./internal/messages -run 'TestValidateEncryptedConversationEnvelope|TestValidateSendContent'`
- `node --check ohmf/apps/web/app.js`

## Remaining To-Do

### Client Rollout
- Android secure messaging client is still not implemented.
- There is no cross-client Android/web encrypted-group interoperability validation yet.

### Cryptography Roadmap
- The active production group path is now MLS-backed, but it is still an OHMF-specific deployment format.
- Full RFC MLS interoperability/export, non-web client support, and wider cross-client verification remain future work.

### Validation and Coverage
- Add broader end-to-end tests for multi-user encrypted group flows, attachment flows, and epoch-refresh retries.
- Expand gateway integration coverage for encrypted-group membership churn and realtime receipt regressions.

### Operational Follow-Up
- Update user-facing docs and release notes for encrypted group support.
- Run full staging verification once Android and wider integration coverage are available.
