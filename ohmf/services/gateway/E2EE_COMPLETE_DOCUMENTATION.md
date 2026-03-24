# End-to-End Encryption (E2EE) Documentation

## Executive Summary

OHMF currently ships:

- direct-message E2EE using Signal-style device bundles and per-device ratchet state
- web-first group E2EE using client-managed encrypted envelopes
- gateway-side validation of sender device ownership, envelope signatures, current conversation epoch, and exact recipient-device coverage

The active production group protocol is not MLS. Existing MLS schema and helper code remain in the repository as scaffolding for future work, but the deployed message path uses Signal-style recipient fanout.

## Implemented Features

### Direct Messages
- X25519 agreement keys, Ed25519 signing keys, signed prekeys, and one-time prekeys
- Signal-style encrypted message envelopes with sender signatures
- Per-device ratchet session state in the web client
- Gateway validation for encrypted message structure and sender device ownership

### Group Messaging
- New groups default to `ENCRYPTED`, with metadata upgrade support retained for legacy plaintext groups
- `e2ee_ready` and `e2ee_blocked_member_ids` surfaced in conversation responses
- One ciphertext per message, one wrapped content key per current recipient device
- Epoch-based invalidation when group membership changes
- Gateway rejection of stale or incomplete encrypted group envelopes
- Read and delivered checkpoints preserved for encrypted groups

### Gateway Enforcement
- Encrypted conversations require OTT transport
- Sender device must belong to the authenticated user
- Encrypted group envelopes must match the current `conversation_epoch`
- Encrypted group envelopes must include the exact compatible member-device set

## Repository Status

### Completed
- DM E2EE gateway flow
- Web DM E2EE client flow
- Web group E2EE send and receive flow
- Conversation readiness reporting for encrypted groups
- Epoch bump and conversation state fanout on encrypted-group membership change
- Focused gateway tests for encrypted-group validation

### To-Do
- Android secure messaging client
- Cross-client encrypted-group interoperability testing
- Broader end-to-end coverage for encrypted group attachments and membership churn
- Full MLS migration or replacement plan if the product moves beyond Signal-style group fanout

## Verification

Verified in this repository with:

```powershell
& 'c:\Users\James\Downloads\Messages\ohmf\.tools\go\bin\go.exe' test ./internal/conversations
& 'c:\Users\James\Downloads\Messages\ohmf\.tools\go\bin\go.exe' test ./internal/messages -run 'TestValidateEncryptedConversationEnvelope|TestForwardMessageCopiesSourceMetadata'
node --check ohmf/apps/web/app.js
```

## Notes

- The full `internal/messages` package still contains unrelated preexisting search-test failures outside the encrypted-group changes.
- Group E2EE is web-first. Android remains a documented follow-up, not a completed client platform.
