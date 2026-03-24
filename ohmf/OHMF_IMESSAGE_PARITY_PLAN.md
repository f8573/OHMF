# OHMF iMessage Parity Plan

Prepared: 2026-03-24  
Purpose: product and engineering to-do list for pursuing the right level of iMessage parity in OHMF, with explicit justification for what to build, defer, cut, or add beyond Apple’s model

## 1. Decision Framework

This is not a clone checklist.

A feature belongs in OHMF if it does at least one of the following:

- materially improves secure messaging reliability
- materially improves group coordination
- materially improves cross-device continuity
- materially improves mini-app/platform differentiation
- materially improves product trust and safety

A feature should be cut or deprioritized if it:

- is mainly decorative and expensive
- depends on a proprietary Apple ecosystem OHMF does not control
- distracts from more important reliability/security/platform work

## 2. Status Legend

- `Build Now`: high-priority backlog item
- `Build Later`: useful, but not yet the highest leverage
- `Cut`: deliberately do not pursue
- `Already Present`: already implemented or largely present in OHMF
- `OHMF Add`: not an iMessage feature, but should be part of OHMF’s product advantage

## 3. Current Snapshot

### 3.1 Where OHMF Already Has Meaningful Parity

| Capability | Decision | Notes |
| --- | --- | --- |
| Phone-number-first reachability | `Already Present` | OHMF supports phone-thread initiation before secure onboarding |
| Secure OTT DMs | `Already Present` | Device-aware encrypted path exists |
| Group E2EE / MLS | `Already Present` | Strong differentiator relative to many messaging products |
| Sent, delivered, and read receipts | `Already Present` | Present in backend and UI |
| Replies, reactions, edits, deletes | `Already Present` | Core lifecycle exists |
| Typing indicators | `Already Present` | Including named typing text in web UI |
| Multi-device crypto recovery/backfill | `Already Present` | Recent work closes a major product gap |
| Search | `Already Present` | Backend and test coverage exist |
| Mini-app platform | `OHMF Add` | Goes beyond iMessage’s app surface in a more explicit platform direction |

### 3.2 Where OHMF Is Behind

| Capability | Decision | Notes |
| --- | --- | --- |
| Visible pair-device UX | `Build Now` | Backend exists; end-user flow is incomplete |
| Human-verifiable trust UX | `Build Now` | Security maturity needs a user ceremony |
| Polished mixed-transport continuity | `Build Now` | Critical to phone-number-first messaging feel |
| Media UX depth | `Build Now` | Needed for mainstream messaging viability |
| Group ergonomics and polish | `Build Later` | Important, but not above trust/device continuity |
| Richer cross-platform clients | `Build Later` | Needed for broad adoption, but heavy investment |

## 4. Detailed Feature Decisions

### 4.1 Identity, Onboarding, and Reachability

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Phone-number-first contact reachability | Core | `Implemented` | `Already Present` | This is fundamental to OHMF and should remain a primary design principle |
| Automatic promotion from phone thread to secure OTT chat | Core concept | `Implemented` | `Already Present` | Recent gating fix aligns this with secure reality instead of optimistic conversion |
| Clear user messaging when secure delivery is unavailable | Apple-style graceful degradation | `Implemented` | `Already Present` | OHMF now blocks mistaken secure sends and instructs users to use phone/SMS path |
| Email-address reachability like Apple Account aliases | Important for Apple ecosystem | `To-do` | `Build Later` | Useful, but phone identity is more important for OHMF first |
| Seamless new-device setup UX | Core | `Partially implemented` | `Build Now` | Backend and crypto restore exist; user-facing flow must catch up |

### 4.2 Multi-Device Continuity

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| New device can read old messages after secure bootstrap | Core expectation | `Implemented` | `Already Present` | Recent backup/restore work should be treated as foundational and hardened |
| Device pairing screen in main client | Core expectation | `To-do` | `Build Now` | Without UI, the feature remains engineering-only |
| Device list management UI | Standard product feature | `To-do` | `Build Now` | Users need visibility and revocation controls |
| Device trust details and recent activity | Helpful trust feature | `To-do` | `Build Later` | Important, but comes after pairing is surfaced |

### 4.3 Delivery, Read, and Typing

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Sent/delivered/read receipts | Core | `Implemented` | `Already Present` | Already present and should remain high-quality |
| Human-readable per-user receipt labeling | Apple implies recipient context | `Implemented` | `Already Present` | Recent QoL work improves comprehension substantially |
| Typing indicators | Core | `Implemented` | `Already Present` | Already there |
| Named typing indicators | Better than baseline | `Implemented` | `Already Present` | Strong UX improvement, especially in groups |
| Fine-grained receipt privacy settings | Common mature messaging feature | `To-do` | `Build Later` | Valuable, but not blocking parity at this stage |

### 4.4 Editing, Unsend, Replies, Reactions

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Edit message | Core | `Implemented` | `Already Present` | Present; keep hardening encrypted edge cases |
| View edit history | Core | `Implemented` | `Already Present` | Important for transparency |
| Reply to specific message | Core | `Implemented` | `Already Present` | Needed for group usability |
| React to message | Core | `Implemented` | `Already Present` | Present |
| Unsend within a short strict window | Core iMessage feature | `Not equivalent` | `Build Later` | Nice parity item, but lower leverage than pairing/trust/media |
| Tombstone transparency for deletes | Product choice | `Implemented` | `Already Present` | OHMF’s current behavior is defensible and audit-friendly |

### 4.5 Group Conversation Ergonomics

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Name group | Core | `Partially surfaced` | `Build Later` | Important but not top-tier vs trust/device flows |
| Group image/icon | Core | `Partially surfaced` | `Build Later` | Same rationale |
| Mentions | Core | `Unclear/incomplete` | `Build Later` | High-value for active groups, but not critical before core parity gaps |
| Leave/remove/add members UX | Core | `Partially present` | `Build Later` | Needed for polished group management |
| Group spam block/delete workflow | Apple has this for some transports | `Partial` | `Build Later` | Useful once moderation/admin surfaces mature |

### 4.6 Media and Rich Content

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Photos/videos in chats | Core | `Implemented baseline` | `Build Now` | Must be polished for a credible messaging product |
| Audio messages | Core | `To-do` | `Build Later` | Valuable, but below image/video and trust work |
| Camera effects / novelty capture features | Expressive | `To-do` | `Cut` | Expensive, platform-specific, low leverage right now |
| Message effects / full-screen effects | Signature iMessage personality | `To-do` | `Build Later` | Nice differentiation layer after fundamentals |
| Text formatting effects | Newer iMessage feature | `To-do` | `Build Later` | Useful but not foundational |
| Rich content previews/gallery polish | Mature platform feature | `In Progress` | `Build Now` | Improves everyday usage more than novelty effects |

### 4.7 Security and Trust

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| End-to-end encryption | Core | `Implemented` | `Already Present` | Foundational |
| Group E2EE | Important | `Implemented` | `Already Present` | Strong platform differentiator |
| Contact Key Verification equivalent | Apple offers high-assurance verification | `To-do` | `Build Now` | OHMF needs a human trust ceremony to match its encryption ambitions |
| Public verification code | Part of CKV | `To-do` | `Build Later` | Good extension after core verification UX lands |
| Device attestation | Apple does not expose as user feature | `Implemented` | `OHMF Add` | Good OHMF-native trust advantage, should be made legible to users/admins |

### 4.8 Discovery, Safety, and Reliability

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| Transport fallback behavior | Strong | `Implemented baseline` | `Build Now` | OHMF’s phone/SMS bridge is strategic and should feel seamless |
| Clear failure states | Strong | `Implemented baseline` | `Build Now` | Users need to understand why a message is SMS, OHMF, or blocked |
| Spam filtering/reporting | Mature ecosystem feature | `Partial` | `Build Later` | Needed, but not before core messaging confidence is higher |
| Privacy settings for receipts/presence | Mature feature | `To-do` | `Build Later` | Useful but not immediate |

### 4.9 App and Platform Layer

| Feature | iMessage Behavior | OHMF Status | Decision | Justification |
| --- | --- | --- | --- | --- |
| App/extensions inside messages | Present in iMessage | `Implemented in stronger form` | `OHMF Add` | OHMF has a clearer mini-app platform than iMessage’s older extension model |
| Registry/review/install/update model | Apple has App Store ecosystem | `Implemented baseline` | `Already Present` | Strategic area to keep investing in |
| Conversation-native app session state | Not emphasized the same way by Apple | `Implemented` | `OHMF Add` | Strong differentiator and worth leaning into |
| Public demo app that proves platform value | Apple has many first-party experiences | `Not yet` | `Build Now` | This is where the requested `8 Ball Pool` app belongs |

## 5. The 8 Ball Pool Decision

### 5.1 Decision

`8 Ball Pool` should be built.

Decision: `Build Now`

### 5.2 Why This Is the Right Demo

It is the best public-facing mini-app demo because it showcases:

- multiplayer state
- participant context
- session recovery
- event streaming
- turn logic
- conversation projection
- a richer experience than a trivial counter or fortune teller

### 5.3 Why Not Stop at Mystic 8-Ball

The existing `Mystic 8-Ball` example is useful for:

- launch context
- state snapshots
- send-message bridge calls
- session storage

But it does not prove:

- complex game state synchronization
- real multi-user flow
- reconnection and resume depth
- broader public appeal

### 5.4 Suggested Delivery Shape

| Item | Decision | Notes |
| --- | --- | --- |
| Web-hosted first version | `Build Now` | Fastest path to a visible demo |
| Turn-based first release | `Build Now` | Lower complexity than full realtime physics |
| Realtime spectator/shot sync later | `Build Later` | Good stretch goal once core gameplay loop exists |
| Conversation message summaries for turns and wins | `Build Now` | Demonstrates projection into thread |
| Shareable install path through registry | `Build Now` | Must demonstrate actual platform distribution |

## 6. Features We Should Deliberately Cut

These cuts are intentional, not omissions.

| Feature | Decision | Why Cut |
| --- | --- | --- |
| Apple Cash parity | `Cut` | Region-limited, compliance-heavy, not core to OHMF’s current product |
| Genmoji parity | `Cut` | Tied to Apple Intelligence/hardware and not a core strategic need |
| Conversation backgrounds/themes as a top priority | `Cut` | Cosmetic, not worth displacing reliability/security work |
| Camera-effects parity | `Cut` | High implementation cost, low strategic value right now |
| Apple-specific SharePlay parity | `Cut` | Tied to Apple media ecosystem and FaceTime assumptions |
| Satellite messaging parity in near term | `Cut` | Requires external connectivity/regulatory/provider stack; too large for current stage |

## 7. Features We Should Add Even Though iMessage Does Not Define Them

These are OHMF-native product bets.

| Feature | Decision | Why |
| --- | --- | --- |
| Explicit secure-readiness gating before first secure send | `OHMF Add` | Prevents false security expectations |
| Device attestation surfaced in trust UI | `OHMF Add` | Extends cryptographic trust beyond Apple’s visible model |
| Mini-app permission consent and review visibility | `OHMF Add` | Necessary for a healthy conversation app ecosystem |
| Conversation-linked app sessions and event logs | `OHMF Add` | Stronger platform story than traditional iMessage app extensions |
| Exportable audit/history tools for power users/admins | `OHMF Add` | Valuable for trust, moderation, enterprise, and support workflows |

## 8. Prioritized Backlog

### 8.1 Priority 0

| Item | Decision | Why |
| --- | --- | --- |
| Pair-device UI in web client | `Build Now` | Converts backend capability into a real feature |
| Device management and revoke flow | `Build Now` | Required for a trustworthy multi-device product |
| Contact/device verification UX | `Build Now` | Encryption without human trust UX is incomplete |
| SMS-to-secure transition polish | `Build Now` | This is a core OHMF differentiator |
| 8 Ball Pool mini-app demo | `Build Now` | Best public proof of the mini-app platform |

### 8.2 Priority 1

| Item | Decision | Why |
| --- | --- | --- |
| Media UX polish and preview quality | `Build Now` | Everyday user value |
| Group admin/management UX | `Build Later` | Important once core direct messaging confidence is strong |
| Message unsend time-window contract | `Build Later` | Good parity item after higher-value gaps close |
| Read-receipt and presence privacy controls | `Build Later` | Mature product feature, not first blocker |

### 8.3 Priority 2

| Item | Decision | Why |
| --- | --- | --- |
| Message effects / formatting | `Build Later` | Adds personality after fundamentals |
| Mentions and deeper group notifications | `Build Later` | Useful for group quality |
| Shared-with-you style cross-app content surfaces | `Build Later` | Potentially interesting after app ecosystem grows |

## 9. What Success Looks Like

OHMF does not need to become a clone of Messages on iPhone.

Success looks like this:

- a user can start from a phone number without understanding the transport stack
- secure messaging becomes available at the correct moment and never over-promises
- a newly linked device can actually read the conversation history it should read
- receipts, typing, replies, reactions, edits, and deletes behave predictably
- trust verification is explicit and understandable
- mini-apps feel like a real platform, not a demo page
- the `8 Ball Pool` demo instantly communicates that OHMF is more than chat

## 10. Final Recommendation

Build parity where it improves trust, continuity, and communication quality.

Do not chase parity where it is mostly cosmetic or locked to Apple’s ecosystem.

Use OHMF’s differentiation aggressively:

- secure onboarding transitions
- stronger explicit device/trust modeling
- conversation-native mini-app platform
- public flagship demo app

The shortest path to a credible "better than clone" story is:

1. finish visible multi-device UX
2. finish trust verification UX
3. polish mixed transport behavior
4. ship `8 Ball Pool` as the flagship mini-app demo
