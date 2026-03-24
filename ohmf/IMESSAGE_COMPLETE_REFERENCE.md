# iMessage Complete Reference

Prepared: 2026-03-24  
Purpose: working reference for Apple Messages/iMessage capabilities so OHMF can make deliberate parity decisions

## 1. Scope and Method

This document describes iMessage and closely related Apple Messages behaviors as reflected in Apple documentation available on 2026-03-24.

It focuses on:

- end-user messaging behavior
- transport and fallback rules
- security and verification features
- group messaging features
- app/platform integrations that materially affect messaging product design

It does not attempt to reverse-engineer private Apple implementation details.

## 2. Core Product Model

iMessage is Apple’s internet-based messaging layer inside the Messages app. It coexists with:

- iMessage
- SMS
- MMS
- RCS

High-level rules:

- Blue bubbles identify iMessage.
- Green bubbles identify SMS, MMS, or RCS.
- The Messages app automatically chooses message type based on device support, settings, connectivity, and carrier support.
- If iMessage cannot deliver, Apple can offer fallback behaviors such as retrying or sending as text message where supported.

Source basis:

- Apple Support: `If you can't send or receive messages on your iPhone or iPad`
- Apple Support: `Send a group text message on your iPhone or iPad`

## 3. Identity and Reachability

### 3.1 Identity Inputs

iMessage can use:

- phone numbers
- Apple Account email addresses

Multi-device behavior is part of the core model:

- the same Apple Account can receive messages across iPhone, iPad, Mac, Apple Watch, and Vision Pro where supported
- users can choose which phone number or email addresses are used for send/receive

### 3.2 Device Reachability

Apple’s user-facing model assumes:

- Messages can appear on multiple Apple devices
- setup on a new device may require updating send/receive settings
- device continuity is normal, not a special case

The user experience emphasizes account-level continuity even though the security and device infrastructure below it are device-aware.

## 4. Core Conversation Types

### 4.1 One-to-One Chats

iMessage supports direct conversations with a single person and can include:

- text
- emoji
- attachments
- reactions
- edited/unsent messages
- app content
- payments
- location and safety flows

### 4.2 Group Chats

Apple documents these group capabilities:

- start a group conversation
- add someone to an existing group if the group size rules allow
- remove someone from a qualifying group
- leave a group if enough participants remain
- name the group
- set a group image/icon
- mention people in a group
- mute notifications
- collaborate on shared files/projects

In mixed transport groups, behavior degrades depending on whether the thread is iMessage, MMS, or RCS.

Apple also documents feature differences:

- group MMS in iOS 17 or later can support tapbacks, message effects, message edits, and replies to specific messages if at least one iMessage user is present
- group SMS is much more limited and responses may be delivered individually
- group RCS in iOS 18 or later supports richer content than SMS/MMS

## 5. Composition and Send Features

### 5.1 Standard Composition

iMessage supports:

- text entry
- emoji
- photo and video sharing
- links
- app-inserted content
- stickers and Memoji-related content

### 5.2 Send Later

Apple documents `Send Later` as an iMessage app capability.

Implications:

- scheduled send is a first-party supported Messages feature
- it is modeled as a Messages app surface, not just a hidden automation trick

### 5.3 Message Effects and Formatting

Apple documents:

- text effects
- bold
- italics
- underline
- strikethrough
- bubble effects
- full-screen effects
- camera effects
- replaying received effects

Effects are positioned as part of expressive messaging, not merely decoration.

### 5.4 iMessage Apps

Apple documents a Messages app ecosystem that includes:

- Send Later
- Genmoji
- Polls
- Store
- Photos
- Apple Cash
- other iMessage apps downloaded through Apple’s ecosystem

This means Apple treats Messages as an extensible host surface rather than a fixed text-only product.

### 5.5 Polls

Apple now documents polls inside iMessage conversations.

Product meaning:

- small collaborative workflows are native to the messaging UI
- conversation-native decision making is treated as a messaging feature

### 5.6 Genmoji and Apple Intelligence Features

Apple documents:

- Genmoji generation through Apple Intelligence
- unread message summaries through Apple Intelligence

Practical implications:

- Apple is using generative features as assistive UI, not only as separate standalone apps
- some features require newer hardware and specific OS versions

## 6. Message Lifecycle Controls

### 6.1 Edit

Apple documents:

- a sent message can be edited up to five times
- edits are allowed within 15 minutes of sending
- a message is marked as edited in the transcript
- users can tap `Edited` to view prior versions
- older recipient operating systems may see fallback behavior instead of native edit history

### 6.2 Unsend

Apple documents:

- recently sent messages can be unsent for up to 2 minutes after sending
- both participants see that a message was unsent
- older operating systems may still expose the original message

### 6.3 Delete and Thread Cleanup

Apple’s platform includes:

- per-message deletion
- conversation deletion
- recently deleted behaviors
- cross-device deletion behavior when Messages in iCloud is enabled

### 6.4 Search

Apple documents:

- in-app Messages search
- limiting search to a person or conversation
- opening a result in context

## 7. Reactions, Replies, and Attention Mechanics

### 7.1 Tapbacks

Tapbacks are a first-class iMessage behavior.

Apple’s broader messaging docs and transport guidance indicate:

- Tapbacks are native in iMessage
- Apple has also expanded some reaction behavior into group MMS/RCS scenarios in newer releases

### 7.2 Inline Replies

Apple documents the ability to reply to specific messages in supported group message contexts.

Threaded reply behavior helps preserve conversational structure in busy groups.

### 7.3 Mentions

Apple documents:

- mention someone by name or with `@name`
- mention notifications can bypass mute depending on settings

This is a practical group coordination feature, not just a decorative tag.

## 8. Delivery and Attention Signals

### 8.1 Delivery Status

The Apple user model clearly distinguishes:

- sent state
- not delivered/error state
- delivered/read status in iMessage contexts
- green-bubble transport fallbacks for non-iMessage paths

### 8.2 Read Receipts

Read receipts are a long-standing iMessage feature and remain part of the product model.

### 8.3 Typing and Lock-Screen Interactions

While Apple’s public docs do not always enumerate typing indicators as a feature checklist item, typing awareness is part of the product experience. Apple also explicitly documents lock-screen reply behavior as a user setting.

### 8.4 Mute / Hide Alerts

Apple documents `Hide Alerts` at the conversation level:

- muting affects one conversation only
- other messages still notify normally

## 9. Safety, Location, and Trust Features

### 9.1 Check In

Apple documents Check In as a Messages-integrated safety feature.

Capabilities include:

- timer-based Check In
- recipient selection from Messages
- configurable shared detail levels
- automatic prompts when the timer ends
- ability to add more time
- automatic trusted-contact notification if the Check In is not completed as expected

Apple documents limited/full data sharing modes, including location and device state context.

### 9.2 Contact Key Verification

Apple documents iMessage Contact Key Verification (CKV) as a high-assurance identity-verification feature.

Documented behaviors include:

- alerting when verification errors occur
- manual on-device code comparison
- public verification code sharing
- verification state surfaced in conversation details and contacts

Apple also explicitly says CKV is not designed to stop phishing or generic text-message scams. It is an identity-assurance mechanism, not a universal abuse solution.

### 9.3 Blocking and Spam Handling

Apple supports:

- blocking contacts/conversations
- reporting or filtering messages
- deleting and blocking some spammy group threads in newer OS releases

## 10. Content Sharing and Collaboration

### 10.1 Shared with You

Apple documents `Shared with You` as a cross-app content organization model.

Messages-shared content can surface in:

- Photos
- Music
- TV
- News
- Podcasts
- Safari

Users can disable the feature globally or per person/thread in supported contexts.

### 10.2 File/Project Collaboration

Apple documents Messages-based collaboration for shared documents and files, including content from:

- Notes
- Freeform
- Reminders
- Safari
- Keynote
- Numbers
- Pages

Messages is therefore part of Apple’s collaboration workflow, not only a chat transcript.

### 10.3 Share Sheets and Cross-App Share Into Messages

Messages is deeply integrated into the Apple share sheet model. Users can send content into Messages from many apps and then continue discussing the shared object in-thread.

## 11. FaceTime and SharePlay Adjacency

Messages integrates with real-time communication and co-experience flows:

- jump from a message thread to FaceTime
- share your screen
- request to see another person’s screen
- use SharePlay to watch, listen, or play together from a conversation context

Important product point:

- Apple does not keep every synchronous feature inside the message transcript itself
- instead, Messages acts as the launch/context layer for adjacent real-time experiences

## 12. Payments and Commerce

Apple documents Apple Cash use in or from Messages.

Capabilities include:

- send money
- request money
- interact with underlined monetary amounts in messages
- in newer group contexts, split bills or track group-related payment flows

This is region-dependent and tied to Apple’s payments stack.

## 13. Satellite Messaging

Apple documents `Messages via satellite` beginning with iOS 18 on supported devices and regions.

Key behaviors Apple documents:

- send and receive iMessages or SMS messages while off-grid
- support for texts, emojis, and Tapbacks
- requirement for clear view of the sky/horizon
- potential fallback to SMS via satellite if iMessage conditions are not met

Apple also documents limitations:

- no photos or videos
- no audio messages
- no stickers
- no group messages

Apple further notes that iMessages sent via satellite remain end-to-end encrypted.

## 14. Cross-Device Behavior

Apple Messages/iMessage spans:

- iPhone
- iPad
- Mac
- Apple Watch
- Apple Vision Pro

Observed/documented product behaviors include:

- send/receive continuity across devices
- account-linked device setup and repair steps
- cross-device deletion when Messages in iCloud is enabled
- continued conversations across nearby devices

## 15. Transport Degradation and Fallback Behavior

Apple’s Messages product is explicitly multi-transport.

That means the UX handles:

- iMessage
- RCS
- MMS
- SMS

The user is not expected to manage the protocol directly. The app chooses based on:

- recipient/device capability
- connectivity
- carrier support
- recent message history in some cases

This is one of Apple’s biggest practical advantages: the user sees one app and one conversation space, while the transport may vary underneath.

## 16. Platform Personality Features

Apple uses a set of product details that materially shape perception:

- blue vs green bubbles
- effects and animation replay
- conversation photos/backgrounds
- personal name and photo sharing
- rich app tray interactions
- visible edit/unsend markers
- trusted safety features in-line with normal messaging

These are not all equally essential to parity, but together they create the "iMessage feel."

## 17. What iMessage Is Not

Important constraints from Apple’s own documentation:

- not every feature works over every transport
- not every feature works in mixed groups
- not every feature works on older OS versions
- satellite mode strips out several media/group features
- Contact Key Verification is not general anti-scam protection

So even Apple’s product is not a single universal capability layer. It is a tiered experience with graceful degradation.

## 18. Implications for OHMF

The important lessons from iMessage are not "copy every visible effect."

The real lessons are:

- transport variation must be abstracted cleanly
- multi-device continuity must feel native
- secure messaging readiness must be explicit
- conversation-level collaboration and app integration matter
- trust and safety features should live in the messaging flow, not only in settings
- degradation paths must be understandable

## 19. Source Appendix

Primary Apple references used for this document:

- Apple Support, `Use Messages on your iPhone or iPad`  
  https://support.apple.com/en-ng/104982
- Apple Support, `If you can't send or receive messages on your iPhone or iPad`  
  https://support.apple.com/en-asia/118433
- Apple Support, `Unsend and edit messages on iPhone`  
  https://support.apple.com/en-mide/guide/iphone/-iphe67195653/ios
- Apple Support, `Have a group conversation in Messages on iPhone`  
  https://support.apple.com/guide/iphone/group-conversations-iphb10c80fc5/18.0/ios/18.0
- Apple Support, `Send a group text message on your iPhone or iPad`  
  https://support.apple.com/en-afri/118236
- Apple Support, `Use message effects with iMessage on your iPhone and iPad`  
  https://support.apple.com/en-us/104970
- Apple Support, `Use iMessage apps on your iPhone and iPad`  
  https://support.apple.com/en-afri/104969
- Apple Support, `Share content in Messages on iPhone`  
  https://support.apple.com/en-kz/guide/iphone/iphb66cfeaad/ios
- Apple Support, `Collaborate on projects with Messages on iPhone`  
  https://support.apple.com/en-is/guide/iphone/iphf08c82a16/ios
- Apple Support, `Share screens using Messages on iPhone`  
  https://support.apple.com/en-lamr/guide/iphone/-iph861568c10/ios
- Apple Support, `Use Check In for Messages on iPhone`  
  https://support.apple.com/guide/personal-safety/use-check-in-for-messages-ips56b5bc469/web
- Apple Support, `About Messages via satellite on your iPhone`  
  https://support.apple.com/en-my/120930
- Apple Support, `About iMessage Contact Key Verification`  
  https://support.apple.com/en-us/118246

## 20. Final Summary

iMessage is not just secure texting. It is a layered product combining:

- device continuity
- multi-transport abstraction
- expressive messaging
- group coordination tools
- trust and safety workflows
- app/platform integrations
- increasingly rich collaboration and AI-assisted features

The right parity strategy for OHMF is therefore selective: match the high-leverage system behaviors, not every cosmetic detail.
