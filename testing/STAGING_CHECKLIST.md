# OHMF Staging And Release Signoff

Use this checklist for the manual/device gates that the repo cannot fully automate today.

## Staging Preconditions

- Gateway stack is healthy and reachable.
- Web frontend is reachable at the configured base URL.
- Test accounts exist for at least:
  - two ordinary users
  - one multi-device user
  - one blocked-user scenario
- Metrics, logs, and request IDs are observable during the run.

## Automated First Pass

Run before manual signoff:

```powershell
npm run test:integration
npm run test:e2e
npm run test:live
npm run test:perf
```

If the environment supports it, `npm run test:staging` with `OHMF_RUN_STAGING_AUTOMATION=1` can run the automated staging subset first.

## Manual Android And Device Coverage

- Mini-app host launch, install, session restore, and permission prompts on Android.
- WebView isolation, allowed origins, and blocked capabilities on Android.
- Push receipt, notification tap, resume-from-background, and reconnect behavior.
- Secure token storage and logout/session revocation across linked devices.
- SMS/MMS send and receive flows on Android where the client is the default handler.
- Relay execution from web through a linked Android device, including retry and failure copy.
- Background reconnect/resume after transient network loss.

## Cross-Feature Staging Matrix

- OTP signup, refresh rotation, logout, and recovery-code access.
- DM and group creation, send, read, edit, react, delete, archive, mute, pin, and block flows.
- Multi-device linking, revocation, privacy toggles, and trust verification refresh.
- Media upload/download and attachment retry after failure.
- Mini-app catalog, launch, session sharing, projected messages, and state refresh.
- Realtime plus sync convergence after reconnect.
- Account export and deletion flows, including degraded-mode responses for incomplete features.
- Search, encrypted group lifecycle, and delete-sync convergence where supported by the deployed stack.

## Observability And Resilience Review

- Health and readiness endpoints return expected results during the run.
- Key errors include request IDs and enough context to trace send, fanout, and sync flows.
- Reconnect and retry paths do not duplicate timeline state.
- Metrics show sane latency and error rates during sustained messaging.

## Release Signoff

- Complete one soak run with sustained messaging, reconnects, group churn, and live metrics review.
- Record which partial features were verified in degraded mode versus fully exercised.
- Record any excluded flows and the blocking reason.
