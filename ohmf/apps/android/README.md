# Android Mini-App Host

This directory now contains a standalone Android host scaffold for the OHMF mini-app platform.

## Included

- `miniapp-host/`
  - Gradle Android app skeleton
  - catalog screen
  - WebView runtime activity
  - registry client
  - install/session persistence
  - bridge shell that mirrors the web `postMessage` envelope

## Notes

- The bridge contract intentionally matches [packages/miniapp/bridge-contract.md](ohmf/packages/miniapp/bridge-contract.md).
- Production hardening still requires stricter WebView isolation, publisher review enforcement, and origin pinning beyond the local scaffold.
- This environment does not include an Android SDK, so the project is added as source but not compiled here.
