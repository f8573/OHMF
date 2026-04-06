# OHMF Web Playwright Suite

This workspace adds a browser-level QA layer on top of the existing `node:test` frontend checks and the Go gateway tests.

## Install

From [apps/web](c:/Users/James/Downloads/Messages/ohmf/apps/web):

```powershell
npm install
npx playwright install chromium
```

## Run

Fast local checks:

```powershell
npm run test:unit
npm run test:smoke
```

Full mocked-browser suite:

```powershell
npm run test:e2e
```

Headed exploratory run:

```powershell
npm run test:e2e:headed
```

## Scope

- `node:test` remains the fastest contract layer for helper modules.
- Playwright covers browser rendering, modals, toggles, attachment staging, and evidence capture.
- Docker-backed gateway validation should still be run separately for live messaging, receipt fanout, mini-app session flows, and migration safety.

## Evidence

The suite writes screenshots into Playwright test outputs and attaches them to the HTML report. Use those as the baseline artifact for signoff and regressions.
