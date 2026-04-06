import { createMiniAppClientFromLocation } from "../../miniapp-sdk.js";
import { createEightballRenderer } from "./renderer.js";

const runtimeParams = new URLSearchParams(window.location.search);
const isPreviewMode = runtimeParams.get("preview") === "1";
const hasBridgeContext = Boolean(runtimeParams.get("channel") && runtimeParams.get("parent_origin"));
const bridge = hasBridgeContext ? createMiniAppClientFromLocation() : null;
const permissionHelpers = window.OHMFEightballPermissions || {};
const rules = window.OHMFEightballRules || {};

const POCKETS = Object.freeze([
  { id: "head-left", label: "Head Left", x: 6, y: 8 },
  { id: "head-center", label: "Head Center", x: 50, y: 5.5 },
  { id: "head-right", label: "Head Right", x: 94, y: 8 },
  { id: "foot-left", label: "Foot Left", x: 6, y: 92 },
  { id: "foot-center", label: "Foot Center", x: 50, y: 94.5 },
  { id: "foot-right", label: "Foot Right", x: 94, y: 92 },
]);

const RACK_APEX_X = 62;
const RACK_CENTER_Y = 50;
const RACK_ROW_X_STEP = 2.94;
const RACK_ROW_Y_STEP = 3.4;
const RACK_BALL_ORDER = Object.freeze([
  { key: "15", label: "15", kind: "stripe", row: 0, slot: 0 },
  { key: "11", label: "11", kind: "stripe", row: 1, slot: 0 },
  { key: "14", label: "14", kind: "stripe", row: 1, slot: 1 },
  { key: "4", label: "4", kind: "solid", row: 2, slot: 0 },
  { key: "8", label: "8", kind: "black", row: 2, slot: 1 },
  { key: "2", label: "2", kind: "solid", row: 2, slot: 2 },
  { key: "6", label: "6", kind: "solid", row: 3, slot: 0 },
  { key: "9", label: "9", kind: "stripe", row: 3, slot: 1 },
  { key: "7", label: "7", kind: "solid", row: 3, slot: 2 },
  { key: "13", label: "13", kind: "stripe", row: 3, slot: 3 },
  { key: "1", label: "1", kind: "solid", row: 4, slot: 0 },
  { key: "5", label: "5", kind: "solid", row: 4, slot: 1 },
  { key: "3", label: "3", kind: "solid", row: 4, slot: 2 },
  { key: "10", label: "10", kind: "stripe", row: 4, slot: 3 },
  { key: "12", label: "12", kind: "stripe", row: 4, slot: 4 },
]);

const TABLE_LAYOUT = Object.freeze([
  { key: "cue", label: "C", kind: "cue", x: 24, y: 50 },
  ...RACK_BALL_ORDER.map((entry) => ({
    ...entry,
    x: RACK_APEX_X + entry.row * RACK_ROW_X_STEP,
    y: RACK_CENTER_Y + (entry.slot - entry.row / 2) * RACK_ROW_Y_STEP * 2,
  })),
]);
const PLAYFIELD_ASPECT = 1771 / 980;

const ASSET_VERSION = (() => {
  try {
    const currentUrl = new URL(import.meta.url);
    return (
      currentUrl.searchParams.get("asset_version") ||
      window.OHMF_RUNTIME_CONFIG?.asset_version ||
      "dev"
    );
  } catch {
    return window.OHMF_RUNTIME_CONFIG?.asset_version || "dev";
  }
})();

function assetUrl(relativePath) {
  const url = new URL(relativePath, import.meta.url);
  url.searchParams.set("asset_version", ASSET_VERSION);
  return url.toString();
}

const HUD_COMPONENT_URLS = Object.freeze({
  ballBackdrop: assetUrl("./assets/render/extracted/hud/hud-component-10-corrected.png"),
  powerFill: assetUrl("./assets/render/extracted/hud/hud-component-34.png"),
  powerShell: assetUrl("./assets/render/extracted/hud/hud-component-35.png"),
  powerOverlay: assetUrl("./assets/render/extracted/hud/hud-component-14.png"),
});

const BALL_SLOT_COMPONENTS = Object.freeze({
  1: assetUrl("./assets/render/extracted/hud/hud-component-41.png"),
  2: assetUrl("./assets/render/extracted/hud/hud-component-58.png"),
  3: assetUrl("./assets/render/extracted/hud/hud-component-44.png"),
  4: assetUrl("./assets/render/extracted/hud/hud-component-59.png"),
  5: assetUrl("./assets/render/extracted/hud/hud-component-60.png"),
  6: assetUrl("./assets/render/extracted/hud/hud-component-54.png"),
  7: assetUrl("./assets/render/extracted/hud/hud-component-48.png"),
  8: assetUrl("./assets/render/extracted/hud/hud-component-56.png"),
  9: assetUrl("./assets/render/extracted/hud/hud-component-64.png"),
  10: assetUrl("./assets/render/extracted/hud/hud-component-51.png"),
  11: assetUrl("./assets/render/extracted/hud/hud-component-55.png"),
  12: assetUrl("./assets/render/extracted/hud/hud-component-43.png"),
  13: assetUrl("./assets/render/extracted/hud/hud-component-57.png"),
  14: assetUrl("./assets/render/extracted/hud/hud-component-47.png"),
  15: assetUrl("./assets/render/extracted/hud/hud-component-28.png"),
});

function cueBallLayoutEntry() {
  return TABLE_LAYOUT.find((entry) => entry.kind === "cue") || TABLE_LAYOUT[0];
}

function cueAngleForPocket(pocketId) {
  const cueBall = cueBallLayoutEntry();
  const pocket = POCKETS.find((entry) => entry.id === pocketId) || POCKETS[POCKETS.length - 1];
  const dx = pocket.x - cueBall.x;
  const dy = cueBall.y - pocket.y;
  return Math.round((Math.atan2(dy, dx) * 180) / Math.PI);
}

const DEFAULT_DRAFT = Object.freeze({
  result: "pocket",
  claimGroup: "solids",
  ownBallsPocketed: 1,
  opponentBallsPocketed: 0,
  breakBallsPocketed: 1,
  calledPocket: "foot-right",
  cuePower: 62,
  cueAngle: cueAngleForPocket("foot-right"),
  cueSpinX: 0,
  cueSpinY: 0,
});

const state = {
  launchContext: null,
  recentMessages: [],
  blockedActions: null,
  shotDraft: { ...DEFAULT_DRAFT },
  composerSaveTimer: 0,
  assets: {
    table: null,
    cue: null,
    ready: false,
    error: "",
  },
  playfieldObserver: null,
  turnTimer: {
    signature: "",
    startedAt: 0,
    intervalId: 0,
  },
  renderer: null,
  playfieldStabilizeTimer: 0,
};

const el = {
  appShell: document.getElementById("app-shell"),
  previewShell: document.getElementById("preview-shell"),
  status: document.getElementById("status-pill"),
  phasePill: document.getElementById("phase-pill"),
  turnPill: document.getElementById("turn-pill"),
  tableSummary: document.getElementById("table-summary"),
  viewerLine: document.getElementById("viewer-line"),
  participantsLine: document.getElementById("participants-line"),
  sharedLine: document.getElementById("shared-line"),
  countLine: document.getElementById("count-line"),
  legalTargetLine: document.getElementById("legal-target-line"),
  instructionLine: document.getElementById("instruction-line"),
  calledPocketLabel: document.getElementById("called-pocket-label"),
  potIndicatorLabel: document.getElementById("pot-indicator-label"),
  potIndicatorGrid: document.getElementById("pot-indicator-grid"),
  cueAngleInput: document.getElementById("cue-angle-input"),
  cueAngleLabel: document.getElementById("cue-angle-label"),
  cueBallSelector: document.getElementById("cue-ball-selector"),
  cueSpinMarker: document.getElementById("cue-spin-marker"),
  cueSpinLabel: document.getElementById("cue-spin-label"),
  cueSpinResetBtn: document.getElementById("cue-spin-reset-btn"),
  cuePowerInput: document.getElementById("cue-power-input"),
  cuePowerLabel: document.getElementById("cue-power-label"),
  cuePowerMeterFill: document.getElementById("cue-power-meter-fill"),
  cuePowerShellArt: document.getElementById("cue-power-shell-art"),
  cuePowerFillArt: document.getElementById("cue-power-fill-art"),
  cuePowerOverlayArt: document.getElementById("cue-power-overlay-art"),
  turnTimerLabel: document.getElementById("turn-timer-label"),
  turnOwnerLine: document.getElementById("turn-owner-line"),
  pottedSummaryLine: document.getElementById("potted-summary-line"),
  groupSummaryLine: document.getElementById("group-summary-line"),
  eightballStatusLine: document.getElementById("eightball-status-line"),
  helperShotLine: document.getElementById("helper-shot-line"),
  matchPlayerLeftName: document.getElementById("match-player-left-name"),
  matchPlayerLeftMeta: document.getElementById("match-player-left-meta"),
  matchPlayerLeftBadge: document.getElementById("match-player-left-badge"),
  matchPlayerLeftTargets: document.getElementById("match-player-left-targets"),
  matchPlayerRightName: document.getElementById("match-player-right-name"),
  matchPlayerRightMeta: document.getElementById("match-player-right-meta"),
  matchPlayerRightBadge: document.getElementById("match-player-right-badge"),
  matchPlayerRightTargets: document.getElementById("match-player-right-targets"),
  matchCenterStatus: document.getElementById("match-center-status"),
  matchCenterCopy: document.getElementById("match-center-copy"),
  scoreboard: document.getElementById("scoreboard"),
  rackProgress: document.getElementById("rack-progress"),
  historyList: document.getElementById("history-list"),
  messageList: document.getElementById("message-list"),
  playfieldShell: document.querySelector(".playfield-shell"),
  playfield: document.getElementById("playfield"),
  playfieldCanvas: document.getElementById("playfield-canvas"),
  playfieldStatus: document.getElementById("playfield-status"),
  pocketMap: document.getElementById("pocket-map"),
  tableBalls: document.getElementById("table-balls"),
  solidsTray: document.getElementById("solids-tray"),
  stripesTray: document.getElementById("stripes-tray"),
  solidsOwner: document.getElementById("solids-owner"),
  stripesOwner: document.getElementById("stripes-owner"),
  claimGroupRow: document.getElementById("claim-group-row"),
  claimGroupSelect: document.getElementById("claim-group-select"),
  pocketCountRow: document.getElementById("pocket-count-row"),
  ownPocketedInput: document.getElementById("own-pocketed-input"),
  opponentPocketedInput: document.getElementById("opponent-pocketed-input"),
  breakCountRow: document.getElementById("break-count-row"),
  breakPocketedInput: document.getElementById("break-pocketed-input"),
  startBtn: document.getElementById("start-btn"),
  submitShotBtn: document.getElementById("submit-shot-btn"),
  refreshBtn: document.getElementById("refresh-btn"),
  projectBtn: document.getElementById("project-btn"),
  actionInputs: Array.from(document.querySelectorAll('input[name="shot-action"]')),
  previewAnswerText: document.getElementById("preview-answer-text"),
  previewCaption: document.getElementById("preview-caption"),
};

if (el.cuePowerShellArt) el.cuePowerShellArt.src = HUD_COMPONENT_URLS.powerShell;
if (el.cuePowerFillArt) el.cuePowerFillArt.src = HUD_COMPONENT_URLS.powerFill;
if (el.cuePowerOverlayArt) el.cuePowerOverlayArt.src = HUD_COMPONENT_URLS.powerOverlay;

function sanitizeText(value, limit = 220) {
  return String(value || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, limit);
}

function resetPlayfieldCanvas() {
  const canvas = el.playfieldCanvas;
  if (!canvas?.parentNode) return canvas;
  const replacement = document.createElement("canvas");
  replacement.id = canvas.id;
  replacement.className = canvas.className;
  const ariaLabel = canvas.getAttribute("aria-label");
  if (ariaLabel) replacement.setAttribute("aria-label", ariaLabel);
  canvas.parentNode.replaceChild(replacement, canvas);
  el.playfieldCanvas = replacement;
  return replacement;
}

function clampNumber(value, min, max, fallback) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return Math.max(min, Math.min(max, numeric));
}

function setStatus(message, isError = false) {
  el.status.textContent = sanitizeText(message, 180);
  el.status.classList.toggle("error", isError);
}

function requireBridge() {
  if (bridge) return bridge;
  throw new Error(isPreviewMode ? "Preview mode has no host bridge." : "Missing host bridge.");
}

function getPlayfieldRenderer() {
  if (!state.renderer && el.playfieldCanvas) {
    try {
      state.renderer = createEightballRenderer({
        canvas: el.playfieldCanvas,
        statusEl: el.playfieldStatus,
      });
    } catch (error) {
      const message = String(error?.message || "");
      if (!/existing context of a different type|context could not be created/i.test(message)) {
        throw error;
      }
      const replacementCanvas = resetPlayfieldCanvas();
      state.renderer = createEightballRenderer({
        canvas: replacementCanvas,
        statusEl: el.playfieldStatus,
      });
    }
  }
  return state.renderer;
}

async function ensureRenderAssets() {
  if (state.assets.ready) return;
  try {
    const renderer = getPlayfieldRenderer();
    if (!renderer) return;
    await renderer.load();
    state.assets.ready = true;
    state.assets.error = "";
    schedulePlayfieldRender();
  } catch (error) {
    console.error(error);
    state.assets.error = sanitizeText(error?.message, 200) || "Render assets failed to load.";
  }
}

function currentSnapshot() {
  const snapshot = state.launchContext?.state_snapshot;
  if (snapshot && typeof snapshot === "object" && typeof rules.normalizeSnapshot === "function") {
    return rules.normalizeSnapshot(snapshot);
  }
  return snapshot && typeof snapshot === "object" ? snapshot : {};
}

function currentParticipants() {
  return Array.isArray(state.launchContext?.participants) ? state.launchContext.participants : [];
}

function currentPlayersFromSnapshot(snapshot) {
  return Array.isArray(snapshot?.players) ? snapshot.players : [];
}

function activePlayer(snapshot = currentSnapshot()) {
  return currentPlayersFromSnapshot(snapshot).find((player) => sanitizeText(player.user_id, 80) === sanitizeText(snapshot?.active_player_id, 80)) || null;
}

function winnerPlayer(snapshot = currentSnapshot()) {
  return currentPlayersFromSnapshot(snapshot).find((player) => sanitizeText(player.user_id, 80) === sanitizeText(snapshot?.winner_user_id, 80)) || null;
}

function viewerId() {
  return sanitizeText(state.launchContext?.viewer?.user_id, 80);
}

function viewerName() {
  return sanitizeText(
    state.launchContext?.viewer?.display_name || state.launchContext?.viewer?.user_id || "Viewer",
    80
  );
}

function seatLabel(player) {
  if (typeof rules.seatLabel === "function") return rules.seatLabel(player);
  return sanitizeText(player?.display_name || player?.user_id || "Player", 80) || "Player";
}

function assignmentLabel(value) {
  const normalized = sanitizeText(value, 16).toLowerCase();
  if (normalized === "solids") return "solids";
  if (normalized === "stripes") return "stripes";
  return "";
}

function titleCaseGroup(value) {
  const normalized = assignmentLabel(value);
  if (normalized === "solids") return "Solids";
  if (normalized === "stripes") return "Stripes";
  return "Open";
}

function playerByAssignment(snapshot, assignment) {
  return currentPlayersFromSnapshot(snapshot).find((player) => assignmentLabel(player.assignment) === assignment) || null;
}

function groupRemaining(snapshot, assignment) {
  const owner = playerByAssignment(snapshot, assignment);
  if (!owner) return 7;
  return clampNumber(owner.object_balls_left, 0, 7, 7);
}

function pocketLabel(pocketId) {
  return POCKETS.find((entry) => entry.id === pocketId)?.label || "Foot Right";
}

function angleLabel(value) {
  const normalized = clampNumber(value, -180, 180, 0);
  if (normalized === 0) return "0°";
  return `${normalized > 0 ? "+" : ""}${normalized}°`;
}

function spinAxisLabel(value, negativeLabel, positiveLabel) {
  if (value <= -0.18) return negativeLabel;
  if (value >= 0.18) return positiveLabel;
  return "center";
}

function spinSummary(spinX, spinY) {
  const horizontal = spinAxisLabel(spinX, "left english", "right english");
  const vertical = spinAxisLabel(spinY, "draw", "follow");
  if (horizontal === "center" && vertical === "center") return "Center ball";
  if (horizontal === "center") return vertical;
  if (vertical === "center") return horizontal;
  return `${vertical} + ${horizontal}`;
}

function currentResult() {
  return sanitizeText(el.actionInputs.find((input) => input.checked)?.value, 20) || state.shotDraft.result || "pocket";
}

function normalizeDraft(snapshot = currentSnapshot()) {
  const active = activePlayer(snapshot);
  const isBreak = sanitizeText(snapshot?.phase, 32) === "break";
  const result = ["pocket", "miss", "scratch", "eight"].includes(state.shotDraft.result)
    ? state.shotDraft.result
    : "pocket";
  const defaultOwn = result === "pocket" ? 1 : 0;
  const defaultBreak = result === "pocket" || result === "eight" ? 1 : 0;
  return {
    result,
    claimGroup: assignmentLabel(state.shotDraft.claimGroup) || assignmentLabel(active?.assignment) || "solids",
    ownBallsPocketed: clampNumber(state.shotDraft.ownBallsPocketed, 0, 7, defaultOwn),
    opponentBallsPocketed: clampNumber(state.shotDraft.opponentBallsPocketed, 0, 7, 0),
    breakBallsPocketed: clampNumber(state.shotDraft.breakBallsPocketed, 0, 7, defaultBreak),
    calledPocket: sanitizeText(state.shotDraft.calledPocket, 40) || DEFAULT_DRAFT.calledPocket,
    cuePower: clampNumber(state.shotDraft.cuePower, 0, 100, DEFAULT_DRAFT.cuePower),
    cueAngle: clampNumber(
      state.shotDraft.cueAngle,
      -180,
      180,
      cueAngleForPocket(sanitizeText(state.shotDraft.calledPocket, 40) || DEFAULT_DRAFT.calledPocket)
    ),
    cueSpinX: clampNumber(state.shotDraft.cueSpinX, -1, 1, DEFAULT_DRAFT.cueSpinX),
    cueSpinY: clampNumber(state.shotDraft.cueSpinY, -1, 1, DEFAULT_DRAFT.cueSpinY),
    isBreak,
  };
}

function canViewerShoot(snapshot = currentSnapshot()) {
  const winner = winnerPlayer(snapshot);
  if (winner) return false;
  const active = activePlayer(snapshot);
  return Boolean(active && viewerId() && sanitizeText(active.user_id, 80) === viewerId());
}

function blockedActionsForLaunchContext() {
  const granted = Array.isArray(state.launchContext?.capabilities_granted) ? state.launchContext.capabilities_granted : [];
  if (typeof permissionHelpers.describeBlockedActions === "function") {
    return permissionHelpers.describeBlockedActions(granted);
  }
  const grantedSet = new Set(granted);
  const writeDisabled = !grantedSet.has("realtime.session");
  const projectDisabled = !grantedSet.has("conversation.send_message");
  const storageDisabled = !grantedSet.has("storage.session");
  const refreshDisabled = !grantedSet.has("conversation.read_context");
  const missing = [];
  if (writeDisabled) missing.push("recording shots");
  if (projectDisabled) missing.push("projecting the rack summary");
  if (storageDisabled) missing.push("remembering local controls");
  if (refreshDisabled) missing.push("refreshing thread context");
  return {
    writeDisabled,
    projectDisabled,
    storageDisabled,
    refreshDisabled,
    blockedSummary: missing.length ? `Blocked: host denied ${missing.join(", ")}.` : "",
  };
}

function permissionDeniedMessage(error) {
  if (typeof permissionHelpers.permissionErrorMessage === "function") {
    return permissionHelpers.permissionErrorMessage(error);
  }
  return sanitizeText(error?.message, 180) || "Blocked: required permission was denied by the host.";
}

function isPermissionDenied(error) {
  return Boolean(
    error?.details?.required_capability ||
      sanitizeText(error?.message, 180).startsWith("Permission denied:")
  );
}

function displayError(error, fallbackMessage) {
  if (state.blockedActions?.blockedSummary) {
    setStatus(state.blockedActions.blockedSummary, true);
    return;
  }
  if (isPermissionDenied(error)) {
    setStatus(permissionDeniedMessage(error), true);
    return;
  }
  setStatus(fallbackMessage, true);
}

function queueComposerSave() {
  if (state.blockedActions?.storageDisabled || !bridge) return;
  if (state.composerSaveTimer) {
    window.clearTimeout(state.composerSaveTimer);
  }
  state.composerSaveTimer = window.setTimeout(() => {
    state.composerSaveTimer = 0;
    void persistComposerPreferences();
  }, 180);
}

async function persistComposerPreferences() {
  if (state.blockedActions?.storageDisabled) return;
  await requireBridge().setSessionStorage("eightball_composer", JSON.stringify({
    result: state.shotDraft.result,
    claimGroup: state.shotDraft.claimGroup,
    calledPocket: state.shotDraft.calledPocket,
    cuePower: state.shotDraft.cuePower,
    cueAngle: state.shotDraft.cueAngle,
    cueSpinX: state.shotDraft.cueSpinX,
    cueSpinY: state.shotDraft.cueSpinY,
  }));
}

async function restoreComposerPreferences() {
  if (state.blockedActions?.storageDisabled) return;
  const raw = await requireBridge().getSessionStorage("eightball_composer");
  if (typeof raw?.value !== "string" || !raw.value) return;
  try {
    const parsed = JSON.parse(raw.value);
    state.shotDraft = {
      ...state.shotDraft,
      result: sanitizeText(parsed?.result, 20) || state.shotDraft.result,
      claimGroup: assignmentLabel(parsed?.claimGroup) || state.shotDraft.claimGroup,
      calledPocket: sanitizeText(parsed?.calledPocket, 40) || state.shotDraft.calledPocket,
      cuePower: clampNumber(parsed?.cuePower, 0, 100, state.shotDraft.cuePower),
      cueAngle: clampNumber(parsed?.cueAngle, -180, 180, state.shotDraft.cueAngle),
      cueSpinX: clampNumber(parsed?.cueSpinX, -1, 1, state.shotDraft.cueSpinX),
      cueSpinY: clampNumber(parsed?.cueSpinY, -1, 1, state.shotDraft.cueSpinY),
    };
  } catch {
    // Ignore malformed saved data and keep local defaults.
  }
}

function syncDraftFromInputs() {
  state.shotDraft = {
    ...state.shotDraft,
    result: currentResult(),
    claimGroup: assignmentLabel(el.claimGroupSelect?.value) || state.shotDraft.claimGroup,
    ownBallsPocketed: clampNumber(el.ownPocketedInput?.value, 0, 7, 1),
    opponentBallsPocketed: clampNumber(el.opponentPocketedInput?.value, 0, 7, 0),
    breakBallsPocketed: clampNumber(el.breakPocketedInput?.value, 0, 7, 1),
    cuePower: clampNumber(el.cuePowerInput?.value, 0, 100, DEFAULT_DRAFT.cuePower),
    cueAngle: clampNumber(el.cueAngleInput?.value, -180, 180, state.shotDraft.cueAngle),
  };
}

function syncActionAvailability() {
  const snapshot = currentSnapshot();
  const canShoot = canViewerShoot(snapshot);
  const writeDisabled = Boolean(state.blockedActions?.writeDisabled || !canShoot);
  el.startBtn.disabled = Boolean(state.blockedActions?.writeDisabled);
  el.submitShotBtn.disabled = writeDisabled;
  el.refreshBtn.disabled = Boolean(state.blockedActions?.refreshDisabled);
  el.projectBtn.disabled = Boolean(state.blockedActions?.projectDisabled);
  el.actionInputs.forEach((input) => {
    input.disabled = writeDisabled;
  });
  el.claimGroupSelect.disabled = writeDisabled;
  el.ownPocketedInput.disabled = writeDisabled;
  el.opponentPocketedInput.disabled = writeDisabled;
  el.breakPocketedInput.disabled = writeDisabled;
  if (el.cueAngleInput) el.cueAngleInput.disabled = writeDisabled;
  if (el.cuePowerInput) el.cuePowerInput.disabled = writeDisabled;
  if (el.cueSpinResetBtn) el.cueSpinResetBtn.disabled = writeDisabled;
  if (el.cueBallSelector) {
    el.cueBallSelector.style.pointerEvents = writeDisabled ? "none" : "auto";
    el.cueBallSelector.style.opacity = writeDisabled ? "0.55" : "1";
  }
}

function projectableSummary(snapshot) {
  if (typeof rules.projectableSummary === "function") {
    return rules.projectableSummary(snapshot);
  }
  return sanitizeText(snapshot?.last_event, 180) || "Rack status updated.";
}

async function projectThreadSummary(text, type = "POOL_TURN") {
  await requireBridge().sendConversationMessage({
    content_type: "app_event",
    content: {
      event_name: type,
      body: { summary: text },
    },
    text,
  });
}

function applyLaunchContext(launchContext) {
  state.launchContext = launchContext || {};
  state.blockedActions = blockedActionsForLaunchContext();
  state.shotDraft = normalizeDraft(currentSnapshot());
  syncActionAvailability();
  render();
}

async function updateSnapshot(nextSnapshot, summary, options = {}) {
  const payload = await requireBridge().updateSessionState({
    ...nextSnapshot,
    projected_summary: summary,
  });
  applyLaunchContext({
    ...(state.launchContext || {}),
    state_snapshot: payload?.state_snapshot || nextSnapshot,
    state_version: payload?.state_version || state.launchContext?.state_version,
  });
  if (options.project !== false && !state.blockedActions?.projectDisabled) {
    await projectThreadSummary(summary, options.projectType || "POOL_TURN");
    await refreshConversationContext();
  }
  setStatus(summary);
}

async function startMatch() {
  const snapshot = typeof rules.buildInitialSnapshot === "function"
    ? rules.buildInitialSnapshot(currentParticipants())
    : {};
  const summary = sanitizeText(snapshot?.last_event, 180) || "Waiting for two players.";
  await updateSnapshot(snapshot, summary, { projectType: "POOL_STARTED" });
}

function buildShotFromDraft(snapshot) {
  const draft = normalizeDraft(snapshot);
  const active = activePlayer(snapshot);
  const isBreak = sanitizeText(snapshot?.phase, 32) === "break";
  const shotMeta = {
    cuePower: draft.cuePower,
    cueAngle: draft.cueAngle,
    cueSpinX: draft.cueSpinX,
    cueSpinY: draft.cueSpinY,
  };

  if (draft.result === "miss") {
    return { type: "miss", calledPocket: draft.calledPocket, ...shotMeta };
  }
  if (draft.result === "scratch") {
    return { type: "scratch", calledPocket: draft.calledPocket, cueBallScratch: true, ...shotMeta };
  }
  if (draft.result === "eight") {
    return isBreak
      ? { type: "eight", calledPocket: draft.calledPocket, otherObjectBallsPocketed: draft.breakBallsPocketed, ...shotMeta }
      : { type: "eight", calledPocket: draft.calledPocket, eightPocketed: true, ...shotMeta };
  }
  if (isBreak) {
    return {
      type: "pocket",
      calledPocket: draft.calledPocket,
      otherObjectBallsPocketed: draft.breakBallsPocketed,
      ...shotMeta,
    };
  }
  return {
    type: "pocket",
    calledPocket: draft.calledPocket,
    ownBallsPocketed: draft.ownBallsPocketed,
    opponentBallsPocketed: draft.opponentBallsPocketed,
    claimGroup: assignmentLabel(active?.assignment) || draft.claimGroup,
    ...shotMeta,
  };
}

async function submitShot() {
  const snapshot = currentSnapshot();
  const players = currentPlayersFromSnapshot(snapshot);
  if (!players.length) {
    await startMatch();
    return;
  }
  if (!activePlayer(snapshot)) {
    setStatus("Refresh the rack state first.", true);
    return;
  }
  const shot = buildShotFromDraft(snapshot);
  const nextSnapshot = typeof rules.applyShot === "function" ? rules.applyShot(snapshot, shot) : snapshot;
  const summary = sanitizeText(nextSnapshot?.last_event, 180) || projectableSummary(nextSnapshot);
  await updateSnapshot(nextSnapshot, summary, {
    projectType: nextSnapshot?.winner_user_id
      ? "POOL_FINISHED"
      : shot.type === "scratch"
      ? "POOL_FOUL"
      : "POOL_TURN",
  });
}

async function refreshLaunchContext() {
  applyLaunchContext(await requireBridge().getLaunchContext());
  setStatus("Bridge context updated.");
}

async function refreshConversationContext() {
  const context = await requireBridge().readConversationContext();
  state.recentMessages = Array.isArray(context?.recent_messages) ? context.recent_messages : [];
  renderRecentMessages();
}

async function projectCurrentState() {
  const snapshot = currentSnapshot();
  const summary = projectableSummary(snapshot);
  await projectThreadSummary(summary, "POOL_STATUS");
  await refreshConversationContext();
  setStatus("Projected the latest table summary.");
}

function selectedPocket() {
  return POCKETS.find((entry) => entry.id === state.shotDraft.calledPocket) || POCKETS[POCKETS.length - 1];
}

function applyPocketSelection(pocketId, options = {}) {
  const nextPocketId = sanitizeText(pocketId, 40) || DEFAULT_DRAFT.calledPocket;
  state.shotDraft = {
    ...state.shotDraft,
    calledPocket: nextPocketId,
    cueAngle: options.preserveAngle
      ? state.shotDraft.cueAngle
      : cueAngleForPocket(nextPocketId),
  };
}

function schedulePlayfieldRender(snapshot = currentSnapshot()) {
  const renderer = getPlayfieldRenderer();
  if (!renderer) return;
  fitPlayfieldToShell();
  const balls = buildBallDisplay(snapshot);
  window.requestAnimationFrame(() => {
    renderer.render({
      balls,
      calledPocket: selectedPocket(),
      cuePower: clampNumber(state.shotDraft.cuePower, 0, 100, DEFAULT_DRAFT.cuePower),
      cueAngle: clampNumber(state.shotDraft.cueAngle, -180, 180, DEFAULT_DRAFT.cueAngle),
      cueSpinX: clampNumber(state.shotDraft.cueSpinX, -1, 1, DEFAULT_DRAFT.cueSpinX),
      cueSpinY: clampNumber(state.shotDraft.cueSpinY, -1, 1, DEFAULT_DRAFT.cueSpinY),
      showCue: canViewerShoot(snapshot),
    });
  });
}

function schedulePlayfieldStabilization(snapshot = currentSnapshot()) {
  if (state.playfieldStabilizeTimer) {
    window.clearTimeout(state.playfieldStabilizeTimer);
    state.playfieldStabilizeTimer = 0;
  }
  window.requestAnimationFrame(() => {
    fitPlayfieldToShell();
    schedulePlayfieldRender(snapshot);
    window.requestAnimationFrame(() => {
      fitPlayfieldToShell();
      schedulePlayfieldRender(snapshot);
    });
  });
  state.playfieldStabilizeTimer = window.setTimeout(() => {
    state.playfieldStabilizeTimer = 0;
    fitPlayfieldToShell();
    schedulePlayfieldRender(snapshot);
  }, 180);
}

function fitPlayfieldToShell() {
  if (!el.playfield || !el.playfieldShell) return;
  const shellRect = el.playfieldShell.getBoundingClientRect();
  if (!shellRect.width || !shellRect.height) return;
  let width = shellRect.width;
  let height = width / PLAYFIELD_ASPECT;
  if (height > shellRect.height) {
    height = shellRect.height;
    width = height * PLAYFIELD_ASPECT;
  }
  el.playfield.style.width = `${Math.round(width)}px`;
  el.playfield.style.height = `${Math.round(height)}px`;
}

function formatClock(msElapsed) {
  const totalSeconds = Math.max(0, Math.floor(msElapsed / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function updateTurnTimerDisplay(snapshot = currentSnapshot()) {
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);
  if (winner) {
    el.turnTimerLabel.textContent = "00:00";
    el.turnOwnerLine.textContent = `${seatLabel(winner)} closed the rack.`;
    return;
  }
  if (!active) {
    el.turnTimerLabel.textContent = "00:00";
    el.turnOwnerLine.textContent = "Waiting for the next shooter.";
    return;
  }
  const elapsed = state.turnTimer.startedAt ? Date.now() - state.turnTimer.startedAt : 0;
  el.turnTimerLabel.textContent = formatClock(elapsed);
  el.turnOwnerLine.textContent = `${seatLabel(active)} is on the clock.`;
}

function ensureTurnTimerLoop() {
  if (state.turnTimer.intervalId) return;
  state.turnTimer.intervalId = window.setInterval(() => {
    updateTurnTimerDisplay();
  }, 1000);
}

function syncTurnTimer(snapshot) {
  const signature = [
    sanitizeText(snapshot?.active_player_id, 80),
    Number(snapshot?.turn_number || 0),
    sanitizeText(snapshot?.winner_user_id, 80),
  ].join("|");
  if (signature !== state.turnTimer.signature) {
    state.turnTimer.signature = signature;
    state.turnTimer.startedAt = Date.now();
  }
  updateTurnTimerDisplay(snapshot);
}

function activeObjective(snapshot) {
  const winner = winnerPlayer(snapshot);
  const active = activePlayer(snapshot);
  if (winner) {
    return `${seatLabel(winner)} finished the rack. Re-rack to play again.`;
  }
  if (!active) {
    return "Rack the table to begin the match.";
  }
  if (!canViewerShoot(snapshot)) {
    return `${seatLabel(active)} is up. You can inspect the rack while waiting.`;
  }
  if (sanitizeText(snapshot?.phase, 32) === "break") {
    return "Break shot live. Choose the break result and record any extra balls that fell.";
  }
  if (!assignmentLabel(active.assignment)) {
    return "Open table. Call a pocket and choose the group you want to claim.";
  }
  if (clampNumber(active.object_balls_left, 0, 7, 7) === 0) {
    return "The 8-ball is live. Call the pocket and finish the rack cleanly.";
  }
  return `Call a pocket and record the result for ${seatLabel(active)}'s turn.`;
}

function legalTargetCopy(snapshot) {
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);
  if (winner) return `${seatLabel(winner)} won the rack.`;
  if (!active) return "Waiting for both players.";
  if (sanitizeText(snapshot?.phase, 32) === "break") return `${seatLabel(active)} is breaking from behind the head string.`;
  if (!assignmentLabel(active.assignment)) return "Open table: first legal pot assigns solids or stripes.";
  if (clampNumber(active.object_balls_left, 0, 7, 7) === 0) return `${seatLabel(active)} is on the 8-ball.`;
  return `${seatLabel(active)} is shooting ${titleCaseGroup(active.assignment)}.`;
}

function renderRecentMessages() {
  el.messageList.replaceChildren();
  if (!state.recentMessages.length) {
    const item = document.createElement("li");
    item.textContent = "No recent thread messages available.";
    el.messageList.append(item);
    return;
  }
  for (const entry of state.recentMessages.slice(0, 4)) {
    const item = document.createElement("li");
    const title = document.createElement("strong");
    title.textContent = sanitizeText(entry.author, 80) || "Unknown";
    const body = document.createElement("p");
    body.textContent = sanitizeText(entry.text, 180);
    item.append(title, body);
    el.messageList.append(item);
  }
}

function renderPockets(snapshot) {
  el.pocketMap.replaceChildren();
  const writeDisabled = Boolean(state.blockedActions?.writeDisabled || !canViewerShoot(snapshot));
  for (const pocket of POCKETS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `pocket-button${state.shotDraft.calledPocket === pocket.id ? " selected" : ""}`;
    button.style.left = `${pocket.x}%`;
    button.style.top = `${pocket.y}%`;
    button.disabled = writeDisabled;
    button.setAttribute("aria-pressed", state.shotDraft.calledPocket === pocket.id ? "true" : "false");
    const label = document.createElement("span");
    label.textContent = pocket.label;
    button.append(label);
    button.addEventListener("click", () => {
      applyPocketSelection(pocket.id);
      queueComposerSave();
      render();
    });
    el.pocketMap.append(button);
  }
}

function renderPotIndicators(snapshot) {
  if (!el.potIndicatorGrid) return;
  el.potIndicatorGrid.replaceChildren();
  const writeDisabled = Boolean(state.blockedActions?.writeDisabled || !canViewerShoot(snapshot));
  for (const pocket of POCKETS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `pot-indicator-button${state.shotDraft.calledPocket === pocket.id ? " active" : ""}`;
    button.disabled = writeDisabled;
    button.setAttribute("aria-pressed", state.shotDraft.calledPocket === pocket.id ? "true" : "false");
    const title = document.createElement("strong");
    title.textContent = pocket.label.replace("Head ", "").replace("Foot ", "");
    const meta = document.createElement("span");
    meta.textContent = pocket.id.startsWith("head") ? "Top rail" : "Bottom rail";
    button.append(title, meta);
    button.addEventListener("click", () => {
      applyPocketSelection(pocket.id);
      queueComposerSave();
      render();
    });
    el.potIndicatorGrid.append(button);
  }
}

function renderCueBallSelector() {
  if (!el.cueBallSelector || !el.cueSpinMarker || !el.cueSpinLabel) return;
  const spinX = clampNumber(state.shotDraft.cueSpinX, -1, 1, 0);
  const spinY = clampNumber(state.shotDraft.cueSpinY, -1, 1, 0);
  const xPercent = 50 + spinX * 30;
  const yPercent = 50 - spinY * 30;
  el.cueSpinMarker.style.left = `${xPercent}%`;
  el.cueSpinMarker.style.top = `${yPercent}%`;
  el.cueSpinLabel.textContent = spinSummary(spinX, spinY);
}

function updateCueSpinFromPointer(clientX, clientY) {
  if (!el.cueBallSelector) return;
  const rect = el.cueBallSelector.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const centerX = rect.left + rect.width / 2;
  const centerY = rect.top + rect.height / 2;
  let offsetX = (clientX - centerX) / (rect.width / 2);
  let offsetY = (centerY - clientY) / (rect.height / 2);
  const distance = Math.hypot(offsetX, offsetY);
  if (distance > 0.82) {
    const scale = 0.82 / distance;
    offsetX *= scale;
    offsetY *= scale;
  }
  state.shotDraft = {
    ...state.shotDraft,
    cueSpinX: Math.round(offsetX * 100) / 100,
    cueSpinY: Math.round(offsetY * 100) / 100,
  };
  renderCueBallSelector();
  queueComposerSave();
  schedulePlayfieldRender();
}

function buildBallDisplay(snapshot) {
  const openTable = !playerByAssignment(snapshot, "solids") && !playerByAssignment(snapshot, "stripes");
  const solidsRemaining = groupRemaining(snapshot, "solids");
  const stripesRemaining = groupRemaining(snapshot, "stripes");
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);
  const activeAssignment = assignmentLabel(active?.assignment);

  return TABLE_LAYOUT.filter((entry) => {
    if (entry.kind === "cue") return Boolean(currentPlayersFromSnapshot(snapshot).length);
    if (entry.kind === "black") return !winner;
    if (openTable) return true;
    if (entry.kind === "solid") return Number(entry.key) <= solidsRemaining;
    if (entry.kind === "stripe") return Number(entry.key) - 8 <= stripesRemaining;
    return false;
  }).map((entry) => {
    const isTarget = Boolean(
      active &&
      !winner &&
      (
        (entry.kind === "black" && clampNumber(active.object_balls_left, 0, 7, 7) === 0 && sanitizeText(snapshot?.phase, 32) !== "break") ||
        (!activeAssignment && sanitizeText(snapshot?.phase, 32) !== "break" && (entry.kind === "solid" || entry.kind === "stripe")) ||
        (activeAssignment === "solids" && entry.kind === "solid") ||
        (activeAssignment === "stripes" && entry.kind === "stripe")
      )
    );
    return { ...entry, isTarget };
  });
}

function renderTableBalls(snapshot) {
  el.tableBalls.replaceChildren();
  for (const entry of buildBallDisplay(snapshot)) {
    const ball = document.createElement("div");
    ball.className = `ball ${entry.kind}${entry.isTarget ? " target" : ""}`;
    ball.dataset.number = entry.key;
    ball.style.left = `${entry.x}%`;
    ball.style.top = `${entry.y}%`;
    const label = document.createElement("span");
    label.textContent = entry.kind === "cue" ? "" : entry.label;
    ball.append(label);
    el.tableBalls.append(ball);
  }
}

function createTrayBall(number, kind, isDown, isEightLive = false) {
  const shell = document.createElement("div");
  shell.className = `tray-ball-shell${isDown ? " down" : ""}${isEightLive ? " live-eight" : ""}`;

  const backdrop = document.createElement("img");
  backdrop.className = "tray-ball-backdrop";
  backdrop.alt = "";
  backdrop.src = HUD_COMPONENT_URLS.ballBackdrop;

  const icon = document.createElement("img");
  icon.className = "tray-ball-icon";
  icon.alt = `${number} ball`;
  icon.src = BALL_SLOT_COMPONENTS[number];

  const fallback = document.createElement("span");
  fallback.className = `tray-ball-fallback ${kind}`;
  fallback.textContent = String(number);

  shell.append(backdrop, icon, fallback);
  return shell;
}

function renderTray(snapshot, assignment, container, ownerLine) {
  container.replaceChildren();
  const owner = playerByAssignment(snapshot, assignment);
  const remaining = groupRemaining(snapshot, assignment);
  ownerLine.textContent = owner ? `${seatLabel(owner)} · ${remaining} left` : "Open";
  const numbers = assignment === "solids" ? [1, 2, 3, 4, 5, 6, 7] : [9, 10, 11, 12, 13, 14, 15];
  numbers.forEach((number, index) => {
    const stillLive = index < remaining || !owner;
    container.append(createTrayBall(number, assignment === "solids" ? "solid" : "stripe", !stillLive));
  });
  if (owner && remaining === 0) {
    container.append(createTrayBall(8, "black", false, true));
  }
}

function createTargetPlaceholder() {
  const placeholder = document.createElement("span");
  placeholder.className = "match-target-placeholder";
  return placeholder;
}

function renderPlayerTargetStrip(player, container) {
  container.replaceChildren();
  const assignment = assignmentLabel(player?.assignment);
  if (!player || !assignment) {
    for (let index = 0; index < 7; index += 1) {
      container.append(createTargetPlaceholder());
    }
    return;
  }

  const remaining = clampNumber(
    player.object_balls_left ?? player.balls_left,
    0,
    7,
    7,
  );
  const numbers = assignment === "solids"
    ? [1, 2, 3, 4, 5, 6, 7]
    : [9, 10, 11, 12, 13, 14, 15];

  numbers.forEach((number, index) => {
    const stillLive = index < remaining;
    container.append(createTrayBall(number, assignment === "solids" ? "solid" : "stripe", !stillLive));
  });
  if (remaining === 0) {
    container.append(createTrayBall(8, "black", false, true));
  }
}

function playerMatchMeta(player) {
  if (!player) return "Waiting for a seat";
  const assignment = assignmentLabel(player.assignment);
  if (!assignment) {
    return `Open table - ${Number(player.fouls || 0)} fouls`;
  }
  const ballsLeft = clampNumber(player.object_balls_left ?? player.balls_left, 0, 7, 7);
  return `${titleCaseGroup(assignment)} - ${ballsLeft} left - ${Number(player.fouls || 0)} fouls`;
}

function playerMatchBadge(player, activeId, winnerId) {
  const playerId = sanitizeText(player?.user_id, 80);
  if (playerId && playerId === winnerId) return "Winner";
  if (playerId && playerId === activeId) return "At table";
  return player ? "Waiting" : "Open";
}

function renderMatchHud(snapshot) {
  const players = currentPlayersFromSnapshot(snapshot);
  const leftPlayer = players[0] || null;
  const rightPlayer = players[1] || null;
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);
  const activeId = sanitizeText(snapshot?.active_player_id, 80);
  const winnerId = sanitizeText(snapshot?.winner_user_id, 80);

  el.matchPlayerLeftName.textContent = leftPlayer ? seatLabel(leftPlayer) : "Player 1";
  el.matchPlayerLeftMeta.textContent = playerMatchMeta(leftPlayer);
  el.matchPlayerLeftBadge.textContent = playerMatchBadge(leftPlayer, activeId, winnerId);
  el.matchPlayerLeftBadge.className = `match-player-badge${
    leftPlayer && sanitizeText(leftPlayer.user_id, 80) === activeId ? " active" : ""
  }${
    leftPlayer && sanitizeText(leftPlayer.user_id, 80) === winnerId ? " winner" : ""
  }`;
  renderPlayerTargetStrip(leftPlayer, el.matchPlayerLeftTargets);

  el.matchPlayerRightName.textContent = rightPlayer ? seatLabel(rightPlayer) : "Player 2";
  el.matchPlayerRightMeta.textContent = playerMatchMeta(rightPlayer);
  el.matchPlayerRightBadge.textContent = playerMatchBadge(rightPlayer, activeId, winnerId);
  el.matchPlayerRightBadge.className = `match-player-badge${
    rightPlayer && sanitizeText(rightPlayer.user_id, 80) === activeId ? " active" : ""
  }${
    rightPlayer && sanitizeText(rightPlayer.user_id, 80) === winnerId ? " winner" : ""
  }`;
  renderPlayerTargetStrip(rightPlayer, el.matchPlayerRightTargets);

  el.matchCenterStatus.textContent = winner
    ? `${seatLabel(winner)} wins the rack`
    : active
    ? `${seatLabel(active)} to shoot`
    : "Rack the match to begin.";
  el.matchCenterCopy.textContent = legalTargetCopy(snapshot);
}

function renderScoreboard(snapshot) {
  el.scoreboard.replaceChildren();
  const players = currentPlayersFromSnapshot(snapshot);
  if (!players.length) {
    const empty = document.createElement("p");
    empty.className = "meta-line";
    empty.textContent = "Seat two players to render the rack.";
    el.scoreboard.append(empty);
    return;
  }
  const activeId = sanitizeText(snapshot?.active_player_id, 80);
  const winnerId = sanitizeText(snapshot?.winner_user_id, 80);
  players.forEach((player) => {
    const card = document.createElement("article");
    card.className = `player-card${sanitizeText(player.user_id, 80) === activeId ? " active" : ""}${sanitizeText(player.user_id, 80) === winnerId ? " winner" : ""}`;
    const row = document.createElement("div");
    row.className = "player-row";
    const name = document.createElement("p");
    name.className = "player-name";
    name.textContent = seatLabel(player);
    const badge = document.createElement("p");
    badge.className = "meta-badge";
    badge.textContent = sanitizeText(player.user_id, 80) === winnerId
      ? "Winner"
      : sanitizeText(player.user_id, 80) === activeId
      ? "At table"
      : "Waiting";
    row.append(name, badge);

    const meta = document.createElement("p");
    meta.className = "player-meta";
    meta.textContent = `${titleCaseGroup(player.assignment)} · ${Number(player.balls_left || 0)} balls left · ${Number(player.fouls || 0)} fouls`;

    const meter = document.createElement("div");
    meter.className = "player-meter";
    for (let index = 0; index < 8; index += 1) {
      const pip = document.createElement("span");
      const liveCount = clampNumber(player.balls_left, 0, 8, 8);
      if (index < liveCount) {
        pip.className = "live";
      }
      meter.append(pip);
    }

    card.append(row, meta, meter);
    el.scoreboard.append(card);
  });
}

function renderHistory(snapshot) {
  el.historyList.replaceChildren();
  const history = Array.isArray(snapshot?.history) ? snapshot.history : [];
  if (!history.length) {
    const item = document.createElement("li");
    item.textContent = "No turns played yet.";
    el.historyList.append(item);
    return;
  }
  history.slice(-6).reverse().forEach((entry) => {
    const item = document.createElement("li");
    const text = document.createElement("strong");
    text.textContent = sanitizeText(entry.text, 180);
    const time = document.createElement("p");
    time.textContent = sanitizeText(entry.at, 120);
    item.append(text, time);
    el.historyList.append(item);
  });
}

function renderHelperSummary(snapshot) {
  const solidsDown = 7 - groupRemaining(snapshot, "solids");
  const stripesDown = 7 - groupRemaining(snapshot, "stripes");
  const totalDown = Math.max(0, solidsDown + stripesDown);
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);

  el.pottedSummaryLine.textContent = `${totalDown} object balls down`;
  el.groupSummaryLine.textContent = `Solids ${solidsDown} | Stripes ${stripesDown}`;

  if (winner) {
    el.eightballStatusLine.textContent = "Rack finished";
    el.helperShotLine.textContent = `${seatLabel(winner)} legally finished the rack.`;
    return;
  }
  if (!active) {
    el.eightballStatusLine.textContent = "8-ball closed";
    el.helperShotLine.textContent = "Start the match to open the rack.";
    return;
  }
  if (sanitizeText(snapshot?.phase, 32) === "break") {
    el.eightballStatusLine.textContent = "Break shot";
    el.helperShotLine.textContent = "Choose the break result and update the called pocket.";
    return;
  }
  if (clampNumber(active.object_balls_left, 0, 7, 7) === 0) {
    el.eightballStatusLine.textContent = "8-ball live";
    el.helperShotLine.textContent = `${seatLabel(active)} can win with a clean called shot.`;
    return;
  }
  el.eightballStatusLine.textContent = `${titleCaseGroup(active.assignment)} live`;
  el.helperShotLine.textContent = assignmentLabel(active.assignment)
    ? `${seatLabel(active)} needs ${Number(active.object_balls_left || 0)} more object balls before the 8-ball.`
    : "First legal pot still decides solids versus stripes.";
}

function renderComposer(snapshot) {
  const draft = normalizeDraft(snapshot);
  state.shotDraft = { ...state.shotDraft, ...draft };
  el.actionInputs.forEach((input) => {
    input.checked = input.value === draft.result;
  });
  el.claimGroupSelect.value = draft.claimGroup;
  el.ownPocketedInput.value = String(draft.ownBallsPocketed);
  el.opponentPocketedInput.value = String(draft.opponentBallsPocketed);
  el.breakPocketedInput.value = String(draft.breakBallsPocketed);
  el.calledPocketLabel.textContent = pocketLabel(draft.calledPocket);
  if (el.potIndicatorLabel) {
    el.potIndicatorLabel.textContent = `${pocketLabel(draft.calledPocket)} selected`;
  }
  if (el.cueAngleInput) el.cueAngleInput.value = String(draft.cueAngle);
  if (el.cueAngleLabel) el.cueAngleLabel.textContent = angleLabel(draft.cueAngle);
  el.cuePowerInput.value = String(draft.cuePower);
  el.cuePowerLabel.textContent = `${draft.cuePower}%`;
  if (el.cuePowerMeterFill) {
    el.cuePowerMeterFill.style.width = `${draft.cuePower}%`;
  }
  if (el.cuePowerFillArt) {
    el.cuePowerFillArt.style.transform = `translateY(${100 - draft.cuePower}%)`;
  }
  if (el.cuePowerOverlayArt) {
    el.cuePowerOverlayArt.style.bottom = `calc(${draft.cuePower}% - 14px)`;
  }
  renderCueBallSelector();

  const active = activePlayer(snapshot);
  const isBreak = sanitizeText(snapshot?.phase, 32) === "break";
  const result = draft.result;
  const showClaimGroup = Boolean(active && !assignmentLabel(active.assignment) && !isBreak && result === "pocket");
  const showBreakCount = isBreak && (result === "pocket" || result === "eight");
  const showPocketCounts = !isBreak && result === "pocket";

  el.claimGroupRow.hidden = !showClaimGroup;
  el.breakCountRow.hidden = !showBreakCount;
  el.pocketCountRow.hidden = !showPocketCounts;
  el.submitShotBtn.textContent = isBreak ? "Record Break" : result === "eight" ? "Record 8-Ball Shot" : "Submit Shot";
  el.startBtn.textContent = currentPlayersFromSnapshot(snapshot).length ? "Re-Rack Match" : "Rack Match";
}

function render() {
  const snapshot = currentSnapshot();
  const players = currentPlayersFromSnapshot(snapshot);
  const displayParticipants = players.length ? players : currentParticipants();
  const active = activePlayer(snapshot);
  const winner = winnerPlayer(snapshot);

  el.viewerLine.textContent = `Viewer: ${viewerName()}`;
  el.participantsLine.textContent = displayParticipants.length
    ? `Players: ${displayParticipants.map((player) => seatLabel(player)).join(" vs ")}`
    : "Players: unavailable";
  el.sharedLine.textContent = sanitizeText(snapshot?.projected_summary, 180) || "No summary projected yet.";
  el.countLine.textContent = `Turn ${Number(snapshot?.turn_number || 0)}`;
  el.phasePill.textContent = sanitizeText(snapshot?.phase, 32) || "Waiting";
  el.turnPill.textContent = winner
    ? `${seatLabel(winner)} wins`
    : active
    ? `${seatLabel(active)} to shoot`
    : "Waiting to start";
  el.tableSummary.textContent = sanitizeText(snapshot?.last_event, 180) || "Start the match to seat players and break the rack.";
  el.legalTargetLine.textContent = legalTargetCopy(snapshot);
  el.instructionLine.textContent = activeObjective(snapshot);
  el.rackProgress.style.width = `${Math.max(0, Math.min(100, Number(snapshot?.rack_progress || 0)))}%`;

  syncTurnTimer(snapshot);
  renderComposer(snapshot);
  renderPockets(snapshot);
  renderPotIndicators(snapshot);
  renderTableBalls(snapshot);
  renderMatchHud(snapshot);
  renderTray(snapshot, "solids", el.solidsTray, el.solidsOwner);
  renderTray(snapshot, "stripes", el.stripesTray, el.stripesOwner);
  renderScoreboard(snapshot);
  renderHistory(snapshot);
  renderHelperSummary(snapshot);
  schedulePlayfieldRender(snapshot);
  syncActionAvailability();

  if (el.previewAnswerText) {
    el.previewAnswerText.textContent = winner
      ? `${seatLabel(winner)} wins`
      : active
      ? `${seatLabel(active)} to shoot`
      : "Open the table.";
  }
  if (el.previewCaption) {
    el.previewCaption.textContent = sanitizeText(snapshot?.last_event, 180) || "Shared match state ready for the thread.";
  }
}

function attachPlayfieldObserver() {
  if (state.playfieldObserver || !el.playfield) return;
  if (typeof ResizeObserver !== "function") return;
  state.playfieldObserver = new ResizeObserver(() => {
    fitPlayfieldToShell();
    schedulePlayfieldRender();
  });
  if (el.playfieldShell) {
    state.playfieldObserver.observe(el.playfieldShell);
  }
}

async function bootstrapPreview() {
  document.body.classList.add("preview-mode");
  el.appShell.hidden = true;
  el.previewShell.hidden = false;
  attachPlayfieldObserver();
  await ensureRenderAssets();
  render();
  schedulePlayfieldStabilization();
  if (!bridge) return;
  try {
    await refreshLaunchContext();
  } catch (error) {
    setStatus(error.message || "Preview bridge unavailable.", true);
  }
}

async function bootstrap() {
  if (typeof rules.buildInitialSnapshot !== "function" || typeof rules.applyShot !== "function") {
    setStatus("8 Ball rules failed to load.", true);
    return;
  }
  attachPlayfieldObserver();
  ensureTurnTimerLoop();
  await ensureRenderAssets();
  if (isPreviewMode) {
    await bootstrapPreview();
    return;
  }
  try {
    await refreshLaunchContext();
    try {
      await restoreComposerPreferences();
    } catch (error) {
      console.error(error);
    }
    await refreshConversationContext();
    render();
    schedulePlayfieldStabilization();
    if (state.blockedActions?.blockedSummary) {
      setStatus(state.blockedActions.blockedSummary, true);
    } else {
      setStatus("Mini-app ready.");
    }
  } catch (error) {
    console.error(error);
    displayError(error, error.message || "Mini-app failed to boot.");
  }
}

if (bridge) {
  bridge.on("session.stateUpdated", (payload) => {
    if (!payload?.state_snapshot) return;
    applyLaunchContext({
      ...(state.launchContext || {}),
      state_snapshot: payload.state_snapshot,
      state_version: payload.state_version || state.launchContext?.state_version,
    });
    setStatus("Shared pool state updated.");
    schedulePlayfieldStabilization(payload.state_snapshot || currentSnapshot());
  });

  bridge.on("session.permissionsUpdated", (payload) => {
    applyLaunchContext({
      ...(state.launchContext || {}),
      capabilities_granted: Array.isArray(payload?.capabilities_granted) ? payload.capabilities_granted : [],
    });
    if (state.blockedActions?.blockedSummary) {
      setStatus(state.blockedActions.blockedSummary, true);
    } else {
      setStatus("Permission grants updated.");
    }
    schedulePlayfieldStabilization();
  });
}

window.addEventListener("resize", () => {
  schedulePlayfieldStabilization();
});

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    schedulePlayfieldStabilization();
  }
});

el.actionInputs.forEach((input) => {
  input.addEventListener("change", () => {
    syncDraftFromInputs();
    queueComposerSave();
    render();
  });
});

el.claimGroupSelect?.addEventListener("change", () => {
  syncDraftFromInputs();
  queueComposerSave();
  render();
});

el.ownPocketedInput?.addEventListener("input", () => {
  syncDraftFromInputs();
  renderComposer(currentSnapshot());
});

el.opponentPocketedInput?.addEventListener("input", () => {
  syncDraftFromInputs();
  renderComposer(currentSnapshot());
});

el.breakPocketedInput?.addEventListener("input", () => {
  syncDraftFromInputs();
  renderComposer(currentSnapshot());
});

el.cuePowerInput?.addEventListener("input", () => {
  syncDraftFromInputs();
  queueComposerSave();
  el.cuePowerLabel.textContent = `${clampNumber(el.cuePowerInput.value, 0, 100, DEFAULT_DRAFT.cuePower)}%`;
  schedulePlayfieldRender();
});

el.cueAngleInput?.addEventListener("input", () => {
  syncDraftFromInputs();
  queueComposerSave();
  if (el.cueAngleLabel) {
    el.cueAngleLabel.textContent = angleLabel(clampNumber(el.cueAngleInput.value, -180, 180, DEFAULT_DRAFT.cueAngle));
  }
  schedulePlayfieldRender();
});

el.cueBallSelector?.addEventListener("pointerdown", (event) => {
  updateCueSpinFromPointer(event.clientX, event.clientY);
  const move = (moveEvent) => updateCueSpinFromPointer(moveEvent.clientX, moveEvent.clientY);
  const release = () => {
    window.removeEventListener("pointermove", move);
    window.removeEventListener("pointerup", release);
  };
  window.addEventListener("pointermove", move);
  window.addEventListener("pointerup", release);
});

el.cueSpinResetBtn?.addEventListener("click", () => {
  state.shotDraft = {
    ...state.shotDraft,
    cueSpinX: 0,
    cueSpinY: 0,
  };
  renderCueBallSelector();
  queueComposerSave();
  schedulePlayfieldRender();
});

el.startBtn?.addEventListener("click", async () => {
  try {
    await startMatch();
  } catch (error) {
    console.error(error);
    displayError(error, error.message || "Unable to rack the match.");
  }
});

el.submitShotBtn?.addEventListener("click", async () => {
  try {
    await submitShot();
  } catch (error) {
    console.error(error);
    displayError(error, error.message || "Unable to record the shot.");
  }
});

el.refreshBtn?.addEventListener("click", async () => {
  try {
    await refreshLaunchContext();
    await refreshConversationContext();
  } catch (error) {
    console.error(error);
    displayError(error, error.message || "Unable to refresh context.");
  }
});

el.projectBtn?.addEventListener("click", async () => {
  try {
    await projectCurrentState();
  } catch (error) {
    console.error(error);
    displayError(error, error.message || "Unable to project state.");
  }
});

render();
void bootstrap();
