const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = __dirname;

test("eightball index exposes a canvas-backed playfield stage", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /<canvas id="playfield-canvas" class="playfield-canvas"/);
  assert.match(html, /id="playfield-status"/);
  assert.match(html, /id="pot-indicator-grid"/);
  assert.match(html, /id="cue-angle-input"/);
  assert.match(html, /id="cue-ball-selector"/);
  assert.match(html, /id="cue-spin-marker"/);
  assert.match(html, /id="cue-power-input"/);
  assert.match(html, /id="cue-power-shell-art"/);
  assert.match(html, /id="cue-power-fill-art"/);
  assert.match(html, /id="cue-power-overlay-art"/);
  assert.match(html, /id="cue-power-meter-fill"/);
  assert.match(html, /id="turn-timer-label"/);
  assert.match(html, /id="potted-summary-line"/);
  assert.match(html, /id="match-player-left-targets"/);
  assert.match(html, /id="match-center-status"/);
  assert.match(html, /id="match-player-right-targets"/);
});

test("eightball app delegates playfield rendering to the three.js renderer module", () => {
  const source = fs.readFileSync(path.join(root, "app.js"), "utf8");
  const rendererSource = fs.readFileSync(path.join(root, "renderer.js"), "utf8");
  assert.match(source, /import \{ createEightballRenderer \} from "\.\/renderer\.js"/);
  assert.match(source, /function ensureRenderAssets\(/);
  assert.match(source, /function schedulePlayfieldRender\(/);
  assert.match(source, /function renderPotIndicators\(/);
  assert.match(source, /function renderCueBallSelector\(/);
  assert.match(source, /function updateCueSpinFromPointer\(/);
  assert.match(source, /function syncTurnTimer\(/);
  assert.match(source, /function renderHelperSummary\(/);
  assert.match(source, /function renderMatchHud\(/);
  assert.match(source, /function renderPlayerTargetStrip\(/);
  assert.match(source, /cueAngle/);
  assert.match(source, /cueSpinX/);
  assert.match(source, /cueSpinY/);
  assert.match(source, /BALL_SLOT_COMPONENTS/);
  assert.match(source, /hud-component-10-corrected\.png/);
  assert.match(source, /hud-component-35\.png/);
  assert.match(source, /hud-component-34\.png/);
  assert.match(source, /hud-component-14\.png/);
  assert.match(rendererSource, /\.\/vendor\/three\.module\.js/);
  assert.match(rendererSource, /assets\/render\/table\/singletableLondon-frame\.png/);
  assert.match(rendererSource, /assets\/render\/table\/singletableLondon-pockets\.png/);
  assert.match(rendererSource, /assets\/render\/cue\/Table_Standard_Cue-ipadhd\.png/);
  assert.match(rendererSource, /assets\/render\/extracted\/table-parts\/table-base\/pocket-arc-large-top-left\.png/);
  assert.match(rendererSource, /assets\/source\/materials\/ball-maps\/ball/);
  assert.match(rendererSource, /cueContactMarker/);
  assert.match(rendererSource, /export function createEightballRenderer/);
  assert.match(rendererSource, /const COLLISION_PART_TEXTURE_URLS/);
});

test("eightball assets are split into render and source buckets", () => {
  assert.equal(fs.existsSync(path.join(root, "assets", "render", "table", "singletableLondon-ipadhd.png")), true);
  assert.equal(fs.existsSync(path.join(root, "assets", "render", "cue", "Table_Standard_Cue-ipadhd.png")), true);
  assert.equal(fs.existsSync(path.join(root, "assets", "source", "atlas", "hud2-ipadhd.png")), true);
  assert.equal(fs.existsSync(path.join(root, "assets", "source", "table-parts", "tableBase-ipadhd.png")), true);
  assert.equal(fs.existsSync(path.join(root, "assets", "source", "materials", "ball-maps", "ball8-ipadhd.png")), true);
  assert.equal(fs.existsSync(path.join(root, "vendor", "three.module.js")), true);
  assert.equal(fs.existsSync(path.join(root, "vendor", "three.core.js")), true);
});

test("eightball atlas extraction pipeline assets exist", () => {
  assert.equal(fs.existsSync(path.join(root, "tools", "atlas-manifest.mjs")), true);
  assert.equal(fs.existsSync(path.join(root, "tools", "extract-atlas-assets.mjs")), true);
  assert.equal(fs.existsSync(path.join(root, "tools", "recrop-hud-components.mjs")), true);
  assert.equal(fs.existsSync(path.join(root, "tools", "split-table-reference.mjs")), true);
});
