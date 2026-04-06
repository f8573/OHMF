import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const APP_ROOT = path.resolve(__dirname, "..");
const SOURCE_ROOT = path.join(APP_ROOT, "assets", "source");
const OUTPUT_ROOT = path.join(APP_ROOT, "assets", "render", "extracted");

function sourcePath(...parts) {
  return path.join(SOURCE_ROOT, ...parts);
}

function outputPath(...parts) {
  return path.join(OUTPUT_ROOT, ...parts);
}

export const atlasManifest = Object.freeze([
  {
    id: "hud",
    source: sourcePath("atlas", "hud-ipadhd.png"),
    outputDir: outputPath("hud"),
    strategy: {
      type: "components",
      prefix: "hud-component",
      minArea: 2000,
      padding: 2,
      cleanupEdgeBackground: false,
      foreground: {
        alphaMin: 10,
        channelMin: 8,
        rgbSumMin: 30,
      },
      background: {
        alphaMax: 12,
        channelMax: 20,
      },
    },
  },
  {
    id: "hud2",
    source: sourcePath("atlas", "hud2-ipadhd.png"),
    outputDir: outputPath("hud2"),
    strategy: {
      type: "components",
      prefix: "ball",
      minArea: 1000,
      padding: 1,
      cleanupEdgeBackground: false,
      names: [
        "cue-ball-close",
        "cue-ball-wide",
        "ball-10",
        "ball-11",
        "ball-12",
        "ball-15",
        "ball-5",
        "ball-13",
        "ball-14",
        "ball-1",
        "ball-3",
        "ball-7",
        "ball-2",
        "ball-4",
        "ball-6",
        "ball-8",
        "ball-9",
      ],
      foreground: {
        alphaMin: 10,
        channelMin: 20,
        rgbSumMin: 40,
      },
      background: {
        alphaMax: 12,
        channelMax: 18,
      },
    },
  },
  {
    id: "table-base",
    source: sourcePath("table-parts", "tableBase-ipadhd.png"),
    outputDir: outputPath("table-parts", "table-base"),
    strategy: {
      type: "components",
      prefix: "table-base-part",
      minArea: 50,
      padding: 1,
      cleanupEdgeBackground: true,
      names: [
        "pocket-arc-large-top-left",
        "pocket-glow-large-top",
        "pocket-arc-small-top-right",
        "pocket-glow-small-right",
        "pocket-arc-large-mid-left",
        "pocket-arc-medium-mid",
        "pocket-ring-large-bottom",
        "pocket-ring-medium-bottom",
        "pocket-arc-small-bottom-right",
        "pocket-arc-large-bottom-left",
      ],
      foreground: {
        alphaMin: 10,
        channelMin: 10,
        rgbSumMin: 24,
      },
      background: {
        alphaMax: 12,
        channelMax: 18,
      },
    },
  },
  {
    id: "table-cushions",
    source: sourcePath("table-parts", "tableCushions-hd.png"),
    outputDir: outputPath("table-parts", "table-cushions"),
    strategy: {
      type: "components",
      prefix: "table-cushion",
      minArea: 50,
      padding: 1,
      cleanupEdgeBackground: true,
      names: [
        "cushion-top",
        "cushion-middle",
        "cushion-bottom",
      ],
      foreground: {
        alphaMin: 10,
        channelMin: 10,
        rgbSumMin: 24,
      },
      background: {
        alphaMax: 12,
        channelMax: 18,
      },
    },
  },
]);

export const extractionRoot = OUTPUT_ROOT;
export const previewOutputPath = path.join(OUTPUT_ROOT, "preview.html");
export const generatedManifestPath = path.join(OUTPUT_ROOT, "manifest.generated.json");
export const validationReportPath = path.join(OUTPUT_ROOT, "validation-report.json");
export const validationScreenshotPath = path.join(OUTPUT_ROOT, "validation", "atlas-preview.png");
