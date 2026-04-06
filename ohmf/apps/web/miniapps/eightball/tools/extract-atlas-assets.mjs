import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";
import {
  atlasManifest,
  extractionRoot,
  generatedManifestPath,
  previewOutputPath,
  validationReportPath,
  validationScreenshotPath,
} from "./atlas-manifest.mjs";

function toPosix(relativePath) {
  return relativePath.split(path.sep).join("/");
}

function padIndex(value) {
  return String(value).padStart(2, "0");
}

function renderPreviewHtml(report) {
  const sections = report.sheets
    .map((sheet) => {
      const cards = sheet.outputs
        .map((output) => {
          const imagePath = toPosix(path.relative(extractionRoot, output.path));
          return `
            <figure class="asset-card">
              <div class="asset-frame">
                <img src="${imagePath}" alt="${output.name}" loading="lazy" />
              </div>
              <figcaption>
                <strong>${output.name}</strong>
                <span>${output.width}×${output.height}</span>
              </figcaption>
            </figure>
          `;
        })
        .join("\n");
      return `
        <section class="sheet">
          <header class="sheet-head">
            <div>
              <p class="eyebrow">${sheet.id}</p>
              <h2>${sheet.sourceName}</h2>
            </div>
            <p class="count">${sheet.outputs.length} extracts</p>
          </header>
          <div class="asset-grid">
            ${cards}
          </div>
        </section>
      `;
    })
    .join("\n");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>8 Ball Atlas Preview</title>
    <style>
      :root {
        color-scheme: dark;
        --bg: #0d1014;
        --surface: #161b21;
        --surface-strong: #1d242c;
        --line: rgba(255,255,255,0.08);
        --text: #eef2f6;
        --muted: #98a4b1;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "Segoe UI", system-ui, sans-serif;
        background:
          radial-gradient(circle at top left, rgba(151, 48, 64, 0.24), transparent 20rem),
          linear-gradient(180deg, #0c1015, #121820 48%, #0c1015);
        color: var(--text);
      }
      main {
        max-width: 1600px;
        margin: 0 auto;
        padding: 24px;
      }
      .hero {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: end;
        margin-bottom: 24px;
      }
      .hero h1 { margin: 0; font-size: 2rem; }
      .hero p { margin: 8px 0 0; color: var(--muted); }
      .sheet {
        margin-bottom: 26px;
        padding: 18px;
        border-radius: 20px;
        background: rgba(22, 27, 33, 0.9);
        border: 1px solid var(--line);
      }
      .sheet-head {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: baseline;
        margin-bottom: 14px;
      }
      .sheet-head h2, .sheet-head p { margin: 0; }
      .eyebrow {
        margin: 0 0 6px;
        color: #f2ae4f;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.75rem;
      }
      .count { color: var(--muted); }
      .asset-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
      }
      .asset-card {
        margin: 0;
        padding: 12px;
        border-radius: 16px;
        background: var(--surface);
        border: 1px solid var(--line);
      }
      .asset-frame {
        min-height: 140px;
        display: grid;
        place-items: center;
        border-radius: 12px;
        background:
          linear-gradient(90deg, rgba(240, 244, 249, 0.96) 0 50%, rgba(27, 34, 43, 0.96) 50% 100%),
          linear-gradient(45deg, rgba(0,0,0,0.08) 25%, transparent 25%, transparent 75%, rgba(0,0,0,0.08) 75%),
          linear-gradient(45deg, rgba(255,255,255,0.12) 25%, transparent 25%, transparent 75%, rgba(255,255,255,0.12) 75%);
        background-position: 0 0, 0 0, 10px 10px;
        background-size: auto, 20px 20px, 20px 20px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.08);
      }
      img {
        display: block;
        max-width: 100%;
        max-height: 220px;
        object-fit: contain;
      }
      figcaption {
        display: flex;
        justify-content: space-between;
        gap: 8px;
        margin-top: 10px;
        font-size: 0.88rem;
        color: var(--muted);
      }
      figcaption strong {
        color: var(--text);
        font-size: 0.92rem;
      }
    </style>
  </head>
  <body>
    <main>
      <header class="hero">
        <div>
          <h1>8 Ball Atlas Extract Preview</h1>
          <p>Crop, save, validate artifacts generated from the source atlas and table-part sheets.</p>
        </div>
        <p>${report.totalOutputs} total extracts</p>
      </header>
      ${sections}
    </main>
  </body>
</html>`;
}

async function cleanOutputRoot() {
  await fs.rm(extractionRoot, { recursive: true, force: true });
  await fs.mkdir(extractionRoot, { recursive: true });
  await fs.mkdir(path.dirname(validationScreenshotPath), { recursive: true });
}

async function extractSheet(page, sheet) {
  const sourceBytes = await fs.readFile(sheet.source);
  const sourceName = path.basename(sheet.source);
  const result = await page.evaluate(
    async ({ base64, strategy, sheetId }) => {
      function clamp(value, min, max) {
        return Math.max(min, Math.min(max, value));
      }

      function detectForeground(data, pixelIndex, foreground) {
        const offset = pixelIndex * 4;
        const r = data[offset];
        const g = data[offset + 1];
        const b = data[offset + 2];
        const a = data[offset + 3];
        return (
          a > foreground.alphaMin &&
          (r > foreground.channelMin || g > foreground.channelMin || b > foreground.channelMin) &&
          r + g + b > foreground.rgbSumMin
        );
      }

      function extractComponents(imageData, foreground, minArea) {
        const { data, width, height } = imageData;
        const visited = new Uint8Array(width * height);
        const components = [];
        for (let y = 0; y < height; y += 1) {
          for (let x = 0; x < width; x += 1) {
            const flat = y * width + x;
            if (visited[flat]) continue;
            if (!detectForeground(data, flat, foreground)) continue;
            visited[flat] = 1;
            const queue = [flat];
            let count = 0;
            let minX = x;
            let maxX = x;
            let minY = y;
            let maxY = y;
            while (queue.length) {
              const current = queue.pop();
              const cy = Math.floor(current / width);
              const cx = current - cy * width;
              count += 1;
              if (cx < minX) minX = cx;
              if (cx > maxX) maxX = cx;
              if (cy < minY) minY = cy;
              if (cy > maxY) maxY = cy;
              const neighbors = [current - 1, current + 1, current - width, current + width];
              for (const next of neighbors) {
                if (next < 0 || next >= visited.length || visited[next]) continue;
                const ny = Math.floor(next / width);
                const nx = next - ny * width;
                if (Math.abs(nx - cx) + Math.abs(ny - cy) !== 1) continue;
                if (!detectForeground(data, next, foreground)) continue;
                visited[next] = 1;
                queue.push(next);
              }
            }
            if (count >= minArea) {
              components.push({
                area: count,
                minX,
                minY,
                maxX,
                maxY,
                width: maxX - minX + 1,
                height: maxY - minY + 1,
              });
            }
          }
        }
        components.sort((left, right) => left.minY - right.minY || left.minX - right.minX);
        return components;
      }

      function removeEdgeBackground(imageData, background) {
        const { data, width, height } = imageData;
        const visited = new Uint8Array(width * height);
        const queue = [];

        function isBackgroundPixel(pixelIndex) {
          const offset = pixelIndex * 4;
          return (
            data[offset + 3] <= background.alphaMax ||
            (data[offset] <= background.channelMax &&
              data[offset + 1] <= background.channelMax &&
              data[offset + 2] <= background.channelMax)
          );
        }

        function pushIfBackground(x, y) {
          if (x < 0 || y < 0 || x >= width || y >= height) return;
          const flat = y * width + x;
          if (visited[flat] || !isBackgroundPixel(flat)) return;
          visited[flat] = 1;
          queue.push(flat);
        }

        for (let x = 0; x < width; x += 1) {
          pushIfBackground(x, 0);
          pushIfBackground(x, height - 1);
        }
        for (let y = 0; y < height; y += 1) {
          pushIfBackground(0, y);
          pushIfBackground(width - 1, y);
        }

        while (queue.length) {
          const current = queue.pop();
          const cy = Math.floor(current / width);
          const cx = current - cy * width;
          data[current * 4 + 3] = 0;
          pushIfBackground(cx - 1, cy);
          pushIfBackground(cx + 1, cy);
          pushIfBackground(cx, cy - 1);
          pushIfBackground(cx, cy + 1);
        }

        return imageData;
      }

      async function decodeImage(base64) {
        const bytes = Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
        const blob = new Blob([bytes], { type: "image/webp" });
        const url = URL.createObjectURL(blob);
        const image = new Image();
        image.src = url;
        await image.decode();
        return { image, url };
      }

      const { image, url } = await decodeImage(base64);
      const sourceCanvas = document.createElement("canvas");
      sourceCanvas.width = image.naturalWidth;
      sourceCanvas.height = image.naturalHeight;
      const sourceContext = sourceCanvas.getContext("2d", { willReadFrequently: true });
      sourceContext.drawImage(image, 0, 0);

      const components = extractComponents(
        sourceContext.getImageData(0, 0, sourceCanvas.width, sourceCanvas.height),
        strategy.foreground,
        strategy.minArea
      );

      const outputs = [];
      for (let index = 0; index < components.length; index += 1) {
        const component = components[index];
        const x = clamp(component.minX - strategy.padding, 0, sourceCanvas.width);
        const y = clamp(component.minY - strategy.padding, 0, sourceCanvas.height);
        const cropWidth = clamp(component.width + strategy.padding * 2, 1, sourceCanvas.width - x);
        const cropHeight = clamp(component.height + strategy.padding * 2, 1, sourceCanvas.height - y);
        const name =
          strategy.names?.[index] ||
          `${strategy.prefix}-${String(index + 1).padStart(2, "0")}`;
        const cropCanvas = document.createElement("canvas");
        cropCanvas.width = cropWidth;
        cropCanvas.height = cropHeight;
        const cropContext = cropCanvas.getContext("2d", { willReadFrequently: true });
        cropContext.drawImage(
          sourceCanvas,
          x,
          y,
          cropWidth,
          cropHeight,
          0,
          0,
          cropWidth,
          cropHeight
        );
        if (strategy.cleanupEdgeBackground) {
          const imageData = cropContext.getImageData(0, 0, cropWidth, cropHeight);
          removeEdgeBackground(imageData, strategy.background);
          cropContext.putImageData(imageData, 0, 0);
        }
        outputs.push({
          name,
          area: component.area,
          sourceRect: { x, y, width: cropWidth, height: cropHeight },
          pngBase64: cropCanvas.toDataURL("image/png").split(",")[1],
          width: cropWidth,
          height: cropHeight,
        });
      }

      URL.revokeObjectURL(url);
      return {
        sheetId,
        source: { width: sourceCanvas.width, height: sourceCanvas.height },
        outputs,
      };
    },
    {
      base64: sourceBytes.toString("base64"),
      strategy: sheet.strategy,
      sheetId: sheet.id,
    }
  );

  await fs.mkdir(sheet.outputDir, { recursive: true });
  const outputs = [];
  for (const [index, output] of result.outputs.entries()) {
    const fileName = `${output.name}.png`;
    const outputPath = path.join(sheet.outputDir, fileName);
    await fs.writeFile(outputPath, Buffer.from(output.pngBase64, "base64"));
    outputs.push({
      name: output.name,
      index: index + 1,
      area: output.area,
      width: output.width,
      height: output.height,
      sourceRect: output.sourceRect,
      path: outputPath,
      fileName,
      relativePath: toPosix(path.relative(extractionRoot, outputPath)),
    });
  }

  return {
    id: sheet.id,
    sourceName,
    sourcePath: sheet.source,
    sourceSize: result.source,
    outputDir: sheet.outputDir,
    outputs,
  };
}

async function writePreview(report) {
  await fs.writeFile(previewOutputPath, renderPreviewHtml(report), "utf8");
}

async function captureValidationScreenshot(browser) {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1200 } });
  await page.goto(`file:///${previewOutputPath.replace(/\\/g, "/")}`, { waitUntil: "load" });
  await page.screenshot({ path: validationScreenshotPath, fullPage: true });
  await page.close();
}

async function main() {
  await cleanOutputRoot();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto("data:text/html,<html><body></body></html>", { waitUntil: "load" });
    const sheets = [];
    for (const sheet of atlasManifest) {
      sheets.push(await extractSheet(page, sheet));
    }
    await page.close();

    const report = {
      generatedAt: new Date().toISOString(),
      totalOutputs: sheets.reduce((sum, sheet) => sum + sheet.outputs.length, 0),
      sheets,
      previewPath: previewOutputPath,
      validationScreenshotPath,
    };
    await fs.writeFile(generatedManifestPath, JSON.stringify(report, null, 2));
    await writePreview(report);
    await captureValidationScreenshot(browser);
    await fs.writeFile(
      validationReportPath,
      JSON.stringify(
        {
          generatedAt: report.generatedAt,
          totalOutputs: report.totalOutputs,
          previewPath: previewOutputPath,
          validationScreenshotPath,
          sheets: sheets.map((sheet) => ({
            id: sheet.id,
            sourceName: sheet.sourceName,
            count: sheet.outputs.length,
          })),
        },
        null,
        2
      )
    );

    console.log(`Generated ${report.totalOutputs} extracted assets in ${extractionRoot}`);
    for (const sheet of sheets) {
      console.log(`${sheet.id}: ${sheet.outputs.length} files`);
    }
    console.log(`Preview: ${previewOutputPath}`);
    console.log(`Validation screenshot: ${validationScreenshotPath}`);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
