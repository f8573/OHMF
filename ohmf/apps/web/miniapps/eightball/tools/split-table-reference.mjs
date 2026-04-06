import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const SOURCE_IMAGE = "C:/Users/James/Downloads/test.png";
const OUTPUT_DIR = "C:/Users/James/Downloads/Messages/ohmf/apps/web/miniapps/eightball/assets/render/table";
const FRAME_OUTPUT = path.join(OUTPUT_DIR, "singletableLondon-frame.png");
const POCKETS_OUTPUT = path.join(OUTPUT_DIR, "singletableLondon-pockets.png");
const REPORT_OUTPUT = path.join(OUTPUT_DIR, "singletableLondon-split-report.json");

function buildDataUrl(buffer) {
  return `data:image/png;base64,${buffer.toString("base64")}`;
}

async function main() {
  const sourceBuffer = await fs.readFile(SOURCE_IMAGE);
  const sourceDataUrl = buildDataUrl(sourceBuffer);
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();
    await page.setContent("<!doctype html><html><body></body></html>");

    const result = await page.evaluate(async ({ imageUrl }) => {
      const loadImage = async (src) => {
        const image = new Image();
        image.src = src;
        await image.decode();
        return image;
      };

      const cropHalf = (context, source, sx, sy, sw, sh) => {
        const canvas = document.createElement("canvas");
        canvas.width = sw;
        canvas.height = sh;
        const nextContext = canvas.getContext("2d", { willReadFrequently: true });
        nextContext.drawImage(source, sx, sy, sw, sh, 0, 0, sw, sh);
        return { canvas, context: nextContext };
      };

      const image = await loadImage(imageUrl);
      const halfWidth = Math.floor(image.naturalWidth / 2);
      const height = image.naturalHeight;

      const pocketHalf = cropHalf(null, image, 0, 0, halfWidth, height);
      const tableHalf = cropHalf(null, image, halfWidth, 0, halfWidth, height);

      return {
        sourceWidth: image.naturalWidth,
        sourceHeight: image.naturalHeight,
        outputWidth: halfWidth,
        outputHeight: height,
        frameBase64: tableHalf.canvas.toDataURL("image/png").split(",")[1],
        pocketsBase64: pocketHalf.canvas.toDataURL("image/png").split(",")[1],
      };
    }, { imageUrl: sourceDataUrl });

    await fs.writeFile(FRAME_OUTPUT, Buffer.from(result.frameBase64, "base64"));
    await fs.writeFile(POCKETS_OUTPUT, Buffer.from(result.pocketsBase64, "base64"));
    await fs.writeFile(
      REPORT_OUTPUT,
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          sourceImage: SOURCE_IMAGE,
          sourceWidth: result.sourceWidth,
          sourceHeight: result.sourceHeight,
          outputWidth: result.outputWidth,
          outputHeight: result.outputHeight,
          outputs: {
            frame: FRAME_OUTPUT,
            pockets: POCKETS_OUTPUT,
          },
        },
        null,
        2
      )
    );

    console.log(`Frame: ${FRAME_OUTPUT}`);
    console.log(`Pockets: ${POCKETS_OUTPUT}`);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
