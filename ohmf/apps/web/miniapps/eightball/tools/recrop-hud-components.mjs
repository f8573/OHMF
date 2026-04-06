import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const SOURCE_IMAGE = "C:/Users/James/Downloads/Messages/ohmf/apps/web/miniapps/eightball/assets/source/atlas/hud-ipadhd.png";
const OUTPUT_DIR = "C:/Users/James/Downloads/Messages/ohmf/apps/web/miniapps/eightball/assets/render/extracted/hud";
const OUTPUT_PATH = path.join(OUTPUT_DIR, "hud-component-10-corrected.png");
const REPORT_PATH = path.join(OUTPUT_DIR, "hud-component-10-corrected.json");

const CROP = {
  x: 2080,
  y: 225,
  width: 82,
  height: 82,
};

async function main() {
  const sourceBuffer = await fs.readFile(SOURCE_IMAGE);
  const sourceDataUrl = `data:image/webp;base64,${sourceBuffer.toString("base64")}`;
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.setContent("<!doctype html><html><body></body></html>");
    const result = await page.evaluate(async ({ imageUrl, crop }) => {
      const image = new Image();
      image.src = imageUrl;
      await image.decode();
      const canvas = document.createElement("canvas");
      canvas.width = crop.width;
      canvas.height = crop.height;
      const context = canvas.getContext("2d");
      context.drawImage(
        image,
        crop.x,
        crop.y,
        crop.width,
        crop.height,
        0,
        0,
        crop.width,
        crop.height
      );
      return {
        base64: canvas.toDataURL("image/png").split(",")[1],
        width: crop.width,
        height: crop.height,
      };
    }, { imageUrl: sourceDataUrl, crop: CROP });

    await fs.writeFile(OUTPUT_PATH, Buffer.from(result.base64, "base64"));
    await fs.writeFile(
      REPORT_PATH,
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          sourceImage: SOURCE_IMAGE,
          outputPath: OUTPUT_PATH,
          crop: CROP,
          width: result.width,
          height: result.height,
        },
        null,
        2
      )
    );
    console.log(`Wrote ${OUTPUT_PATH}`);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
