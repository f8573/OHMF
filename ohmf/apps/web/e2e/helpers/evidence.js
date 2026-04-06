const path = require("node:path");

async function captureEvidence(page, testInfo, name, options = {}) {
  const safeName = String(name || "evidence").replace(/[^a-z0-9-_]+/gi, "-").toLowerCase();
  const screenshotPath = path.join(testInfo.outputDir, `${safeName}.png`);
  await page.screenshot({
    path: screenshotPath,
    fullPage: options.fullPage !== false,
    animations: "disabled",
  });
  await testInfo.attach(`${safeName}-screenshot`, {
    path: screenshotPath,
    contentType: "image/png",
  });
}

module.exports = {
  captureEvidence,
};
