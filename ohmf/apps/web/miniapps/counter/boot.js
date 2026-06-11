const runtimeParams = new URLSearchParams(window.location.search);
const assetVersion = encodeURIComponent(runtimeParams.get("asset_version") || "dev");

await import(`./app.js?v=${assetVersion}`);
