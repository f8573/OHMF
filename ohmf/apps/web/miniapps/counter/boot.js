let assetVersion = "dev";
const runtimeParams = new URLSearchParams(window.location.search);

try {
  assetVersion =
    runtimeParams.get("asset_version")
    || window.OHMF_RUNTIME_CONFIG?.asset_version
    || assetVersion;
} catch {}

await import(`./app.js?v=${encodeURIComponent(assetVersion)}`);
