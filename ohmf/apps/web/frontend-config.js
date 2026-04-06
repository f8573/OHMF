(() => {
  const runtimeConfig = window.OHMF_RUNTIME_CONFIG || {};
  const DEFAULT_FRONTEND_PORT = String(runtimeConfig.frontend_port || "5173");
  const DEFAULT_API_HOST_PORT = String(runtimeConfig.api_host_port || "18080");
  const storedFrontendPort = window.localStorage.getItem("ohmf.frontend_port");
  const storedAPIHostPort = window.localStorage.getItem("ohmf.api_host_port");
  const resolvedFrontendPort = runtimeConfig.frontend_port ? DEFAULT_FRONTEND_PORT : (storedFrontendPort || DEFAULT_FRONTEND_PORT);
  const resolvedAPIHostPort = runtimeConfig.api_host_port ? DEFAULT_API_HOST_PORT : (storedAPIHostPort || DEFAULT_API_HOST_PORT);
  const storedAPIBaseURL = runtimeConfig.api_base_url || window.localStorage.getItem("ohmf.apiBaseUrl") || `http://localhost:${resolvedAPIHostPort}`;

  function normalizeAPIBaseURL(value) {
    const fallback = `http://localhost:${resolvedAPIHostPort}`;
    if (!value) return fallback;
    try {
      const url = new URL(value);
      const localHosts = new Set(["localhost", "127.0.0.1"]);
      if (localHosts.has(url.hostname) && url.port !== resolvedAPIHostPort) {
        url.port = resolvedAPIHostPort;
        const normalized = url.toString().replace(/\/+$/, "");
        window.localStorage.setItem("ohmf.apiBaseUrl", normalized);
        return normalized;
      }
      return url.toString().replace(/\/+$/, "");
    } catch {
      return fallback;
    }
  }

  window.localStorage.setItem("ohmf.frontend_port", resolvedFrontendPort);
  window.localStorage.setItem("ohmf.api_host_port", resolvedAPIHostPort);

  window.OHMF_WEB_CONFIG = Object.freeze({
    frontend_port: resolvedFrontendPort,
    api_host_port: resolvedAPIHostPort,
    api_base_url: normalizeAPIBaseURL(storedAPIBaseURL),
    developer_mode: Boolean(runtimeConfig.developer_mode),
    use_real_otp_provider: Boolean(runtimeConfig.use_real_otp_provider),
    web_push_enabled: Boolean(runtimeConfig.web_push_enabled),
    web_push_vapid_public_key: String(runtimeConfig.web_push_vapid_public_key || ""),
  });
})();
