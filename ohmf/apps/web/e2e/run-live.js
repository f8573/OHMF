"use strict";

const { spawn } = require("node:child_process");

const command = process.platform === "win32" ? "cmd.exe" : "npx";
const args = process.platform === "win32"
  ? ["/d", "/s", "/c", "npx", "playwright", "test", "--grep", "@live", ...process.argv.slice(2)]
  : ["playwright", "test", "--grep", "@live", ...process.argv.slice(2)];

const child = spawn(command, args, {
  stdio: "inherit",
  env: {
    ...process.env,
    OHMF_E2E_LIVE: "1",
  },
});

child.on("exit", (code) => {
  process.exit(code ?? 1);
});
