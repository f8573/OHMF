#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");

const repoRoot = path.resolve(__dirname, "..");
const webDir = path.join(repoRoot, "ohmf", "apps", "web");
const gatewayDir = path.join(repoRoot, "ohmf", "services", "gateway");
const isWindows = process.platform === "win32";
const npmCmd = isWindows ? (process.env.ComSpec || "cmd.exe") : "npm";
const stagingChecklistPath = path.join(repoRoot, "testing", "STAGING_CHECKLIST.md");

function resolveGoCommand() {
  const local = path.join(repoRoot, "ohmf", ".tools", "go", "bin", isWindows ? "go.exe" : "go");
  if (fs.existsSync(local)) return local;
  return "go";
}

const goCmd = resolveGoCommand();

function npmRunArgs(scriptName) {
  if (isWindows) {
    return ["/d", "/s", "/c", "npm", "run", scriptName];
  }
  return ["run", scriptName];
}

function dockerCommandAndArgs(command, args) {
  if (!isWindows) {
    return { command, args };
  }
  return {
    command: process.env.ComSpec || "cmd.exe",
    args: ["/d", "/s", "/c", command, ...args],
  };
}

function loadSuiteArgs() {
  const composeFile = process.env.OHMF_LOAD_COMPOSE_FILE || path.join(repoRoot, "ohmf", "infra", "docker", "docker-compose.yml");
  const artifactDir = process.env.OHMF_LOAD_ARTIFACT_DIR || path.join(repoRoot, "ohmf", "docs", "reports", "validation");
  const args = [
    "run",
    "./tools/loadsuite",
    "--mode",
    process.env.OHMF_LOAD_MODE || "full",
    "--base-url",
    process.env.OHMF_LOAD_BASE_URL || "http://localhost:18080",
    "--compose-file",
    composeFile,
    "--artifact-dir",
    artifactDir,
    "--profile",
    process.env.OHMF_LOAD_PROFILE || "active_sustain",
    "--initial-devices",
    process.env.OHMF_LOAD_INITIAL_DEVICES || "100",
    "--max-devices",
    process.env.OHMF_LOAD_MAX_DEVICES || "3000",
    "--prove-duration",
    process.env.OHMF_LOAD_PROVE_DURATION || "3m",
    "--sustain-duration",
    process.env.OHMF_LOAD_SUSTAIN_DURATION || "30m",
    "--warmup-duration",
    process.env.OHMF_LOAD_WARMUP_DURATION || "90s",
    "--fixture-dir",
    process.env.OHMF_LOAD_FIXTURE_DIR || path.join(artifactDir, "fixture"),
    "--connect-wave-size",
    process.env.OHMF_LOAD_CONNECT_WAVE_SIZE || "0",
    "--connect-wave-interval",
    process.env.OHMF_LOAD_CONNECT_WAVE_INTERVAL || "15s",
    "--ready-threshold",
    process.env.OHMF_LOAD_READY_THRESHOLD || "0.98",
    "--seed",
    process.env.OHMF_LOAD_SEED || String(Date.now()),
  ];
  return args;
}

const gates = {
  unit: {
    description: "Fast backend unit and contract coverage.",
    steps: [
      {
        name: "go-unit",
        cwd: repoRoot,
        command: isWindows ? "powershell.exe" : "bash",
        args: isWindows
          ? ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ".\\scripts\\run-tests.ps1"]
          : ["./scripts/run-tests.sh"],
        tags: [
          "auth",
          "messages",
          "conversations",
          "sync",
          "realtime",
          "devices",
          "privacy",
          "miniapp",
          "media",
          "relay",
          "e2ee",
          "search",
          "migration",
        ],
      },
    ],
  },
  integration: {
    description: "Container-backed integration coverage.",
    steps: [
      {
        name: "go-integration",
        cwd: repoRoot,
        command: isWindows ? "powershell.exe" : "bash",
        args: isWindows
          ? ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ".\\scripts\\run-tests.ps1", "-Integration"]
          : ["./scripts/run-tests.sh", "--integration"],
        env: {
          POSTGRES_PORT: "55432",
        },
        tags: [
          "auth",
          "messages",
          "conversations",
          "sync",
          "realtime",
          "devices",
          "privacy",
          "miniapp",
          "media",
          "relay",
          "e2ee",
          "search",
          "migration",
        ],
      },
    ],
  },
  web: {
    description: "Fast web shell and helper coverage.",
    steps: [
      {
        name: "web-unit",
        cwd: webDir,
        command: npmCmd,
        args: npmRunArgs("test:unit"),
        tags: [
          "auth",
          "messages",
          "devices",
          "privacy",
          "miniapp",
          "media",
          "realtime",
          "e2ee",
        ],
      },
    ],
  },
  e2e: {
    description: "Mocked browser UI regression coverage.",
    steps: [
      {
        name: "web-playwright-mocked",
        cwd: webDir,
        command: npmCmd,
        args: npmRunArgs("test:e2e"),
        tags: [
          "messages",
          "conversations",
          "devices",
          "privacy",
          "miniapp",
          "media",
        ],
      },
    ],
  },
  live: {
    description: "Live browser end-to-end coverage against a running stack.",
    steps: [
      {
        name: "web-playwright-live",
        cwd: webDir,
        command: npmCmd,
        args: npmRunArgs("test:e2e:live"),
        tags: [
          "auth",
          "messages",
          "conversations",
          "devices",
          "privacy",
          "realtime",
          "miniapp",
          "media",
          "e2ee",
        ],
      },
    ],
  },
  perf: {
    description: "Race detection and targeted performance baselines.",
    steps: [
      {
        name: "gateway-race",
        cwd: gatewayDir,
        ...dockerCommandAndArgs(isWindows ? "docker" : goCmd, isWindows
          ? [
              "run",
              "--rm",
              "-v",
              `${repoRoot}:/src`,
              "golang:1.25-alpine",
              "/bin/sh",
              "/src/scripts/run-perf-race.sh",
            ]
          : [
              "test",
              "-race",
              "./internal/e2ee/...",
              "./internal/messages/...",
              "./internal/realtime/...",
              "-count=1",
              "-timeout=10m",
            ]),
        tags: ["perf", "e2ee", "messages", "realtime"],
      },
      {
        name: "gateway-bench",
        cwd: gatewayDir,
        command: goCmd,
        args: [
          "test",
          "-run",
          "^$",
          "-bench",
          "Benchmark",
          "-benchmem",
          "./internal/e2ee/...",
          "-count=1",
          "-timeout=10m",
        ],
        tags: ["perf", "e2ee"],
      },
    ],
  },
  load: {
    description: "Single-host OTT load suite with stack bring-up, readiness checks, and sustained load reporting.",
    steps: [
      {
        name: "load-stack-up",
        cwd: repoRoot,
        ...dockerCommandAndArgs("docker", [
          "compose",
          "-f",
          process.env.OHMF_LOAD_COMPOSE_FILE || path.join(repoRoot, "ohmf", "infra", "docker", "docker-compose.yml"),
          "up",
          "-d",
          "--build",
          "--force-recreate",
          "db",
          "redis",
          "kafka",
          "cassandra",
          "apps",
          "api",
          "messages-processor",
          "delivery-processor",
          "sms-processor",
          "prometheus",
        ]),
        tags: ["perf", "messages", "realtime", "devices"],
      },
      {
        name: "load-stack-wait",
        cwd: repoRoot,
        command: isWindows ? "powershell.exe" : "bash",
        args: isWindows
          ? [
              "-NoProfile",
              "-ExecutionPolicy",
              "Bypass",
              "-Command",
              `
              $ErrorActionPreference = 'Stop'
              $deadline = (Get-Date).AddMinutes(2)
              $targets = @(
                'http://localhost:18080/healthz',
                'http://localhost:18086/healthz',
                'http://localhost:19090/-/healthy'
              )
              while ((Get-Date) -lt $deadline) {
                $allOk = $true
                foreach ($target in $targets) {
                  try {
                    $response = Invoke-WebRequest -UseBasicParsing -Uri $target -TimeoutSec 5
                    if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 300) { $allOk = $false }
                  } catch {
                    $allOk = $false
                  }
                }
                if ($allOk) { exit 0 }
                Start-Sleep -Seconds 3
              }
              throw 'OHMF services did not become healthy in time.'
              `,
            ]
          : [
              "-lc",
              `
              set -euo pipefail
              deadline=$((SECONDS + 120))
              while [ "$SECONDS" -lt "$deadline" ]; do
                curl -fsS http://localhost:18080/healthz >/dev/null &&
                curl -fsS http://localhost:18086/healthz >/dev/null &&
                curl -fsS http://localhost:19090/-/healthy >/dev/null &&
                exit 0
                sleep 3
              done
              echo "OHMF services did not become healthy in time." >&2
              exit 1
              `,
            ],
        tags: ["perf", "messages", "realtime", "devices"],
      },
      {
        name: "loadsuite-unit",
        cwd: path.join(repoRoot, "ohmf"),
        command: goCmd,
        args: ["test", "./tools/loadsuite", "-count=1"],
        env: {
          GOCACHE: path.join(repoRoot, "ohmf", ".gocache"),
        },
        tags: ["perf", "messages", "realtime", "devices"],
      },
      {
        name: "loadsuite-run",
        cwd: path.join(repoRoot, "ohmf"),
        command: goCmd,
        args: loadSuiteArgs(),
        env: {
          GOCACHE: path.join(repoRoot, "ohmf", ".gocache"),
          OHMF_LOAD_SKIP_STACK_UP: "1",
        },
        tags: ["perf", "messages", "realtime", "devices"],
      },
    ],
  },
  staging: {
    description: "Manual and staging signoff gate with optional automated checks.",
    steps: [],
  },
};

function parseArgs(argv) {
  const args = [...argv];
  const gate = args.shift() || "list";
  let tag = process.env.OHMF_TEST_TAG || "";
  for (let i = 0; i < args.length; i += 1) {
    const value = args[i];
    if (value === "--tag" && args[i + 1]) {
      tag = args[i + 1];
      i += 1;
      continue;
    }
    if (value.startsWith("--tag=")) {
      tag = value.slice("--tag=".length);
    }
  }
  return { gate, tag: String(tag || "").trim().toLowerCase() };
}

function printUsage() {
  console.log("Usage: node ./scripts/test-gates.js <gate> [--tag <capability>]");
  console.log("");
  console.log("Available gates:");
  for (const [name, config] of Object.entries(gates)) {
    console.log(`- ${name}: ${config.description}`);
  }
  console.log("");
  console.log("Capability tags:");
  console.log("  auth, conversations, devices, e2ee, media, messages, migration, miniapp, perf, privacy, realtime, relay, search, sync");
}

function printGateList() {
  printUsage();
  console.log("");
  console.log(`Staging checklist: ${stagingChecklistPath}`);
}

function stepMatchesTag(step, tag) {
  if (!tag) return true;
  return Array.isArray(step.tags) && step.tags.some((item) => item.toLowerCase() === tag);
}

function runStep(step) {
  return new Promise((resolve, reject) => {
    console.log(`\n== ${step.name} ==`);
    console.log(`cwd: ${step.cwd}`);
    console.log(`tags: ${step.tags.join(", ")}`);

    const child = spawn(step.command, step.args, {
      cwd: step.cwd,
      stdio: "inherit",
      env: {
        ...process.env,
        ...(step.env || {}),
      },
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${step.name} failed with exit code ${code ?? 1}`));
    });
  });
}

function printStagingChecklist() {
  console.log(`Manual/staging checklist: ${stagingChecklistPath}`);
  console.log("Set OHMF_RUN_STAGING_AUTOMATION=1 to also run integration and live automation before signoff.");
}

async function runStagingGate(tag) {
  printStagingChecklist();
  if (process.env.OHMF_RUN_STAGING_AUTOMATION !== "1") {
    return;
  }

  const integrationSteps = gates.integration.steps.filter((step) => stepMatchesTag(step, tag));
  const liveSteps = gates.live.steps.filter((step) => stepMatchesTag(step, tag));
  for (const step of [...integrationSteps, ...liveSteps]) {
    await runStep(step);
  }
}

async function main() {
  const { gate, tag } = parseArgs(process.argv.slice(2));
  if (gate === "list" || gate === "--list" || gate === "-l") {
    printGateList();
    return;
  }

  const config = gates[gate];
  if (!config) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  if (gate === "staging") {
    await runStagingGate(tag);
    return;
  }

  const steps = config.steps.filter((step) => stepMatchesTag(step, tag));
  if (!steps.length) {
    console.error(`No steps matched gate "${gate}" with tag "${tag || "all"}".`);
    process.exitCode = 1;
    return;
  }

  for (const step of steps) {
    await runStep(step);
  }
}

main().catch((error) => {
  console.error(error.message || error);
  process.exitCode = 1;
});
