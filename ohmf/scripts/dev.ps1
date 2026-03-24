param(
  [string]$Action = "up",
  [int]$PollMilliseconds = 1000,
  [switch]$RunTests
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root "infra\docker\docker-compose.yml"
$watchScript = Join-Path $PSScriptRoot "watch-api.ps1"

switch ($Action) {
  "up" {
    docker compose -f $composeFile up --build
  }
  "down" {
    docker compose -f $composeFile down -v
  }
  "logs" {
    docker compose -f $composeFile logs -f api
  }
  "watch-api" {
    & $watchScript -PollMilliseconds $PollMilliseconds -RunTests:$RunTests
  }
  default {
    Write-Error "Unsupported action: $Action"
  }
}
