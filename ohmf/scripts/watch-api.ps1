param(
  [int]$PollMilliseconds = 1000,
  [switch]$RunTests
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Find-Docker {
  $docker = Get-Command docker -ErrorAction SilentlyContinue
  if ($docker) {
    return $docker.Source
  }

  $dockerDesktop = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
  if (Test-Path $dockerDesktop) {
    return $dockerDesktop
  }

  throw "docker not found. Ensure Docker Desktop is installed and running."
}

function Get-GoCommand {
  param(
    [Parameter(Mandatory = $true)]
    [string]$OHMFRoot
  )

  $localGo = Join-Path $OHMFRoot ".tools\go\bin\go.exe"
  if (Test-Path $localGo) {
    return $localGo
  }

  throw "Local Go toolchain not found at $localGo"
}

function Get-GoSourceStamp {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  $files = Get-ChildItem -Path $Path -Recurse -Filter *.go -File |
    Where-Object { $_.FullName -notmatch "\\vendor\\|\\_tools\\" } |
    Sort-Object FullName

  return ($files | ForEach-Object {
      "{0}|{1}|{2}" -f $_.FullName, $_.Length, $_.LastWriteTimeUtc.Ticks
    }) -join "`n"
}

function Refresh-Api {
  param(
    [Parameter(Mandatory = $true)]
    [string]$DockerPath,
    [Parameter(Mandatory = $true)]
    [string]$ComposeFile,
    [Parameter(Mandatory = $true)]
    [string]$GatewayDir,
    [Parameter(Mandatory = $true)]
    [string]$GoCmd,
    [Parameter(Mandatory = $true)]
    [string]$Reason,
    [switch]$RunTests
  )

  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Write-Host "[$timestamp] Refreshing API ($Reason)..."

  if ($RunTests) {
    Push-Location $GatewayDir
    try {
      & $GoCmd test ./...
      if ($LASTEXITCODE -ne 0) {
        throw "Gateway tests failed."
      }
    } finally {
      Pop-Location
    }
  }

  & $DockerPath compose -f $ComposeFile up -d --build api
  if ($LASTEXITCODE -ne 0) {
    throw "docker compose failed while refreshing api"
  }

  Write-Host "[$timestamp] API refresh complete."
}

$ohmfRoot = Split-Path -Parent $PSScriptRoot
$gatewayDir = Join-Path $ohmfRoot "services\gateway"
$composeFile = Join-Path $ohmfRoot "infra\docker\docker-compose.yml"
$docker = Find-Docker
$goCmd = Get-GoCommand -OHMFRoot $ohmfRoot

if (-not (Test-Path $gatewayDir)) {
  throw "Gateway directory not found: $gatewayDir"
}

if (-not (Test-Path $composeFile)) {
  throw "Compose file not found: $composeFile"
}

$previousStamp = Get-GoSourceStamp -Path $gatewayDir

Write-Host "Watching $gatewayDir for .go changes."
Write-Host "API refresh command: docker compose -f $composeFile up -d --build api"
if ($RunTests) {
  Write-Host "Gateway tests will run before each refresh using $goCmd"
} else {
  Write-Host "Gateway tests are skipped. Pass -RunTests to gate refreshes on go test."
}
Write-Host "Press Ctrl+C to stop watching."

while ($true) {
  Start-Sleep -Milliseconds $PollMilliseconds
  $nextStamp = Get-GoSourceStamp -Path $gatewayDir
  if ($nextStamp -eq $previousStamp) {
    continue
  }

  $previousStamp = $nextStamp
  try {
    Refresh-Api -DockerPath $docker -ComposeFile $composeFile -GatewayDir $gatewayDir -GoCmd $goCmd -Reason "gateway .go change detected" -RunTests:$RunTests
  } catch {
    Write-Warning $_
  }
}
