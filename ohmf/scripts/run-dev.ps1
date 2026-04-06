param(
  [int]$CLIENT_PORT = 5173,
  [int]$CONTAINER_PORT = 8080,
  [int]$HOST_PORT = 18080
)

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

function Test-PortAvailable {
  param(
    [Parameter(Mandatory = $true)]
    [int]$Port
  )

  $listener = $null
  try {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
    $listener.Start()
    return $true
  } catch {
    return $false
  } finally {
    if ($listener) {
      $listener.Stop()
    }
  }
}

function Get-AvailablePort {
  param(
    [Parameter(Mandatory = $true)]
    [int]$StartPort,
    [Parameter(Mandatory = $true)]
    [AllowEmptyCollection()]
    [System.Collections.Generic.HashSet[int]]$ReservedPorts
  )

  $port = $StartPort
  while ($ReservedPorts.Contains($port) -or -not (Test-PortAvailable -Port $port)) {
    $port++
  }
  $ReservedPorts.Add($port) | Out-Null
  return $port
}

function Write-RuntimeConfig {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path,
    [Parameter(Mandatory = $true)]
    [int]$FrontendPort,
    [Parameter(Mandatory = $true)]
    [int]$ApiHostPort,
    [Parameter(Mandatory = $true)]
    [string]$AssetVersion
  )

  $content = @(
    "window.OHMF_RUNTIME_CONFIG = Object.freeze({"
    "  frontend_port: `"$FrontendPort`","
    "  api_host_port: `"$ApiHostPort`","
    "  api_base_url: `"http://localhost:$ApiHostPort`","
    "  developer_mode: true,"
    "  miniapp_sandbox_port: `"$FrontendPort`","
    "  miniapp_sandbox_url: `"http://localhost:$FrontendPort`","
    "  asset_version: `"$AssetVersion`","
    "});"
  )
  Set-Content -Path $Path -Value $content -Encoding ascii
}

function Remove-ExistingOHMFContainers {
  param(
    [Parameter(Mandatory = $true)]
    [string]$DockerPath
  )

  $containerNames = @(
    "ohmf-db",
    "ohmf-redis",
    "ohmf-cassandra",
    "ohmf-kafka",
    "ohmf-kafka-init",
    "ohmf-api",
    "ohmf-client",
    "ohmf-messages-processor",
    "ohmf-delivery-processor",
    "ohmf-sms-processor",
    "ohmf-prometheus",
    "ohmf-grafana"
  )

  foreach ($name in $containerNames) {
    $rawId = & $DockerPath ps -aq -f "name=^${name}$"
    $id = if ($null -eq $rawId) { "" } else { ($rawId | Out-String).Trim() }
    if ($id) {
      & $DockerPath rm -f $name | Out-Null
    }
  }
}

$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root "infra\docker\docker-compose.yml"
$clientComposeFile = Join-Path $root "infra\docker\docker-compose.client.yml"
$runtimeConfigFile = Join-Path $root "apps\web\runtime-config.js"
$docker = Find-Docker

$reservedPorts = [System.Collections.Generic.HashSet[int]]::new()
$selectedClientPort = Get-AvailablePort -StartPort $CLIENT_PORT -ReservedPorts $reservedPorts
$selectedContainerPort = Get-AvailablePort -StartPort $CONTAINER_PORT -ReservedPorts $reservedPorts
$selectedHostPort = Get-AvailablePort -StartPort $HOST_PORT -ReservedPorts $reservedPorts
$assetVersion = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds().ToString()

Write-Host "Stopping existing OHMF Docker containers..."
& $docker compose -f $composeFile -f $clientComposeFile down --remove-orphans | Out-Null
Remove-ExistingOHMFContainers -DockerPath $docker

Write-RuntimeConfig -Path $runtimeConfigFile -FrontendPort $selectedClientPort -ApiHostPort $selectedHostPort -AssetVersion $assetVersion

$env:CLIENT_PORT = [string]$selectedClientPort
$env:API_CONTAINER_PORT = [string]$selectedContainerPort
$env:API_HOST_PORT = [string]$selectedHostPort

Write-Host "Starting db, api, client, messages-processor, and delivery-processor containers..."
& $docker compose -f $composeFile -f $clientComposeFile up -d --build db api client messages-processor delivery-processor

Write-Host ""
Write-Host "Selected ports:"
Write-Host "CLIENT_PORT=$selectedClientPort"
Write-Host "CONTAINER_PORT=$selectedContainerPort"
Write-Host "HOST_PORT=$selectedHostPort"
Write-Host ""
Write-Host "Client: http://localhost:$selectedClientPort"
Write-Host "API:    http://localhost:$selectedHostPort"
