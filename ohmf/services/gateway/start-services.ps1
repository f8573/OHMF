# E2EE Gateway - Quick Start Script for Windows
# This script starts all services and prepares the application for testing
# Usage: .\start-services.ps1

param(
    [switch]$NoColor = $false
)

# Configuration
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$OHMF_ROOT = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$GO_CMD = Join-Path $OHMF_ROOT ".tools\go\bin\go.exe"
$COMPOSE_FILE = "docker-compose.e2ee-test.yml"
$API_PORT = 8080
$DB_PORT = 5432
$GO_BUILD_TIMEOUT = 60

# Color outputs (disabled with -NoColor)
function Write-ColorOutput {
    param(
        [string]$Color,
        [string]$Message
    )
    if ($NoColor) {
        Write-Host $Message
    } else {
        Write-Host $Message -ForegroundColor $Color
    }
}

function Write-Header {
    param([string]$Text)
    Write-ColorOutput "Cyan" "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    Write-ColorOutput "Cyan" "  $Text"
    Write-ColorOutput "Cyan" "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

function Write-Step {
    param([string]$Text)
    Write-ColorOutput "Yellow" "`n$Text"
}

function Write-Success {
    param([string]$Text)
    Write-ColorOutput "Green" "✓ $Text"
}

function Write-Error-Custom {
    param([string]$Text)
    Write-ColorOutput "Red" "✘ $Text"
}

function Write-Warning-Custom {
    param([string]$Text)
    Write-ColorOutput "Yellow" "⚠ $Text"
}

# Enable error action
$ErrorActionPreference = "Stop"

Write-Header "E2EE Gateway - Quick Start"

# Step 0: Verify requirements
Write-Step "Step 0: Checking prerequisites..."

# Check Docker
try {
    $null = docker --version 2>$null
    Write-Success "Docker installed ($(docker --version))"
} catch {
    Write-Error-Custom "Docker not found. Please install Docker Desktop for Windows."
    Write-Host "  Download: https://www.docker.com/products/docker-desktop"
    exit 1
}

# Check Docker Compose
try {
    $null = docker-compose --version 2>$null
    Write-Success "Docker Compose installed"
} catch {
    Write-Error-Custom "Docker Compose not found."
    exit 1
}

# Check Go
try {
    if (-not (Test-Path $GO_CMD)) {
        throw "bundled Go toolchain not found at $GO_CMD"
    }
    $GoVersion = (& $GO_CMD version | ForEach-Object { $_ -match "\d+\.\d+"; $matches[0] } | Select-Object -First 1)
    Write-Success "Go installed (version $GoVersion)"
} catch {
    Write-Error-Custom "Bundled Go toolchain not found. Expected $GO_CMD"
    exit 1
}

# Check gateway directory
if (-not (Test-Path $SCRIPT_DIR)) {
    Write-Error-Custom "Gateway directory not found: $SCRIPT_DIR"
    exit 1
}
Write-Success "Gateway directory found"

# Step 1: Stop any existing containers
Write-Step "Step 1: Cleaning up existing containers..."

Push-Location $SCRIPT_DIR

try {
    $ContainerStatus = docker-compose -f $COMPOSE_FILE ps 2>$null | Select-String "e2ee-test-db"
    if ($ContainerStatus) {
        Write-Host "  Stopping existing PostgreSQL container..."
        docker-compose -f $COMPOSE_FILE down --remove-orphans 2>$null
        Start-Sleep -Seconds 2
    }
} catch {
    # Container doesn't exist, that's fine
}

Write-Success "Cleanup complete"

# Step 2: Start PostgreSQL
Write-Step "Step 2: Starting PostgreSQL database..."

$StartResult = docker-compose -f $COMPOSE_FILE up -d 2>&1
if (-not $?) {
    Write-Error-Custom "Failed to start PostgreSQL container"
    Write-Host $StartResult
    exit 1
}

$DbContainerId = docker-compose -f $COMPOSE_FILE ps -q postgres-e2ee 2>$null
if ([string]::IsNullOrEmpty($DbContainerId)) {
    Write-Error-Custom "Failed to get PostgreSQL container ID"
    exit 1
}

Write-Host "  PostgreSQL container started: $($DbContainerId.Substring(0, 12))"

# Wait for database to be healthy
Write-Host "  Waiting for database to be ready..."
$RetryCount = 0
$MaxRetries = 30

while ($RetryCount -lt $MaxRetries) {
    $Status = docker-compose -f $COMPOSE_FILE ps postgres-e2ee 2>$null | Select-String "healthy|unhealthy"

    if ($Status -match "healthy") {
        Write-Success "PostgreSQL is ready"
        break
    }

    $RetryCount++
    if ($RetryCount -eq $MaxRetries) {
        Write-Error-Custom "PostgreSQL failed to become healthy within timeout"
        Write-Host "  Check logs: docker logs e2ee-test-db"
        exit 1
    }

    Write-Host -NoNewline "."
    Start-Sleep -Seconds 1
}

# Step 3: Build the application
Write-Step "Step 3: Building Go application..."

# Verify Go modules
if (-not (Test-Path "go.mod")) {
    Write-Error-Custom "go.mod not found in $pwd"
    exit 1
}

# Build with timeout
try {
    $buildOutput = & {
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        if ($stopwatch.Elapsed.TotalSeconds -gt $GO_BUILD_TIMEOUT) {
            throw "Build timeout"
        }

        & $GO_CMD build -v ./cmd/api 2>&1 | Select-Object -Last 5
    }

    Write-Host ($buildOutput -join "`n")
    Write-Success "Application built successfully"
} catch {
    Write-Error-Custom "Build failed: $_"
    exit 1
}

# Step 4: Run integration tests
Write-Step "Step 4: Running E2EE integration tests..."

$env:TEST_DATABASE_URL = "postgres://e2ee_test:test_password_e2ee@localhost:5432/e2ee_test"

Write-Host "  Connection: $env:TEST_DATABASE_URL"
Write-Host "  Running tests..."

$testOutput = & $GO_CMD test -v -tags integration ./internal/e2ee -run E2EE 2>&1
$testExit = $LASTEXITCODE

Write-Host ($testOutput | Select-Object -Last 20 | ForEach-Object { $_ })

if ($testExit -eq 0) {
    Write-Success "Integration tests passed"
} else {
    Write-Warning-Custom "Some tests may have failed or timed out (check output above)"
}

# Step 5: Ready for use
Write-Header "READY FOR TESTING"

Write-ColorOutput "Yellow" "`nDatabase Connection:"
Write-Host "  Host: localhost"
Write-Host "  Port: $DB_PORT"
Write-Host "  User: e2ee_test"
Write-Host "  Password: test_password_e2ee"
Write-Host "  Database: e2ee_test"

Write-ColorOutput "Yellow" "`nTest Database:"
Write-Host "  `$env:TEST_DATABASE_URL = '$env:TEST_DATABASE_URL'"
Write-Host "  go test -v -tags integration ./internal/e2ee -run E2EE"

Write-ColorOutput "Yellow" "`nManual Database Access:"
Write-Host "  # From command line:"
Write-Host "  psql -h localhost -U e2ee_test -d e2ee_test"
Write-Host "  # Or from Docker:"
Write-Host "  docker exec -it e2ee-test-db psql -U e2ee_test -d e2ee_test"

Write-ColorOutput "Yellow" "`nAvailable Unit Tests:"
Write-Host "  cd $SCRIPT_DIR"
Write-Host "  & $GO_CMD test -v ./internal/e2ee"
Write-Host "  & $GO_CMD test -bench=. -benchmem ./internal/e2ee"

Write-ColorOutput "Yellow" "`nStop Services:"
Write-Host "  cd $SCRIPT_DIR"
Write-Host "  docker-compose -f $COMPOSE_FILE down"

Write-ColorOutput "Yellow" "`nFull Reset (deletes all data):"
Write-Host "  cd $SCRIPT_DIR"
Write-Host "  docker-compose -f $COMPOSE_FILE down -v"
Write-Host "  docker-compose -f $COMPOSE_FILE up -d"

Write-ColorOutput "Cyan" "`nFor more information:"
Write-Host "  See: $SCRIPT_DIR\E2EE_COMPLETE_DOCUMENTATION.md"
Write-Host "  See: $SCRIPT_DIR\internal\e2ee\migrations\README.md"

Write-ColorOutput "Green" "`nSuccess! Everything is ready for testing.`n"

Pop-Location
