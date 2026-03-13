# install-service.ps1 — Install htb-replicator as a Windows service using NSSM
# Run as Administrator in PowerShell.
#
# Usage:
#   .\install-service.ps1 [-Binary "C:\tools\replicator.exe"] [-Config "C:\htb-replicator\config.yaml"]
#
# Requires NSSM: https://nssm.cc/
# Install NSSM first: winget install NSSM.NSSM

param(
    [string]$Binary  = "C:\Program Files\htb-replicator\replicator.exe",
    [string]$Config  = "C:\ProgramData\htb-replicator\config.yaml",
    [string]$Service = "htb-replicator",
    [string]$NSSM    = "nssm"
)

$ErrorActionPreference = "Stop"

Write-Host "Installing $Service as a Windows service..." -ForegroundColor Cyan

# Verify NSSM is available
if (-not (Get-Command $NSSM -ErrorAction SilentlyContinue)) {
    Write-Error "NSSM not found. Install from https://nssm.cc/ or via: winget install NSSM.NSSM"
    exit 1
}

# Verify the binary exists
if (-not (Test-Path $Binary)) {
    Write-Error "Binary not found: $Binary"
    exit 1
}

# Create data directory
$DataDir = "C:\ProgramData\htb-replicator"
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

# Install the service
& $NSSM install    $Service $Binary
& $NSSM set        $Service AppParameters "start --config `"$Config`""
& $NSSM set        $Service DisplayName   "HTB-Replicator Replication"
& $NSSM set        $Service Description   "Continuously replicates S3 objects to configured destinations"
& $NSSM set        $Service Start         SERVICE_AUTO_START
& $NSSM set        $Service AppStdout     "$DataDir\stdout.log"
& $NSSM set        $Service AppStderr     "$DataDir\stderr.log"
& $NSSM set        $Service AppRotateFiles 1
& $NSSM set        $Service AppRotateBytes 10485760  # 10 MB

Write-Host ""
Write-Host "Service installed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Commands:" -ForegroundColor Yellow
Write-Host "  Start:   nssm start $Service"
Write-Host "  Stop:    nssm stop $Service"
Write-Host "  Status:  nssm status $Service"
Write-Host "  Logs:    Get-Content $DataDir\stdout.log -Wait"
Write-Host ""
Write-Host "Starting service..." -ForegroundColor Cyan
& $NSSM start $Service
