# demo-ha-windows.ps1 — Demonstrate HA replication + failover on Windows
# Requirements: iscsi-target.exe built, curl available, Windows iSCSI Initiator service running
#
# Usage:
#   .\demo-ha-windows.ps1 [-BinaryPath .\iscsi-target.exe] [-DataDir C:\temp\ha-demo]
#
# What it does:
#   1. Creates primary + replica volumes
#   2. Assigns roles via admin HTTP
#   3. Sets up WAL shipping (primary -> replica)
#   4. Connects Windows iSCSI Initiator to primary
#   5. Writes test data
#   6. Kills primary, promotes replica
#   7. Reconnects iSCSI to replica
#   8. Verifies data survived failover

param(
    [string]$BinaryPath = ".\iscsi-target.exe",
    [string]$DataDir = "C:\temp\ha-demo",
    [string]$VolumeSize = "1G"
)

$ErrorActionPreference = "Stop"

# --- Config ---
$PrimaryPort    = 3260
$ReplicaPort    = 3261
$PrimaryAdmin   = "127.0.0.1:8080"
$ReplicaAdmin   = "127.0.0.1:8081"
$PrimaryIQN     = "iqn.2024.com.seaweedfs:ha-primary"
$ReplicaIQN     = "iqn.2024.com.seaweedfs:ha-replica"
$PrimaryVol     = "$DataDir\primary.blk"
$ReplicaVol     = "$DataDir\replica.blk"
$ReplicaDataPort = 9011
$ReplicaCtrlPort = 9012
$TestFile       = $null  # set after drive letter is known

# --- Helpers ---
function Write-Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Write-OK($msg)   { Write-Host "  OK: $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "  WARN: $msg" -ForegroundColor Yellow }
function Write-Fail($msg) { Write-Host "  FAIL: $msg" -ForegroundColor Red }

function Invoke-Admin($addr, $path, $method = "GET", $body = $null) {
    $uri = "http://$addr$path"
    $params = @{ Uri = $uri; Method = $method; ContentType = "application/json" }
    if ($body) { $params.Body = $body }
    try {
        $resp = Invoke-RestMethod @params
        return $resp
    } catch {
        Write-Fail "HTTP $method $uri failed: $_"
        return $null
    }
}

function Wait-ForAdmin($addr, $label, $timeoutSec = 10) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-RestMethod -Uri "http://$addr/status" -TimeoutSec 2
            Write-OK "$label admin is up (epoch=$($r.epoch), role=$($r.role))"
            return $true
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    Write-Fail "$label admin not responding after ${timeoutSec}s"
    return $false
}

function Find-ISCSIDrive($iqn) {
    # Find the disk connected via iSCSI with the given target
    $session = Get-IscsiSession | Where-Object { $_.TargetNodeAddress -eq $iqn } | Select-Object -First 1
    if (-not $session) { return $null }
    $disk = Get-Disk | Where-Object { $_.BusType -eq "iSCSI" -and $_.FriendlyName -match "BlockVol" } |
            Sort-Object Number | Select-Object -Last 1
    if (-not $disk) { return $null }
    $part = Get-Partition -DiskNumber $disk.Number -ErrorAction SilentlyContinue |
            Where-Object { $_.DriveLetter } | Select-Object -First 1
    if ($part) { return "$($part.DriveLetter):" }
    return $null
}

# --- Cleanup from previous run ---
Write-Step "Cleanup"
# Disconnect any leftover iSCSI sessions
foreach ($iqn in @($PrimaryIQN, $ReplicaIQN)) {
    $sessions = Get-IscsiSession -ErrorAction SilentlyContinue | Where-Object { $_.TargetNodeAddress -eq $iqn }
    foreach ($s in $sessions) {
        Write-Host "  Disconnecting leftover session: $iqn"
        Disconnect-IscsiTarget -SessionIdentifier $s.SessionIdentifier -Confirm:$false -ErrorAction SilentlyContinue
    }
}
# Remove target portals
foreach ($port in @($PrimaryPort, $ReplicaPort)) {
    Remove-IscsiTargetPortal -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $port -Confirm:$false -ErrorAction SilentlyContinue
}
# Kill leftover processes
Get-Process -Name "iscsi-target" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# Create data directory
if (Test-Path $DataDir) { Remove-Item $DataDir -Recurse -Force }
New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
Write-OK "Data dir: $DataDir"

# --- Step 1: Start Primary ---
Write-Step "1. Starting Primary"
$primaryProc = Start-Process -FilePath $BinaryPath -PassThru -NoNewWindow -ArgumentList @(
    "-create", "-size", $VolumeSize,
    "-vol", $PrimaryVol,
    "-addr", ":$PrimaryPort",
    "-iqn", $PrimaryIQN,
    "-admin", $PrimaryAdmin
)
Write-Host "  PID: $($primaryProc.Id)"
if (-not (Wait-ForAdmin $PrimaryAdmin "Primary")) { exit 1 }

# --- Step 2: Start Replica ---
Write-Step "2. Starting Replica"
$replicaProc = Start-Process -FilePath $BinaryPath -PassThru -NoNewWindow -ArgumentList @(
    "-create", "-size", $VolumeSize,
    "-vol", $ReplicaVol,
    "-addr", ":$ReplicaPort",
    "-iqn", $ReplicaIQN,
    "-admin", $ReplicaAdmin,
    "-replica-data", ":$ReplicaDataPort",
    "-replica-ctrl", ":$ReplicaCtrlPort"
)
Write-Host "  PID: $($replicaProc.Id)"
if (-not (Wait-ForAdmin $ReplicaAdmin "Replica")) { exit 1 }

# --- Step 3: Assign Roles ---
Write-Step "3. Assigning Roles (epoch=1)"
$r = Invoke-Admin $PrimaryAdmin "/assign" "POST" '{"epoch":1,"role":1,"lease_ttl_ms":300000}'
if ($r.ok) { Write-OK "Primary assigned: role=PRIMARY epoch=1" } else { Write-Fail "Primary assign failed"; exit 1 }

$r = Invoke-Admin $ReplicaAdmin "/assign" "POST" '{"epoch":1,"role":2,"lease_ttl_ms":300000}'
if ($r.ok) { Write-OK "Replica assigned: role=REPLICA epoch=1" } else { Write-Fail "Replica assign failed"; exit 1 }

# --- Step 4: Set up WAL Shipping ---
Write-Step "4. Setting Up WAL Shipping (primary -> replica)"
$body = @{ data_addr = "127.0.0.1:$ReplicaDataPort"; ctrl_addr = "127.0.0.1:$ReplicaCtrlPort" } | ConvertTo-Json
$r = Invoke-Admin $PrimaryAdmin "/replica" "POST" $body
if ($r.ok) { Write-OK "WAL shipping configured" } else { Write-Fail "Replica config failed"; exit 1 }

# --- Step 5: Connect Windows iSCSI to Primary ---
Write-Step "5. Connecting Windows iSCSI Initiator to Primary"
New-IscsiTargetPortal -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $PrimaryPort -ErrorAction SilentlyContinue | Out-Null
Start-Sleep -Seconds 2

$target = Get-IscsiTarget -ErrorAction SilentlyContinue | Where-Object { $_.NodeAddress -eq $PrimaryIQN }
if (-not $target) {
    Write-Fail "Target $PrimaryIQN not discovered. Check that iscsi-target is running."
    exit 1
}
Write-OK "Target discovered: $PrimaryIQN"

Connect-IscsiTarget -NodeAddress $PrimaryIQN -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $PrimaryPort -ErrorAction Stop | Out-Null
Start-Sleep -Seconds 3
Write-OK "iSCSI connected to primary"

# --- Step 6: Initialize Disk ---
Write-Step "6. Initializing Disk"
$disk = Get-Disk | Where-Object { $_.BusType -eq "iSCSI" -and $_.OperationalStatus -eq "Online" -and $_.FriendlyName -match "BlockVol" } |
        Sort-Object Number | Select-Object -Last 1

if (-not $disk) {
    # Try offline disks
    $disk = Get-Disk | Where-Object { $_.BusType -eq "iSCSI" -and $_.FriendlyName -match "BlockVol" } |
            Sort-Object Number | Select-Object -Last 1
    if ($disk -and $disk.OperationalStatus -ne "Online") {
        Set-Disk -Number $disk.Number -IsOffline $false
        Start-Sleep -Seconds 1
    }
}

if (-not $disk) {
    Write-Warn "No iSCSI disk found. You may need to initialize manually in Disk Management."
} else {
    Write-OK "Found disk $($disk.Number): $($disk.FriendlyName)"
    if ($disk.PartitionStyle -eq "RAW") {
        Initialize-Disk -Number $disk.Number -PartitionStyle GPT -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
        Write-OK "Initialized as GPT"
    }
    # Create partition and format
    $part = New-Partition -DiskNumber $disk.Number -UseMaximumSize -AssignDriveLetter -ErrorAction SilentlyContinue
    if ($part) {
        Start-Sleep -Seconds 2
        Format-Volume -DriveLetter $part.DriveLetter -FileSystem NTFS -NewFileSystemLabel "HA-Demo" -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
        Start-Sleep -Seconds 1
        Write-OK "Formatted NTFS on $($part.DriveLetter):"
        $driveLetter = "$($part.DriveLetter):"
    }
}

if (-not $driveLetter) {
    $driveLetter = Find-ISCSIDrive $PrimaryIQN
}

if (-not $driveLetter) {
    Write-Warn "Could not determine drive letter. Please enter it manually."
    $driveLetter = Read-Host "Drive letter (e.g. F:)"
}

$TestFile = "$driveLetter\ha-test-data.txt"
Write-OK "Test drive: $driveLetter"

# --- Step 7: Write Test Data ---
Write-Step "7. Writing Test Data to Primary"
$testContent = "Hello from SeaweedFS HA demo! Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Set-Content -Path $TestFile -Value $testContent -Force
Write-OK "Wrote: $testContent"

# Verify
$readBack = Get-Content -Path $TestFile
if ($readBack -eq $testContent) {
    Write-OK "Verified: data reads back correctly"
} else {
    Write-Fail "Read-back mismatch!"
}

# Check replication status
$primaryStatus = Invoke-Admin $PrimaryAdmin "/status"
$replicaStatus = Invoke-Admin $ReplicaAdmin "/status"
Write-Host "  Primary: epoch=$($primaryStatus.epoch) role=$($primaryStatus.role) wal_head=$($primaryStatus.wal_head_lsn)"
Write-Host "  Replica: epoch=$($replicaStatus.epoch) role=$($replicaStatus.role) wal_head=$($replicaStatus.wal_head_lsn)"

# --- Step 8: Simulate Primary Failure ---
Write-Step "8. SIMULATING PRIMARY FAILURE (killing primary)"
Write-Host "  Press Enter to kill primary..." -ForegroundColor Yellow
Read-Host | Out-Null

# Flush filesystem
Write-Host "  Flushing filesystem..."
[System.IO.File]::Open($TestFile, "Open", "Read", "Read").Close()  # force close handles
Start-Sleep -Seconds 1

# Disconnect iSCSI (before killing, so Windows doesn't hang)
Disconnect-IscsiTarget -NodeAddress $PrimaryIQN -Confirm:$false -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# Kill primary
Stop-Process -Id $primaryProc.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Write-OK "Primary killed (PID $($primaryProc.Id))"

# --- Step 9: Promote Replica ---
Write-Step "9. Promoting Replica to Primary (epoch=2)"
$r = Invoke-Admin $ReplicaAdmin "/assign" "POST" '{"epoch":2,"role":1,"lease_ttl_ms":300000}'
if ($r.ok) { Write-OK "Replica promoted to PRIMARY (epoch=2)" } else { Write-Fail "Promotion failed"; exit 1 }

$newStatus = Invoke-Admin $ReplicaAdmin "/status"
Write-Host "  New primary: epoch=$($newStatus.epoch) role=$($newStatus.role) wal_head=$($newStatus.wal_head_lsn)"

# --- Step 10: Reconnect iSCSI to New Primary ---
Write-Step "10. Reconnecting iSCSI to New Primary (port $ReplicaPort)"
# Remove old portal, add new
Remove-IscsiTargetPortal -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $PrimaryPort -Confirm:$false -ErrorAction SilentlyContinue
New-IscsiTargetPortal -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $ReplicaPort -ErrorAction SilentlyContinue | Out-Null
Start-Sleep -Seconds 2

$target = Get-IscsiTarget -ErrorAction SilentlyContinue | Where-Object { $_.NodeAddress -eq $ReplicaIQN }
if (-not $target) {
    Write-Fail "New primary target not discovered"
    exit 1
}

Connect-IscsiTarget -NodeAddress $ReplicaIQN -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $ReplicaPort -ErrorAction Stop | Out-Null
Start-Sleep -Seconds 3
Write-OK "Connected to new primary"

# Wait for disk to appear
Start-Sleep -Seconds 3
$newDrive = Find-ISCSIDrive $ReplicaIQN
if (-not $newDrive) {
    Write-Warn "Disk not auto-mounted. You may need to bring it online manually."
    Write-Host "  Try: Get-Disk | Where BusType -eq iSCSI | Set-Disk -IsOffline `$false"
    $newDrive = Read-Host "  Drive letter of the reconnected disk (e.g. G:)"
}

# --- Step 11: Verify Data Survived ---
Write-Step "11. Verifying Data on New Primary"
$newTestFile = "$newDrive\ha-test-data.txt"
if (Test-Path $newTestFile) {
    $recovered = Get-Content -Path $newTestFile
    Write-Host "  Read: $recovered"
    if ($recovered -eq $testContent) {
        Write-Host ""
        Write-Host "  ============================================" -ForegroundColor Green
        Write-Host "  === FAILOVER SUCCESS - DATA PRESERVED! ===" -ForegroundColor Green
        Write-Host "  ============================================" -ForegroundColor Green
        Write-Host ""
    } else {
        Write-Fail "Data mismatch after failover!"
        Write-Host "  Expected: $testContent"
        Write-Host "  Got:      $recovered"
    }
} else {
    Write-Warn "Test file not found at $newTestFile"
    Write-Host "  The disk may have a different drive letter. Check Disk Management."
}

# --- Cleanup ---
Write-Step "12. Cleanup"
Write-Host "  Press Enter to cleanup (disconnect iSCSI, stop replica, delete volumes)..." -ForegroundColor Yellow
Read-Host | Out-Null

Disconnect-IscsiTarget -NodeAddress $ReplicaIQN -Confirm:$false -ErrorAction SilentlyContinue
Remove-IscsiTargetPortal -TargetPortalAddress "127.0.0.1" -TargetPortalPortNumber $ReplicaPort -Confirm:$false -ErrorAction SilentlyContinue
Stop-Process -Id $replicaProc.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1
Remove-Item $DataDir -Recurse -Force -ErrorAction SilentlyContinue
Write-OK "Cleaned up. Demo complete."
