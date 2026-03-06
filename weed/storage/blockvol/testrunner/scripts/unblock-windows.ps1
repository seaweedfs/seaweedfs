# unblock-windows.ps1 — Unblock downloaded binaries and add Defender exclusion.
# Run as Administrator: powershell -ExecutionPolicy Bypass -File .\unblock-windows.ps1
#
# This is needed because Windows SmartScreen blocks unsigned binaries downloaded
# from the internet. Code signing is future work (requires certificate).

$ErrorActionPreference = "Stop"
$dir = (Get-Location).Path

Write-Host "Unblocking binaries in $dir ..."

$binaries = @("sw-test-runner.exe", "iscsi-target.exe", "weed.exe")
foreach ($bin in $binaries) {
    $path = Join-Path $dir $bin
    if (Test-Path $path) {
        Unblock-File $path
        Write-Host "  Unblocked: $bin"
    }
}

Write-Host "Adding Defender exclusion for $dir ..."
Add-MpPreference -ExclusionPath $dir
Write-Host "Done. Binaries should no longer trigger SmartScreen."
