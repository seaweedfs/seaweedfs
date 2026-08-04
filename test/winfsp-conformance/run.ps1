# Runs WinFsp's own conformance suite against a mounted SeaweedFS drive.
#
# winfsp-tests is what WinFsp uses to check a filesystem behaves like NTFS,
# and --fuse-external points it at somebody else's filesystem instead of the
# bundled memfs. It is the Windows counterpart of the pjdfstest run the FUSE
# mount already goes through, and it is scored the same way: anything failing
# that is not in known_failures.txt is a regression.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$MountPoint,
    [string]$KnownFailures = "$PSScriptRoot\known_failures.txt",
    [string]$WinFspTestsVersion = '2.1.25156'
)

$ErrorActionPreference = 'Stop'

$toolDir = Join-Path $env:TEMP 'winfsp-tests'
$exe = Join-Path $toolDir 'winfsp-tests-x64.exe'
if (-not (Test-Path $exe)) {
    # Shipped as its own archive rather than in the MSI.
    $url = "https://github.com/winfsp/winfsp/releases/download/v2.1/winfsp-tests-$WinFspTestsVersion.zip"
    $zip = Join-Path $env:TEMP 'winfsp-tests.zip'
    Write-Host "downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
    Expand-Archive -LiteralPath $zip -DestinationPath $toolDir -Force
}
if (-not (Test-Path $exe)) {
    throw "winfsp-tests-x64.exe not found under $toolDir"
}

$excluded = @()
if (Test-Path $KnownFailures) {
    $excluded = Get-Content $KnownFailures |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -and -not $_.StartsWith('#') }
}

# The suite refuses to run anywhere but a drive, and works in the current
# directory, so it has to be driven from inside the mount.
$workDir = Join-Path $MountPoint 'winfsp-conformance'
New-Item -ItemType Directory -Force -Path $workDir | Out-Null
Push-Location $workDir
try {
    # --fuse-external: a third-party FUSE filesystem, not the bundled memfs.
    # --resilient:     tolerate operations this filesystem does not implement.
    # --no-abort:      report every failure instead of stopping at the first.
    $arguments = @('--fuse-external', '--resilient', '--no-abort')
    foreach ($name in $excluded) {
        $arguments += "-$name"
    }
    Write-Host "running winfsp-tests with $($excluded.Count) excluded entries"
    & $exe @arguments
    $code = $LASTEXITCODE
} finally {
    Pop-Location
    Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
}

if ($code -ne 0) {
    Write-Host "::error::winfsp-tests reported failures outside known_failures.txt"
    exit $code
}
Write-Host 'winfsp-tests passed'
