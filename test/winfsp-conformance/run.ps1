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

# "S:" and "S:\" mean different things to Join-Path: without the separator the
# result is relative to the drive's current directory, not its root.
if ($MountPoint -notmatch '[\\/]$') { $MountPoint = $MountPoint + '\' }

# winfsp-tests links against winfsp-x64.dll, which the MSI puts somewhere the
# loader does not look by default.
foreach ($candidate in @("${env:ProgramFiles(x86)}\WinFsp\bin", "$env:ProgramFiles\WinFsp\bin")) {
    if (Test-Path $candidate) {
        $env:PATH = "$candidate;$env:PATH"
        Write-Host "using WinFsp binaries from $candidate"
    }
}

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

# A missing or unreadable list would otherwise run with nothing excluded and
# report that as normal, which reads like a pass with no known failures.
if (-not (Test-Path $KnownFailures)) {
    throw "known failures list not found at $KnownFailures"
}
$excluded = @(Get-Content $KnownFailures |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') })
Write-Host "read $($excluded.Count) exclusions from $KnownFailures"
if ($excluded.Count -eq 0) {
    throw "known failures list at $KnownFailures parsed to nothing"
}

# The suite refuses to run anywhere but a drive, and works in the current
# directory, so it has to be driven from inside the mount.
$workDir = Join-Path $MountPoint 'winfsp-conformance'
New-Item -ItemType Directory -Force -Path $workDir | Out-Null
Push-Location $workDir
try {
    # --fuse-external: a third-party FUSE filesystem, not the bundled memfs.
    # --resilient:     tolerate operations this filesystem does not implement.
    #
    # One test per invocation, each in its own directory. Batched with
    # --no-abort, a test that failed part way left its files behind and the
    # next one failed creating them, reporting a cascade of failures that were
    # really one. This costs a process start per test and makes the list mean
    # what it says.
    $base = @('--fuse-external', '--resilient')
    $names = @(& $exe @base '--list' 2>&1 |
        ForEach-Object { if ($_ -match '^([a-z_0-9]+)\s*$') { $Matches[1] } })
    if ($names.Count -eq 0) { throw "could not list tests" }
    Write-Host "listed $($names.Count) tests"

    $failed = @()
    $ran = 0
    foreach ($name in $names) {
        if ($excluded | Where-Object { $name -like $_ }) { continue }
        $ran++
        $caseDir = Join-Path $workDir $name
        # -Force creates the directory but leaves anything already in it, and
        # a leftover file is the very thing this isolation exists to avoid.
        Remove-Item $caseDir -Recurse -Force -ErrorAction SilentlyContinue
        New-Item -ItemType Directory -Force -Path $caseDir | Out-Null
        Push-Location $caseDir
        try {
            $out = & $exe @base $name 2>&1
            $out | ForEach-Object { Write-Host $_ }
            if ($out -match '\s+KO\s*$' -or $LASTEXITCODE -ne 0) { $failed += $name }
        } finally {
            Pop-Location
            Remove-Item $caseDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host "ran $ran tests, $($failed.Count) failed"
    $code = if ($failed.Count -gt 0) { 1 } else { 0 }
} finally {
    Pop-Location
    Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
}

if ($failed.Count -gt 0) {
    Write-Host "::error::winfsp-tests failures outside known_failures.txt: $($failed -join ', ')"
    exit 1
}
if ($code -ne 0) {
    Write-Host "::error::winfsp-tests exited $code with no failure reported"
    exit $code
}
Write-Host 'winfsp-tests passed'
