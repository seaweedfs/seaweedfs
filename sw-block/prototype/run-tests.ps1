param(
    [string[]]$Packages = @(
        './sw-block/prototype/fsmv2',
        './sw-block/prototype/volumefsm',
        './sw-block/prototype/distsim'
    )
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $root

$cacheDir = Join-Path $root '.gocache_v2'
$tmpDir = Join-Path $root '.gotmp_v2'
New-Item -ItemType Directory -Force -Path $cacheDir,$tmpDir | Out-Null
$env:GOCACHE = $cacheDir
$env:GOTMPDIR = $tmpDir

foreach ($pkg in $Packages) {
    $name = Split-Path $pkg -Leaf
    $out = Join-Path $root ("sw-block\\prototype\\{0}\\{0}.test.exe" -f $name)
    Write-Host "==> building $pkg"
    go test -c -o $out $pkg
    if (!(Test-Path $out)) {
        throw "go test -c build failed for $pkg"
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "go test -c reported a non-zero exit code for $pkg, but the test binary was produced. Continuing."
    }
    Write-Host "==> running $out"
    cmd /c "cd /d $root && $out -test.v -test.count=1"
    if ($LASTEXITCODE -ne 0) {
        throw "test binary failed for $pkg"
    }
}

Write-Host "Done."
