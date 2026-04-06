param(
    [ValidateSet("list", "stage0", "stage1", "stage2-readiness", "stage3-compare")]
    [string]$Stage = "list",

    [string]$RunnerPath = "sw-test-runner",

    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..\..")).Path,

    [string]$ResultsDir = "results/phase20-t6",

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Scenario {
    param(
        [string]$Id,
        [string]$Path,
        [string]$Purpose
    )

    return [pscustomobject]@{
        Id      = $Id
        Path    = $Path
        Purpose = $Purpose
    }
}

function Write-Pack {
    param(
        [string]$Title,
        [object[]]$Scenarios
    )

    Write-Host ""
    Write-Host $Title
    Write-Host ("-" * $Title.Length)
    foreach ($scenario in $Scenarios) {
        Write-Host ("{0}: {1}" -f $scenario.Id, $scenario.Path)
        Write-Host ("  purpose: {0}" -f $scenario.Purpose)
    }
}

function Invoke-Pack {
    param(
        [string]$Title,
        [object[]]$Scenarios
    )

    Write-Pack -Title $Title -Scenarios $Scenarios
    Write-Host ""
    Write-Host ("Runner: {0}" -f $RunnerPath)
    Write-Host ("RepoRoot: {0}" -f $RepoRoot)
    Write-Host ("ResultsDir: {0}" -f $ResultsDir)

    foreach ($scenario in $Scenarios) {
        $scenarioPath = Join-Path $RepoRoot $scenario.Path
        if (-not (Test-Path $scenarioPath)) {
            throw ("Scenario not found: {0}" -f $scenarioPath)
        }

        $runResultsDir = Join-Path $RepoRoot (Join-Path $ResultsDir $scenario.Id)
        $cmd = @(
            $RunnerPath,
            "run",
            $scenarioPath,
            "--results-dir",
            $runResultsDir
        )

        Write-Host ""
        Write-Host ("[{0}] {1}" -f $scenario.Id, $scenario.Purpose)
        Write-Host ("CMD: {0}" -f ($cmd -join " "))

        if (-not $DryRun) {
            & $RunnerPath run $scenarioPath --results-dir $runResultsDir
            if ($LASTEXITCODE -ne 0) {
                throw ("Scenario failed: {0}" -f $scenario.Id)
            }
        }
    }
}

$stage0Pack = @(
    (New-Scenario -Id "P20-H0" -Path "weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml" -Purpose "Stage 0 bootstrap closure: promoted primary learns replica membership and can reach publish_healthy on the healthy RF=2 sync_all path")
)

$stage1Pack = @(
    (New-Scenario -Id "P20-T6-H1A" -Path "weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml" -Purpose "V1 auto-failover baseline with data continuity"),
    (New-Scenario -Id "P20-T6-H1B" -Path "weed/storage/blockvol/testrunner/scenarios/internal/suite-ha-failover.yaml" -Purpose "HA failover with cluster health checks"),
    (New-Scenario -Id "P20-T6-H1C" -Path "weed/storage/blockvol/testrunner/scenarios/cp11b3-manual-promote.yaml" -Purpose "Manual promote plus preflight and rejoin recovery"),
    (New-Scenario -Id "P20-T6-H1D" -Path "weed/storage/blockvol/testrunner/scenarios/lease-expiry-write-gate.yaml" -Purpose "Lease/write gate regression while T6 work proceeds")
)

$stage2ReadinessPack = @(
    (New-Scenario -Id "P20-T6-H2" -Path "weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml" -Purpose "Future Stage 2 copy with --block.v2Promotion=true for durability-first failover"),
    (New-Scenario -Id "P20-T6-H3" -Path "weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml" -Purpose "Future ambiguous-evidence variant with fail-closed expectation"),
    (New-Scenario -Id "P20-T6-H4" -Path "weed/storage/blockvol/testrunner/scenarios/internal/v2-failover-gate.yaml" -Purpose "Future gated-promotion scenario; expected to be added during T6/T7")
)

switch ($Stage) {
    "list" {
        Write-Pack -Title "Stage 0 Pack (Bootstrap Closure First)" -Scenarios $stage0Pack
        Write-Pack -Title "Stage 1 Pack (Runnable Today)" -Scenarios $stage1Pack
        Write-Pack -Title "Stage 2 Readiness Pack (Do Not Run Until Ready)" -Scenarios $stage2ReadinessPack
        Write-Host ""
        Write-Host "Notes:"
        Write-Host "  - Stage 0 is the first hardware gate: close P20-H0 / P20-A2 / P20-A5 before stronger V2 failover claims."
        Write-Host "  - Current master flag spelling is --block.v2Promotion."
        Write-Host "  - Stage 1 uses existing YAMLs because V1 failover is the default path."
        Write-Host "  - Stage 2 requires dedicated YAML copies or overlays with start_weed_master.extra_args += -block.v2Promotion=true."
    }

    "stage0" {
        Write-Host "Stage 0 runs the bootstrap closure baseline before any stronger T6/T7 claims."
        Write-Host "It is the hardware entrypoint for P20-H0 plus acceptance overlay P20-A2/P20-A5."
        Write-Host ""
        Write-Host "Record during each run:"
        Write-Host "  1. /debug/block/shipper on both candidate volume servers"
        Write-Host "  2. /block/volume/<name> on master before failover, immediately after failover, and after recovery window"
        Write-Host "  3. whether ReplicaIDs, ShipperConfigured, and publish_healthy converge together"
        Write-Host ""
        Write-Host "Pass requires:"
        Write-Host "  1. promoted primary no longer shows ReplicaIDs=[] on the healthy path"
        Write-Host "  2. ShipperConfigured=true can be reached on the promoted primary"
        Write-Host "  3. publish_healthy=true can be reached without transport-contact shortcuts"
        Invoke-Pack -Title "Stage 0 Pack" -Scenarios $stage0Pack
    }

    "stage1" {
        Write-Host "Stage 1 runs existing YAMLs with V1 failover authority and V2 observation surfaces."
        Write-Host "Use --block.v2Promotion=false semantics; existing YAMLs can remain unchanged."
        Invoke-Pack -Title "Stage 1 Pack" -Scenarios $stage1Pack
    }

    "stage2-readiness" {
        Write-Pack -Title "Stage 2 Readiness Pack" -Scenarios $stage2ReadinessPack
        Write-Host ""
        Write-Host "Do not run Stage 2 until all of the following are true:"
        Write-Host "  1. ReplicaIDs is non-empty on the healthy promoted primary path."
        Write-Host "  2. ShipperConfigured=true and publish_healthy=true can be reached on hardware."
        Write-Host "  3. Evidence transport is real, not placeholder-only."
        Write-Host "  4. Scenario copies explicitly set --block.v2Promotion=true on the master."
        Write-Host ""
        Write-Host "This wrapper intentionally does not rewrite YAML files."
    }

    "stage3-compare" {
        Write-Host "Stage 3 compare pack:"
        Write-Host "  1. Run Stage 1 baseline scenarios under V1 failover."
        Write-Host "  2. Re-run equivalent Stage 2 copies under V2 failover."
        Write-Host "  3. Compare selected primary, epoch, data continuity, engine_projection_mode, cluster_replication_mode, and gate behavior."
        Write-Host ""
        Write-Host "Use:"
        Write-Host "  .\\weed\\storage\\blockvol\\testrunner\\scripts\\run-phase20-t6.ps1 -Stage stage0"
        Write-Host "  .\\weed\\storage\\blockvol\\testrunner\\scripts\\run-phase20-t6.ps1 -Stage stage1"
        Write-Host "  .\\weed\\storage\\blockvol\\testrunner\\scripts\\run-phase20-t6.ps1 -Stage stage2-readiness"
    }
}
