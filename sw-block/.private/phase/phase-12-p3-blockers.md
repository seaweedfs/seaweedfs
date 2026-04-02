# Phase 12 P3 — Blocker Ledger

Date: 2026-04-02
Scope: bounded diagnosability / blocker accounting for the accepted RF=2 sync_all chosen path

## Diagnosed and Bounded

| ID | Symptom | Evidence Surface | Owning Truth | Status |
|----|---------|-----------------|--------------|--------|
| B1 | Failover does not converge | failover logs + registry Lookup epoch/primary | registry authority | Diagnosed: convergence depends on lease expiry + heartbeat cycle; bounded by lease TTL |
| B2 | Lookup publication stale after failover | LookupBlockVolume response vs registry entry | registry ISCSIAddr/VolumeServer | Diagnosed: publication updates on failover assignment delivery; bounded by assignment queue delivery |
| B3 | Recovery tasks remain after volume delete | RecoveryManager.DiagnosticSnapshot | RecoveryManager task map | Diagnosed: tasks drain on shutdown/cancel; bounded by RecoveryManager lifecycle |

## Unresolved but Explicit

| ID | Symptom | Current Evidence | Why Unresolved | Blocks P4/Rollout? |
|----|---------|-----------------|----------------|-------------------|
| U1 | V2 engine accepts stale-epoch assignments at orchestrator level | V2 idempotence check skips only same-epoch; lower epoch creates new sender | Engine ApplyAssignment does not check epoch monotonicity on Reconcile | No — V1 HandleAssignment rejects epoch regression; V2 is secondary |
| U2 | Single-process test cannot exercise Primary→Rebuilding role transition | HandleAssignment rejects transition in shared store | Test harness limitation, not production bug | No — production VS has separate stores |
| U3 | gRPC stream transport not exercised in control-loop tests | All logic above/below stream is real; stream itself bypassed | Would require live master+VS gRPC servers in test | Blocks full integration test, not correctness |

## Out of Scope for P3

- Performance floor characterization
- Rollout-gate criteria
- Hours/days soak
- RF>2 topology
- NVMe runtime transport proof
- CSI snapshot/expand
