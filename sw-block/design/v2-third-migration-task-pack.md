# V2 Third Migration Task Pack

Date: 2026-04-04
Status: active

## Purpose

This note turns the third separation batch into validate-able engineering
tasks.

The key remaining concentration is no longer in `v2bridge`, but in
`weed/server/block_recovery.go`, where host wiring and reusable recovery
coordination still live together.

## Shared Rules

All tasks in this pack inherit these rules:

1. `weed/server` should remain the runtime host shell
2. reusable coordination should move toward `sw-block`
3. legacy no-core support may stay, but only as compatibility-only logic
4. no task in this pack may redefine recovery policy or core semantics

## Task H: Pending Execution Coordinator Extraction

### Goal

Extract pending-execution caching and fail-closed command matching from
`weed/server/block_recovery.go` into reusable runtime helpers.

### Source

1. `weed/server/block_recovery.go`
2. `pendingRecoveryExecution`
3. `storePendingExecution`
4. `takePendingExecution`
5. `peekPendingExecution`
6. `hasPendingExecution`
7. `cancelPendingExecution`
8. `ExecutePendingCatchUp`
9. `ExecutePendingRebuild`

### Destination

1. reusable coordinator helpers in `sw-block/engine/replication/runtime`
2. thin host-side wiring in `weed/server/block_recovery.go`

### Authority Rule

Pending execution ownership is runtime coordination logic. It belongs closer to
the engine/runtime boundary than to the product adapter shell.

The host shell may store concrete handles, but it should not own the reusable
matching/cancelation semantics.

### Adapter Boundary

`weed/` may:

1. supply concrete volume IDs, replica IDs, and IO bindings
2. trigger coordinator actions from host callbacks

`weed/` must not:

1. keep the only implementation of fail-closed pending command matching
2. duplicate target-mismatch cancellation logic in multiple host sites

### Acceptance

1. pending execution matching/cancelation logic is reusable outside
   `weed/server`
2. `weed/server/block_recovery.go` shrinks to host-side calls into that helper
3. fail-closed mismatch behavior remains explicit and covered

### Validation

1. `go test ./sw-block/engine/replication/...`
2. `go test ./weed/server -run "TestP16B_|TestP4_"`

### Current proof anchors

1. `TestP16B_RunCatchUp_EscalatesNeedsRebuildIntoCoreProjection`
2. `TestP16B_RunRebuild_FailClosedWithoutFreshStartRebuildCommand`
3. `TestP4_LivePath_RealVol_ReachesPlan`

## Task I: Recovery Execution Helper Extraction

### Goal

Extract reusable catch-up/rebuild plan execution helpers so `weed/server`
supplies only:

1. concrete IO bindings
2. host callbacks
3. logging/context shell

### Source

1. `weed/server/block_recovery.go`
2. `runCatchUp`
3. `runRebuild`
4. `executeCatchUpPlan`
5. `executeRebuildPlan`

### Destination

1. reusable execution helpers in `sw-block/engine/replication/runtime`
2. thin host-side volume/session access in `weed/server`

### Authority Rule

The engine still decides plan outcome. This task does not move policy.

What moves is the reusable execution-path coordination that applies an existing
plan using supplied IO and emits the corresponding completion callbacks.

### Adapter Boundary

`weed/` may:

1. fetch real `BlockVol` and build concrete `Reader` / `Pinner` / `Executor`
2. look up sender/session state
3. host goroutines and cancellation contexts

`weed/` must not:

1. remain the sole owner of reusable catch-up/rebuild execution wiring
2. mix host concerns and execution-helper concerns in one large function

### Acceptance

1. reusable execution helper logic no longer requires `weed/server` ownership
2. `runCatchUp` and `runRebuild` become noticeably smaller host-shell methods
3. catch-up and rebuild still preserve the current bounded command-driven path

### Validation

1. `go test ./sw-block/engine/replication/...`
2. `go test ./weed/server -run "TestP16B_|TestP4_"`
3. `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart)"`

### Current proof anchors

1. `TestP16B_RunCatchUp_UpdatesCoreProjectionFromLiveRecovery`
2. `TestP16B_RunRebuild_UsesCoreStartRebuildCommandOnLivePath`
3. `TestP4_SerializedReplacement_DrainsBeforeStart`
4. `TestP4_ShutdownDrain`

## Task J: Legacy No-Core Isolation

### Goal

Make no-core startup behavior explicitly legacy-scoped so the core-present path
and the compatibility path are structurally separate.

### Source

1. `weed/server/block_recovery.go`
2. `HandleAssignmentResult`
3. no-core branches inside `runCatchUp` and `runRebuild`
4. `sw-block/design/v2-legacy-runtime-exit-criteria.md`

### Destination

1. explicit legacy-only entry points or helper section in `weed/server`
2. updated design note if the isolation shape needs to be recorded

### Authority Rule

Legacy compatibility may remain, but it must stop looking like part of the
mainline runtime owner path.

### Adapter Boundary

`weed/` may:

1. keep no-core compatibility while the product still needs it
2. retain `legacy P4` coverage as compatibility guard

`weed/` must not:

1. hide compatibility startup inside the same mainline path used for
   core-present ownership
2. let no-core behavior continue to blur the supported owner model

### Acceptance

1. no-core startup paths are clearly labeled and structurally separated
2. core-present runtime ownership remains the obvious default path
3. legacy proofs remain compatibility-only and are not strengthened into
   semantic-authority claims

### Validation

1. `go test ./weed/server -run "TestP4_"`
2. `go test ./weed/server -run "TestP16B_|TestBlockService_ApplyAssignments_"`

### Current proof anchors

1. `TestP4_LivePath_RealVol_ReachesPlan`
2. `TestP4_SerializedReplacement_DrainsBeforeStart`
3. `TestP4_ShutdownDrain`
4. `TestBlockService_ApplyAssignments_PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp`
5. `TestBlockService_ApplyAssignments_RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart`

## Recommended Execution Order

Recommended order:

1. Task H
2. Task I
3. Task J

Reason:

1. the pending coordinator is the narrowest reusable slice
2. execution helper extraction should build on that coordinator boundary
3. legacy isolation should happen after the mainline path is already cleaner
