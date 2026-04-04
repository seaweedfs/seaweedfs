# Phase 16 Rev 3 Manager Re-review

Date: 2026-04-04
Status: ready for re-review

## Purpose

This note is only for the delta since the prior `manager` review of widened
`16B Rev 3`.

Please review only whether the two requested fixes are now satisfied:

1. positive live-path rebuild ownership proof now exists
2. `Phase 16` wording is tightened from `first bounded` to `current widened bounded`

## Delta Since Prior Review

### 1. Positive live-path rebuild ownership proof added

Previous gap:

1. positive rebuild proof seeded pending execution directly
2. that proved command consumption, but not the full live `runRebuild()` chain

Current proof:

1. `weed/server/block_recovery_test.go`
2. `TestP16B_RunRebuild_UsesCoreStartRebuildCommandOnLivePath`
3. proved chain:
   - `runRebuild()`
   - cache pending rebuild
   - emit `RebuildStarted`
   - core emits `StartRebuildCommand`
   - adapter consumes pending rebuild
   - rebuild completion observation returns into core

Observed outcomes asserted by the test:

1. executed command list ends with `start_rebuild`
2. cached projection returns to `RecoveryIdle`
3. sender returns to `StateInSync`

This closes the exact positive-path gap identified in the previous review.

### 2. Wording hygiene tightened

Updated file:

1. `sw-block/.private/phase/phase-16.md`

Updated wording:

1. from: `the first bounded integrated runtime checkpoint after Phase 15 closeout`
2. to: `the current widened bounded runtime checkpoint after Phase 15 closeout`

This keeps the wording aligned with the real review object.

## Validation

1. `go test ./weed/server -run "TestP(4_LivePath_RealVol_ReachesPlan|16B_(Run(CatchUp|Rebuild)_|StartRebuildCommand_))"`
2. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
3. result: `PASS`

## Bounded Claim Unchanged

This re-review still asks you to review only:

1. bounded recovery execution ownership on catch-up and rebuild
2. not full recovery-loop closure
3. not broad end-to-end failover/recovery/publication closure
4. not multi-replica rebuild ownership
5. not launch / rollout readiness

## Requested Output

Please reply with one of:

1. `ACCEPT`
2. `ACCEPT WITH MINOR FIXES`
3. `REJECT`

If not `ACCEPT`, please keep findings bounded to this delta only.
