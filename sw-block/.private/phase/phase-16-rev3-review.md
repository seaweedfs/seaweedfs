# Phase 16 Rev 3 Review

Date: 2026-04-04
Status: ready for review

## Review Object

Review the current widened `Phase 16` working state as:

1. `Phase 15` delivered
2. `16A` delivered
3. `16B` bounded recovery execution ownership:
   - live recovery observations return into the core
   - bounded `start_catchup` execution is core-command-driven
   - bounded `start_rebuild` execution is core-command-driven

This is a new review object beyond the previously accepted catch-up-only
checkpoint.

## What Is In Scope

### `Phase 15` closeout

1. bounded surface/store/outward consume-chain rebinding to core-owned truth
2. cluster-level status surface extraction and closure proof preserved

### `16A` delivered

Bounded command-driven adapter ownership covers:

1. `apply_role`
2. `start_receiver`
3. `configure_shipper`
4. `invalidate_session`

Expected judgment:

1. these paths execute because the core emitted commands
2. the adapter remains executor, not semantic owner

### `16B` widened bounded closure

Bounded live recovery closure now covers:

1. live recovery observations return into the core on catch-up / rebuild
   entry/exit points
2. bounded `start_catchup` execution runs from `StartCatchUpCommand`
3. bounded `start_rebuild` execution runs from `StartRebuildCommand`
4. if no fresh rebuild command is emitted, pending rebuild does not run
   implicitly
5. old no-core compatibility remains preserved

Expected judgment:

1. this is still a bounded runtime-ownership step
2. catch-up and rebuild execution ownership are both now in scope
3. it is still not full recovery-loop closure

## What Is Explicitly Out Of Scope

Do NOT review this widened checkpoint as claiming:

1. full recovery-loop closure
2. broad end-to-end failover/recovery/publication closure
3. broad multi-replica rebuild ownership
4. launch / rollout readiness

## Primary Files

Phase tracking:

1. `sw-block/.private/phase/phase-15.md`
2. `sw-block/.private/phase/phase-15-log.md`
3. `sw-block/.private/phase/phase-16.md`
4. `sw-block/.private/phase/phase-16-log.md`

Integrated runtime code:

1. `weed/server/volume_server_block.go`
2. `weed/server/volume_server_block_test.go`
3. `weed/server/master_server_handlers_block.go`
4. `weed/server/master_block_observability_test.go`
5. `weed/server/block_recovery.go`
6. `weed/server/block_recovery_test.go`

## Evidence Summary

### Surface/store closure preserved

Focused proof suite:

1. `go test ./weed/server -run "Test(BlockService_(ApplyAssignments|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
2. result: `PASS`

### Recovery ownership closure

Focused recovery proof suite:

1. `go test ./weed/server -run "TestP(4_LivePath_RealVol_ReachesPlan|16B_(Run(CatchUp|Rebuild)_|StartRebuildCommand_))"`
2. result: `PASS`

Key new rebuild proofs:

1. `TestP16B_RunRebuild_UsesCoreStartRebuildCommandOnLivePath`
   - proves the live chain:
     `runRebuild()` -> cache pending rebuild -> emit `RebuildStarted` ->
     `StartRebuildCommand` -> adapter consumption -> rebuild completion
   - proves rebuild completion observation closes back into core projection
2. `TestP16B_RunRebuild_FailClosedWithoutFreshStartRebuildCommand`
   - proves pending rebuild does not execute implicitly without a fresh command

## Review Questions

### For `sw`

Please check implementation correctness and commit-readiness:

1. Is the widened `16B` boundary still coherent as one bounded checkpoint?
2. Is the rebuild ownership implementation internally consistent with the
   existing catch-up ownership pattern?
3. Are there any cleanup/refactor issues that should be fixed before commit,
   without broadening scope?

Suggested commit boundary if accepted:

1. `sw-block/.private/phase/phase-16.md`
2. `sw-block/.private/phase/phase-16-log.md`
3. `sw-block/.private/phase/phase-16-rev3-review.md`
4. `weed/server/block_recovery.go`
5. `weed/server/block_recovery_test.go`

### For `tester`

Please challenge the proof posture:

1. Does `16B Rev 3` now prove the positive live `start_rebuild` ownership chain,
   not just structural command plumbing?
2. Is the fail-closed proof strong enough to show pending rebuild does not run
   implicitly?
3. Are there any remaining surfaces where rebuild truth could still diverge
   from the core on the bounded path?
4. Are these new tests proving semantic claim rather than implementation shape?

### For `manager`

Please challenge boundaries and overclaim:

1. Does widening `16B` from catch-up-only to catch-up+rebuild still keep the
   slice bounded?
2. Is the wording still disciplined that this is not full recovery-loop closure?
3. Does the updated `Phase 16` wording clearly separate:
   - bounded recovery execution ownership
   - broader end-to-end scenario closure
4. Is this a reasonable next stage checkpoint?

## Requested Output Shape

Please reply with one of:

1. `ACCEPT`
2. `ACCEPT WITH MINOR FIXES`
3. `REJECT`

If not `ACCEPT`, list findings ordered by severity and keep them bounded to
this widened `16B` claim set.
