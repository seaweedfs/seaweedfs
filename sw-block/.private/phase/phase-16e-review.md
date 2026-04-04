# Phase 16E Review

Date: 2026-04-04
Status: ready for review

## Review Object

Review the current bounded `Phase 16E` working state as:

1. `Phase 15` delivered
2. `16A` delivered
3. `16B` delivered
4. `16C` delivered
5. `16D` delivered
6. `16E` bounded catch-up recovery-task startup ownership on the
   single-replica primary path

## What Is In Scope

### Previously delivered closure

Please treat these as already accepted background:

1. `Phase 15` surface/store/outward consume-chain rebinding
2. `16A` command-driven adapter ownership
3. `16B` live recovery execution ownership
4. `16C` rebuilding-assignment entry ownership
5. `16D` rebuild recovery-task startup ownership

### `16E` current bounded refinement

Review only this new bounded step:

1. primary assignment with one replica now marks `RecoveryTarget=SessionCatchUp`
   in the core assignment event
2. the core emits `start_recovery_task` for that bounded catch-up startup path
3. the adapter starts the recovery goroutine from that command, not from
   orchestrator `SessionsCreated` / `SessionsSuperseded`
4. the bounded command sequence for this path is now:
   - `apply_role`
   - `configure_shipper`
   - `start_recovery_task`
   - `start_catchup`
5. assignment change resets the startup dedupe key, so endpoint/version change
   still emits a fresh task-start command
6. legacy `P4` remains preserved only as a compatibility guard

Expected judgment:

1. this is still a bounded runtime-ownership refinement
2. catch-up task startup is now core-command-driven on the bounded single-replica
   primary path
3. this is not yet full recovery-loop ownership

## What Is Explicitly Out Of Scope

Do NOT review `16E` as claiming:

1. multi-replica catch-up startup ownership
2. full recovery-loop closure
3. broad end-to-end failover/recovery/publication closure
4. launch / rollout readiness

## Primary Files

Phase tracking:

1. `sw-block/.private/phase/phase-16.md`
2. `sw-block/.private/phase/phase-16-log.md`

Core/runtime code:

1. `sw-block/engine/replication/command.go`
2. `sw-block/engine/replication/state.go`
3. `sw-block/engine/replication/engine.go`
4. `sw-block/engine/replication/phase14_command_test.go`
5. `weed/server/block_recovery.go`
6. `weed/server/volume_server_block.go`
7. `weed/server/volume_server_block_test.go`

## Evidence Summary

### Engine command proof

1. `go test ./sw-block/engine/replication/...`
2. result: `PASS`
3. key proof:
   - `TestPhase14_CommandSequence_PrimaryAssignmentIsBounded`
   - proves primary assignment now emits:
     - `apply_role`
     - `configure_shipper`
     - `start_recovery_task`
     - `publish_projection`
4. supporting proof:
   - `TestPhase14_CommandSequence_AssignmentChangeAllowsFreshRecoveryStart`
   - proves assignment change re-emits fresh `start_recovery_task`

### Focused integrated proof

1. `go test ./weed/server -run "TestBlockService_ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart|RebuildingRole_PreservesLegacyFallbackWithoutCore)"`
2. result: `PASS`
3. key new catch-up proof:
   - `TestBlockService_ApplyAssignments_PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp`
   - proves executed command sequence:
     - `apply_role`
     - `configure_shipper`
     - `start_recovery_task`
     - `start_catchup`
   - proves sender reaches `StateInSync`
   - proves projection returns to `RecoveryIdle`

### Compatibility and aggregate proof

1. `go test ./weed/server -run "TestP4_(LivePath_RealVol_ReachesPlan|SerializedReplacement_DrainsBeforeStart|ShutdownDrain)"`
2. result: `PASS`
3. `legacy P4` is still preserved as compatibility guard only
4. `go test ./weed/server -run "Test(P4_|P16B_|BlockService_(ApplyAssignments_(PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|RebuildingRole_|ExecutesCoreCommands_)|BarrierRejected|DebugInfoForVolume|CollectBlockVolumeHeartbeat|ReadinessSnapshot|HeartbeatReplicaDegraded)|Registry_(ReplicaReadyRequiresReplicaHeartbeat|UpdateFullHeartbeat|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded|UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady)|EntryToVolumeInfo_(IncludesHealthState|ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(LookupHandler_ReflectsCoreInfluencedReadyConsume|ListHandler_ReflectsCoreInfluencedDegradedConsume)|BlockStatusHandler_(IncludesHealthCounts|ReflectsCoreInfluencedConsumeCounts)|LookupResponseFromEntry_PublicationMinimalSurface)"`
5. result: `PASS`

## Review Questions

### For `tester`

Please challenge the proof posture:

1. Does `16E` really prove catch-up task startup is command-driven on the bounded
   core-present primary path?
2. Are the proofs behavioral enough, rather than just proving command plumbing?
3. Is assignment-change reissue of `start_recovery_task` bounded and correct?
4. Are there any remaining bounded single-replica catch-up startup paths that
   still bypass the core when the core is present?

### For `manager`

Please challenge boundaries and overclaim:

1. Is `16E` still a bounded refinement rather than a disguised move toward full
   recovery-loop closure?
2. Is the claim narrow enough:
   - single-replica primary catch-up startup only
   - not multi-replica startup ownership
   - not full runtime-loop ownership
3. Is `legacy P4` positioning now disciplined enough:
   - compatibility guard
   - not semantic authority proof for the core-present path
4. Is this a reasonable review/commit boundary?

## Requested Output Shape

Please reply with one of:

1. `ACCEPT`
2. `ACCEPT WITH MINOR FIXES`
3. `REJECT`

If not `ACCEPT`, keep findings bounded to this `16E` claim only.
