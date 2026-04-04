# Phase 16 Finish-Line Review

Date: 2026-04-04
Status: ready for review

## Review Object

Review the current bounded runtime checkpoint as:

1. `Phase 15` delivered
2. `16A-16T` delivered on the previously accepted bounded runtime path
3. `16U-16W` delivered as the last visible bounded heartbeat/restart truth
   closure slices

This checkpoint should be judged as the bounded `Phase 16` finish-line review,
not as a broad product-readiness or launch review.

## What Is In Scope

### Current bounded runtime claim

The checkpoint may now claim that, on the chosen bounded heartbeat/master/API
path:

1. explicit primary truth survives steady-state sparse heartbeats and bounded
   restart reconstruction
2. restart primary swap rebases explicit primary truth to the winning heartbeat
3. replica explicit readiness no longer silently falls back to address-shaped
   semantics after explicit truth has already been accepted
4. empty full block inventory delete behavior is explicit rather than inferred
   from emptiness alone
5. one real sender-side path truthfully emits non-authoritative inventory

### Expected judgment

1. the checkpoint is a real bounded runtime-closure step, not only protocol
   plumbing
2. the accepted claim set is explicit and evidence-backed
3. residual gaps are named rather than hidden

## What Is Explicitly Out Of Scope

Do NOT review this checkpoint as claiming:

1. broad recovery-loop closure
2. broad end-to-end failover/recovery/publication closure
3. full restart-window policy for all loading/not-yet-authoritative states
4. broad multi-replica startup / reconciliation ownership
5. launch / rollout readiness

## Primary Files

Checkpoint framing:

1. `sw-block/.private/phase/phase-16.md`
2. `sw-block/.private/phase/phase-16-log.md`
3. `sw-block/design/v2-product-completion-overview.md`
4. `sw-block/design/v2-protocol-truths.md`
5. `sw-block/design/v2-protocol-claim-and-evidence.md`

Checkpoint code:

1. `weed/server/master_block_registry.go`
2. `weed/server/master_block_registry_test.go`
3. `weed/server/volume_server_block.go`
4. `weed/server/volume_grpc_client_to_master.go`
5. `weed/server/master_grpc_server.go`
6. `weed/server/volume_server_test.go`

## Accepted Claim Set

1. steady-state and restart reconstruction preserve accepted explicit primary
   heartbeat truth on the bounded chosen path
2. sparse primary and replica heartbeats no longer silently erase already
   accepted explicit truth on existing entries
3. empty full block inventory delete behavior is explicit rather than heuristic
4. one real sender-side non-authoritative inventory path is now implemented and
   tested

## Explicit Non-Claims

1. full recovery-loop ownership
2. broad failover/publication proof
3. broad restart/disturbance hardening
4. launch-envelope freeze or rollout approval

## Residual Gaps

1. broader recovery-loop closure beyond the chosen bounded path
2. broader failover/publication whole-chain statement
3. long-window restart/disturbance policy and soak hardening
4. launch-envelope and rollout-gate work

## Evidence Summary

### Heartbeat truth closure and sparse-field retention

1. `go test ./weed/storage/blockvol -count=1 -run "TestInfoMessage_(ReplicaReady|NeedsRebuild|PublishHealthy|VolumeMode|VolumeModeReason)"`
2. `go test ./weed/server -count=1 -timeout 180s -run "Test(Registry_UpdateFullHeartbeat_(ConsumesCoreInfluencedReplicaReady|ReplicaReadyFallsBackToAddressesWhenFieldAbsent|ReplicaReadyMissingFieldPreservesAcceptedExplicitTruth|ReplicaReadyMissingFieldFreshEntryStillFallsBack|ConsumesExplicitNeedsRebuildFromPrimaryHeartbeat|NeedsRebuildFallsBackWhenFieldAbsent|ExplicitHealthySuppressesStaleNeedsRebuildHeuristic|ConsumesExplicitPublishHealthyFromPrimaryHeartbeat|ExplicitUnhealthySuppressesStalePublishHealthyHeuristic|ConsumesExplicitVolumeModeFromPrimaryHeartbeat|VolumeModeFallsBackWhenFieldAbsent|AutoRegisterPreservesExplicitPrimaryTruthOnRestart|MissingFieldsPreserveAcceptedExplicitPrimaryTruth|MissingFieldsDoNotInventExplicitTruthOnFreshEntry))"`
3. result: `PASS`

### Restart reconciliation and disturbance surfaces

1. `go test ./weed/server -count=1 -timeout 180s -run "Test(MasterRestart_(HigherEpochWins|HigherEpochRebasesExplicitPrimaryTruth|HigherEpochSparsePrimaryClearsOldExplicitTruth|LowerEpochBecomesReplica|SameEpoch_HigherLSNWins|SameEpoch_SameLSN_ExistingWins|SameEpoch_RoleTrusted)|P11P3_HeartbeatReconstruction|P12P1_Restart_SameLineage)"`
2. `go test ./weed/server -count=1 -timeout 180s -run "Test(StartBlockService_ScanFailureEmitsNonAuthoritativeInventory|CollectBlockVolumeHeartbeat_IncludesInventoryAuthority|Registry_UpdateFullHeartbeatWithInventoryAuthority_(NonAuthoritativeEmptyDoesNotDelete|AuthoritativeEmptyStillDeletes)|Master_ExpandCoordinated_B10_HeartbeatDoesNotDeleteDuringExpand|QA_Reg_FullHeartbeatEmptyServer)"`
3. result: `PASS`

### Outward surface coherence

1. `go test ./weed/server -count=1 -timeout 180s -run "Test(EntryToVolumeInfo_(ReflectsCoreInfluencedReadyConsume|ReflectsCoreInfluencedDegradedConsume)|BlockVolume(Get|List)Handler_ReflectsCoreInfluencedDegradedConsume)"`
2. result: `PASS`

## Review Questions

### For `sw`

Please check implementation correctness and checkpoint coherence:

1. Is the finish-line boundary coherent as one bounded runtime checkpoint?
2. Are the `16U-16W` changes internally consistent with the existing `16M-16T`
   truth-closure discipline?
3. Are there any small cleanup issues that should be fixed before a checkpoint
   commit, without widening scope?

### For `tester`

Please challenge the proof posture:

1. Do the new tests prove semantic claim rather than implementation shape?
2. Is restart primary-truth rebase adequately covered for the bounded chosen
   path?
3. Is the replica sparse-heartbeat retention proof strong enough to support the
   bounded claim?

### For `manager`

Please challenge overclaim and stop-line discipline:

1. Does the checkpoint wording stay disciplined about broad residual gaps?
2. Is `Phase 16` the right place to stop and package a runtime checkpoint rather
   than continue indefinite edge-case slicing?
3. Are the explicit non-claims and residuals sufficient to prevent product
   overreach?

## Requested Output Shape

Please reply with one of:

1. `ACCEPT`
2. `ACCEPT WITH MINOR FIXES`
3. `REJECT`

If not `ACCEPT`, list findings ordered by severity and keep them bounded to this
checkpoint's actual claim set.
