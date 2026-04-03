# CP13-1 Baseline Report

Date: 2026-04-02
Commit: c0a805184 (feature/sw-block HEAD)
Runner: `go test ./weed/storage/blockvol/ -v -count=1 -timeout 120s`
Protocol changes in this checkpoint: NONE — test-first baseline only

## Category 1: Address Truth

| Result | Test | Reason |
|--------|------|--------|
| PASS | `TestCanonicalizeAddr_WildcardIPv4_UsesAdvertised` | canonicalization infra works |
| PASS | `TestCanonicalizeAddr_WildcardIPv6_UsesAdvertised` | canonicalization infra works |
| PASS | `TestCanonicalizeAddr_NilIP_UsesAdvertised` | canonicalization infra works |
| PASS | `TestCanonicalizeAddr_AlreadyCanonical_Unchanged` | no-op on canonical input |
| PASS | `TestCanonicalizeAddr_Loopback_Unchanged` | loopback preserved intentionally |
| PASS | `TestCanonicalizeAddr_NoAdvertised_FallsBackToOutbound` | fallback path works |
| PASS* | `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` | documents gap: ReplicaReceiver may return `:port` not `ip:port` on wildcard bind; test passes as documentation, not as proof of fix → CP13-2 |

## Category 2: Durable Progress Truth

| Result | Test | Reason |
|--------|------|--------|
| PASS | `TestReplicaProgress_BarrierUsesFlushedLSN` | barrier now gates on replicaFlushedLSN (CP13-3 done) |
| PASS | `TestReplicaProgress_FlushedLSNMonotonicWithinEpoch` | flushedLSN monotonic within epoch (CP13-3 done) |
| PASS | `TestBarrier_RejectsReplicaNotInSync` | barrier rejects non-InSync replica |
| PASS | `TestBarrier_EpochMismatchRejected` | barrier rejects epoch mismatch |
| PASS | `TestBarrier_DuringCatchup_Rejected` | barrier rejected during CatchingUp state (CP13-4 done) |
| PASS | `TestBarrier_ReplicaSlowFsync_Timeout` | barrier timeout on slow replica |
| PASS | `TestBarrierResp_FlushedLSN_Roundtrip` | barrier response wire format carries flushedLSN |
| PASS | `TestBarrierResp_BackwardCompat_1Byte` | backward compat with old 1-byte response |
| PASS | `TestReplica_FlushedLSN_OnlyAfterSync` | flushedLSN only updated after fdatasync |
| PASS | `TestReplica_FlushedLSN_NotOnReceive` | flushedLSN not updated on entry receive |
| PASS | `TestShipper_ReplicaFlushedLSN_UpdatedOnBarrier` | shipper tracks replica flushedLSN from barrier |
| PASS | `TestShipper_ReplicaFlushedLSN_Monotonic` | tracked flushedLSN is monotonic |
| PASS | `TestShipperGroup_MinReplicaFlushedLSN` | group computes min flushedLSN across replicas |
| PASS | `TestDistSync_SyncAll_NilGroup_Succeeds` | sync_all with no replicas succeeds locally |
| PASS | `TestDistSync_SyncAll_AllDegraded_Fails` | sync_all fails when all replicas degraded |
| PASS | `TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers` | after recovery, catch-up + barrier succeeds (CP13-5 done) |
| PASS | `TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail` | SyncCache correctly fails during degraded |

## Category 3: Reconnect / Catch-up

| Result | Test | Reason |
|--------|------|--------|
| PASS | `TestReconnect_CatchupFromRetainedWal` | reconnect + WAL catch-up works (CP13-5 done) |
| PASS* | `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | correctly fails SyncCache after large gap, but does NOT assert NeedsRebuild state transition — asserts barrier failure only → CP13-5+CP13-7 |
| PASS | `TestReconnect_EpochChangeDuringCatchup_Aborts` | catch-up aborts on epoch change |
| PASS | `TestReconnect_CatchupTimeout_TransitionsDegraded` | catch-up timeout → degraded |
| PASS | `TestAdversarial_FreshShipperUsesBootstrapNotReconnect` | fresh shipper uses bootstrap path |
| FAIL | `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | **gap: degraded shipper with prior flushed progress reconnects but barrier fails** — shipper does not catch up before attempting barrier → CP13-5 |
| PASS | `TestAdversarial_ReplicaRejectsDuplicateLSN` | replica rejects duplicate LSN |
| PASS | `TestAdversarial_ReplicaRejectsGapLSN` | replica rejects LSN gap |
| FAIL | `TestAdversarial_CatchupMultipleDisconnects` | **gap: catch-up across multiple disconnect/reconnect cycles fails** — first reconnect barrier fails, subsequent cycles never recover → CP13-5 |
| PASS | `TestAdversarial_ConcurrentBarrierDoesNotCorruptCatchupFailures` | concurrent barriers don't corrupt counter |

## Category 4: Retention / Rebuild Boundary

| Result | Test | Reason |
|--------|------|--------|
| PASS | `TestWalRetention_RequiredReplicaBlocksReclaim` | replica-aware WAL retention works (CP13-6 done) |
| PASS | `TestWalRetention_TimeoutTriggersNeedsRebuild` | retention timeout → NeedsRebuild (CP13-6 done) |
| PASS* | `TestWalRetention_MaxBytesTriggersNeedsRebuild` | passes but logs "max-bytes retention trigger not implemented yet" — shipper stays Degraded, does not transition to NeedsRebuild → CP13-6 |
| FAIL | `TestAdversarial_NeedsRebuildBlocksAllPaths` | **gap: after large WAL gap, shipper stays Degraded instead of NeedsRebuild; Ship/Barrier not blocked** → CP13-5+CP13-7 |
| FAIL | `TestAdversarial_CatchupDoesNotOverwriteNewerData` | **gap: catch-up after disconnect fails at barrier level** — catch-up doesn't complete, so newer-data safety not actually exercised → CP13-5 |
| PASS | `TestHeartbeat_ReportsPerReplicaState` | heartbeat reports per-replica shipper state |
| PASS | `TestHeartbeat_ReportsNeedsRebuild` | heartbeat reports NeedsRebuild per-replica |
| PASS | `TestReplicaState_RebuildComplete_ReentersInSync` | full rebuild cycle: NeedsRebuild → rebuild → InSync |
| PASS | `TestRebuild_AbortOnEpochChange` | rebuild aborts on epoch change |
| PASS | `TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint` | post-rebuild flushedLSN = checkpoint |

## Summary

| Category | PASS | FAIL | PASS* | Total |
|----------|------|------|-------|-------|
| 1. Address Truth | 6 | 0 | 1 | 7 |
| 2. Durable Progress Truth | 17 | 0 | 0 | 17 |
| 3. Reconnect / Catch-up | 7 | 2 | 1 | 10 |
| 4. Retention / Rebuild | 7 | 2 | 1 | 10 |
| **Total** | **37** | **4** | **3** | **44** |

## Failure → Checkpoint Mapping

| FAIL Test | Root Cause | Closes In |
|-----------|-----------|-----------|
| `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | degraded shipper reconnects but doesn't catch up before barrier | CP13-5 |
| `TestAdversarial_CatchupMultipleDisconnects` | repeated disconnect/reconnect cycles don't recover | CP13-5 |
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | shipper stays Degraded after large gap, should be NeedsRebuild | CP13-5 + CP13-7 |
| `TestAdversarial_CatchupDoesNotOverwriteNewerData` | catch-up fails, so newer-data safety not exercised | CP13-5 |

## PASS* → Checkpoint Mapping

| PASS* Test | Why Not Full Proof | Closes In |
|------------|-------------------|-----------|
| `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` | documents gap, doesn't prove fix | CP13-2 |
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | asserts barrier failure, not NeedsRebuild state | CP13-5 + CP13-7 |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | logs "not implemented", shipper stays Degraded | CP13-6 |

## What Was NOT Changed

This baseline was captured on current code without any protocol modifications:

- No reconnect handshake changes
- No WAL catch-up logic changes
- No retention policy changes
- No rebuild behavior changes
- No barrier protocol changes
- No state machine changes
- No new protocol code of any kind

All 4 FAILs and 3 PASS* entries expose real gaps that exist in the current codebase.
