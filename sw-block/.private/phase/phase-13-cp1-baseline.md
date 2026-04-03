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
| PASS | `TestReplicaProgress_BarrierUsesFlushedLSN` | current code passes this test; suggests CP13-3 behavior may already exist |
| PASS | `TestReplicaProgress_FlushedLSNMonotonicWithinEpoch` | current code passes this test; suggests CP13-3 behavior may already exist |
| PASS | `TestBarrier_RejectsReplicaNotInSync` | barrier rejects non-InSync replica |
| PASS | `TestBarrier_EpochMismatchRejected` | barrier rejects epoch mismatch |
| PASS | `TestBarrier_DuringCatchup_Rejected` | current code passes this test; suggests CP13-4 behavior may already exist |
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
| PASS | `TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers` | current code passes this test; suggests CP13-5 behavior may already exist |
| PASS | `TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail` | SyncCache correctly fails during degraded |

## Category 3: Reconnect / Catch-up

| Result | Test | Reason |
|--------|------|--------|
| PASS | `TestReconnect_CatchupFromRetainedWal` | current code passes this test; suggests CP13-5 catch-up behavior may already exist |
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
| PASS | `TestWalRetention_RequiredReplicaBlocksReclaim` | current code passes this test; suggests CP13-6 retention behavior may already exist |
| PASS | `TestWalRetention_TimeoutTriggersNeedsRebuild` | current code passes this test; suggests CP13-6 timeout behavior may already exist |
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

| FAIL Test | Root Cause | Expected to close in |
|-----------|-----------|----------------------|
| `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | degraded shipper reconnects but doesn't catch up before barrier | CP13-5 (reconnect handshake) |
| `TestAdversarial_CatchupMultipleDisconnects` | repeated disconnect/reconnect cycles don't recover | CP13-5 (reconnect handshake) |
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | shipper stays Degraded after large gap, should be NeedsRebuild | CP13-5 (gap detection) + CP13-7 (rebuild fallback) |
| `TestAdversarial_CatchupDoesNotOverwriteNewerData` | catch-up fails at barrier, newer-data safety not exercised | CP13-5 (catch-up protocol) |

Main remaining failures cluster around CP13-5 (reconnect/catch-up), but CP13-7 (rebuild fallback) and part of CP13-6 (max-bytes retention) also remain open.

## PASS* → Checkpoint Mapping

| PASS* Test | Why Not Full Proof | Expected to close in |
|------------|-------------------|----------------------|
| `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` | documents gap, doesn't prove fix | CP13-2 (canonical addressing) |
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | asserts barrier failure, not NeedsRebuild state transition | CP13-5 (gap detection) + CP13-7 (rebuild fallback) |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | logs "not implemented", shipper stays Degraded | CP13-6 (max-bytes retention) |

## Remaining Open Checkpoints

This baseline does NOT close any checkpoint. Checkpoint closure requires dedicated review per checkpoint. The baseline only records which tests pass or fail on current code.

Tests passing on current code **suggests** the behavior may already exist, but does not constitute checkpoint acceptance. The following checkpoints still require dedicated review:

- **CP13-2** (canonical addressing): 1 PASS* test documents the gap
- **CP13-5** (reconnect/catch-up): 2 FAILs + 1 PASS* directly expose missing protocol
- **CP13-6** (WAL retention): 1 PASS* exposes missing max-bytes trigger
- **CP13-7** (rebuild fallback): 1 FAIL + 1 PASS* expose missing NeedsRebuild transition

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
