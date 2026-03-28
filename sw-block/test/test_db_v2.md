# V2 Test Database

Date: 2026-03-27
Status: working subset

## Purpose

This is the V2-focused review subset derived from:

- `sw-block/test/test_db.md`
- `learn/projects/sw-block/phases/phase13_test.md`
- `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`

Use this file to review and track the tests that most directly help:

- V2 protocol design
- simulator coverage
- V1 / V1.5 / V2 comparison
- V2 acceptance boundaries

This is intentionally much smaller than the full `test_db.md`.

## Review Codes

### Status

- `picked`
- `reviewed`
- `mapped`

### Sim

- `sim_core`
- `sim_reduced`
- `real_only`
- `v2_boundary`
- `sim_not_needed_yet`

## V2 Boundary Tests

| # | Test Name | File | Line | Level | Status | Sim | Notes |
|---|---|---|---|---|---|---|---|
| 1 | `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | `sync_all_adversarial_test.go` |  | `unit` | `picked` | `v2_boundary` | V1/V1.5 sender identity loss; should become V2 acceptance case |
| 2 | `TestAdversarial_NeedsRebuildBlocksAllPaths` | `sync_all_adversarial_test.go` |  | `unit` | `picked` | `v2_boundary` | `NeedsRebuild` must remain sticky under stable per-replica sender identity |
| 3 | `TestAdversarial_CatchupDoesNotOverwriteNewerData` | `sync_all_adversarial_test.go` |  | `unit` | `picked` | `v2_boundary` | Catch-up correctness depends on identity continuity and proper recovery ownership |
| 4 | `TestAdversarial_CatchupMultipleDisconnects` | `sync_all_adversarial_test.go` |  | `unit` | `picked` | `v2_boundary` | Multiple reconnect cycles are a V2 sender-loop / recovery-session acceptance target |

## Core Protocol Tests

| # | Test Name | File | Line | Level | Status | Sim | Notes |
|---|---|---|---|---|---|---|---|
| 1 | `TestRecovery` | `recovery_test.go` |  | `unit` | `picked` | `sim_core` | Crash recovery correctness is fundamental to block protocol reasoning |
| 2 | `TestReplicaProgress_BarrierUsesFlushedLSN` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Durable-progress truth; barrier must count flushed progress, not send progress |
| 3 | `TestReplicaProgress_FlushedLSNMonotonicWithinEpoch` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Progress monotonicity invariant |
| 4 | `TestBarrier_RejectsReplicaNotInSync` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Only eligible replica states count for strict durability |
| 5 | `TestBarrier_EpochMismatchRejected` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Epoch fencing on barrier path |
| 6 | `TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail` | `sync_all_bug_test.go` |  | `unit` | `picked` | `sim_core` | `sync_all` strictness during degraded state |
| 7 | `TestSyncAll_FullRoundTrip_WriteAndFlush` | `sync_all_bug_test.go` |  | `unit` | `picked` | `sim_core` | End-to-end strict replication contract |
| 8 | `TestBestEffort_FlushSucceeds_ReplicaDown` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Contrasts best_effort vs strict modes |
| 9 | `TestShip_DegradedDoesNotSilentlyCountAsHealthy` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | No false durability from degraded shipper |
| 10 | `TestDistSync_SyncAll_AllDegraded_Fails` | `dist_group_commit_test.go` |  | `unit` | `picked` | `sim_core` | Availability semantics under strict mode |
| 11 | `TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers` | `sync_all_bug_test.go` |  | `unit` | `picked` | `sim_core` | Recoverability after degraded shipper |
| 12 | `TestReconnect_CatchupFromRetainedWal` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Short-gap catch-up |
| 13 | `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Catch-up vs rebuild boundary |
| 14 | `TestReconnect_EpochChangeDuringCatchup_Aborts` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Recovery fencing during catch-up |
| 15 | `TestCatchupReplay_DataIntegrity_AllBlocksMatch` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Recovery data correctness |
| 16 | `TestCatchupReplay_DuplicateEntry_Idempotent` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Replay idempotence |
| 17 | `TestBarrier_DuringCatchup_Rejected` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | State-machine correctness during recovery |
| 18 | `TestReplicaState_RebuildComplete_ReentersInSync` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_core` | Rebuild lifecycle closure |
| 19 | `TestRebuild_AbortOnEpochChange` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_core` | Rebuild fencing |
| 20 | `TestRebuild_MissingTailRestartsOrFailsCleanly` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_core` | Safe rebuild failure behavior |
| 21 | `TestWalRetention_RequiredReplicaBlocksReclaim` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Retention rule under lag |
| 22 | `TestWalRetention_TimeoutTriggersNeedsRebuild` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Retention timeout boundary |
| 23 | `TestWalRetention_MaxBytesTriggersNeedsRebuild` | `sync_all_protocol_test.go` |  | `unit` | `picked` | `sim_core` | Retention budget boundary |
| 24 | `TestComponent_FailoverPromote` | `component_test.go` |  | `component` | `picked` | `sim_core` | Core failover baseline |
| 25 | `TestCP13_SyncAll_FailoverPromotesReplica` | `cp13_protocol_test.go` |  | `component` | `picked` | `sim_core` | Strict-mode failover |
| 26 | `TestCP13_SyncAll_ReplicaRestart_Rejoin` | `cp13_protocol_test.go` |  | `component` | `picked` | `sim_core` | Restart/rejoin lifecycle |
| 27 | `TestQA_LSNLag_StaleReplicaSkipped` | `qa_block_edge_cases_test.go` |  | `unit` | `picked` | `sim_core` | Promotion safety and stale candidate rejection |
| 28 | `TestQA_CascadeFailover_RF3_EpochChain` | `qa_block_edge_cases_test.go` |  | `unit` | `picked` | `sim_core` | Multi-promotion lineage |
| 29 | `TestDurabilityMode_Validate_SyncQuorum_RF2_Rejected` | `durability_mode_test.go` |  | `unit` | `picked` | `sim_core` | Mode normalization |
| 30 | `TestCP13_BestEffort_SurvivesReplicaDeath` | `cp13_protocol_test.go` |  | `component` | `picked` | `sim_core` | Best-effort contract |
| 31 | `CP13-8 T4a: sync_all blocks during outage` | `manual` |  | `integration` | `picked` | `sim_core` | Strict outage semantics |

## Reduced / Supporting Tests

| # | Test Name | File | Line | Level | Status | Sim | Notes |
|---|---|---|---|---|---|---|---|
| 1 | `testRecoverExtendedScanPastStaleHead` | `recovery_test.go` |  | `unit` | `picked` | `sim_reduced` | Advisory WAL-head recovery shape |
| 2 | `testRecoverNoSuperblockPersist` | `recovery_test.go` |  | `unit` | `picked` | `sim_reduced` | Recovery despite optimized persist behavior |
| 3 | `TestQAGroupCommitter` | `blockvol_qa_test.go` |  | `unit` | `picked` | `sim_reduced` | Commit batching semantics |
| 4 | `TestQA_Admission_WriteLBAIntegration` | `qa_wal_admission_test.go` |  | `unit` | `picked` | `sim_reduced` | Backpressure behavior |
| 5 | `TestSyncAll_MultipleFlush_NoWritesBetween` | `sync_all_bug_test.go` |  | `unit` | `picked` | `sim_reduced` | Idempotent flush shape |
| 6 | `TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_reduced` | Progress initialization after rebuild |
| 7 | `TestComponent_ManualPromote` | `component_test.go` |  | `component` | `picked` | `sim_reduced` | Manual control-path shape |
| 8 | `TestHeartbeat_ReportsPerReplicaState` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_reduced` | Heartbeat observability |
| 9 | `TestHeartbeat_ReportsNeedsRebuild` | `rebuild_v1_test.go` |  | `unit` | `picked` | `sim_reduced` | Control-plane visibility |
| 10 | `TestComponent_ExpandThenFailover` | `component_test.go` |  | `component` | `picked` | `sim_reduced` | State continuity across operations |
| 11 | `TestCP13_DurabilityModeDefault` | `cp13_protocol_test.go` |  | `component` | `picked` | `sim_reduced` | Default mode behavior |
| 12 | `CP13-8 T4b: recovery after restart` | `manual` |  | `integration` | `picked` | `sim_reduced` | Recovery-time shape and control-plane/local-reconnect interaction |

## Notes

- This file is the actionable V2 subset, not the master inventory.
- If `tester` later finalizes a broader 70-case picked set, expand this file from that selection.
- The 4 V2-boundary tests must remain present even if they fail on V1/V1.5.
