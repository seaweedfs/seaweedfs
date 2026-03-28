# V2-Selected Test Worklist

Date: 2026-03-27
Status: working

## Purpose

This is the V2-facing subset of the larger block-service test database.

Sources:

- `sw-block/test/test_db.md`
- `learn/projects/sw-block/phases/phase13_test.md`
- `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`

This file is for:

- tests that should help V2 design and simulator work
- explicit inclusion of the 4 Phase 13 V2-boundary failures
- a working set that `tester`, `sw`, and design can refine further

## Current Inclusion Rule

Include tests that are:

- `sim_core`
- `sim_reduced`
- `v2_boundary`

Prefer tests that directly inform:

- barriers and durability truth
- catch-up vs rebuild
- failover / promotion
- WAL retention / tail-chasing
- mode semantics
- endpoint / identity / reassignment behavior

## Phase 13 V2-Boundary Tests

These must stay visible in the V2 worklist:

| Test | File | Why It Matters To V2 |
|---|---|---|
| `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | `sync_all_adversarial_test.go` | Sender identity and reconnect ownership |
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | `sync_all_adversarial_test.go` | `NeedsRebuild` must remain sticky and identity-safe |
| `TestAdversarial_CatchupDoesNotOverwriteNewerData` | `sync_all_adversarial_test.go` | Catch-up must preserve data correctness under identity continuity |
| `TestAdversarial_CatchupMultipleDisconnects` | `sync_all_adversarial_test.go` | Multiple reconnect cycles require stable per-replica sender ownership |

## High-Value V2 Working Set

This is the current distilled working set from `phase13_test.md`.

| Test | File | Current Result | Mapping | Why It Helps V2 |
|---|---|---|---|---|
| `TestRecovery` | `recovery_test.go` | PASS | `sim_core` | Crash recovery correctness |
| `TestReplicaProgress_BarrierUsesFlushedLSN` | `sync_all_protocol_test.go` | PASS | `sim_core` | Barrier truth / durable progress |
| `TestReplicaProgress_FlushedLSNMonotonicWithinEpoch` | `sync_all_protocol_test.go` | PASS | `sim_core` | Monotonic progress invariant |
| `TestBarrier_RejectsReplicaNotInSync` | `sync_all_protocol_test.go` | PASS | `sim_core` | State-gated strict durability |
| `TestBarrier_EpochMismatchRejected` | `sync_all_protocol_test.go` | PASS | `sim_core` | Epoch fencing |
| `TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail` | `sync_all_bug_test.go` | PASS | `sim_core` | `sync_all` strictness during outage |
| `TestSyncAll_FullRoundTrip_WriteAndFlush` | `sync_all_bug_test.go` | PASS | `sim_core` | End-to-end strict replication |
| `TestBestEffort_FlushSucceeds_ReplicaDown` | `sync_all_protocol_test.go` | PASS | `sim_core` | Mode difference vs strict sync |
| `TestShip_DegradedDoesNotSilentlyCountAsHealthy` | `sync_all_protocol_test.go` | PASS | `sim_core` | No false durability |
| `TestDistSync_SyncAll_AllDegraded_Fails` | `dist_group_commit_test.go` | PASS | `sim_core` | Availability semantics |
| `TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers` | `sync_all_bug_test.go` | PASS | `sim_core` | Recoverability after degraded shipper |
| `TestReconnect_CatchupFromRetainedWal` | `sync_all_protocol_test.go` | PASS | `sim_core` | Short-gap catch-up |
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | `sync_all_protocol_test.go` | PASS | `sim_core` | Catch-up vs rebuild boundary |
| `TestReconnect_EpochChangeDuringCatchup_Aborts` | `sync_all_protocol_test.go` | PASS | `sim_core` | Recovery fencing |
| `TestCatchupReplay_DataIntegrity_AllBlocksMatch` | `sync_all_protocol_test.go` | PASS | `sim_core` | Recovery data correctness |
| `TestCatchupReplay_DuplicateEntry_Idempotent` | `sync_all_protocol_test.go` | PASS | `sim_core` | Replay idempotence |
| `TestBarrier_DuringCatchup_Rejected` | `sync_all_protocol_test.go` | PASS | `sim_core` | State-machine correctness |
| `TestReplicaState_RebuildComplete_ReentersInSync` | `rebuild_v1_test.go` | PASS | `sim_core` | Rebuild lifecycle |
| `TestRebuild_AbortOnEpochChange` | `rebuild_v1_test.go` | PASS | `sim_core` | Rebuild fencing |
| `TestRebuild_MissingTailRestartsOrFailsCleanly` | `rebuild_v1_test.go` | PASS | `sim_core` | No partial/unsafe rebuild success |
| `TestWalRetention_RequiredReplicaBlocksReclaim` | `sync_all_protocol_test.go` | PASS | `sim_core` | Retention rule |
| `TestWalRetention_TimeoutTriggersNeedsRebuild` | `sync_all_protocol_test.go` | PASS | `sim_core` | Retention timeout boundary |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | `sync_all_protocol_test.go` | PASS | `sim_core` | Retention budget boundary |
| `TestComponent_FailoverPromote` | `component_test.go` | PASS | `sim_core` | Failover baseline |
| `TestCP13_SyncAll_FailoverPromotesReplica` | `cp13_protocol_test.go` | PASS | `sim_core` | Strict-mode failover |
| `TestCP13_SyncAll_ReplicaRestart_Rejoin` | `cp13_protocol_test.go` | PASS | `sim_core` | Restart/rejoin lifecycle |
| `TestQA_LSNLag_StaleReplicaSkipped` | `qa_block_edge_cases_test.go` | PASS | `sim_core` | Promotion safety |
| `TestQA_CascadeFailover_RF3_EpochChain` | `qa_block_edge_cases_test.go` | PASS | `sim_core` | Multi-promotion lineage |
| `TestDurabilityMode_Validate_SyncQuorum_RF2_Rejected` | `durability_mode_test.go` | PASS | `sim_core` | Mode normalization |
| `TestCP13_BestEffort_SurvivesReplicaDeath` | `cp13_protocol_test.go` | PASS | `sim_core` | Best-effort contract |
| `CP13-8 T4a: sync_all blocks during outage` | `manual` | PASS | `sim_core` | Strict outage semantics |
| `CP13-8 T4b: recovery after restart` | `manual` | PASS | `sim_reduced` | Recovery-time shape |

## Reduced / Supporting Cases To Keep In View

| Test | File | Current Result | Mapping | Why It Helps V2 |
|---|---|---|---|---|
| `testRecoverExtendedScanPastStaleHead` | `recovery_test.go` | PASS | `sim_reduced` | Advisory WAL-head recovery shape |
| `testRecoverNoSuperblockPersist` | `recovery_test.go` | PASS | `sim_reduced` | Recoverability despite optimized persist behavior |
| `TestQAGroupCommitter` | `blockvol_qa_test.go` | PASS | `sim_reduced` | Commit batching semantics |
| `TestQA_Admission_WriteLBAIntegration` | `qa_wal_admission_test.go` | PASS | `sim_reduced` | Backpressure behavior |
| `TestSyncAll_MultipleFlush_NoWritesBetween` | `sync_all_bug_test.go` | PASS | `sim_reduced` | Idempotent flush shape |
| `TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint` | `rebuild_v1_test.go` | PASS | `sim_reduced` | Progress initialization |
| `TestComponent_ManualPromote` | `component_test.go` | PASS | `sim_reduced` | Manual control-path shape |
| `TestHeartbeat_ReportsPerReplicaState` | `rebuild_v1_test.go` | PASS | `sim_reduced` | Heartbeat observability |
| `TestHeartbeat_ReportsNeedsRebuild` | `rebuild_v1_test.go` | PASS | `sim_reduced` | Control-plane visibility |
| `TestComponent_ExpandThenFailover` | `component_test.go` | PASS | `sim_reduced` | Cross-operation state continuity |
| `TestCP13_DurabilityModeDefault` | `cp13_protocol_test.go` | PASS | `sim_reduced` | Default mode behavior |

## Working Note

`phase13_test.md` currently contains the mapped subset from the real test inventory.

This V2 copy is intentionally narrower:

- preserve the core tests that define the protocol story
- preserve the 4 V2-boundary tests explicitly
- keep a smaller reduced set for supporting invariants

If `tester` finalizes a broader 70-case working set, extend this file rather than editing the full copied database directly.
