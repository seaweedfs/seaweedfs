# V2 Validation Matrix

Date: 2026-04-08
Status: active

## Purpose

This document defines the concrete validation matrix for the V2 protocol and
runtime.

It answers four practical questions:

1. what must be green before a stage can be called ready
2. which existing V1 tests should be reused
3. which new V2-only tests are required because truth ownership changed
4. what the final validation signal must be for each scenario

This document is the stage-oriented companion to:

1. `sw-block/design/v2-proof-and-retest-pyramid.md`
2. `sw-block/design/v2-reuse-replacement-boundary.md`
3. `sw-block/design/v2-sync-recovery-protocol.md`
4. `sw-block/design/v2-rebuild-mvp-session-protocol.md`

## Validation Stages

V2 validation is gated in three layers:

1. `Rebuild Ready`: primary-driven rebuild is correct, convergent, and fail-closed
2. `Restore Ready`: exact snapshot/export/import and snapshot-tail recovery are correct
3. `V2 Ready`: assignment, sync facts, keep/catch/rebuild, failover/rejoin, and publication semantics close end-to-end

The intent is to avoid claiming overall V2 readiness from rebuild-only proof.

## Reuse Policy From V1

### Reuse unchanged

Reuse existing V1 tests unchanged when the observable contract is still the
same in V2:

1. WAL append/replay correctness
2. flusher and checkpoint correctness
3. dirty-map and extent correctness
4. receiver data-plane correctness
5. barrier/fsync data integrity when semantic meaning did not change
6. snapshot export/import integrity tests
7. fail-closed mid-transfer and corruption detection tests

### Reuse with adapter

Reuse the intent, but update the trigger path when V2 moved the decision point
to primary-owned sync facts:

1. rebuild tests that formerly used direct local install helpers
2. catch-up tests that formerly bypassed sync-driven entry
3. reconnect/rejoin tests that now must enter via `syncAck -> primary decision`
4. failover tests that now must prove assignment/session/projection layering

### Retire or replace

Do not reuse tests that encode V1.5 semantics that V2 intentionally removed:

1. shipper or replica self-escalation to rebuild
2. `CP13-6` style max-bytes retention-triggered rebuild decisions
3. direct local shortcut paths used as if they were protocol truth

## Stage Gate Summary

| Stage | Closure meaning | Must-have scope |
|---|---|---|
| `Rebuild Ready` | V2 rebuild is safe and correct on the real session-controlled path | rebuild kernel, runtime, trigger scenarios, data identity |
| `Restore Ready` | exact base restore and snapshot-tail recovery are safe and exact | snapshot boundary, integrity, partial-failure safety, tail convergence |
| `V2 Ready` | primary-owned assignment/session/projection semantics close on real flows | bootstrap, keepup, catchup, rebuild, failover, rejoin, publish gating |

## Matrix Linkage

Read this matrix together with:

1. `v2-capability-map.md` for ownership of the capability tier
2. `v2-integration-matrix.md` for real scenario coverage

This matrix answers "is the capability closed at all?" It does not by itself
guarantee that enough real topology/workload/failure scenarios have been
exercised. That second question belongs to the integration matrix.

| Validation rows | Capability tier | Primary protocol refs | Main integration refs |
|---|---|---|---|
| `R1`-`R12` | Tier 3: RF=2 Recovery And Failover | `v2-sync-recovery-protocol.md`, `v2-rebuild-mvp-session-protocol.md` | `I-R1`-`I-R8` |
| `S1`-`S10` | Tier 5: Lifecycle Capability | `v2-rebuild-mvp-session-protocol.md` and snapshot/restore execution rules | `I-S1`-`I-S4` |
| `V1`-`V2` | Tier 2: RF=2 Replication Base | `v2-sync-recovery-protocol.md` | `I-V1`, `I-V2` |
| `V3`-`V8` | Tier 3: RF=2 Recovery And Failover | `v2-sync-recovery-protocol.md`, `v2-rebuild-mvp-session-protocol.md` | `I-V3`, `I-V4`, `I-V5` |
| `V9`-`V10` | Tier 4: Multi-Replica Runtime (`RF>=3`) | `v2-sync-recovery-protocol.md` | future RF>=3 integrated rows; currently bounded engine/component proof only |
| `V11` | Tier 3 / Tier 8 boundary | recovery protocol plus launch-envelope disturbance claims | `I-V5` |
| `V12`-`V13` | Tier 6: Control Plane And Operations | ownership / observability docs and control-plane protocol surfaces | `I-V4`, `I-V6` |
| `V14` | Tier 0: Semantic Foundation | `v2-protocol-truths.md`, `v2-sync-recovery-protocol.md` | exercised across `I-V*` and negative component packs |

## Matrix A: Rebuild Ready

| ID | Priority | Scenario | Trigger / entry | Reuse | Main proof | Final validation | Coverage | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| `R1` | P0 | Fresh replica join rebuild | replica reports `applied_lsn=0`, primary decides rebuild | New | canonical primary-decided rebuild entry exists | session completes and replica returns to steady state | Covered | `weed/storage/blockvol/test/component/rebuild_matrix_gaps_test.go` | `TestRebuild_R1_SyncAckDrivenDecision` — (1) protocol engine decides rebuild from syncAck(applied=0, wal_tail=N), (2) SendSessionControl over real TCP to receiver ctrl port, (3) accepted ack read from TCP, (4) base lane over real TCP via RebuildTransportServer/Client, (5) data verified block-by-block |
| `R2` | P0 | Primary-initiated 1GB rebuild with live writes | explicit primary rebuild decision | New | two-line rebuild under realistic size, 4KB blocks, live WAL, flusher active | stop writes, flush both, full extent SHA-256 match | Covered | `weed/storage/blockvol/test/component/rebuild_primary_initiated_test.go` | `TestRebuild_PrimaryInitiated_1GB_WithLiveWrites` |
| `R3` | P0 | Stale replica restart beyond WAL window | reconnect with `applied_lsn < wal_tail` | New | stale restart naturally enters rebuild | final extent digest match | Covered | `weed/storage/blockvol/test/component/rebuild_matrix_gaps_test.go` | `TestRebuild_R3_StaleReplicaRestartBeyondWAL` |
| `R4` | P0 | Rebuild completion dual gate | base finishes before WAL or WAL before base | Reuse with adapter | no premature completion | complete only after `base_complete && wal_applied_lsn >= target_lsn` | Covered | `weed/storage/blockvol/test/component/rebuild_crash_test.go` | `TestRebuild_CompletionRequiresBothLanes` |
| `R5` | P0 | Mid-transfer failure is fail-closed | connection drop or server death mid-base | Reuse | partial rebuild does not commit mixed state | replica remains logically unchanged after failure | Covered | `weed/storage/blockvol/test/component/rebuild_matrix_gaps_test.go` | `TestRebuild_R5_ConnectionDropMidBase` |
| `R6` | P0 | Wrong session / epoch rejected | stale control/data/ack frames | Reuse with adapter | stale traffic cannot mutate active rebuild | explicit reject or ignore; active session remains valid | Covered | `weed/storage/blockvol/test/component/rebuild_crash_test.go` | `TestRebuild_EpochMismatch_WALEntryRejected`, `TestRebuild_ControlSurface_StartSupersedeAndComplete` |
| `R7` | P0 | Overlap correctness | same LBA hit by base and WAL in different orders | Reuse | bitmap and WAL-wins semantics are correct | block-by-block compare on overlap set | Covered | `weed/storage/blockvol/test/component/rebuild_mvp_test.go` | `TestRebuild_WALApplied_NeverOverwrittenByBase`, `TestRebuild_BitmapSetOnApplied_NotReceived`, `TestRebuild_BasePlusWAL_ConvergesToTarget` |
| `R8` | P1 | Rebuild timeout fails closed | no progress ack | New | watchdog and cancel path are real | failed session, no silent success, pin cleared | Covered | `weed/server/volume_server_block_test.go` | `TestBlockService_WireLocalReplicaRebuildSessionAcks_TimeoutFailsClosedAndClearsPin` |
| `R9` | P1 | Progress pin tracks rebuild truth | rebuild emits progress | New | retention floor follows `wal_applied_lsn`, not barrier-only closure | observed floor moves and later clears | Covered | `weed/storage/blockvol/test/component/rebuild_retention_pin_test.go`, `weed/server/volume_server_block_test.go` | `TestRebuild_RetentionPin_FlusherRespectsRebuildPin`, `TestBlockService_WireLocalReplicaRebuildSessionAcks_ProgressUpdatesCoreAndPin` |
| `R10` | P1 | Failover-rejoin rebuild | old primary comes back as replica | New | rebuild survives real topology change | final extent digest match | Covered | `weed/storage/blockvol/test/component/rebuild_failover_rejoin_test.go` | `TestRebuild_R10_FailoverRejoinRebuild` — forced WAL recycling past nodeA position, engine strictly asserts rebuild (not catchup/keepup), CRC validated |
| `R11` | P1 | Non-empty stale replica full overwrite | replica has old dirty/WAL state | New | full-base rebuild discards stale local runtime correctly | final extent digest match | Covered | `weed/storage/blockvol/test/component/rebuild_r11_r12_test.go` | `TestRebuild_R11_DivergentReplicaFullOverwrite` |
| `R12` | P1 | Restart rebuild, not resume rebuild | crash mid-session | New | current MVP restart semantics are explicit and safe | fresh rebuild converges without durable base-progress | Covered | `weed/storage/blockvol/test/component/rebuild_r11_r12_test.go` | `TestRebuild_R12_CrashMidRebuild_FreshSessionConverges` |

### Rebuild Ready minimum gate

`Rebuild Ready` requires all of:

1. `R1`
2. `R2`
3. `R3`
4. `R4`
5. `R5`
6. `R6`
7. `R7`

## Matrix B: Restore Ready

| ID | Priority | Scenario | Trigger / entry | Reuse | Main proof | Final validation | Coverage | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| `S1` | P0 | Exact snapshot export at requested boundary | explicit snapshot request at `BaseLSN` | Reuse | snapshot base is exact, not approximate | manifest boundary and export boundary match exactly | Covered (V1) | `weed/storage/blockvol/v2bridge/transfer_test.go` | V1 snapshot export tests |
| `S2` | P0 | Snapshot checksum mismatch fails | corrupt payload or wrong digest | Reuse + New | no silent bad restore | restore rejected before commit | Covered | `weed/storage/blockvol/v2bridge/transfer_test.go`, `weed/storage/blockvol/test/component/restore_s2_corruption_test.go` | Epoch mismatch: `TestP1_TransferFullBase_EpochMismatch`; Payload corruption: `TestRestore_S2_CorruptWALEntryRejected` (truncated + empty payloads rejected), `TestRestore_S2_CorruptBaseBlockDetected` (short block documented as transport-layer responsibility) |
| `S3` | P0 | Partial snapshot transfer does not commit | disconnect mid-stream | Reuse | no half-installed snapshot state | original state preserved | Covered (V1) | `weed/storage/blockvol/v2bridge/transfer_test.go` | V1 partial-transfer fail-closed tests |
| `S4` | P0 | Snapshot import exactness | import known snapshot image | Reuse | imported extent equals exported snapshot image | full extent digest match | Covered (V1) | `weed/storage/blockvol/v2bridge/transfer_test.go` | V1 import exactness tests |
| `S5` | P0 | Snapshot-tail rebuild | exact snapshot install plus WAL tail replay | New | restore and live convergence close together | final extent digest match | Covered | `weed/storage/blockvol/test/component/restore_ready_test.go` | `TestRestore_S5_SnapshotTailRebuild` |
| `S6` | P0 | Boundary mismatch rejected | server returns wrong base boundary | Reuse | exact restore contract is enforced | explicit failure, no commit | Covered (V1) | `weed/storage/blockvol/v2bridge/transfer_test.go` | V1 boundary mismatch tests |
| `S7` | P1 | Restart after snapshot install before tail replay | crash between base and tail phases | New | snapshot state is durable and fresh decision can continue recovery | final extent digest match | Covered | `weed/storage/blockvol/test/component/restore_ready_test.go` | `TestRestore_S7_CrashBetweenBaseAndTail` |
| `S8` | P1 | Snapshot under concurrent writes | writes continue after snapshot boundary | Reuse with adapter | exact base is preserved and later writes arrive through WAL tail | final extent digest match | Covered | `weed/storage/blockvol/test/component/restore_ready_test.go` | `TestRestore_S8_SnapshotUnderConcurrentWrites` |
| `S9` | P1 | Stale snapshot request rejected | requested historical boundary no longer satisfiable | New | protocol refuses unverifiable restore request | explicit failure | Covered (unit) | `sw-block/engine/replication/restore_ready_test.go` | `TestRestore_S9_StaleSnapshotRequestRejected` + `TestRestore_S9_BoundaryJustOutsideRetention` — gap_beyond_retention when replica LSN < WAL tail, edge case at tail boundary |
| `S10` | P1 | Snapshot-tail chosen from trusted checkpoint | primary selects exact restore path | New | planner/runtime choose snapshot-tail only when allowed | reaches steady state without semantic drift | Covered (unit) | `sw-block/engine/replication/restore_ready_test.go` | `TestRestore_S10_SnapshotTailChosenFromTrustedCheckpoint` — 5 cases: trusted+covered→snapshot-tail, untrusted→full-base, WAL gap→full-base, no checkpoint→full-base, checkpoint>committed edge |

### Restore Ready minimum gate

`Restore Ready` requires all of:

1. `S1`
2. `S2`
3. `S3`
4. `S4`
5. `S5`
6. `S6`

## Matrix C: V2 Ready

| ID | Priority | Scenario | Trigger / entry | Reuse | Main proof | Final validation | Coverage | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| `V1` | P0 | Bootstrap to healthy primary | fresh assignment/bootstrap | Reuse | stage-0 bootstrap closure still holds | healthy publish with correct projection | Covered | `weed/storage/blockvol/test/component/publish_healthy_test.go` | `TestPublishHealthy_WholeChain_FreshRF2`; Stage 0 hardware PASS (`phase20-t6-stage0`) |
| `V2` | P0 | Sustained write plus barrier closure | fio/dd/fsync style load | Reuse | healthy data-plane remains correct under workload | data checksum and barrier success | Covered | `weed/storage/blockvol/test/component/bootstrap_shipping_test.go` | `TestBootstrap_SyncCacheIsDurabilityFence_NotWriteLBA`; Stage 1 hardware 32/33 |
| `V3` | P0 | Sync timeout to rebuild | timeout fact enters primary decision | New | fact-driven recovery entry is real | rebuild completes, projection returns to steady state | Covered (server) | `weed/server/block_recovery_test.go` | `TestP16B_FactTriggeredRebuildCycle_AutoInstallsRebuildAndReachesInSync` |
| `V4` | P0 | Sync facts choose keepup vs catchup vs rebuild | different replica reported positions | New | primary owns recovery classification | expected path is chosen from facts | Covered (engine) | `sw-block/protocol/engine_test.go` | `TestSyncAck_ReplicaCaughtUp_KeepUp`, `TestSyncAck_ReplicaBehindWithinWAL_CatchUp`, `TestSyncAck_ReplicaBeyondWAL_Rebuild` |
| `V5` | P0 | Rebuild complete requires durability proof for publish | rebuild finishes but publish requires DurableLSN > 0 | New | DurableLSN=0 after rebuild blocks publish; DurableLSN>0 (from rebuild completion or barrier) enables publish | no publish without durability evidence | Covered (engine) | `sw-block/protocol/v2_ready_test.go` | `TestV2Ready_V5_RebuildCompleteNotPublishReady` — 3 cases: (1) rebuild+DurableLSN>0 → publish (engine design choice: rebuild completion is durability proof), (2) DurableLSN=0 → hard-asserts no publish, (3) barrier after DurableLSN=0 rebuild → hard-asserts publish |
| `V6` | P0 | Only one live session per replica | repeated triggers or supersede | New | no dual-contract ambiguity | at most one active session per replica | Covered | `weed/storage/blockvol/test/component/rebuild_mvp_test.go` | `TestRebuild_ControlSurface_StartSupersedeAndComplete` |
| `V7` | P0 | Session failure re-enters facts path | catchup/rebuild fail or timeout | New | no local self-escalation survives in V2 | new action always comes from fresh primary decision | Covered (server) | `weed/server/block_recovery_test.go` | `TestP16B_OnCatchUpFailed_ReentersFactDecisionForRebuild` |
| `V8` | P0 | Primary failover and old-primary rejoin | failover then rejoin | New | assignment/session/projection layering closes end-to-end | rejoined node converges and health surfaces are correct | Partial | `weed/storage/blockvol/test/component/rebuild_failover_rejoin_test.go` | `TestRebuild_R10_FailoverRejoinRebuild` — proves role swap + engine rebuild decision + CRC convergence, but does NOT verify weed/server projection or master health surface |
| `V9` | P1 | Mixed health aggregate projection | one in-sync, one rebuilding, one stale | Reuse with adapter | volume health is derived from aggregate replica state | projection matches expected degraded or healthy mode | Covered (engine) | `sw-block/protocol/v2_ready_test.go` | `TestV2Ready_V9_MixedHealthAggregateProjection` — hard-asserts: one rebuilding → needs_rebuild; all converged + barrier → publish_healthy |
| `V10` | P1 | Retention floor under active rebuild | rebuild with live WAL pressure | New | active recovery truth is reflected into WAL retention | no premature WAL loss while rebuild is active | Covered | `weed/storage/blockvol/test/component/rebuild_retention_pin_test.go` | `TestRebuild_RetentionPin_FlusherRespectsRebuildPin` |
| `V11` | P1 | Long-haul write through recovery | workload continues during fault and recovery | New | no hidden divergence across long runtime | final extent digest match | Partial | `weed/storage/blockvol/test/component/rebuild_primary_initiated_test.go` | `TestRebuild_PrimaryInitiated_1GB_WithLiveWrites` — rebuild-only, not full fault+recovery cycle |
| `V12` | P1 | Operator-triggered rebuild hint | admin or explicit rebuild assignment | New | alternate entry still converges into same session protocol | rebuild completes on same execution path | Missing | — | — |
| `V13` | P1 | Observability coherence | running V2 recovery | Reuse with adapter | logs, projections, diagnostics, and state snapshots agree | diagnostic surfaces remain aligned | Missing | — | — |
| `V14` | P1 | Negative fail-closed matrix | wrong epoch, wrong session, stale ack, wrong kind | Reuse with adapter | ambiguity always biases toward reject or degrade | explicit failure or ignore path only | Covered (engine) | `sw-block/protocol/v2_ready_test.go`, `weed/storage/blockvol/test/component/rebuild_crash_test.go` | `TestV2Ready_V14_NegativeFailClosedMatrix`, `TestRebuild_EpochMismatch_WALEntryRejected` |

### V2 Ready minimum gate

`V2 Ready` requires:

1. all `Rebuild Ready` minimum rows
2. all `Restore Ready` minimum rows
3. `V1`
4. `V2`
5. `V3`
6. `V4`
7. `V5`
8. `V6`
9. `V7`
10. `V8`

## Current Anchor Tests

The following tests are already strong anchors for the matrix and should be
treated as seed evidence instead of being replaced:

| Matrix row | Current anchor |
|---|---|
| `R2` | `weed/storage/blockvol/test/component/rebuild_primary_initiated_test.go` |
| `R4`, `R5`, `R6`, `R7` | rebuild session, transport, and executor tests under `weed/storage/blockvol/` and `weed/storage/blockvol/v2bridge/` |
| `V3`, `V6`, `V7` | focused recovery/runtime tests under `weed/server/` |
| `V1`, `V2` | existing bootstrap/workload acceptance and component packs reused from current runner and `weed/storage/blockvol/test/component/` |
| `S1`-`S4` | existing snapshot export/import tests under `weed/storage/blockvol/v2bridge/` |

## Recommended Next Pass

When turning this matrix into execution work, use this order:

1. mark existing tests against `R*`, `S*`, and `V*`
2. classify each row as `covered`, `partial`, or `missing`
3. fill `Rebuild Ready` gaps first
4. fill `Restore Ready` next
5. keep `V2 Ready` small and stage-gated, not as one giant acceptance bucket

The working rule is:

- reuse V1 execution-muscle tests whenever the contract is unchanged
- add new V2 tests only where primary-owned truth changed the entry path or the closure meaning

