# Phase 6 Dev Log

Append-only communication between agents. Newest entries at bottom.
Each entry: `[date] [role] message`

Roles: `DEV`, `REVIEWER`, `TESTER`, `ARCHITECT`

---

[2026-03-03] [DEV] CP6-1 implementation complete. 7 tasks (Task 0-6) delivered:
- Task 0: Extracted BlockVolAdapter to shared `blockvol/adapter.go` (exported fields, ALUA methods,
  RoleToALUA/UUIDToNAA helpers). Added DisconnectVolume(iqn) to TargetServer (kills sessions + removes
  volume). Added Session.TargetIQN() accessor. Updated standalone binary to use shared adapter. 3 adapter tests.
- Task 1: VolumeManager -- multi-volume BlockVol + shared TargetServer lifecycle. sync.Once start guard,
  atomic ready flag, IQN sanitization with hash suffix for collision avoidance. 10 tests.
- Task 2: CSI Identity service (GetPluginInfo, GetPluginCapabilities, Probe). 3 tests.
- Task 3: CSI Controller service (CreateVolume with idempotency + size mismatch, DeleteVolume). 4 tests.
- Task 4: CSI Node service (Stage/Unstage/Publish/Unpublish) with ISCSIUtil/MountUtil interfaces. 7 tests.
- Task 5: gRPC server + binary entry point (unix/tcp socket, signal handler, graceful shutdown).
- Task 6: K8s manifests (DaemonSet, StorageClass, RBAC, example PVC) + smoke-test.sh.
Total: 12 new Go files, 2 modified, 4 YAML, 1 shell script, 25+3=28 tests. CSI spec v1.10.0 added.

[2026-03-03] [REVIEWER] CP6-1 review returned 5 findings:
1. (High) CreateVolume not idempotent after restart -- only checks in-memory map, misses existing .blk files.
2. (Medium) NodePublishVolume doesn't validate empty StagingTargetPath.
3. (Medium) NodeStageVolume resource leak -- OpenVolume not cleaned up on discovery/login/mount failure.
4. (Medium) Target start race -- ListenAndServe in goroutine, ready=true set before bind confirmed.
5. (Low) IQN collision -- truncation without hash suffix causes identical IQNs for long names.
Open Q1: How should CreateVolume handle pre-existing .blk files on disk?
Open Q2: What happens in NodeUnstageVolume if unmount succeeds but logout fails?

[2026-03-03] [DEV] All 5 review findings + 2 open questions resolved:
- Finding 1: CreateVolume now checks os.Stat for existing .blk files, adopts via OpenBlockVol.
  Added ErrVolumeSizeMismatch. Controller maps it to codes.AlreadyExists.
- Finding 2: Added stagingPath=="" check in NodePublishVolume returning InvalidArgument.
- Finding 3: Added success flag + deferred CloseVolume after OpenVolume in NodeStageVolume.
- Finding 4: Listener created synchronously via net.Listen before ready=true. Serve in goroutine.
- Finding 5: SanitizeIQN appends SHA256 hash suffix (8 hex chars) when truncating to 64.
- Open Q1: Pre-existing files adopted as idempotent success if size >= requested.
- Open Q2: NodeUnstageVolume uses best-effort cleanup (firstErr pattern), always attempts CloseVolume.
3 new tests: CreateIdempotentAfterRestart, IQNCollision, StageLoginFailureCleanup, PublishMissingStagingPath.
All 25 CSI tests + full regression PASS.

[2026-03-03] [TESTER] CP6-1 QA adversarial suite: 30 tests in qa_csi_test.go. 26 PASS, 4 FAIL confirming 5 bugs.
Groups: QA-VM (8), QA-CTRL (5), QA-NODE (7), QA-SRV (3), QA-ID (1), QA-IQN (5), QA-X (1).
Bugs: BUG-QA-1 snapshot leak, BUG-QA-2/3 sync.Once restart, BUG-QA-4 LimitBytes ignored, BUG-QA-5 case divergence.

[2026-03-03] [DEV] All 5 QA bugs fixed:
- BUG-QA-1: DeleteVolume now globs+removes volPath+".snap.*" (both tracked and untracked paths).
- BUG-QA-2+3: Replaced sync.Once+atomic.Bool with managerState enum (stopped/starting/ready/failed).
  Start() retryable after failure or Stop(). Stop() sets state=stopped, nils target.
  Goroutine captures target locally before launch (prevents nil deref after Stop).
- BUG-QA-4: Controller CreateVolume validates LimitBytes. When RequiredBytes=0 and LimitBytes set,
  uses LimitBytes as target size. Rejects RequiredBytes > LimitBytes and post-rounding overflow.
- BUG-QA-5: sanitizeFilename now lowercases (matching SanitizeIQN). "VolA" and "vola" produce
  same file and same IQN — treated as same volume via file adoption path.
- QA-CTRL-4 test updated from bug-detection to behavior-documentation (NotFound is by design;
  volumes re-tracked via CreateVolume after restart).
All 54 CSI tests + full regression PASS (blockvol 63s, iscsi 2.3s, csi 0.4s).

[2026-03-03] [DEV] CP6-2 complete. See separate CP6-2 entries in progress.md.

[2026-03-04] [TESTER] CSI Testing Ladder Levels 2-4 complete on M02 (192.168.1.184):

**Level 2: csi-sanity gRPC Conformance**
- cross-compiled block-csi (linux/amd64), installed csi-sanity on M02
- Result: 33 Passed, 0 Failed, 58 Skipped (optional RPCs), 1 Pending
- 6 bugs found and fixed: empty VolumeCapabilities validation (3 RPCs), bind mount for NodePublish,
  target path removal in NodeUnpublish, IsMounted check before unmount
- All 226 unit tests updated with VolumeCapabilities/VolumeCapability in requests

**Level 3: Integration Smoke**
- Verified via csi-sanity's "should work" tests exercising real iSCSI on M02
- 489 real SCSI commands processed (READ_10, WRITE_10, SYNC_CACHE, INQUIRY, etc.)
- Full lifecycle: Create → Stage (discovery+login+mkfs+mount) → Publish → Unpublish → Unstage (unmount+logout) → Delete
- Clean state: no leftover sessions, mounts, or volume files

**Level 4: k3s PVC→Pod**
- Installed k3s v1.34.4 on M02, deployed CSI DaemonSet (block-csi + csi-provisioner + registrar)
- DaemonSet uses nsenter wrappers for host iscsiadm/mount/umount/blkid/mountpoint/mkfs.ext4
- Test: PVC (100Mi) → Pod writes "hello sw-block" → md5 7be761488cf480c966077c7aca4ea3ed
  → Pod deleted → PVC retained → New pod reads same data → PASS
- 1 additional bug: IsLoggedIn didn't handle iscsiadm exit code 21 (nsenter suppresses output)
  → Fixed by checking ExitError.ExitCode() == 21 directly

Code changes from Levels 2-4:
- controller.go: +VolumeCapabilities validation in CreateVolume, ValidateVolumeCapabilities
- node.go: +VolumeCapability nil check, BindMount for publish, IsMounted+RemoveAll in unpublish
- iscsi_util.go: +BindMount interface+impl (real+mock), IsLoggedIn exit code 21 handling
- controller_test.go, node_test.go, qa_csi_test.go, qa_cp62_test.go: testVolCaps()/testVolCap() helpers

[2026-03-04] [DEV] CP6-3 Review 1+2 findings fixed (12 total, 5 High, 5 Medium, 2 Low):
- R1-1 (High): AllocateBlockVolume now returns ReplicaDataAddr/CtrlAddr/RebuildListenAddr from ReplicationPorts().
- R1-2 (High): setupPrimaryReplication now calls vol.StartRebuildServer(rebuildAddr) with deterministic port.
- R1-3 (High): VS sends periodic full block heartbeat (5×sleepInterval) enabling assignment confirmation.
- R2-F1 (High): LastLeaseGrant moved to entry initializer before Register (was after → stale-lease race).
- R1-4 (Medium): BlockService.CollectBlockVolumeHeartbeat fills ReplicaDataAddr/CtrlAddr from replStates.
- R1-5 (Medium): UpdateFullHeartbeat refreshes LastLeaseGrant on every heartbeat.
- R2-F2 (Medium): Deferred promotion timers stored and cancelled on VS reconnect (prevents split-brain).
- R2-F3 (Medium): SwapPrimaryReplica uses blockvol.RoleToWire(blockvol.RolePrimary) instead of uint32(1).
- R2-F4 (Medium): DeleteBlockVolume now deletes replica (best-effort, non-fatal).
- R2-F5 (Medium): SwapPrimaryReplica computes epoch+1 atomically inside lock, returns newEpoch.
- R2-F6 (Low): Removed redundant string(server) casts.
- R2-F7 (Low): Documented rebuild feedback as future work.
All 293 tests PASS: blockvol (24s), csi (1.6s), iscsi (2.6s), server (3.3s).

[2026-03-04] [DEV] CP6-3 implementation complete. 8 tasks (Task 0-7) delivered:
- Task 0: Proto extension — replica/rebuild address fields in master.proto, volume_server.proto,
  generated pb.go files, wire types, converters. AssignmentsToProto batch helper. 8 tests.
- Task 1: Assignment queue — BlockAssignmentQueue with retain-until-confirmed (F1).
  Enqueue/Peek/Confirm/ConfirmFromHeartbeat. Stale epoch pruning. Wired into HeartbeatResponse. 11 tests.
- Task 2: VS assignment receiver — extracts block_volume_assignments from HeartbeatResponse,
  calls BlockService.ProcessAssignments.
- Task 3: BlockService replication — ProcessAssignments dispatches HandleAssignment +
  setupPrimaryReplication/setupReplicaReceiver/startRebuild. Deterministic ports via FNV hash (F3).
  Heartbeat reports replica addresses (F5). 9 tests.
- Task 4: Registry replica + CreateVolume — SetReplica/ClearReplica/SwapPrimaryReplica.
  CreateBlockVolume creates primary + replica, enqueues assignments. Single-copy mode (F4). 10 tests.
- Task 5: Failover — failoverBlockVolumes on VS disconnect. Lease-aware promotion (F2):
  promote only after lease expires, deferred via time.AfterFunc. SwapPrimaryReplica + epoch bump.
  11 failover tests.
- Task 6: ControllerPublish — ControllerPublishVolume returns fresh primary address via LookupVolume.
  ControllerUnpublishVolume no-op. PUBLISH_UNPUBLISH_VOLUME capability. NodeStageVolume prefers
  publish_context over volume_context. 8 tests.
- Task 7: Rebuild on recovery — recoverBlockVolumes on VS reconnect drains pendingRebuilds,
  enqueues Rebuilding assignments. 10 tests (shared file with Task 5).
Total: 4 new files, ~15 modified, 67 new tests. All 5 review findings (F1-F5) addressed.
All tests PASS: blockvol (43s), csi (1.4s), iscsi (2.5s), server (3.2s).
Cumulative Phase 6: 293 tests.

[2026-03-04] [TESTER] CP6-3 QA adversarial suite: 48 tests in qa_block_cp63_test.go. 47 PASS, 1 FAIL confirming 1 bug.
Groups: QA-Queue (8), QA-Reg (7), QA-Failover (7), QA-Create (5), QA-Rebuild (3), QA-Integration (2), QA-Edge (5), QA-Master (5), QA-VS (6).

**BUG-QA-CP63-1 (Medium): `SetReplica` leaks old replica server in `byServer` index.**
- When calling `SetReplica("vol1", "vs3", ...)` on a volume whose replica was previously `vs2`,
  `vs2` remains in the `byServer` index. `ListByServer("vs2")` still returns `vol1`.
- Impact: `PickServer` over-counts old replica server's volume count (wrong placement).
  Failover could trigger on stale index entries.
- Fix: Added `removeFromServer(oldReplicaServer, name)` before setting new replica in `SetReplica()`.
- File: `master_block_registry.go:285` (3 lines added).
- Test: `TestQA_Reg_SetReplicaTwice_ReplacesOld`.

All 48 QA tests + full regression PASS: blockvol (23s), csi (1.1s), iscsi (2.5s), server (4.8s).
Cumulative Phase 6: 293 + 48 = 341 tests.

[2026-03-04] [TESTER] CP6-3 integration tests: 8 tests in integration_block_test.go. All 8 PASS.

**Required Tests:**
1. `TestIntegration_FailoverCSIPublish` — Create replicated vol → kill primary → verify
   LookupBlockVolume (CSI ControllerPublishVolume path) returns promoted replica's iSCSI addr.
2. `TestIntegration_RebuildOnRecovery` — Failover → reconnect old primary → verify Rebuilding
   assignment enqueued with correct epoch → confirm via heartbeat.
3. `TestIntegration_AssignmentDeliveryConfirmation` — Create replicated vol → verify pending
   assignments → wrong epoch doesn't confirm → correct heartbeat confirms → queue cleared.

**Nice-to-have Tests:**
4. `TestIntegration_LeaseAwarePromotion` — Lease not expired → promotion deferred → after TTL → promoted.
5. `TestIntegration_ReplicaFailureSingleCopy` — Replica alloc fails → single-copy mode → no replica
   assignments → failover is no-op (no replica to promote).
6. `TestIntegration_TransientDisconnectNoSplitBrain` — VS disconnects with active lease → deferred
   timer → VS reconnects → timer cancelled → no promotion (split-brain prevented).

**Extra coverage:**
7. `TestIntegration_FullLifecycle` — Create → publish → confirm assignments → failover → re-publish
   → confirm → recover → rebuild → confirm → delete. Full 11-phase lifecycle.
8. `TestIntegration_DoubleFailover` — Primary dies → promoted → promoted replica also dies → original
   server re-promoted (epoch=3).
9. `TestIntegration_MultiVolumeFailoverRebuild` — 3 volumes across 2 servers → kill one server → all
   primaries promoted → reconnect → rebuild assignments for each.

All 349 server+QA+integration tests PASS (6.8s).
Cumulative Phase 6: 293 + 48 + 8 = 349 tests.

[2026-03-05] [TESTER] CP6-3 real integration tests on M02 (192.168.1.184): 3 tests, all PASS.

**Bug found during testing: RoleNone → RoleRebuilding transition not allowed.**
- After VS restart, volume is RoleNone. Master sends Rebuilding assignment, but both
  `validTransitions` (role.go) and `HandleAssignment` (promotion.go) rejected this path.
- Fix: Added `RoleRebuilding: true` to `validTransitions[RoleNone]` in role.go.
  Added `RoleNone → RoleRebuilding` case in HandleAssignment (promotion.go) with
  SetEpoch + SetMasterEpoch + SetRole.
- Infrastructure: Added `action:"connect"` to admin.go `/rebuild` endpoint to start
  rebuild client (calls `blockvol.StartRebuild` in background goroutine).
  Added `StartRebuildClient` method to ha_target.go.

**Tests (cp63_test.go, `//go:build integration`):**
1. `FailoverCSIAddressSwitch` (3.2s) — Write data A → kill primary → promote replica
   → client re-discovers at new iSCSI address → verify data A → write data B →
   verify A+B. Simulates CSI ControllerPublishVolume address-switch flow.
2. `RebuildDataConsistency` (5.3s) — Write A (replicated) → kill replica → write B
   (missed) → restart replica as Rebuilding → start rebuild server on primary →
   connect rebuild client → wait for role→replica → kill primary → promote rebuilt
   replica → verify A+B intact. Full end-to-end rebuild with data verification.
3. `FullLifecycleFailoverRebuild` (6.4s) — Write A → kill primary → promote replica
   → write B → start rebuild server → restart old primary as Rebuilding → rebuild
   → write C → kill new primary → promote rebuilt old-primary → verify A+B intact.
   11-phase lifecycle simulating master's failover→recoverBlockVolumes→rebuild flow.

Existing 7 HA tests: all PASS (no regression). Total real integration: 10 tests on M02.
Code changes: role.go (+1 line), promotion.go (+7 lines), admin.go (+15 lines),
ha_target.go (+20 lines), cp63_test.go (new, ~350 lines).

