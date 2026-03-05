# Phase 6 Progress

## Status
- CP6-1 complete. 54 CSI tests (25 dev + 30 QA - 1 removed).
- CP6-2 complete. 172 CP6-2 tests (118 dev/review + 54 QA). 1 QA bug found and fixed.
- **Phase 6 cumulative: 226 tests, all PASS.**

## Completed
- CP6-1 Task 0: Extracted BlockVolAdapter to shared `blockvol/adapter.go`, added DisconnectVolume to TargetServer, added Session.TargetIQN().
- CP6-1 Task 1: VolumeManager (multi-volume BlockVol + shared TargetServer lifecycle). 10 tests.
- CP6-1 Task 2: CSI Identity service (GetPluginInfo, GetPluginCapabilities, Probe). 3 tests.
- CP6-1 Task 3: CSI Controller service (CreateVolume, DeleteVolume, ValidateVolumeCapabilities). 4 tests.
- CP6-1 Task 4: CSI Node service (NodeStageVolume, NodeUnstageVolume, NodePublishVolume, NodeUnpublishVolume). 7 tests.
- CP6-1 Task 5: gRPC server + binary entry point (`csi/cmd/block-csi/main.go`).
- CP6-1 Task 6: K8s manifests (DaemonSet, StorageClass, RBAC, example PVC) + smoke-test.sh.
- CP6-1 Review fixes: 5 findings + 2 open questions resolved, 3 new tests added.
  - Finding 1: CreateVolume idempotency after restart (adopts existing .blk files on disk).
  - Finding 2: NodePublishVolume validates empty StagingTargetPath.
  - Finding 3: Resource leak cleanup on error paths (success flag + deferred CloseVolume).
  - Finding 4: Synchronous listener creation (bind errors surface immediately).
  - Finding 5: IQN collision avoidance (SHA256 hash suffix on truncation).

- CP6-1 QA adversarial: 30 tests in qa_csi_test.go. 5 bugs found and fixed:
  - BUG-QA-1 (Medium): DeleteVolume leaked .snap.* delta files. Fixed: glob+remove snapshot files.
  - BUG-QA-2 (High): Start not retryable after failure (sync.Once). Fixed: state machine.
  - BUG-QA-3 (High): Stop then Start broken (sync.Once already fired). Fixed: same state machine.
  - BUG-QA-4 (Low): CreateVolume ignored LimitBytes. Fixed: validate and cap size.
  - BUG-QA-5 (Medium): sanitizeFilename case divergence with SanitizeIQN. Fixed: lowercase both.
  - Additional: goroutine captured m.target by reference (nil after Stop). Fixed: local capture.

- CP6-2 complete. All 7 tasks done. 63 CSI tests + 48 server block tests = 111 CP6-2 tests, all PASS.

## CP6-2: Control-Plane Integration

### Completed Tasks

- **Task 0: Proto Extension + Code Generation** — block volume messages in master.proto/volume_server.proto, Go stubs regenerated, conversion helpers + 5 tests.
- **Task 1: Master Block Volume Registry** — in-memory registry with Pending→Active status tracking, full/delta heartbeat reconciliation, per-name inflight lock (TOCTOU prevention), placement (fewest volumes), block-capable server tracking. 11 tests.
- **Task 2: Volume Server Block Volume gRPC** — AllocateBlockVolume/DeleteBlockVolume gRPC handlers on VolumeServer, CreateBlockVol/DeleteBlockVol on BlockService, shared naming (blockvol/naming.go). 5 tests.
- **Task 3: Master Block Volume RPC Handlers** — CreateBlockVolume (idempotent, inflight lock, retry up to 3 servers), DeleteBlockVolume (idempotent), LookupBlockVolume. Mock VS call injection for testability. 9 tests.
- **Task 4: Heartbeat Wiring** — block volume fields in heartbeat stream, volume server sends initial full heartbeat + deltas, master processes via UpdateFullHeartbeat/UpdateDeltaHeartbeat.
- **Task 5: CSI Controller Refactor** — VolumeBackend interface (LocalVolumeBackend + MasterVolumeClient), controller uses backend instead of VolumeManager, returns volume_context with iscsiAddr+iqn, mode flag (controller/node/all). 5 backend tests.
- **Task 6: CSI Node Refactor + K8s Manifests** — Node reads volume_context for remote targets, staged volume tracking with IQN derivation fallback on restart, split K8s manifests (csi-driver.yaml, csi-controller.yaml Deployment, csi-node.yaml DaemonSet). 4 new node tests (11 total).

### New Files (CP6-2)
| File | Description |
|------|-------------|
| `blockvol/naming.go` | Shared SanitizeIQN + SanitizeFilename |
| `blockvol/naming_test.go` | 4 naming tests |
| `blockvol/block_heartbeat_proto.go` | Go wire type ↔ proto conversion |
| `blockvol/block_heartbeat_proto_test.go` | 5 conversion tests |
| `server/master_block_registry.go` | Block volume registry + placement |
| `server/master_block_registry_test.go` | 11 registry tests |
| `server/volume_grpc_block.go` | VS block volume gRPC handlers |
| `server/volume_grpc_block_test.go` | 5 VS tests |
| `server/master_grpc_server_block.go` | Master block volume RPC handlers |
| `server/master_grpc_server_block_test.go` | 9 master handler tests |
| `csi/volume_backend.go` | VolumeBackend interface + clients |
| `csi/volume_backend_test.go` | 5 backend tests |
| `csi/deploy/csi-controller.yaml` | Controller Deployment manifest |
| `csi/deploy/csi-node.yaml` | Node DaemonSet manifest |

### Modified Files (CP6-2)
| File | Changes |
|------|---------|
| `pb/master.proto` | Block volume messages, Heartbeat fields 24-27, RPCs |
| `pb/volume_server.proto` | AllocateBlockVolume, VolumeServerDeleteBlockVolume |
| `server/master_server.go` | BlockVolumeRegistry + VS call fields |
| `server/master_grpc_server.go` | Block volume heartbeat processing |
| `server/volume_grpc_client_to_master.go` | Block volume in heartbeat stream |
| `server/volume_server_block.go` | CreateBlockVol/DeleteBlockVol on BlockService |
| `csi/controller.go` | VolumeBackend instead of VolumeManager |
| `csi/controller_test.go` | Updated for VolumeBackend |
| `csi/node.go` | Remote target support + staged volume tracking |
| `csi/node_test.go` | 4 new remote target tests |
| `csi/server.go` | Mode flag, MasterAddr, VolumeBackend config |
| `csi/cmd/block-csi/main.go` | --master, --mode flags |
| `csi/deploy/csi-driver.yaml` | CSIDriver object only (split out workloads) |
| `csi/qa_csi_test.go` | Updated for VolumeBackend |

### CP6-2 Review Fixes
All findings from both reviewers addressed. 4 new tests added (118 total CP6-2 tests).

| # | Finding | Severity | Fix |
|---|---------|----------|-----|
| R1-F1 | DeleteBlockVol doesn't terminate active sessions | High | Use DisconnectVolume instead of RemoveVolume |
| R1-F2 | Block registry server list never pruned | Medium | UnmarkBlockCapable on VS disconnect in SendHeartbeat defer |
| R1-F3 | Block volume status never updates after create | Medium | Mark StatusActive immediately after successful VS allocate |
| R1-F4 | IQN generation on startup scan doesn't sanitize | Low | Apply blockvol.SanitizeIQN(name) in scan path |
| R1-F5/R2-F3 | CreateBlockVol idempotent path skips TargetServer | Medium | Re-add adapter to TargetServer on idempotent path |
| R2-F1 | UpdateFullHeartbeat doesn't update SizeBytes | Low | Copy info.VolumeSize to existing.SizeBytes |
| R2-F2 | inflightEntry.done channel is dead code | Low | Removed done channel, simplified to empty struct |
| R2-F4 | CreateBlockVolume idempotent check doesn't validate size | Medium | Return error if existing size < requested size |
| R2-F5 | Full + delta heartbeat can fire on same message | Low | Changed second `if` to `else if` + comment |
| R2-F6 | NodeUnstageVolume deletes staged entry before cleanup | Medium | Delete from staged map only after successful cleanup |

New tests: TestMaster_CreateIdempotentSizeMismatch, TestRegistry_UnmarkDeadServer, TestRegistry_FullHeartbeatUpdatesSizeBytes, TestNode_UnstageRetryKeepsStagedEntry.

### CP6-2 QA Adversarial Tests
54 tests across 2 files. 1 bug found and fixed.

| File | Tests | Areas |
|------|-------|-------|
| `server/qa_block_cp62_test.go` | 22 | Registry (8), Master RPCs (8), VS BlockService (6) |
| `csi/qa_cp62_test.go` | 32 | Node remote (6), Controller backend (5), Backend (2), Naming (2), Lifecycle (4), Server/Driver (2), VolumeManager (4), Edge cases (7) |

**BUG-QA-CP62-1 (Medium): `NewCSIDriver` accepts invalid mode strings.**
- `NewCSIDriver(DriverConfig{Mode: "invalid"})` returns nil error. Driver runs with only identity server — no controller, no node. K8s reports capabilities but all operations fail `Unimplemented`.
- Fix: Added `switch` validation after mode defaulting. Returns `"csi: invalid mode %q, must be controller/node/all"`.
- Test: `TestQA_ModeInvalid`.

**Final CP6-2 test count: 118 dev/review + 54 QA = 172 CP6-2 tests, all PASS.**

**Cumulative Phase 6 test count: 54 CP6-1 + 172 CP6-2 = 226 tests.**

## CSI Testing Ladder

| Level | What | Tools | Status |
|-------|------|-------|--------|
| 1. Unit tests | Mock iscsiadm/mount. Confirm idempotency, error handling, edge cases. | `go test` | DONE (226 tests) |
| 2. gRPC conformance | `csi-sanity` tool validates all CSI RPCs against spec. No K8s needed. | [csi-sanity](https://github.com/kubernetes-csi/csi-test) | DONE (33 pass, 58 skip) |
| 3. Integration smoke | Full iSCSI lifecycle with real filesystem (via csi-sanity "should work" tests). | csi-sanity + iscsiadm | DONE (489 SCSI cmds) |
| 4. Single-node K8s (k3s) | Deploy CSI DaemonSet on k3s. PVC → Pod → write data → delete/recreate → verify persistence. | k3s v1.34.4 | DONE |
| 5. Failure/chaos | Kill CSI controller pod; ensure no IO outage for existing volumes. Node restart with staged volumes. | chaos-mesh or manual | TODO |
| 6. K8s E2E suite | SIG-Storage tests validate provisioning, attach/detach, resize, snapshots. | `e2e.test` binary | TODO |

### Level 2: csi-sanity Conformance (M02)

**Result: 33 Passed, 0 Failed, 58 Skipped, 1 Pending.**

Run on M02 (192.168.1.184) with block-csi in local mode. Used helper scripts for staging/target path management.

Bugs found and fixed during csi-sanity:
| # | Bug | Severity | Fix |
|---|-----|----------|-----|
| BUG-SANITY-1 | CreateVolume accepted empty VolumeCapabilities | Medium | Added `len(req.VolumeCapabilities) == 0` check |
| BUG-SANITY-2 | ValidateVolumeCapabilities accepted empty VolumeCapabilities | Medium | Same check added |
| BUG-SANITY-3 | NodeStageVolume accepted nil VolumeCapability | Medium | Added nil check |
| BUG-SANITY-4 | NodePublishVolume used `mount -t ext4` instead of bind mount | High | Added BindMount method to MountUtil interface |
| BUG-SANITY-5 | NodeUnpublishVolume didn't remove target path | Medium | Added os.RemoveAll per CSI spec |
| BUG-SANITY-6 | NodeUnpublishVolume failed on unmounted path | Medium | Added IsMounted check before unmount |

All existing unit tests updated with VolumeCapabilities/VolumeCapability in test requests.

### Level 3: Integration Smoke (M02)

Verified through csi-sanity's full lifecycle tests which exercised real iSCSI:
- 489 real SCSI commands processed (READ_10, WRITE_10, SYNC_CACHE, INQUIRY, etc.)
- Full cycle: CreateVolume → NodeStageVolume (iSCSI login + mkfs.ext4 + mount) → NodePublishVolume → NodeUnpublishVolume → NodeUnstageVolume (unmount + iSCSI logout) → DeleteVolume
- Clean state verified: no leftover iSCSI sessions, mounts, or volume files

### Level 4: k3s PVC→Pod (M02)

**Result: PASS — data persists across pod deletion/recreation.**

k3s v1.34.4 single-node on M02. CSI deployed as DaemonSet with 3 containers:
1. block-csi (privileged, nsenter wrappers for host iscsiadm/mount/umount/mkfs/blkid/mountpoint)
2. csi-provisioner (v5.1.0, --node-deployment for single-node)
3. csi-node-driver-registrar (v2.12.0)

Test sequence:
1. Created PVC (100Mi, sw-block StorageClass) → Bound
2. Created pod → wrote "hello sw-block" to /data/test.txt → md5: `7be761488cf480c966077c7aca4ea3ed`
3. Deleted pod (PVC retained) → iSCSI session cleanly closed
4. Recreated pod with same PVC → read "hello sw-block" → same md5 verified
5. Appended "persistence works!" → confirmed read-write

Additional bug fixed during k3s testing:
| # | Bug | Severity | Fix |
|---|-----|----------|-----|
| BUG-K3S-1 | IsLoggedIn didn't handle iscsiadm exit code 21 (nsenter suppresses output) | Medium | Added `exitErr.ExitCode() == 21` check |

DaemonSet manifest: `learn/projects/sw-block/test/csi-k3s-node.yaml`

- CP6-3 complete. 67 CP6-3 tests. All PASS.

## CP6-3: Failover + Rebuild in Kubernetes

### Completed Tasks

- **Task 0: Proto Extension + Wire Type Updates** — Added replica_data_addr, replica_ctrl_addr to BlockVolumeInfoMessage/BlockVolumeAssignment; rebuild_addr to BlockVolumeAssignment; replica_server to Create/LookupBlockVolumeResponse; replica fields to AllocateBlockVolumeResponse. Updated wire types and converters. 8 tests.
- **Task 1: Master Assignment Queue + Delivery** — BlockAssignmentQueue with Enqueue/Peek/Confirm/ConfirmFromHeartbeat. Retain-until-confirmed pattern (F1): assignments resent on every heartbeat until VS confirms via matching (path, epoch, role). Stale epoch pruning during Peek. Wired into HeartbeatResponse delivery. 11 tests.
- **Task 2: VS Assignment Receiver Wiring** — VS extracts block_volume_assignments from HeartbeatResponse and calls BlockService.ProcessAssignments.
- **Task 3: BlockService Replication Support** — ProcessAssignments dispatches to HandleAssignment + setupPrimaryReplication/setupReplicaReceiver/startRebuild per role. ReplicationPorts deterministic hash (F3). Heartbeat reports replica addresses (F5). 9 tests.
- **Task 4: Registry Replica Tracking + CreateVolume** — Added SetReplica/ClearReplica/SwapPrimaryReplica to registry. CreateBlockVolume creates on 2 servers (primary + replica), enqueues assignments. Single-copy mode if only 1 server or replica fails (F4). LookupBlockVolume returns ReplicaServer. 10 tests.
- **Task 5: Master Failover Detection** — failoverBlockVolumes on VS disconnect. Lease-aware promotion (F2): promote only after LastLeaseGrant + LeaseTTL expires. Deferred promotion via time.AfterFunc for unexpired leases. promoteReplica swaps primary/replica, bumps epoch, enqueues new primary assignment. 11 tests.
- **Task 6: ControllerPublishVolume/UnpublishVolume** — ControllerPublishVolume calls backend.LookupVolume, returns publish_context{iscsiAddr, iqn}. ControllerUnpublishVolume is no-op. Added PUBLISH_UNPUBLISH_VOLUME capability. NodeStageVolume prefers publish_context over volume_context (reflects current primary after failover). 8 tests.
- **Task 7: Rebuild on Recovery** — recoverBlockVolumes on VS reconnect drains pendingRebuilds, sets reconnected server as replica, enqueues Rebuilding assignments. 10 tests (shared with Task 5 test file).

### Design Review Findings Addressed

| # | Finding | Severity | Resolution |
|---|---------|----------|------------|
| F1 | Assignment delivery can be dropped | Critical | Retain-until-confirmed: Peek+Confirm pattern, assignments resent every heartbeat |
| F2 | Failover without lease check → split-brain | Critical | Gate promotion on `now > lastLeaseGrant + leaseTTL`; deferred promotion for unexpired leases |
| F3 | Replication ports change on VS restart | Critical | Deterministic port = FNV hash of path, offset from base iSCSI port |
| F4 | Partial create (replica fails) | Medium | Single-copy mode with ReplicaServer="", skip replica assignments |
| F5 | UpdateFullHeartbeat ignores replica addresses | Medium | VS includes replica_data/ctrl in InfoMessage; registry updates on heartbeat |

### Code Review 1 Findings Addressed

| # | Finding | Severity | Resolution |
|---|---------|----------|------------|
| R1-1 | AllocateBlockVolume missing repl addrs | High | AllocateBlockVolume now returns ReplicaDataAddr/CtrlAddr/RebuildListenAddr from ReplicationPorts() |
| R1-2 | Primary never starts rebuild server | High | setupPrimaryReplication now calls vol.StartRebuildServer(rebuildAddr) |
| R1-3 | Assignment queue never confirms after startup | High | VS sends periodic full block heartbeat (5×sleepInterval tick) enabling master confirmation |
| R1-4 | Replica addresses not reported in heartbeat | Medium | BlockService.CollectBlockVolumeHeartbeat wraps store's collector, fills ReplicaDataAddr/CtrlAddr from replStates |
| R1-5 | Lease never refreshed after create | Medium | UpdateFullHeartbeat refreshes LastLeaseGrant on every heartbeat; periodic block heartbeats keep it current |

### Code Review 2 Findings Addressed

| # | Finding | Severity | Resolution |
|---|---------|----------|------------|
| R2-F1 | LastLeaseGrant set AFTER Register → stale-lease race | High | Moved to entry initializer BEFORE Register |
| R2-F2 | Deferred promotion timer has no cancellation | Medium | Timers stored in blockFailoverState.deferredTimers; cancelled in recoverBlockVolumes on reconnect |
| R2-F3 | SwapPrimaryReplica hardcodes uint32(1) | Medium | Changed to blockvol.RoleToWire(blockvol.RolePrimary) |
| R2-F4 | DeleteBlockVolume doesn't delete replica | Medium | Added best-effort replica delete (non-fatal if replica VS is down) |
| R2-F5 | promoteReplica reads epoch without lock | Medium | SwapPrimaryReplica now computes epoch+1 atomically inside lock, returns newEpoch |
| R2-F6 | Redundant string(server) casts | Low | Removed — servers already typed as string |
| R2-F7 | startRebuild goroutine has no feedback path | Low | Documented as future work (VS could report via heartbeat) |

### New Files (CP6-3)

| File | Description |
|------|-------------|
| `server/master_block_assignment_queue.go` | Assignment queue with retain-until-confirmed |
| `server/master_block_assignment_queue_test.go` | 11 queue tests |
| `server/master_block_failover.go` | Failover detection + rebuild on recovery |
| `server/master_block_failover_test.go` | 21 failover + rebuild tests |

### Modified Files (CP6-3)

| File | Changes |
|------|---------|
| `pb/master.proto` | Replica/rebuild fields on assignment/info/response messages |
| `pb/volume_server.proto` | Replica/rebuild fields on AllocateBlockVolumeResponse |
| `pb/master_pb/master.pb.go` | New fields + getters |
| `pb/volume_server_pb/volume_server.pb.go` | New fields + getters |
| `storage/blockvol/block_heartbeat.go` | ReplicaDataAddr/CtrlAddr on InfoMessage, RebuildAddr on Assignment |
| `storage/blockvol/block_heartbeat_proto.go` | Updated converters + AssignmentsToProto |
| `server/master_server.go` | blockAssignmentQueue, blockFailover, blockAllocResult struct |
| `server/master_grpc_server.go` | Assignment delivery in heartbeat, failover on disconnect, recovery on reconnect |
| `server/master_grpc_server_block.go` | Replica creation, assignment enqueueing, tryCreateReplica; R2-F1 LastLeaseGrant fix; R2-F4 replica delete; R2-F6 cast cleanup |
| `server/master_block_registry.go` | Replica fields, lease fields, SetReplica/ClearReplica/SwapPrimaryReplica; R2-F3 RoleToWire; R2-F5 atomic epoch; R1-5 lease refresh |
| `server/volume_grpc_client_to_master.go` | Assignment processing from HeartbeatResponse; R1-3 periodic block heartbeat tick |
| `server/volume_grpc_block.go` | R1-1 replication ports in AllocateBlockVolumeResponse |
| `server/volume_server_block.go` | ProcessAssignments, replication setup, ReplicationPorts; R1-2 StartRebuildServer; R1-4 CollectBlockVolumeHeartbeat with repl addrs |
| `server/master_block_failover.go` | R2-F2 deferred timer cancellation; R2-F5 new SwapPrimaryReplica API; R2-F7 rebuild feedback comment |
| `storage/store_blockvol.go` | WithVolume (exported) |
| `csi/controller.go` | ControllerPublishVolume/UnpublishVolume, PUBLISH_UNPUBLISH capability |
| `csi/node.go` | Prefer publish_context over volume_context |

### CP6-3 Test Count

| File | New Tests |
|------|-----------|
| `blockvol/block_heartbeat_proto_test.go` | 7 |
| `server/master_block_assignment_queue_test.go` | 11 |
| `server/volume_server_block_test.go` | 9 |
| `server/master_block_registry_test.go` | 5 |
| `server/master_grpc_server_block_test.go` | 6 |
| `server/master_block_failover_test.go` | 21 |
| `csi/controller_test.go` | 6 |
| `csi/node_test.go` | 2 |
| **Total CP6-3** | **67** |

**Cumulative Phase 6 test count: 54 CP6-1 + 172 CP6-2 + 67 CP6-3 = 293 tests.**

### CP6-3 QA Adversarial Tests
48 tests in `server/qa_block_cp63_test.go`. 1 bug found and fixed.

| Group | Tests | Areas |
|-------|-------|-------|
| Assignment Queue | 8 | Wrong epoch confirm, partial heartbeat confirm, same-path different roles, concurrent ops |
| Registry | 7 | Double swap, swap no-replica, concurrent swap+lookup, SetReplica replace, heartbeat clobber |
| Failover | 7 | Deferred cancel on reconnect, double disconnect, mixed lease states, volume deleted during timer |
| Create+Delete | 5 | Lease non-zero after create, replica delete on vol delete, replica delete failure |
| Rebuild | 3 | Double reconnect, nil failover state, full cycle |
| Integration | 2 | Failover enqueues assignment, heartbeat confirms failover assignment |
| Edge Cases | 5 | Epoch monotonic, cancel timers no rebuilds, replica server dies, empty batch |
| Master-level | 5 | Delete VS unreachable, sanitized name, concurrent create/delete, all VS fail, slow allocate |
| VS-level | 6 | Concurrent create, concurrent create/delete, delete cleans snapshots, sanitization collision, idempotent re-add, nil block service |

**BUG-QA-CP63-1 (Medium): `SetReplica` leaks old replica server in `byServer` index.**
- `SetReplica` didn't remove old replica server from `byServer` when replacing with a new one.
- Fix: Added `removeFromServer(oldReplicaServer, name)` before setting new replica (3 lines).
- Test: `TestQA_Reg_SetReplicaTwice_ReplacesOld`.

**Final CP6-3 test count: 67 dev/review + 48 QA = 115 CP6-3 tests, all PASS.**

### CP6-3 Integration Tests
8 tests in `server/integration_block_test.go`. Full cross-component flows.

| # | Test | What it proves |
|---|------|----------------|
| 1 | FailoverCSIPublish | LookupBlockVolume returns new iSCSI addr after failover |
| 2 | RebuildOnRecovery | Rebuilding assignment enqueued + heartbeat confirms it |
| 3 | AssignmentDeliveryConfirmation | Queue retains until heartbeat confirms matching (path, epoch) |
| 4 | LeaseAwarePromotion | Promotion deferred until lease TTL expires |
| 5 | ReplicaFailureSingleCopy | Single-copy mode: no replica assignments, failover is no-op |
| 6 | TransientDisconnectNoSplitBrain | Deferred timer cancelled on reconnect, no split-brain |
| 7 | FullLifecycle | 11-phase lifecycle: create→publish→confirm→failover→re-publish→recover→rebuild→delete |
| 8 | DoubleFailover | Two successive failovers: epoch 1→2→3 |
| 9 | MultiVolumeFailoverRebuild | 3 volumes, kill 1 server, rebuild all affected |

**Final CP6-3 test count: 67 dev/review + 48 QA + 8 mock integration + 3 real integration = 126 CP6-3 tests, all PASS.**

**Cumulative Phase 6 with QA: 54 CP6-1 + 172 CP6-2 + 126 CP6-3 = 352 tests.**

### CP6-3 Real Integration Tests (M02)
3 tests in `blockvol/test/cp63_test.go`, run on M02 (192.168.1.184) with real iSCSI.

**Bug found: RoleNone → RoleRebuilding transition not allowed.**
After VS restart, volume is RoleNone. Master sends Rebuilding assignment, but both
`validTransitions` (role.go) and `HandleAssignment` (promotion.go) rejected this path.
- Fix: Added `RoleRebuilding: true` to `validTransitions[RoleNone]` in role.go.
  Added `RoleNone → RoleRebuilding` case in HandleAssignment with SetEpoch + SetRole.
- Admin API: Added `action:"connect"` to `/rebuild` endpoint (starts rebuild client).

| # | Test | Time | What it proves |
|---|------|------|----------------|
| 1 | FailoverCSIAddressSwitch | 3.2s | Write A → kill primary → promote replica → re-discover at new iSCSI address → verify A → write B → verify A+B. Simulates CSI ControllerPublishVolume address-switch. |
| 2 | RebuildDataConsistency | 5.3s | Write A (replicated) → kill replica → write B (missed) → restart replica as Rebuilding → rebuild server + client → wait role→Replica → kill primary → promote rebuilt → verify A+B. Full end-to-end rebuild with data verification. |
| 3 | FullLifecycleFailoverRebuild | 6.4s | Write A → kill primary → promote → write B → rebuild old primary → write C → kill new primary → promote old → verify A+B. 11-phase lifecycle: failover→recoverBlockVolumes→rebuild. |

All 7 existing HA tests: PASS (no regression). Total real integration: 10 tests on M02.

## In Progress
- None.

## Blockers
- None.

## Next Steps
- CP6-4: Soak testing, lease renewal timers, monitoring dashboards.

## Notes
- CSI spec dependency: `github.com/container-storage-interface/spec v1.10.0`.
- Architecture: CSI binary embeds TargetServer + BlockVol in-process (loopback iSCSI).
- Interface-based ISCSIUtil/MountUtil for unit testing without real iscsiadm/mount.
- k3s deployment requires: hostNetwork, hostPID, privileged, /dev mount, nsenter wrappers for host commands.
- Known pre-existing flaky: `TestQAPhase4ACP1/role_concurrent_transitions` (unrelated to CSI).

## CP6-1 Test Catalog

### VolumeManager (`csi/volume_manager_test.go`) — 10 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | CreateOpenClose | Create, verify IQN, close, reopen lifecycle |
| 2 | DeleteRemovesFile | .blk file removed on delete |
| 3 | DuplicateCreate | Same size idempotent; different size returns ErrVolumeSizeMismatch |
| 4 | ListenAddr | Non-empty listen address after start |
| 5 | OpenNonExistent | Error on opening non-existent volume |
| 6 | CloseAlreadyClosed | Idempotent close of non-tracked volume |
| 7 | ConcurrentCreateDelete | 10 parallel create+delete, no races |
| 8 | SanitizeIQN | Special char replacement, truncation to 64 chars |
| 9 | CreateIdempotentAfterRestart | Existing .blk file adopted on restart |
| 10 | IQNCollision | Long names with same prefix get distinct IQNs via hash suffix |

### Identity (`csi/identity_test.go`) — 3 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | GetPluginInfo | Returns correct driver name + version |
| 2 | GetPluginCapabilities | Returns CONTROLLER_SERVICE capability |
| 3 | Probe | Returns ready=true |

### Controller (`csi/controller_test.go`) — 4 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | CreateVolume | Volume created and tracked |
| 2 | CreateIdempotent | Same name+size succeeds, different size returns AlreadyExists |
| 3 | DeleteVolume | Volume removed after delete |
| 4 | DeleteNotFound | Delete non-existent returns success (CSI spec) |

### Node (`csi/node_test.go`) — 7 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | StageUnstage | Full stage flow (discovery+login+mount) and unstage (unmount+logout+close) |
| 2 | PublishUnpublish | Bind mount from staging to target path |
| 3 | StageIdempotent | Already-mounted staging path returns OK without side effects |
| 4 | StageLoginFailure | iSCSI login error propagated as Internal |
| 5 | StageMkfsFailure | mkfs error propagated as Internal |
| 6 | StageLoginFailureCleanup | Volume closed after login failure (no resource leak) |
| 7 | PublishMissingStagingPath | Empty StagingTargetPath returns InvalidArgument |

### Adapter (`blockvol/adapter_test.go`) — 3 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | AdapterALUAProvider | ALUAState/TPGroupID/DeviceNAA correct values |
| 2 | RoleToALUA | All role→ALUA state mappings |
| 3 | UUIDToNAA | NAA-6 byte layout from UUID |

## CP6-2 Test Catalog

### Registry (`server/master_block_registry_test.go`) — 11 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | RegisterLookup | Register + Lookup returns entry |
| 2 | DuplicateRegister | Second register same name errors |
| 3 | Unregister | Unregister removes entry |
| 4 | ListByServer | Returns only entries for given server |
| 5 | FullHeartbeat | Marks active, removes stale, adds new |
| 6 | DeltaHeartbeat | Add/remove deltas applied correctly |
| 7 | PickServer | Fewest-volumes placement |
| 8 | Inflight | AcquireInflight blocks duplicate, ReleaseInflight unblocks |
| 9 | BlockCapable | MarkBlockCapable / UnmarkBlockCapable tracking |
| 10 | UnmarkDeadServer | R1-F2 regression test |
| 11 | FullHeartbeatUpdatesSizeBytes | R2-F1 regression test |

### Master RPCs (`server/master_grpc_server_block_test.go`) — 9 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | CreateHappyPath | Create → register → lookup works |
| 2 | CreateIdempotent | Same name+size returns same entry |
| 3 | CreateIdempotentSizeMismatch | Same name, smaller size → error |
| 4 | CreateInflightBlock | Concurrent create same name → one fails |
| 5 | Delete | Delete → VS called → unregistered |
| 6 | DeleteNotFound | Delete non-existent → success |
| 7 | Lookup | Lookup returns entry |
| 8 | LookupNotFound | Lookup non-existent → NotFound |
| 9 | CreateRetryNextServer | First VS fails → retries on next |

### VS Block gRPC (`server/volume_grpc_block_test.go`) — 5 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | Allocate | Create via gRPC returns path+iqn+addr |
| 2 | AllocateEmptyName | Empty name → error |
| 3 | AllocateZeroSize | Zero size → error |
| 4 | Delete | Delete via gRPC succeeds |
| 5 | DeleteNilService | Nil blockService → error |

### Naming (`blockvol/naming_test.go`) — 4 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | SanitizeFilename | Lowercases, replaces invalid chars |
| 2 | SanitizeIQN | Lowercases, replaces, truncates with hash |
| 3 | IQNMaxLength | 64-char names pass through unchanged |
| 4 | IQNHashDeterministic | Same input → same hash suffix |

### Proto conversion (`blockvol/block_heartbeat_proto_test.go`) — 5 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | RoundTrip | Go→proto→Go preserves all fields |
| 2 | NilSafe | Nil input → nil output |
| 3 | ShortRoundTrip | Short info round-trip |
| 4 | AssignmentRoundTrip | Assignment round-trip |
| 5 | SliceHelpers | Slice conversion helpers |

### Backend (`csi/volume_backend_test.go`) — 5 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | LocalCreate | LocalVolumeBackend.CreateVolume creates + returns info |
| 2 | LocalDelete | LocalVolumeBackend.DeleteVolume removes volume |
| 3 | LocalLookup | LocalVolumeBackend.LookupVolume returns info |
| 4 | LocalLookupNotFound | Lookup non-existent returns not-found |
| 5 | LocalDeleteNotFound | Delete non-existent returns success |

### Node remote (`csi/node_test.go` additions) — 4 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | StageRemoteTarget | volume_context drives iSCSI instead of local mgr |
| 2 | UnstageRemoteTarget | Staged map IQN used for logout |
| 3 | UnstageAfterRestart | IQN derived from iqnPrefix when staged map empty |
| 4 | UnstageRetryKeepsStagedEntry | R2-F6 regression: staged entry preserved on failure |

### QA Server (`server/qa_block_cp62_test.go`) — 22 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | Reg_FullHeartbeatCrossTalk | Heartbeat from s2 doesn't remove s1 volumes |
| 2 | Reg_FullHeartbeatEmptyServer | Empty heartbeat marks server block-capable |
| 3 | Reg_ConcurrentHeartbeatAndRegister | 10 goroutines heartbeat+register, no races |
| 4 | Reg_DeltaHeartbeatUnknownPath | Delta for unknown path is no-op |
| 5 | Reg_PickServerTiebreaker | PickServer returns first server on tie |
| 6 | Reg_ReregisterDifferentServer | Re-register same name on different server fails |
| 7 | Reg_InflightIndependence | Inflight lock for vol-a doesn't block vol-b |
| 8 | Reg_BlockCapableServersAfterUnmark | Unmark removes from block-capable list |
| 9 | Master_DeleteVSUnreachable | Delete fails if VS delete fails (no orphan) |
| 10 | Master_CreateSanitizedName | Names with special chars go through |
| 11 | Master_ConcurrentCreateDelete | Concurrent create+delete on same name, no panic |
| 12 | Master_AllVSFailNoOrphan | All 3 servers fail → error, no registry entry |
| 13 | Master_SlowAllocateBlocksSecond | Inflight lock blocks concurrent same-name create |
| 14 | Master_CreateZeroSize | Zero size → InvalidArgument |
| 15 | Master_CreateEmptyName | Empty name → InvalidArgument |
| 16 | Master_EmptyNameValidation | Whitespace-only name → InvalidArgument |
| 17 | VS_ConcurrentCreate | 20 goroutines create same vol, no crash |
| 18 | VS_ConcurrentCreateDelete | 20 goroutines create+delete interleaved |
| 19 | VS_DeleteCleansSnapshots | Delete removes .snap.* files |
| 20 | VS_SanitizationCollision | Idempotent create after sanitization matches |
| 21 | VS_CreateIdempotentReaddTarget | Idempotent create re-adds adapter to TargetServer |
| 22 | VS_GrpcNilBlockService | Nil blockService returns error (not panic) |

### QA CSI (`csi/qa_cp62_test.go`) — 32 tests
| # | Test | What it proves |
|---|------|----------------|
| 1 | Node_RemoteUnstageNoCloseVolume | Remote unstage doesn't call CloseVolume |
| 2 | Node_RemoteUnstageFailPreservesStaged | Failed unstage preserves staged entry |
| 3 | Node_ConcurrentStageUnstage | 20 concurrent stage+unstage, no races |
| 4 | Node_RemotePortalUsedCorrectly | Remote portal used for discovery (not local) |
| 5 | Node_PartialVolumeContext | Missing iqn falls back to local mgr |
| 6 | Node_UnstageNoMgrNoPrefix | No mgr + no prefix → empty IQN (graceful) |
| 7 | Ctrl_VolumeContextPresent | CreateVolume returns iscsiAddr+iqn in context |
| 8 | Ctrl_ValidateUsesBackend | ValidateVolumeCapabilities uses backend lookup |
| 9 | Ctrl_CreateLargerSizeRejected | Existing vol + larger size → AlreadyExists |
| 10 | Ctrl_ExactBlockSizeBoundary | Exact 4MB boundary succeeds |
| 11 | Ctrl_ConcurrentCreate | 10 concurrent creates, one succeeds |
| 12 | Backend_LookupAfterRestart | Volume found after VolumeManager restart |
| 13 | Backend_DeleteThenLookup | Lookup after delete → not found |
| 14 | Naming_CrossLayerConsistency | CSI and blockvol SanitizeIQN produce same result |
| 15 | Naming_LongNameHashCollision | Two 70-char names → distinct IQNs |
| 16 | RemoteLifecycleFull | Full remote stage→publish→unpublish→unstage→delete |
| 17 | ModeControllerNoMgr | Controller mode with masterAddr, no local mgr |
| 18 | ModeNodeOnly | Node mode creates mgr but no controller |
| 19 | ModeInvalid | Invalid mode → error (BUG-QA-CP62-1) |
| 20 | Srv_AllModeLocalBackend | All mode without master uses local backend |
| 21 | Srv_DoubleStop | Double Stop doesn't panic |
| 22 | VM_CreateAfterStop | Create after stop returns error |
| 23 | VM_OpenNonExistent | Open non-existent returns error |
| 24 | VM_ListenAddrAfterStop | ListenAddr after stop returns empty |
| 25 | VM_VolumeIQNSanitized | VolumeIQN applies sanitization |
| 26 | Edge_MinSize | Minimum 4MB volume succeeds |
| 27 | Edge_BelowMinSize | Below minimum → error |
| 28 | Edge_RequiredEqualsLimit | Required == limit succeeds |
| 29 | Edge_RoundingExceedsLimit | Rounding up exceeds limit → error |
| 30 | Edge_EmptyVolumeIDNode | Empty volumeID → InvalidArgument |
| 31 | Node_PublishWithoutStaging | Publish unstaged vol → still works (mock) |
| 32 | Node_DoubleUnstage | Double unstage → idempotent success |
