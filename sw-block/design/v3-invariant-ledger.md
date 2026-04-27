# V3 Invariant Ledger

**Date**: 2026-04-20
**Status**: **SHELL** — Pre-T1 Gate deliverable P3 (see `v3-phase-15-qa-system.md` §9.1)
**Primary owner**: QA Lead
**Co-owner**: V3 Architect
**Purpose**: living table of every engine invariant V3 claims, with pointer(s) to the re-runnable test(s) that prove it, and the most recent verification timestamp

---

## 1. What This Ledger Is

This is the **single source of truth** for answering the question:

> **"Which V3 engine claims are actually tested, and when were they last verified?"**

Rules (from [`v3-quality-system.md`](./v3-quality-system.md) §6):

1. Every invariant listed in [`v3-semantic-constraint-checklist.md`](./v3-semantic-constraint-checklist.md) **must have a ledger row**
2. Every ledger row **must point to at least one concrete test**
3. Every release candidate **runs every ledger-listed test** and stamps the "Last verified" column
4. A protocol evolution (PEP) **must include a ledger diff**
5. **An invariant without a test is a wish**, not an invariant — either add a test, or remove the row

---

## 2. Row ID Scheme

Row IDs use one of these prefixes:

| Prefix | Source | Example |
|--------|--------|---------|
| `INV-` | Engine invariant from `v3-semantic-constraint-checklist.md` | `INV-AUTH-001` |
| `EXT-` | External failure taxonomy entry from `external-failure-taxonomy.md` | `EXT-CEPH-MON-001` |
| `PCDD-` | PCDD bad-state family from 14A sidecar / closed phases | `PCDD-STALE-POLICY-001` |
| `V2SCN-` | V1/V2 testrunner scenario inheritance | `V2SCN-HA-FAILOVER-001` |

**Sub-family conventions**:
- `INV-AUTH-*` — authority / epoch / role transitions
- `INV-FENCE-*` — fencing / stale primary prevention
- `INV-PUB-*` — publication intents / convergence
- `INV-SESSION-*` — session terminal truth
- `INV-OBS-*` — observation institution
- `INV-CONV-*` — convergence honesty
- `INV-RESTART-*` — restart / durable authority
- `INV-REMOVE-*` — removal / demotion

IDs are **append-only**. If an invariant is retired, move its row to §6 (archive) with the retirement reason; do not renumber remaining rows.

---

## 3. Ledger

**Status column meaning**:
- `SHELL` — example row included to establish the schema (not load-bearing for P15 yet)
- `ACTIVE` — fully populated; test exists and has been run
- `STUB` — row exists but test pointer is TBD (must be resolved during P15 QA establishment)
- `ARCHIVED` — retired; see §6

| ID | Statement | First introduced | Owner layer | Test(s) | Last verified | Status |
|----|-----------|------------------|-------------|---------|---------------|--------|
| `INV-AUTH-001` | Authority line is monotonic per volume; no roll-back of a per-volume authority line once advanced | P14 S5 | engine | `core/host/t0_l2_subprocess_test.go:TestT0Process_KillRestartCycle20`, `core/host/g1_adversarial_test.go:TestG1_ReconnectCatchesCurrentLine_NoReverse`, `core/host/route_test.go:TestT0Route_*` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `INV-FENCE-001` | No stale primary can emit a fresh decision after fence has been published for that primary | P14 S3 | adapter + engine | (pending G3+ tracks) | — | STUB |
| `INV-PUB-001` | Publication intents are semantically one-way; `IntentReassign` / `IntentRefreshEndpoint` never silently reverse | P14 S3 / S6 | engine | (pending G3+ tracks) | — | STUB |
| `INV-SESSION-001` | Session terminal truth is narrow: recovery is not "successful" until explicit session close says so | Phase 07 | engine | (pending G3+ tracks) | — | STUB |
| `INV-RESTART-001` | On restart, durable authority state reconstructs the current per-volume line without reviving old truth | P14 S5 | engine + persistence | `core/host/t0_l2_subprocess_test.go:TestT0Process_KillRestartCycle20`, `core/host/t0_l2_subprocess_test.go:TestT0Process_KillRestartCycle20_Volume`, `core/host/t0_l2_subprocess_test.go:TestT0Process_DeadMasterReconnect`, `core/authority/authority_store_concurrent_test.go:TestAuthorityStore_ConcurrentWriteReload_100Cycles` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `INV-HOST-001` | Product process (cmd/blockmaster + cmd/blockvolume) hosts P14 authority without direct assignment stuffing; adapter.AssignmentInfo construction confined to named decode helper | P15 T0 | host | `core/host/t0_l2_subprocess_test.go:TestT0Process_RealMasterVolumeRoute`, `core/host/volume/boundary_guard_test.go:TestNoOtherAssignmentInfoConstruction`, `core/authority/authority_test.go:TestNonForgeability_*`, `core/authority/policy_test.go:TestPolicy_NoDirectAssignmentInfoConstruction` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `INV-RPC-001` | Master-volume RPC carries observation and published assignment only; no authority-minting fields accepted on wire | P15 T0 | protocol | `core/host/master/services.go:validateHeartbeat`, `core/host/g1_adversarial_test.go:TestG1_MalformedHeartbeat_Rejected`, proto file `core/rpc/proto/v3_control.proto` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `INV-OBS-REALPROC-001` | Volume observation reaches master observation host in a real separate process, not via in-process helper | P15 T0 | host | `core/host/t0_l2_subprocess_test.go:TestT0Process_RealMasterVolumeRoute` (real `cmd/blockmaster` + `cmd/blockvolume` binaries over TCP gRPC) | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-STUFFING-001` | No product path mints, synthesizes, or mutates `adapter.AssignmentInfo` outside `core/authority/*`; sole permitted decode site is `core/host/volume/subscribe.go:decodeAssignmentFact` (field-copy only) | P15 T0 | host (decode boundary) | `core/host/volume/boundary_guard_test.go:TestNoOtherAssignmentInfoConstruction` (AST/grep-based static guard), `core/authority/authority_test.go:TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority`, `core/authority/policy_test.go:TestPolicy_NoDirectAssignmentInfoConstruction` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-STALE-HB-001` | Stale heartbeat (older epoch) ignored by authority; no regression of current line | P14 S3 + P15 T0 | engine (ingestion) | `core/host/g1_adversarial_test.go:TestG1_StaleHeartbeat_IgnoredNotApplied` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-DELAYED-HB-001` | Delayed heartbeat arriving after supersession recorded as superseded (diagnostic counter), not applied; no assignment reverse | P14 S6 + P15 T0 | engine (observation) | `core/host/g1_adversarial_test.go:TestG1_DelayedHeartbeat_SupersededNotReverted` (asserts `supersededHeartbeatCount` advances while authority tuple unchanged) | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-MALFORMED-001` | Malformed RPC (nil slot, empty DataAddr/CtrlAddr, missing required fields) rejected with named-field error; no partial ingest | P15 T0 | protocol (validation) | `core/host/g1_adversarial_test.go:TestG1_MalformedHeartbeat_Rejected`, `core/host/master/services.go:validateHeartbeat` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-DEAD-PEER-001` | Volume silent beyond freshness window → master marks replica unreachable in topology within bounded time; evidence surface reflects unreachable | P15 T0 | host (observation) | `core/host/g1_adversarial_test.go:TestG1_DeadPeer_BoundedDetection` (via `ObservationHost.SetNowForTest` clock seam) | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-RPC-TIMEOUT-001` | Slow subscriber detected and disconnected after bounded timeout; master does not queue unbounded state | P15 T0 | protocol (stream mgmt) | `core/host/g1_adversarial_test.go:TestG1_RPCTimeout_FailClosed` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-CONCURRENT-HB-001` | Concurrent heartbeats from same (volume, replica) serialized; final state reflects one fully, never torn | P15 T0 | engine (concurrency) | `core/host/g1_adversarial_test.go:TestG1_ConcurrentHeartbeatSameReplica_Serialized` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-SUB-TIMING-001` | Volume subscribe before master ready: subscription blocks or returns retry signal; never receives empty/placeholder assignment | P15 T0 | host (lifecycle) | `core/host/g1_adversarial_test.go:TestG1_VolumeSubscribeBeforeMasterReady_BoundedWait`, `core/host/g1_adversarial_test.go:TestG1_SubscribeBeforeTopologyKnown_FailsFastWithNamedError` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `PCDD-STORE-RACE-001` | Concurrent writes to durable authority store: rename retry classifies retryable errors (ACCESS_DENIED / SHARING_VIOLATION) vs real failures; no silent absorption; monotonic WriteSeq preserved under 4-writer contention | P15 T0 | persistence | `core/authority/authority_store_concurrent_test.go:TestAuthorityStore_ConcurrentWriteReload_100Cycles` (100 cycles × 4 writers × 4 readers × 8 volumes), `core/authority/authority_store_rename.go:renameWithRetry` | 2026-04-21 (P15 T0 closure) | ACTIVE |
| `INV-FRONTEND-001` | Frontend readiness derives from **V3 adapter projection** (`VolumeReplicaAdapter.Projection()`); provider cannot mint authority or select primary; open is blocked until projection reports healthy | P15 T1 | host (frontend) | Unit: `core/frontend/provider_test.go:TestProvider_OpenHealthyProjection_ReturnsBackend`, `TestProvider_OpenNotReady_ReturnsErrNotReady`. Component: `core/frontend/t1_route_test.go:TestT1Route_HealthyOpenWriteStaleRejectNewOpen_EpochDriftOnly` (rev-4: now uses real `AdapterProjectionView` wrapping `VolumeReplicaAdapter.Projection()` via `HealthyPathExecutor`); `core/host/volume/projection_bridge_test.go:TestAdapterProjectionView_*`. Integration: `core/frontend/t1_l2_subprocess_test.go:TestT1Process_FrontendProviderOverProductRoute` (real `cmd/blockmaster` + 3× `cmd/blockvolume` over TCP + HTTP `/status`); `core/host/volume/supersede_integration_test.go:TestHost_RecordOtherLine_*` (supersede fail-closed chain) | 2026-04-21 (P15 T1-CLOSED rev-4) | **ACTIVE (Unit + Component + Integration)** |
| `INV-FRONTEND-SEAM-001` | Frontend `ProjectionView` seam correctly enforces per-I/O stale fence regardless of specific view implementation; any drift in observed lineage rejects read/write | P15 T1 | host (frontend) | `core/frontend/t1_route_test.go:TestT1Route_HealthyOpenWriteStaleRejectNewOpen_EpochDriftOnly` — Component-layer seam invariant (now layered with real adapter projection view as of rev-4, but the seam property is distinct from the source-of-truth property) | 2026-04-21 (P15 T1-CLOSED) | ACTIVE (Component) |
| `INV-FRONTEND-002` | Stale primary cannot serve reads or writes after authority moves; every Read/Write re-validates full lineage (VolumeID + ReplicaID + Epoch + EndpointVersion + Healthy) — **facet-split coverage (see facet rows below)** | P15 T1 | host (frontend) | See facet rows: `INV-FRONTEND-002.EPOCH`, `.EV`, `.REPLICA`, `.HEALTHY` | 2026-04-21 (P15 T1-CLOSED rev-4) | **PARTIAL (facet-split — see below)** |
| `INV-FRONTEND-002.EPOCH` | Backend re-validates Epoch on every Read/Write; epoch advance → `ErrStalePrimary` | P15 T1 | host (frontend) | Unit: `backend_test.go:TestBackend_WriteStaleLineage_ReturnsErrStalePrimary/epoch_advance`, `TestBackend_ReadStaleLineage_ReturnsErrStalePrimary`. Component: `t1_route_test.go:TestT1Route_*_EpochDriftOnly`. Integration: covered via EV drift path in L2 (epoch advance is detected by same lineage compare). | 2026-04-21 (T1-CLOSED) | **ACTIVE (Unit + Component + Integration)** |
| `INV-FRONTEND-002.EV` | Backend re-validates EndpointVersion on every Read/Write; EV advance → `ErrStalePrimary` | P15 T1 | host (frontend) | Unit: `backend_test.go:TestBackend_WriteStaleLineage_ReturnsErrStalePrimary/endpoint_version_advance`. Integration: `t1_l2_subprocess_test.go:TestT1Process_FrontendProviderOverProductRoute` (real product-route `IntentRefreshEndpoint` triggers EV advance → old backend rejects) | 2026-04-21 (T1-CLOSED) | **ACTIVE (Unit + Integration); Component STUB** (L1 harness does not manipulate EV independently of epoch; deferred to T5 if needed) |
| `INV-FRONTEND-002.REPLICA` | Backend re-validates ReplicaID on every Read/Write; cross-replica primary change (r1 → r2) → `ErrStalePrimary` | P15 T1 | host (frontend) | Unit: `backend_test.go:TestBackend_WriteStaleLineage_ReturnsErrStalePrimary/replica_change`. Component: `supersede_integration_test.go:TestHost_RecordOtherLine_FlipsProjectionHealthyToFalse` (real Host.recordOtherLine → IsSuperseded → AdapterProjectionView chain) | 2026-04-21 (T1-CLOSED; L2 deferred) | **ACTIVE (Unit + Component); STUB (Integration)** — deferred to G8/T5 (TopologyController-driven cross-replica reassign at L2 is failover-continuity scope, not T1) |
| `INV-FRONTEND-002.HEALTHY` | Backend re-validates Healthy on every Read/Write; health loss → `ErrStalePrimary` | P15 T1 | host (frontend) | Unit: `backend_test.go:TestBackend_WriteStaleLineage_ReturnsErrStalePrimary/health_loss`. Component: `supersede_integration_test.go` (supersede flips Healthy to false) | 2026-04-21 (T1-CLOSED) | ACTIVE (Unit + Component); Integration deferred to G8/T5 |
| `PCDD-STALE-PRIMARY-READ-001` | Read after authority lineage drift returns `ErrStalePrimary` immediately; no stale bytes returned | P15 T1 | host (frontend) | `core/frontend/backend_test.go:TestBackend_ReadStaleLineage_ReturnsErrStalePrimary` | 2026-04-21 (P15 T1 contract-ready) | ACTIVE |
| `PCDD-STALE-PRIMARY-WRITE-001` | Write after authority lineage drift returns `ErrStalePrimary` immediately; no bytes accepted; covers epoch / endpoint-version / replica / health drift (4 subcases) | P15 T1 | host (frontend) | `core/frontend/backend_test.go:TestBackend_WriteStaleLineage_ReturnsErrStalePrimary` (4 subtests: epoch_advance, endpoint_version_advance, replica_change, health_loss) | 2026-04-21 (P15 T1 contract-ready) | ACTIVE |
| `PCDD-BACKEND-CLOSED-USE-001` | Read/Write after Close returns `ErrBackendClosed`; no panic, no silent no-op; Close is idempotent | P15 T1 | host (frontend) | `core/frontend/backend_test.go:TestBackend_Close_ReturnsErrBackendClosed` | 2026-04-21 (P15 T1 contract-ready) | ACTIVE |
| `PCDD-FRONTEND-NOT-READY-001` | Open on unhealthy projection within deadline returns `ErrNotReady`; no partial backend leaks | P15 T1 | host (frontend) | `core/frontend/provider_test.go:TestProvider_OpenNotReady_ReturnsErrNotReady` | 2026-04-21 (P15 T1 contract-ready) | ACTIVE |
| `V2SCN-DATAPATH-001` | V2 data-path scenario classified as T1 contract-smoke only; failover data continuity remains G8 (T5+); no claim that pre-move bytes survive on new primary via T1 memback | P15 T1 | scenario audit | Audit entry in `v3-phase-15-t1-closure-report.md`; `core/frontend/t1_route_test.go` contains explicit `EXPLICIT NON-CLAIM` comment asserting G8 boundary | 2026-04-21 (P15 T1 contract-ready) | ACTIVE (audit) |
| `PCDD-ISCSI-VPD00-ADVERTISED-LIST-001` | VPD 0x00 advertised-pages list equals exactly {0x00, 0x80, 0x83}; every advertised page actually serves; forbidden pages (0xB0, 0xB2) reject | P15 T2 Batch 10.5 | frontend (iscsi) | `core/frontend/iscsi/t2_v2port_scsi_inquiry_test.go:TestT2V2Port_SCSI_InquiryVPD00_AdvertisesOnlyImplemented` | 2026-04-22 (T2 Batch 10.5) | ACTIVE |
| `PCDD-ISCSI-VPD83-NAA-DETERMINISM-001` | VPD 0x83 NAA-6 derivation is deterministic per VolumeID and collision-free across VolumeIDs; EUI-64 consistency; empty VolumeID produces non-zero NAA | P15 T2 Batch 10.5 | frontend (iscsi) | `core/frontend/iscsi/t2_v2port_scsi_inquiry_test.go:TestT2V2Port_SCSI_InquiryVPD83_NAADerivationDeterministic` (3 sub-tests) | 2026-04-22 (T2 Batch 10.5) | ACTIVE |
| `PCDD-ISCSI-VPD80-SERIAL-DETERMINISM-001` | VPD 0x80 Serial derivation symmetric to NAA: same VolumeID → same Serial; different → different; hex shape; operator override passthrough | P15 T2 Batch 10.5 addendum A | frontend (iscsi) | `core/frontend/iscsi/t2_v2port_scsi_inquiry_test.go:TestT2V2Port_SCSI_InquiryVPD80_SerialDerivationDeterministic` (4 sub-tests) | 2026-04-22 (T2 Batch 10.5 addendum A) | ACTIVE |
| `PCDD-NVME-IDENTIFY-NS-NGUID-DETERMINISM-001` | NVMe Identify NS NGUID/EUI-64 derived from (SubsysNQN, VolumeID); deterministic + cross-volume + cross-subsystem collision guards; EUI-64 == NGUID[:8]; non-zero on empty VolumeID | P15 T2 Batch 11a | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_identify_ns_test.go:TestT2V2Port_NVMe_IdentifyNS_NGUIDDerivationDeterministic` (6 sub-tests) | 2026-04-22 (T2 Batch 11a) | ACTIVE |
| `PCDD-NVME-IDENTIFY-CTRL-ADVERTISED-LIST-001` | Advertised ≡ implemented for OACS / OAES / ONCS / CMIC / ANACAP / ANAGRPMAX / NANAGRPID / NS.ANAGRPID; all zero in 11a/11b (ANA deferred 11c-conditional); OAES ↔ ANA cross-consistency | P15 T2 Batch 11a | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_identify_ctrl_test.go` (3 tests: OACSAllBitsZero + OAESAllBitsZero + OAESANACrossConsistency) + sw `identify_test.go:ONCSAllZero + ANAFieldsAllZero` | 2026-04-22 (T2 Batch 11a) | ACTIVE |
| `PCDD-NVME-FABRIC-CNTLID-ECHO-001` | Admin Connect allocates monotonic CNTLID; IO Connect must echo; wrong CNTLID → SCT=1 SC=0x82; multi-host allocation is collision-free; Target-scoped (not process-scoped) | P15 T2 Batch 11a | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_fabric_cntlid_test.go` (3 tests: MultipleHostsGetDistinctCNTLIDs + CNTLIDMonotonicallyIncreasing + CNTLIDSeparateTargetsStartFresh) + sw `identify_test.go:IOConnect_UnknownCNTLID_Rejected + AdminConnect_PresetCNTLID_Rejected` | 2026-04-22 (T2 Batch 11a) | ACTIVE |
| `PCDD-NVME-IO-NO-TARGET-RETRY-001` | Target returns IO errors to host verbatim; backend.WriteCalls == 1 on injected fault; no persistent gate; symmetric for reads (no amplification) | P15 T2 Batch 11b | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_no_retry_test.go:TestT2V2Port_NVMe_IO_ErrorsReturnedVerbatim` (3 sub-tests) | 2026-04-22 (T2 Batch 11b) | ACTIVE |
| `PCDD-NVME-IO-H2CDATA-CHUNKED-R2T-001` | Multi-chunk H2CData collection correct under kernel pipelining; V2-strict single-R2T-outstanding invariant holds at unit + real m01 kernel; `r2t exceeded` does NOT reappear | P15 T2 Batch 11c (BUG-001) | frontend (nvme) | Unit: `core/frontend/nvme/t2_v2port_nvme_pipelined_writes_test.go` + `t2_v2port_nvme_pipeline_extended_test.go` + `t2_v2port_nvme_write_chunked_r2t_test.go`. Integration: m01 fs-workload via `scripts/iterate-m01-nvme.sh` — Matrix A (10 cycles) + Matrix B (65 writes across 32K/256K/1M) clean 2026-04-22 | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-FABRIC-DISCONNECT-001` | Fabric Disconnect handler cleanly tears down session; no "Invalid Opcode" response; idempotent on already-closed session | P15 T2 Batch 11c (BUG-002) | frontend (nvme) | `core/frontend/nvme/fabric.go:handleDisconnect` + sw test in `fabric_property_test.go` | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-FABRIC-CONNECT-KATO-001` | KATO in Fabric Connect CDW12 is plumbed into `ctrl.kato`; Get Features round-trip matches stored value; Set Features 0x0F last-write-wins vs Connect CDW12 | P15 T2 Batch 11c (BUG-003) | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_connect_kato_test.go` (3 tests) | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-SQHD-WRAP-001` | Submission Queue Head Doorbell wraps at queueSize (parsed from Connect SQSIZE); no unbounded monotonic counter | P15 T2 Batch 11c (BUG-004) | frontend (nvme) | `core/frontend/nvme/session.go:advanceSQHD` + sw unit test | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-SESSION-RECONNECT-LOOP-001` | 50 attach/write/readback/detach cycles complete cleanly; no goroutine leak (delta < +20); integrity holds per-iteration payload distinct | P15 T2 (L1B-1 early-landed per §8C QA capacity) | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_reconnect_loop_test.go:TestT2V2Port_NVMe_Process_ReconnectLoop50` (L2 subprocess) | 2026-04-22 | ACTIVE |
| `PCDD-NVME-SESSION-DISCONNECT-MID-R2T-001` | Abrupt TCP close mid-H2CData-stream (after N of M chunks sent) unwinds server's recvH2CData loop cleanly; no panic, no partial data to backend, no goroutine leak | P15 T2 (L1B-4 early-landed) | frontend (nvme) | `core/frontend/nvme/t2_v2port_nvme_disconnect_mid_r2t_test.go:TestT2V2Port_NVMe_IO_DisconnectMidH2CDataStream` | 2026-04-22 | ACTIVE |
| `PCDD-NVME-IO-CONCURRENT-QUEUE-STRESS-001` | 8 queues × 50 iters × 32 KiB writes = 400 concurrent writes; backend.calls and backend.bytes exact; no deadlock, no goroutine leak | P15 T2 Batch 11c sign-prep (L1-A) | frontend (nvme) | `core/frontend/nvme/t2_a_concurrent_queue_stress_test.go` | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-IO-LARGE-WRITE-MULTI-R2T-001` | Writes at 64K / 128K / 256K / 512K / 1M (5 sizes × 100 iters sequential) preserve single-R2T-per-cmd invariant; round-trip byte-exact integrity | P15 T2 Batch 11c sign-prep (L1-A) | frontend (nvme) | `core/frontend/nvme/t2_a_large_write_stress_test.go` | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `PCDD-NVME-SCENARIO-SMOKE-001` | Declarative scenario (YAML spec + Go replay) covers attach / mkfs-simulation / write / read / size mix / overwrite / clean detach; CI-runnable | P15 T2 Batch 11c sign-prep (L2-A) | frontend (nvme) / scenario | `testrunner/scenarios/testdata/t2-nvme-smoke.yaml` + `core/frontend/nvme/t2_l2_nvme_smoke_replay_test.go:TestT2Scenario_NVMeSmoke` | 2026-04-22 (T2 Batch 11c) | ACTIVE |
| `INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT` | Binary doesn't declare role via CLI; role is per-volume per-assignment from master via `fact.ReplicaID == self.ReplicaID`. Master is sole authority for role binding (T4d §B + INV-AUTH-*). Volume-scoped fan-out delivers same fact to all subscribers; each self-determines | P15 G5-4 | host (binary) | `cmd/blockvolume/g5_4_l2_replication_test.go:TestG54_BinaryWiring_RoleSplit_2NodeSmoke` (real `cmd/blockmaster` + 2× `cmd/blockvolume` over TCP, asserts primary reaches Healthy=true via fact.ReplicaID match while replica records supersede on the same fact) | 2026-04-26 (G5-4 close) | ACTIVE |
| `INV-BIN-WIRING-PEER-SET-FROM-ASSIGNMENT-FACT` | Peer addresses come from `AssignmentFact.Peers` (master-minted); binary never accumulates peers from local observation (option R rejected per T4a-5.0). `decodeReplicaTargets` (`core/host/volume/subscribe.go`) is the sole permitted decode path | P15 G5-4 | host (binary) | `cmd/blockvolume/g5_4_l2_replication_test.go:TestG54_BinaryWiring_RoleSplit_2NodeSmoke` (the real master mint→fan-out→host.applyFact→decodeReplicaTargets→ReplicationVolume.UpdateReplicaSet chain runs end-to-end through subprocess binaries; assertions on Healthy=true on primary verify the chain reached UpdateReplicaSet AND adapter.OnAssignment). Backstop: `core/host/volume/boundary_guard_test.go:TestNoOtherAssignmentInfoConstruction` (AST fence on stuffing) | 2026-04-26 (G5-4 close) | ACTIVE |
| `INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO` | `ReplicaListener` Stop runs BEFORE durable storage close in `cmd/blockvolume/main.go` shutdown chain (LIFO with construction). Listener borrows `LogicalStorage` from `DurableProvider`; tearing down storage first would race with in-flight handler conns | P15 G5-4 | host (binary lifecycle) | `cmd/blockvolume/g5_4_l2_replication_test.go:TestG54_BinaryWiring_RoleSplit_2NodeSmoke` (TCP dial on replica's `--data-addr` succeeds during host lifetime — pins the listener IS bound during the data-addr window). Source-side check: `cmd/blockvolume/main.go` shutdown block — `replListen.Stop()` before `durableProv.Close()` | 2026-04-26 (G5-4 close) | ACTIVE |
| `INV-BIN-WIRING-ASSIGNMENT-DRIVES-MEMBERPRESENT` | Binary doesn't fake `MemberPresent`; it waits for first `AssignmentFact` with `fact.ReplicaID == self.ReplicaID` to set it via the engine apply path. Replica role (no self-named fact) keeps `MemberPresent=false` indefinitely (correct: replica isn't frontend-primary-write-ready) | P15 G5-4 | engine + host (binary) | `cmd/blockvolume/g5_4_l2_replication_test.go:TestG54_BinaryWiring_RoleSplit_2NodeSmoke` (replica's status response shows `Healthy=false` even after 10s — pins MemberPresent never flips for replica role; primary's `Healthy=true` pins MemberPresent flips on self-named fact). Backstop: `core/engine/apply.go:applyAssignment` test coverage in `apply_test.go` | 2026-04-26 (G5-4 close) | ACTIVE |
| `INV-BIN-WIRING-SESSIONID-VIA-ADAPTER` | Binary path mints sessionIDs ONLY through `core/adapter` (process-wide `sessionIDCounter atomic.Uint64` at `adapter.go:70`); MUST NOT bypass adapter with hardcoded sessionIDs the way component-framework shortcuts (`WithLiveShip`, `CatchUpReplica`) do. Adapter-routed dispatch is the production path; framework shortcuts are test conveniences with a known sessionID-collision gap (T4c §I carry; QA G5-1 round 1 SKIP). Pinning this invariant ensures the gap stays test-side | P15 G5-4 | adapter | `cmd/blockvolume/main.go` (binary path: peer-set updates flow through `replication.UpdateReplicaSet` → `ReplicaPeer` → `BlockExecutor` calls dispatched by `adapter.VolumeReplicaAdapter`, which mints fresh sessionIDs at every dispatch site). Source-side check: `cmd/blockvolume/main.go` does not import `core/replication/component` (mini-plan v0.4 §7.1 #2 fence). Backstop: `core/adapter/adapter.go:70` declares `sessionIDCounter` with file-local visibility; only `adapter.go:353,359,377,392,402` mint via `Add(1)` | 2026-04-26 (G5-4 close) | ACTIVE |

> **The table above is a schema seed.** P15 QA establishment populates it fully from the four sources listed in §2, cross-referenced with [`v3-semantic-constraint-checklist.md`](./v3-semantic-constraint-checklist.md).

---

## 4. Population Plan (P15)

The ledger is populated in this order:

| Step | Source | Expected rows |
|------|--------|---------------|
| 1 | Every invariant in `v3-semantic-constraint-checklist.md` | 30–50 rows, `INV-*` |
| 2 | Every entry in `external-failure-taxonomy.md` (20 real-world bugs) | 20 rows, `EXT-*` |
| 3 | Every named bad-state family from Phases 01–14 + 14A | 20–40 rows, `PCDD-*` |
| 4 | Every V1/V2 scenario audit result marked `port` or `adapt` | ≤148 rows (subset), `V2SCN-*` |

**Pre-T1 commitment (this shell)**: schema is valid, 5 example rows exist.
**End-of-P15 commitment**: every row is `ACTIVE` (i.e., has a test pointer and a `Last verified` stamp from at least one Final Gate run).

---

## 5. Modification Discipline

(From `v3-quality-system.md` §6 and §7.)

**Adding a row**:
1. New invariant identified (from new phase, new PEP, or new bug)
2. Row added with `STUB` status
3. Test added in the same PR (or tracked as explicit follow-up with owning track)
4. Status promoted to `ACTIVE` once test exists

**Modifying a row**:
1. Any change to the **Statement** column is a **protocol evolution** → requires PEP (see [`protocol-evolutions/README.md`](./protocol-evolutions/README.md))
2. Changing **Test(s)** column is normal PR discipline; must document why
3. Changing **Owner layer** requires V3 Architect + QA Lead sign-off

**Retiring a row**:
1. Row moves to §6 with retirement reason
2. PEP is required **unless** the invariant has been strictly strengthened by a replacement row (cross-reference the replacement)

**Never**:
- Delete a ledger row (always archive to §6)
- Weaken a statement without a PEP
- Leave a row in `STUB` status across a release boundary (either promote or retire)

---

## 6. Archive (Retired Invariants)

No retirements yet.

Entry format (when first retirement happens):

```
### INV-XXX-NNN (retired YYYY-MM-DD)
- Original statement: ...
- Retirement reason: [superseded | protocol evolved | proved redundant | ...]
- Replacement: [row ID, or "none"]
- PEP: [`protocol-evolutions/NNN-<title>.md`, or "not required (strengthened)"]
```

---

## 7. Release Manifest Stamp

At each release candidate, the Release Manager runs all ledger tests and appends a stamp here:

```
## Release RC-YYYY-MM-DD-NN
- Total rows: X (ACTIVE: Y, STUB: Z, ARCHIVED: W)
- Tests passed: Y/Y
- Tests failed: 0 (release blocker if >0)
- Final Gate Layer B: PASS/FAIL
- Stamped by: <Release Manager name>
```

No stamps yet (P15 not started).

---

## 8. Change Log

| Date | Change | Who |
|------|--------|-----|
| 2026-04-20 | Ledger shell created as Pre-T1 Gate deliverable P3 | QA Owner (per architect-approved P15 plan) |
| 2026-04-21 | P15 T0 closure: 11 new rows (`INV-HOST-001`, `INV-RPC-001`, `INV-OBS-REALPROC-001`, `PCDD-STUFFING-001`, `PCDD-STALE-HB-001`, `PCDD-DELAYED-HB-001`, `PCDD-MALFORMED-001`, `PCDD-DEAD-PEER-001`, `PCDD-RPC-TIMEOUT-001`, `PCDD-CONCURRENT-HB-001`, `PCDD-SUB-TIMING-001`, `PCDD-STORE-RACE-001`) stamped ACTIVE; `INV-AUTH-001` + `INV-RESTART-001` promoted SHELL → ACTIVE with test pointers; `INV-FENCE-001` / `INV-PUB-001` / `INV-SESSION-001` demoted to STUB (deferred to G3+ tracks) | QA Owner |
| 2026-04-21 | P15 T1 contract-ready: 6 new rows ACTIVE — `INV-FRONTEND-001` (ACTIVE Unit+Component; Integration BLOCKED-BY-T0); `INV-FRONTEND-002` (epoch-drift facet ACTIVE; replica-change facet STUB BLOCKED-BY-T0); `PCDD-STALE-PRIMARY-READ-001`; `PCDD-STALE-PRIMARY-WRITE-001`; `PCDD-BACKEND-CLOSED-USE-001`; `PCDD-FRONTEND-NOT-READY-001`; `V2SCN-DATAPATH-001` (audit). `PCDD-STUFFING-001` boundary extended to `core/frontend/`. Commit 7d6cbfc. Two sw Phase 3 findings resolved as spec clarifications (no PEP): frontend.Projection adopted per sketch §5 allowance; L1 scope narrowed to epoch-drift facet with replica-change deferred to L2 | QA Owner |
| 2026-04-21 (rev-3, status downgrade per §8B.8 Rule 2) | **`INV-FRONTEND-001` L1 ACTIVE stamp retracted** — L1 harness's `latestLineageView` wraps Publisher output, not real `VolumeReplicaAdapter.Projection()`, so L1 does not prove "readiness from adapter projection". Row is now **PARTIAL** (Unit ACTIVE contract-level only; Component STUB; Integration STUB). New narrower invariant `INV-FRONTEND-SEAM-001` introduced to correctly carry the Component-layer fence-seam proof that L1.1 actually demonstrates. T1 status re-labeled **T1-L0-COMPLETE** (no downstream unlock per §8B.8 Rule 1); `T1-CONTRACT-READY` label retired as ambiguous. | QA Owner |
| 2026-04-21 (rev-4, T1-CLOSED) | **T1-CLOSED**. sw delivered rev-4 patch: `core/host/volume/projection_bridge.go` (real `AdapterProjectionView`), `healthy_executor.go` (drives engine to ModeHealthy), `status_server.go` (HTTP `/status` for cross-process ProjectionView), `cmd/blockvolume` `--t1-readiness` + `--status-addr` flags, L2 subprocess test with `reserveLoopbackPort`. L1 helper rewrote to use real `VolumeReplicaAdapter.Projection()`. Architect-line review raised 4 findings (M1 supersede integration proof gap; M2 L2 scope claim; L1 fixed-port flake risk; L2 dot-import guard gap); all 4 resolved in same round. Ledger updates: `INV-FRONTEND-001` promoted to ACTIVE Unit+Component+Integration (source-of-truth now real `VolumeReplicaAdapter.Projection()` at all layers); `INV-FRONTEND-002` split into 4 facet rows (`.EPOCH` ACTIVE U+C+I, `.EV` ACTIVE U+I with C STUB, `.REPLICA` ACTIVE U+C with I deferred to G8/T5, `.HEALTHY` ACTIVE U+C with I deferred to G8/T5); `PCDD-STUFFING-001` AST guard extended to dot-import detection. T1 unlocks T2 (First Real Frontend) per §8B.8 Rule 1. | QA Owner |
| 2026-04-22 (T2-end prep) | **T2 closure batch — 16 new rows ACTIVE**. iSCSI Batch 10.5: `PCDD-ISCSI-VPD00-ADVERTISED-LIST-001`, `PCDD-ISCSI-VPD83-NAA-DETERMINISM-001`, `PCDD-ISCSI-VPD80-SERIAL-DETERMINISM-001` (addendum A). NVMe Batch 11a: `PCDD-NVME-IDENTIFY-NS-NGUID-DETERMINISM-001`, `PCDD-NVME-IDENTIFY-CTRL-ADVERTISED-LIST-001`, `PCDD-NVME-FABRIC-CNTLID-ECHO-001`. NVMe Batch 11b: `PCDD-NVME-IO-NO-TARGET-RETRY-001`. NVMe Batch 11c (post BUG-001 V2-strict revert at commit `c0c0a1d`): `PCDD-NVME-IO-H2CDATA-CHUNKED-R2T-001`, `PCDD-NVME-FABRIC-DISCONNECT-001`, `PCDD-NVME-FABRIC-CONNECT-KATO-001`, `PCDD-NVME-SQHD-WRAP-001`. QA early-landed B-tier: `PCDD-NVME-SESSION-RECONNECT-LOOP-001`, `PCDD-NVME-SESSION-DISCONNECT-MID-R2T-001`. Sign-prep L1-A/L2-A (commit `94ffb15`): `PCDD-NVME-IO-CONCURRENT-QUEUE-STRESS-001`, `PCDD-NVME-IO-LARGE-WRITE-MULTI-R2T-001`, `PCDD-NVME-SCENARIO-SMOKE-001`. BUG-001/002/003/004 all CLOSED. m01 fs-workload end-to-end clean via `iterate-m01-nvme.sh` 2026-04-22 — Matrix A 10 cycles + Matrix B 65 writes across 3 sizes, 0 dmesg errors. Awaiting T2-end three-sign per §8B.9 (transition to §8C accelerated-cadence governance for T3+). | QA Owner |
| 2026-04-22 (T3a close) | **T3a CLOSED (§8C.2 QA single-sign). 2 new rows ACTIVE.** Commit `0e1595c`: `core/frontend/durable/storage_adapter.go` (G-int.1+.2+.3 adapter) + `frontend.Backend` interface extended (`Sync(ctx)` + `SetOperational`) + superblock `ImplKind`+`ImplVersion` fields (Addendum A #2) + matrix test infra (Addendum A #1, `logicalStorageFactories()` shared helper + `TestT3a_MatrixCoverage` canary). Full regression 17/17 packages green. Rows ACTIVE: `INV-DURABLE-OPGATE-001` (operational gate + preserves `INV-FRONTEND-002.*` under durable) + `INV-DURABLE-IMPL-IDENTITY-001` (superblock records impl kind+version; mismatch fail-fast). T3b mini-plan opens next (Provider + Recovery + frontend sync wire). | QA Owner |
| 2026-04-22 (T3b close) | **T3b CLOSED (§8C.2 QA single-sign). 3 new rows ACTIVE.** Commit `72d0d40`: `core/frontend/durable/provider.go` (DurableProvider + ImplKind mismatch fail-fast + impl caching) + `core/frontend/durable/recovery.go` (Recover wrapper with RecoveryReport) + iSCSI `SYNCHRONIZE_CACHE(10/16)` → `backend.Sync` (ASCWriteError on fault) + NVMe `ioFlush` → `backend.Sync` + `cmd/blockvolume` durable-root wire (memback fallback when flag empty preserves T0/T1/T2 unit+subprocess regression). Spot-check confirmed: NO new publication path (zero `PublishAssignment|MintEpoch|AdvanceEpoch` in cmd wire), Sync errors propagated not swallowed. Full regression 17/17 packages green; T3a's 12 tests still ACTIVE. Rows ACTIVE: `INV-DURABLE-PROVIDER-SELECT-001` (config-driven impl + fail-fast mismatch) + `INV-DURABLE-RECOVERY-READSIDE-001` (recovery via SetOperational + `/status` HTTP; no publish) + `INV-DURABLE-SYNC-WIRED-001` (SYNC_CACHE/Flush → Sync(ctx), spec-legal error mapping). T3c opens next (scenarios + continuity smoke + perf baseline + T3-end three-sign). | QA Owner |
| 2026-04-22 (T3c close + QA sign) | **T3c CLOSED (§8C.2 QA single-sign). 4 new rows queued ACTIVE pending T3-end three-sign.** Commits `829c6a9` (4 scenarios × 2 impls + perf first-light + YAML shape docs) + `5c33460` (T3b follow-up: iSCSI + NVMe → durable matrix integration). Rows queued: `INV-DURABLE-001` (crash-recovery byte-exact) + `INV-DURABLE-WAL-REPLAY-001` (N acked writes recoverable) + `INV-DURABLE-FSYNC-BOUNDARY-001` (fsync semantics) + `PCDD-DURABLE-DISK-FULL-001` (graceful fail-hard). **m01 fs-workload verification PASS 2026-04-22** via `scripts/iterate-m01-nvme.sh` against **BOTH durable impls** (`DURABLE_IMPL=smartwal` + `walstore`, per Addendum A #1 matrix discipline at L3): Matrix A 10 cycles + Matrix B 65 writes (32K/256K/1M) + Matrix C 500/500 small-files (group_commit batching stressed) + Matrix D 5 remount cycles with pattern integrity — all clean on both impls. dmesg clean. Full regression 17/17 green; durable package 76+ subtests matrix-parameterized walstore+smartwal. QA signed closure report `v3-phase-15-t3-closure-report.md` §E; awaiting architect+PM sign → T3 CLOSED / Gate G4 pass. | QA Owner |
