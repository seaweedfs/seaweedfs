# V3 Phase 15 Product Gates Canonical

Date: 2026-04-20
Status: canonical acceptance gate candidate - ready to drive P15 product-plan refactor
Purpose: define the full single-active-master V3 production MVP scope with pass/fail gates, V2 port guidance, explicit deferrals, and no silent protocol-only closure

## 1. Rule

`P15` is not closed by an API shell, a CSI skeleton, a memback frontend, or local unit tests.

`P15` closes only when the accepted single-active-master MVP can run as a product:

1. real product-process hosting (pinned P15 beta default: `cmd/blockmaster` / `cmd/blockvolume` transitional V3 daemons; future track may fold into `weed master` / `weed volume` — see §3 G0 "Product host shape")
2. real master-volume control RPC
3. real frontend/client I/O
4. real local durable data path
5. real replicated write path
6. bounded catch-up and rebuild
7. failover with data continuity
8. lifecycle, diagnostics, security, deployment, and cluster validation

Every item below has an explicit **Pass / Fail** gate. If a gate is deferred, the document must say **Deferred** and name the user-visible consequence.

## 2. Baseline Assumptions

### A1. Master Availability

Decision required before P15 implementation continues:

1. **Option A - inherit SeaweedFS master HA / Raft hosting**: V3 authority is hosted under the existing master HA envelope, but still has exactly one active authority owner for a volume at a time.
2. **Option B - single-node master for beta**: V3 beta is single master only; master outage pauses control-plane changes, while already-attached frontend behavior is whatever the data path can safely support.

**Pass gate**: `v3-phase-15-product-plan.md` names one option and every cluster test uses the same assumption.

**Fail gate**: docs or tests use "single-active-master" sometimes to mean "one primary per volume" and sometimes to mean "one master process" without clarification.

### A2. Multi-Master

Multi-master leader election, distributed authority store, and split-brain prevention across multiple active masters are out of P15 scope.

**Pass gate**: no P15 track claims multi-master HA.

**Fail gate**: any P15 test or doc implies distributed authority safety without a dedicated phase.

### A3. V2 Port Rule

Port V2 product muscle aggressively, but do not port V2 authority semantics.

**Port**:

1. frontend protocols
2. local storage / WAL / rebuild machinery
3. CSI / API / operator shape
4. testrunner / scenarios / reports
5. monitoring and deployment assets

**Do not port**:

1. `promotion.go`
2. `HandleAssignment`
3. `promote` / `demote`
4. heartbeat-as-authority
5. local-role-as-authority
6. volume-local assignment mutation

**Pass gate**: each P15 track names V2 files to port and V2 semantics rejected.

**Fail gate**: "ported from V2" is used without naming the rejected authority paths.

## 2.5 Relation To QA System

This document defines **what must pass**. The QA system defines **how each claim is tracked and audited**.

Every gate below must create or update:

1. `v3-invariant-ledger.md` rows for durable claims (`INV-*`, `EXT-*`, `PCDD-*`, or `V2SCN-*`)
2. `v3-test-matrix.md` coverage rows for the same IDs
3. `v3-phase-15-qa-system.md` gate status when the gate enters implementation
4. a Protocol Evolution Proposal if the gate changes an accepted semantic invariant

Minimum gate-to-QA rule:

1. every P0 gate needs at least one matrix row with `Component` and `Scenario` coverage planned
2. every safety or data-continuity gate needs a ledger row before implementation starts
3. every deferred gate needs a matrix row marked `DEFERRED` plus customer implication
4. every Final Gate claim must be traceable to a ledger row and a concrete scenario artifact

If a gate cannot be represented in the ledger/matrix, the gate definition is incomplete.

## 3. P15 Hard Gates

### G0. Master / VolumeServer Product Hosting

**Target**: P14's `ObservationHost`, `TopologyController`, `Publisher`, durable authority store, and evidence surfaces run inside real product processes, not only `cmd/sparrow` tests.

**Product host shape (round-2 T0-sketch sync)**: P15 beta product host may be EITHER:

1. Direct integration of the P14 authority components into `weed master` / `weed volume`, OR
2. Transitional V3-native product daemons `cmd/blockmaster` / `cmd/blockvolume` in the `seaweed-block` module, provided they meet the product-daemon contract (long-lived, operator-managed, stable advertised flags, graceful shutdown, no hidden smoke/test fallbacks, structured logs).

**Option 2 is the P15 beta default**; Option 1 is a later integration track. Both satisfy G0's pass gate — the gate cares about real daemons, not which source tree they live in.

**Must ship** (whichever option chosen):

1. Product master process hosts durable authority and controller.
2. Product volume process reports local replica facts.
3. Startup order is explicit: durable authority reload before publication.
4. Process restart preserves authority and supportability semantics.
5. Explicit decision on `cmd/sparrow` after P15: dev tool, conformance runner, or retired.

**Port from V2**:

1. `weed/server/volume_server_block.go`
2. `weed/server/volume_grpc_block.go`
3. `weed/server/master_grpc_server_block.go`
4. `weed/server/block_heartbeat_loop.go`
5. lifecycle/wiring patterns from `weed/server/volume_server_block.go`

**Make new**:

1. V3 hosting adapters from product process to `core/authority`.
2. product config for authority store path and topology input.
3. process lifecycle tests using real `weed` / `sparrow` binaries as appropriate.

**Pass gate**: L2 process test starts product master + product volume process, reloads durable authority, ingests volume facts, and the volume's adapter projection reflects the assigned lineage (`Epoch` / `EndpointVersion` advance to published values) — without `StaticDirective` or direct `AssignmentInfo`. **Note**: G0 pass gate does NOT require `ModeHealthy`; Mode / fence / probe / session evidence belongs to G3 / G4+ (see T0 sketch §2.1).

**Fail gate**: route still depends on `cmd/sparrow` hidden smoke mode or in-process test helper as the only non-unit evidence.

### G1. Master-Volume Control RPC

**Target**: master and volume servers communicate over a real control route.

**Must ship**:

1. heartbeat / observation report route
2. assignment publication / subscription route
3. status / evidence query route
4. backpressure / timeout behavior for dead peers

**Port from V2**:

1. `weed/server/master_grpc_server_block.go`
2. `weed/server/volume_grpc_block.go`
3. `weed/server/master_server_handlers_block.go`
4. `weed/storage/blockvol/block_heartbeat.go`
5. `weed/storage/blockvol/block_heartbeat_proto.go`

**Make new**:

1. V3-safe RPC messages that carry observation and product status, not authority mutations.
2. assignment subscription route backed by `Publisher`, not by volume-local state.

**Pass gate**: L2 test sends heartbeat over RPC, master synthesizes `ClusterSnapshot`, publisher mints assignment, volume process receives assignment through subscription, and stale RPC messages are rejected or ignored.

**Fail gate**: tests still mutate authority directly or stuff `AssignmentInfo`.

### G2. Frontend Contract Smoke

**Target**: first frontend backend contract proves lineage gating and stale-primary rejection. Memback is allowed only here.

**Must ship**:

1. frontend backend interface
2. readiness from V3 adapter projection
3. stale primary read/write rejection
4. real-process heartbeat ingress for smoke

**Port from V2**:

1. frontend adapter shape from `weed/storage/blockvol/adapter.go`
2. test patterns from `weed/storage/blockvol/test/component/publish_healthy_test.go`

**Make new**:

1. `core/frontend` contract
2. lineage-gated memback provider
3. L1/L2 route smoke

**Pass gate**: old backend returns `ErrStalePrimary` after authority moves; new backend can write/read its own post-failover bytes.

**Fail gate**: claims replicated data continuity from memback.

### G3. First Real Frontends (iSCSI **and** NVMe/TCP — both required per CEO pin 2026-04-21)

**Target**: **both** iSCSI and NVMe/TCP are usable real client-facing block protocols served from product host. Single-protocol delivery does **not** satisfy G3 in P15.

**Scope change history**: G3 originally read as "at least one real frontend (choice of iSCSI, NVMe/TCP, or approved minimal)". Effective 2026-04-21 (CEO pin, captured in `v3-phase-15-t2-sketch.md` §2), G3 is widened to mandate **both** iSCSI and NVMe/TCP as P15 product requirement. The minimal block API fallback is retired as a G3 option; CSI alone still does not satisfy G3.

**Execution structure** (reflected in T2 assignment):

1. **T2A** — iSCSI path to L0/L1/L2 green
2. **T2B** — NVMe/TCP path to L0/L1/L2 green
3. **T2 closure** — requires both T2A and T2B complete; partial (one-protocol-only) closure is not permitted

T2A and T2B may be implemented in separate commits, but the G3 gate does not pass until both protocol paths have L2 product-process evidence.

**Port from V2** (both required):

1. `weed/storage/blockvol/iscsi/` (pdu / params / login / session / scsi / dataio / target)
2. `weed/storage/blockvol/nvme/` (protocol / wire / fabric / admin / controller / io / server / adapter / write_retry)
3. Frontend tests from `weed/storage/blockvol/test/`

**Make new** (both protocols):

1. V3 projection-to-frontend readiness bridge **shared** between iSCSI and NVMe/TCP (must not duplicate readiness logic per protocol)
2. V3 stale-primary fence at protocol boundary for each protocol
3. Protocol-specific integration tests against V3 authority, including per-protocol stale-path rejection

**Stale-path rule (applies to both protocols, both directions)**:

After an authority move (RefreshEndpoint EV advance is acceptable; cross-replica failover is G8/T6), the old frontend path must reject **both directions**:
- **Stale WRITE**: must not ACK success to the initiator. Protocol-appropriate failure (SCSI CHECK CONDITION / NVMe status) required.
- **Stale READ**: must not return data success. Stale read can silently poison upper layers (especially after catch-up/rebuild lands in later tracks), so "write-only fence" is insufficient.

Both protocols must have L0 + L1 + L2 evidence for **both stale WRITE and stale READ** rejection.

**Pass gate** (all required; single protocol or write-only insufficient):

1. **iSCSI stale-path proven**:
   a. L2 product-process test attaches through iSCSI, writes+reads (round-trip), triggers authority move, asserts old iSCSI **WRITE** rejects
   b. Same L2 (or companion L2) asserts old iSCSI **READ** rejects after the same move
2. **NVMe/TCP stale-path proven**: same two-direction shape (a) + (b) as above
3. V2 port audit covers both protocol trees with per-function verdict
4. Ledger rows `INV-FRONTEND-ISCSI-001` + `INV-FRONTEND-NVME-001` both ACTIVE at Integration layer, and `INV-FRONTEND-PROTOCOL-002` (stale-path fail-closed) ACTIVE at Integration for **both** directions on **both** protocols (4 scenario intersections)
5. CSI presence does not count toward G3 (belongs to G9 / T7)
6. L2 evidence conforms to the initiator-evidence rules below

**Fail gate** (any one fails the gate):

1. Only one protocol has L2 evidence; the other is Unit/Component-only or deferred
2. Stale WRITE proven but stale READ not proven (or vice versa) on either protocol — "write-only fence" is insufficient
3. "Frontend ready" means only internal Go interface or memback contract smoke
4. Minimal block API is used as an escape hatch in place of iSCSI or NVMe/TCP
5. Readiness bridge is duplicated across iSCSI and NVMe/TCP (indicates both protocols re-deriving readiness instead of consuming T1 contract)
6. Either protocol's stale-WRITE or stale-READ on L2 is silently acknowledged as success
7. T2 closes using only L2-harness evidence; OS-initiator evidence absent entirely (see initiator-evidence rules)

**Initiator-evidence rules (what counts as L2)**:

L2 evidence is tiered. Two classes are distinguished so intermediate states are honest:

| Evidence class | What it is | Counts for |
|----------------|-----------|------------|
| **L2-OS** | Real OS initiator (Linux `iscsiadm` / `nvme connect`, or equivalent native initiator on a declared target environment) attaches to `cmd/blockvolume` | Required for **T2-CLOSED**. Proves kernel-side behavior: logout/reconnect, device path, timeout handling, protocol error propagation. |
| **L2-harness** | V2 Go initiator harness (or equivalent in-repo client) attaches to real `cmd/blockvolume` process over TCP | Counts for **T2A-/T2B-provisional** intermediate status. Does NOT close T2 alone. Proves protocol wire correctness but not kernel/user-space behavior. |

**Rule**: T2-CLOSED requires L2-OS evidence for at least the stale-WRITE + stale-READ pass-gate assertions on **both protocols**. L2-harness evidence may substitute for other L2 assertions (e.g., authorities-assign round-trip) but the stale-path critical assertions require L2-OS.

If the primary CI environment lacks OS initiators, QA may declare a target environment (e.g., a specific Linux test host) and mark CI-blocked L2-OS assertions as environment-gated; "no environment available" is not acceptable for T2-CLOSED.

### G4. Local Durable Data Path

**Target**: V3 frontend I/O reaches real local block storage with durable write/recovery semantics.

**Port from V2**:

1. `weed/storage/blockvol/logical_storage*.go`
2. `weed/storage/blockvol/smartwal*.go`
3. `weed/storage/blockvol/wal_*.go`
4. `weed/storage/blockvol/dirty_map.go`
5. `weed/storage/blockvol/group_commit.go`
6. `weed/storage/blockvol/write_gate.go`
7. `weed/storage/blockvol/superblock.go`

**Make new**:

1. V3 local storage adapter with epoch/session/fence awareness.
2. recovery integration with V3 publication state.

**Pass gate**: crash/restart test writes acknowledged data through the real frontend, kills/restarts the local process, and reads the acknowledged data back.

**Fail gate**: only in-memory storage or mock storage backs the frontend.

### G5. Replicated Write Path

**Target**: primary writes are replicated to at least one replica with explicit ack/durability semantics.

**Port from V2**:

1. `weed/storage/blockvol/wal_shipper.go`
2. `weed/storage/blockvol/shipper_group.go`
3. `weed/storage/blockvol/repl_proto.go`
4. `weed/storage/blockvol/replica_apply.go`
5. `weed/storage/blockvol/replica_barrier.go`
6. V2 replication tests and QA cases

**Make new**:

1. V3 lineage-carrying replication route.
2. write acknowledgment policy for beta.
3. stale replica apply rejection tied to V3 epoch / endpointVersion.

**Pass gate**: L2/L3 test writes through primary, confirms replica has the same committed data, kills primary, and verifies the candidate has the acknowledged write.

**Fail gate**: primary-only write path is called replicated.

### G6. Incremental WAL Catch-Up

**Target**: lagging replicas inside the WAL retention window catch up using WAL delta, not full block shipping.

**Port from V2**:

1. WAL retention and shipper mechanics from `wal_shipper.go`
2. `shipper_group.go`
3. component tests such as `fast_rejoin_catchup_test.go`
4. relevant testrunner scenarios around fast reconnect

**Make new**:

1. V3 `StartCatchUp` execution that streams only the required WAL delta.
2. achieved-frontier feedback to adapter/engine session close.
3. no rebuild escalation on transient transport loss.

**Pass gate**: test creates a short lag, restarts replica, verifies catch-up transfers only delta range and reaches byte-for-byte match.

**Fail gate**: catch-up still ships every primary block while claiming production catch-up.

### G7. Rebuild / Replica Re-Creation

**Target**: replicas outside WAL window, lost replicas, and re-created replicas can be rebuilt safely.

**Must distinguish**:

1. rebuild from existing stale local data
2. replica re-creation after disk loss or permanent local state loss

**Port from V2**:

1. `weed/storage/blockvol/rebuild*.go`
2. `weed/storage/blockvol/rebuild_bitmap.go`
3. `weed/storage/blockvol/rebuild_transport.go`
4. `weed/storage/blockvol/rebuild_session.go`
5. component tests under `weed/storage/blockvol/test/component/rebuild_*`

**Make new**:

1. V3 rebuild coordinator that consumes P14 authority but does not own authority.
2. admission control / backoff / bandwidth policy.
3. disk-lost replica re-creation path.

**Pass gate**: L3 test loses or invalidates a replica, creates/rebuilds a replacement, reaches byte-for-byte match, and records bounded evidence for every step.

**Fail gate**: rebuild assumes local data still exists when the failure was a disk loss.

### G8. Failover Data Continuity

**Target**: after primary failure, the new primary can serve previously acknowledged data and the old primary cannot corrupt future state.

**Port from V2**:

1. `ha-io-continuity.yaml`
2. `ha-failover.yaml`
3. `ha-full-lifecycle.yaml`
4. HA component tests under `weed/storage/blockvol/test/`

**Make new**:

1. V3 authority-to-frontend failover integration.
2. client reconnect / retry behavior.
3. data verification across failover.

**Pass gate**: cluster test writes known data, kills primary, waits for V3 reassignment, reattaches or reconnects, and verifies the new primary reads the exact acknowledged data.

**Ledger anchors**:

1. `INV-CONT-001` Failover Data Continuity - composite claim
2. `INV-AUTH-001` authority line monotonicity
3. `INV-FENCE-001` stale primary fencing
4. `INV-REPL-001` acknowledged write reaches eligible replica
5. `INV-CATCHUP-001` WAL-window catch-up preserves data
6. `INV-REBUILD-001` rebuild/re-create reaches byte-for-byte match

**Matrix coverage required**: `INV-CONT-001` must have Scenario and Soak coverage before P15 closes.

**Fail gate**: failover test only checks authority moved or frontend stale rejection, without data verification.

### G9. Volume Lifecycle: Create / Delete / Attach / Detach / Publish / Unpublish

**Target**: users and orchestrators can manage volume lifecycle without manual authority stuffing.

**Port from V2**:

1. `weed/storage/blockvol/csi/`
2. `weed/storage/blockvol/blockapi/`
3. `weed/server/master_grpc_server_block.go`
4. V2 lifecycle tests

**Make new**:

1. V3 desired-volume model.
2. safe lifecycle bridge into P14 controller.
3. idempotent lifecycle operations.

**Pass gate**: API or CSI test creates volume, attaches, writes/reads through real frontend, detaches, deletes, and verifies no orphan authority line or data path remains.

**Fail gate**: lifecycle green status can be returned before data path is usable.

### G9A. Desired Topology / Placement Controller MVP

**Target**: users can create and repair replicated volumes without hand-authoring every slot, while authority remains publisher-minted (no V2-style heartbeat-as-authority).

**Source rationale**: see [`v3-product-placement-authority-rationale.md`](./v3-product-placement-authority-rationale.md). Production block storage requires V2-like operational ergonomics (`operator asks for intent → system computes placement → system drives authority + replication`), but V3's authority discipline must be preserved (no heartbeat-as-authority, no local-role-as-authority). G9A bridges the two: minimal master placement feature on top of V3 desired-topology + authority model.

**Port from V2** (selectively):

1. `weed/topology/` placement candidate selection — flat-topology subset only
2. V2 master placement loop — concept, NOT direct code (V3 reframes to desired-topology generation)

**Make new**:

1. Desired topology generation: durable, master-owned, versioned (replaces V2's "master invents placement on the fly")
2. Flat-topology RF placement for create-volume requests (no rack/AZ awareness in P15)
3. Eligibility filter: pick nodes/slots from observed capacity + supportability — but observation is *evidence*, not authority
4. Replacement placement on disk-loss / drain
5. Plan/explain output: why selected, why rejected (operator-facing)
6. Master/publisher mints assignment ONLY from desired topology, never directly from heartbeat observation

**Must ship**:

1. Flat-topology RF placement for create volume
2. Durable desired topology generation
3. Explainable candidate filtering and rejection reasons
4. Replacement placement for drain/disk-loss workflows
5. Assignment minted only from desired topology, never directly from heartbeat observation

**Explicit non-scope for P15** (defer to G20 / P16):

- Rack/AZ-aware placement
- Hot-volume rebalance
- Automatic load-based movement
- Multi-master HA / replicated authority store
- Advanced scheduler scoring
- V2 `promote/demote` semantics

**Pass gate**: API/CSI or admin test creates RF=2/RF=3 volume from intent, observes desired topology generation, master publishes assignment, volumes bind roles, and no product path stuffs `AssignmentInfo` directly. Replacement-on-drain test passes.

**Fail gate**: P15 docs imply V2-like dynamic assignment while operators still manually edit topology, OR heartbeat/register directly changes authority, OR `AssignmentInfo` constructed outside `authority.Publisher`.

### G10. Snapshot

**Target**: decide whether P15 beta supports volume snapshots.

**Option A - implement minimal snapshot**:

1. CSI `VolumeSnapshot` / `VolumeSnapshotClass` if CSI is shipped.
2. point-in-time read-only snapshot with documented consistency boundary.

**Option B - explicit defer**:

1. P15 beta does not support native snapshots.
2. users must use external backup or frontend/filesystem-level backup.

**Port from V2**:

1. `weed/storage/blockvol/snapshot*.go`
2. `weed/storage/blockvol/snapshot_export*.go`
3. CSI snapshot id helpers and tests

**Pass gate**: either a snapshot create/restore/read-only validation test passes, or the beta docs explicitly state "no native snapshot" and name the backup implication.

**Fail gate**: CSI is claimed production-ready while snapshot behavior is silent.

### G11. Resize / Expand

**Target**: decide whether P15 beta supports volume expansion.

**Option A - implement resize**:

1. CSI `ControllerExpandVolume`
2. CSI `NodeExpandVolume`
3. local storage and frontend capacity update

**Option B - explicit defer**:

1. P15 beta volumes are fixed-size.
2. users must create a larger volume and migrate data externally.

**Port from V2**:

1. `weed/storage/blockvol/expand_test.go`
2. `weed/storage/blockvol/resize_test.go`
3. CSI resize helpers/tests

**Pass gate**: either online/offline expansion test passes through selected frontend, or beta docs explicitly mark resize unsupported with user workaround.

**Fail gate**: PVC resize is silently unsupported.

### G12. Disk Failure Handling

**Target**: physical/local storage failure is handled differently from process restart.

**Must ship or explicitly defer**:

1. detect bad disk / unreadable extent / corrupt local state
2. mark node/replica unsuitable
3. evict or recreate replica
4. prevent rebuild from assuming local data still exists

**Port from V2**:

1. WAL corruption and disk-full tests
2. `fault-disk-full.yaml`
3. rebuild and re-creation test patterns

**Make new**:

1. V3 disk-health observation fact.
2. policy path from bad disk to unsupported / recreate.
3. operator evidence.

**Pass gate**: L3 test simulates disk loss/corruption, V3 stops using the bad replica, creates/rebuilds a replacement, and maintains data correctness if redundancy exists.

**Fail gate**: disk loss is treated only as volume-server process restart.

### G13. Node Lifecycle: Join / Drain / Decommission

**Target**: production operators can add, maintain, and remove nodes.

**Must ship**:

1. node join
2. node drain
3. node decommission
4. no new assignment to draining nodes
5. safe migration/rebuild of affected replicas or explicit unsupported state
6. admin CLI/gRPC verbs for drain and decommission; Kubernetes operator coordination is separate and may be deferred under G20.

**Port from V2**:

1. operator workflow patterns
2. testrunner action vocabulary
3. master-side placement and evidence code as reference

**Make new**:

1. V3 node state model.
2. placement exclusion rules.
3. drain/decommission API and runbook.

**Pass gate**: test drains a node with hosted replicas, verifies no new assignments land there, data remains available or unsupported evidence is explicit, and decommission removes it from topology.

**Fail gate**: planned maintenance requires manual authority edits.

### G14. External API

**Target**: product verbs are safe and usable by tools/tests without exposing authority internals.

**Must ship**:

1. create/delete/get/list volume
2. attach/detach/status
3. read-only topology/authority/convergence
4. safe mutating intent only

**Port from V2**:

1. `weed/storage/blockvol/blockapi/`
2. `weed/server/master_grpc_server_block.go`
3. `weed/server/master_server_handlers_block.go`

**Make new**:

1. V3-safe product verbs.
2. client library used by tests.

**Pass gate**: external client drives create -> attach -> write/read -> status -> delete without constructing `AssignmentInfo` or selecting epoch.

**Fail gate**: API test only validates request/response without route effect.

### G15. CSI Lifecycle

**Target**: if CSI is in beta, Kubernetes can create, publish, mount/use, unpublish, and delete V3 volumes.

**Port from V2**:

1. `weed/storage/blockvol/csi/`
2. `weed/storage/blockvol/csi/deploy/`
3. `op-csi-lifecycle.yaml`

**Make new**:

1. V3 CSI backend.
2. lifecycle mapping to V3 product API.
3. idempotency/retry tests.

**Pass gate**: K8s or local CSI integration test provisions a volume, mounts/uses it through real frontend, and deletes it cleanly.

**Fail gate**: CSI reports ready while selected frontend/data path is not usable.

### G16. Security / Auth

**Target**: production-facing paths are protected.

**Must ship**:

1. authn/authz for mutating APIs
2. TLS/mTLS posture for control endpoints
3. frontend auth where applicable
4. audit records

**Port from V2**:

1. `weed/storage/blockvol/iscsi/auth.go`
2. `weed/storage/blockvol/iscsi/auth_test.go`
3. CSI RBAC manifests
4. operator RBAC patterns

**Explicit defer**:

1. data-at-rest encryption is not P15 unless product owner changes this gate.
2. beta users needing encryption must use LUKS/dm-crypt or cloud-disk encryption under the block device.

**Pass gate**: unauthorized clients cannot mutate lifecycle, authority-related desired state, or frontend attachment; authorized clients can, with audit.

**Fail gate**: unauthenticated mutating endpoint is shipped.

### G17. Diagnostics, Metrics, Structured Logs, Documentation

**Target**: humans and machines can understand and monitor the product.

**Must ship**:

1. human explainability: topology, authority, convergence, unsupported, stuck, frontend readiness
2. Prometheus metrics endpoint
3. structured log schema
4. user docs: install, configure, create/use volume, failover/recovery, backup limitation, known unsupported features
5. support artifact bundle
6. `/healthz` and `/readyz` or equivalent liveness/readiness endpoints for deployment systems

**Port from V2**:

1. `weed/server/master_block_observability.go`
2. `weed/server/master_block_evidence.go`
3. `weed/storage/blockvol/monitoring/`
4. `weed/storage/blockvol/testrunner/metrics.go`
5. `learn/test/evidence-convention.md`

**Make new**:

1. V3 status contracts.
2. V3 metric names.
3. V3 structured log fields.
4. V3 beta docs.

**Pass gate**: every required cluster-validation failure class produces structured evidence, metrics, logs, and a documented operator interpretation.

**Fail gate**: diagnostics require reading engine internals or grep-only logs.

### G18. Configuration / Deployment / Upgrade

**Target**: product is deployable, configurable, restartable, and upgradable.

**Must ship**:

1. configuration model: config file / flags / env / K8s ConfigMap rules
2. master address and volume-server registration config
3. replication factor and accepted topology config
4. storage paths and durable store path
5. timeout and retry knobs
6. systemd/container/K8s deployment guide
7. upgrade/rollback checklist

**Port from V2**:

1. CSI deploy manifests
2. operator config manifests if operator is accepted
3. testrunner deployment patterns

**Make new**:

1. V3 config schema.
2. config validation.
3. deployment examples.

**Pass gate**: fresh machine or test cluster can be deployed from documented config and passes smoke validation without manual code/test harness steps.

**Fail gate**: production setup depends on undocumented flags or hidden smoke modes.

### G19. Migration / Coexistence

**Target**: existing V2 users know whether and how to move to V3.

**Must ship or explicitly defer**:

1. compatibility matrix
2. coexistence isolation rules
3. migration dry-run
4. rollback plan

**Port from V2**:

1. `weed/storage/blockvol/v2bridge/`
2. `op-upgrade-rollback.yaml`
3. V2 status and blockapi mapping patterns

**Pass gate**: the product owner selects one migration story before beta:

1. **Option A - supported migration**: V3 beta supports a named migration path with dry-run, data compatibility check, and rollback.
2. **Option B - explicit non-migration beta**: V3 beta does not support in-place migration; V2 and V3 are independent clusters; migration is P16+.

Both options require a public-facing statement. Option A requires an L3/L4 dry-run artifact; Option B requires documentation and release-note wording.

**Fail gate**: beta users infer in-place upgrade is safe without an explicit path.

### G20. QoS, Rack Awareness, Operator CRD, GC Policy

These are not all mandatory for beta, but cannot be silent.

**QoS / rate limit**:

1. default P15 beta: deferred unless explicitly accepted.
2. client implication: no per-volume noisy-neighbor isolation.

**Rack-aware placement**:

1. default P15 beta: deferred; flat topology only.
2. client implication: do not rely on rack/AZ failure-domain placement.

**Dedicated K8s operator / CRDs**:

1. default P15 beta: deferred unless product owner accepts operator scope.
2. client implication: use CSI manifests / Helm/raw manifests if provided.

**Compaction / GC**:

1. must choose automatic, manual, or not supported.
2. client implication must be documented because no GC can exhaust disk.

**Pass gate**: each item is either implemented with a test or explicitly deferred with customer implication.

**Fail gate**: docs imply production support while behavior is undefined.

### G21. Performance SLO And Regression

**Target**: performance is measurable and compared to V2 baseline.

**Must ship**:

1. latency SLO target
2. throughput baseline on named hardware
3. failover time SLO
4. catch-up/rebuild time baseline
5. V2 regression comparison where comparable

**Port from V2**:

1. `cp103-perf-baseline.yaml`
2. `cp85-perf-baseline.yaml`
3. benchmark scripts under `learn/test`
4. testrunner benchmark actions

**Make new**:

1. V3 performance report template.
2. V3 baseline runs.

**Pass gate**: final gate includes performance report with hardware profile, V2 comparison where available, and explicit pass/fail against SLO.

**Fail gate**: "perf tested" means an ad hoc benchmark log with no SLO.

### G22. Final Cluster Validation

**Target**: independent test agent validates the product as a cluster.

**Must run**:

1. multi-process / multi-node bring-up
2. lifecycle create/use/delete
3. heartbeat -> topology -> assignment
4. replicated write/read
5. primary crash / failover / data continuity
6. replica restart / catch-up
7. disk failure / replica re-creation or explicit unsupported
8. rebuild
9. master restart under chosen master availability model
10. node join/drain/decommission
11. unsupported topology
12. security negative tests
13. metrics/log/artifact validation
14. performance and soak

**Port from V2**:

1. `weed/storage/blockvol/testrunner/`
2. scenario YAMLs under `weed/storage/blockvol/testrunner/scenarios/`
3. artifact/report machinery
4. `learn/test` evidence convention

**Make new**:

1. V3 scenario pack.
2. V3 claim-to-test matrix export.
3. V3 release evidence bundle.

**Pass gate**: final gate emits `manifest.json`, `result.json`, `result.xml`, logs, metrics, and claim matrix; all P0/P1 gates pass or have product-owner-approved residuals.

**Fail gate**: P15 closes with only `go test ./...` or local process smoke.

## 4. P0 / P1 / P2 Classification

In this document, `P0` means **must decide before beta**. A P0 item may close by implementation or by explicit product-owner-approved deferral only when the gate text allows deferral and names customer implication.

| Priority | Gate | Default disposition |
|---|---|---|
| P0 | G0 Product hosting | Must implement |
| P0 | G1 Master-volume RPC | Must implement |
| P0 | G3 Real frontends (iSCSI **and** NVMe/TCP) | Must implement **both** per CEO pin 2026-04-21; single-protocol delivery fails the gate |
| P0 | G4 Local durable data path | Must implement |
| P0 | G5 Replicated write path | Must implement |
| P0 | G6 Incremental catch-up | Must implement |
| P0 | G7 Rebuild / replica re-creation | Must implement |
| P0 | G8 Failover data continuity | Must implement |
| P0 | G9 Lifecycle | Must implement core verbs |
| P0 | G9A Placement Controller MVP | Must implement flat-topology placement OR explicitly mark P15 beta as declared-topology/manual-placement only |
| P0 | G10 Snapshot | Implement or explicit beta defer |
| P0 | G11 Resize | Implement or explicit beta defer |
| P0 | G12 Disk failure | Must implement or explicit unsupported evidence path |
| P0 | G13 Node lifecycle | Must implement join/drain/decommission or explicit beta limitation |
| P0 | A1 Master availability | Must decide before P15 implementation continues |
| P1 | G14 External API | Strongly recommended; required if CSI is not complete |
| P1 | G15 CSI | Required if Kubernetes beta is claimed |
| P1 | G16 Security/Auth | Required for non-local beta |
| P1 | G17 Diagnostics/Metrics/Logs/Docs | Required for beta supportability |
| P1 | G18 Configuration/Deployment/Upgrade | Required for beta deployment |
| P1 | G19 Migration/Coexistence | Implement or explicit beta defer |
| P1 | G21 Performance SLO | Required before beta sign-off |
| P2 | G20 QoS / rack-aware / operator / GC | Implement or explicit defer with implication |
| Release | G22 Final cluster validation | Required to close P15 |

## 4.5 Gate Dependency Sketch

The default scheduling spine is:

```text
G0 Product hosting
  -> G1 Master-volume RPC
    -> G4 Local durable data path
      -> G5 Replicated write path
        -> G6 Incremental catch-up
        -> G7 Rebuild / replica re-creation
          -> G8 Failover data continuity

G2 Frontend contract smoke
  -> G3 First real frontends (iSCSI + NVMe/TCP, both required)
    -> G9 Lifecycle
      -> G9A Placement Controller MVP (depends on G9 lifecycle + G8 failover + G13 node lifecycle observation)
        -> G10 Snapshot / G11 Resize / G12 Disk failure / G13 Node lifecycle
      -> G14 External API / G15 CSI / G16 Security / G17 Diagnostics
        -> G18 Deployment / G19 Migration / G20 Deferred-product decisions / G21 Performance
          -> G22 Final cluster validation
```

Parallelism is allowed only when the producer/consumer contract between gates is already pinned in writing and the downstream gate does not fake an upstream product capability.

## 5. Product Closure Rule

P15 cannot close if any of these are true:

1. master/volume hosting is still test-only
2. frontend is only memback
3. writes are primary-only but called replicated
4. catch-up still full-ships every block while called incremental
5. rebuild cannot handle disk loss / replica re-creation or explicitly reject it
6. failover test does not verify data continuity
7. CSI/API says ready while data path is not usable
8. snapshot/resize/disk failure/node lifecycle are silent
9. security is absent on non-local mutating endpoints
10. metrics/logs/docs are absent
11. performance has no SLO
12. final validation is only local unit tests
13. **placement is silently manual while docs imply V2-like dynamic assignment** — G9A must either ship flat-topology placement OR P15 beta must be explicitly marked as "declared-topology / manual-placement only"

## 6. Immediate Action

Before further T1 coding:

1. update `v3-phase-15-product-plan.md` to reference this gate map as the canonical P15 closure definition
2. revise `v3-phase-15-t1-assignment.md` so T1 is clearly **contract smoke only**, not product data continuity
3. create `P15 T0 Master / VolumeServer Hosting` assignment
4. require every subsequent assignment to include a `Gx` gate list and V2 port/reject list

## 7. After P15 Closure

After G22 passes, ongoing maintenance follows `v3-quality-system.md`:

1. every release stamps `v3-invariant-ledger.md`
2. every protocol-affecting change uses the PEP process under `protocol-evolutions/`
3. every production bug fix uses the route-closure standard: bad-state family, minimum repro, owner layer, closure point, regression
4. Final Gate safety-invariant scenarios are re-run for data-loss, stale-primary, authority, recovery, and migration-risk fixes
5. annual review refreshes `v3-protocol-truths.md`, `v3-semantic-constraint-checklist.md`, and the active test matrix

P15 closure is a release gate, not the end of verification discipline.
