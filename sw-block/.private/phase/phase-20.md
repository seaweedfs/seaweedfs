# Phase 20: V2 Brain into Production Binary

Date: 2026-04-05
Status: planned

## Premise

The V1 binary already works as an RF2 block product on m01/M02:
- HA tests pass (failover, rebuild, split-brain prevention)
- sw-test-runner scenarios pass (11 YAML, 2 hosts, real RDMA)
- CSI driver works

The V2 engine (Phases 14-19) proved the architecture is correct, the authority
split holds, and the composition chain works. But it runs in a parallel
simulation harness, not inside the production binary.

Phase 20 closes this gap. No new simulation layers. Every change lands in the
existing `weed volume` and `weed master` binaries.

The V1 blockvol engine (`weed/storage/blockvol/`) — WAL, flusher, shipper,
rebuild, iSCSI — stays untouched. Phase 20 changes who makes the *decision*
(master failover logic, VS activation logic), not who *executes* it.

## V2 Promise (Non-Negotiable)

These constraints govern every task in this phase and every future phase.

### Truth Ownership

- `master` may own: membership/liveness, desired assignment, epoch/lease/fencing,
  promotion authorization
- `master` must not own: continuous replication truth, rebuild choreography,
  "best effort" recovery heuristics that override fresh evidence
- the selected primary must own: reconstruction judgment, activation gating,
  catch-up/rebuild orchestration, replication-mode truth for its replica set
- `blockvol` / `weed/server` / transport layers are execution/host layers,
  not semantic truth owners

### Evidence Model

- heartbeat is lightweight observation, not the promotion oracle
- promotion must use fresh on-demand evidence at decision time
- stale heartbeat may discover candidates, but cannot be the sole correctness
  basis for promotion
- if fresh evidence cannot be obtained → fail closed by default
- any temporary legacy fallback must be: explicit, rollout-gated, observable,
  removable

### Promotion Selection

- durability-first: CommittedLSN dominates health-score heuristics
- health/readiness may only filter or tie-break after durability ordering
- all candidates ineligible → no promotion (fail closed)
- "pick something healthy-looking" is not allowed when durability truth is
  ambiguous

### Activation Gate

- assignment delivery is not permission to serve
- new primary must gate activation on reconstruction result locally
- `degraded`, `needs_rebuild`, `epoch_mismatch` → node must not serve
- enforcement point is local on the promoted node (not "wait for next
  heartbeat")
- heartbeat describes the already-gated state, it does not enforce it

### Mode Semantics (Two Distinct Concepts)

Two modes must be explicitly separate in code and operator surface:

1. **`EngineProjectionMode`** (local, VS-emitted):
   - emitted by the volume server from V2 engine state
   - answers "what is this node/volume currently projecting locally"
   - examples: `allocated_only`, `bootstrap_pending`, `publish_healthy`,
     `degraded`, `needs_rebuild`

2. **`ClusterReplicationMode`** (cluster-level, master-computed):
   - computed from multi-replica facts on the master
   - answers "what is the RF2 set health and continuity posture"
   - examples: `keepup`, `catching_up`, `degraded`, `needs_rebuild`

Do not call both `mode`. Do not let operators see two fields with the same
name. Code and API must make the distinction explicit.

### Recovery / Rebuild

- `needs_rebuild` is a real stop condition, never advisory
- every degraded path has one explicit exit: catch-up, rebuild, or fail-closed
  stop
- rebuild orchestration may reuse V1 execution, but the decision stays V2-owned
- repair/rebuild success feeds back into the same truth model that blocked
  activation

### Frontend / CSI / Operator

- read from runtime-owned truth
- do not define separate readiness semantics
- do not silently reinterpret degraded as healthy
- published address/target must correspond to the currently authorized and
  activated primary

### Legacy Migration

- replace the brain, not the body
- keep: existing binaries, host lifecycle, transport, `blockcmd`/`v2bridge`
  execution
- replace: ad-hoc failover selection, stale-heartbeat-only promotion, ad-hoc
  mode semantics, silent serve-after-promotion
- if old and new logic coexist temporarily: the active authority must be
  unambiguous and feature-flagged

## Hard Constraint

1. Every task must change code in `weed/server/` or `weed/storage/blockvol/`
2. No new files in `sw-block/runtime/volumev2/` unless adapter stubs
3. Validation is `sw-test-runner` on m01/M02, not new POC tests
4. The V1 blockvol engine must not change and must not regress
5. Fresh promotion evidence is mandatory for V2-mode failover
6. Durability-first candidate selection is mandatory
7. Local activation gate is mandatory before serving
8. `needs_rebuild` is mandatory fail-closed, never advisory
9. Local projection mode and cluster replication mode are separate concepts
10. Legacy fallback is explicit and temporary, never silent default

## What Already Works (VS Side)

The volume server already uses V2 core for assignment processing:

```
Assignment arrives via heartbeat response
  → v2Bridge.ConvertAssignment() → engine.AssignmentIntent
  → v2Core.ApplyEvent(AssignmentDelivered) → commands
  → blockcmd.Dispatcher.Run() → v2bridge.CommandBindings → blockvol
  → host effects emit observations back to core
```

This path is live. It handles: ApplyRole, StartReceiver, ConfigureShipper,
StartCatchUp / StartRebuild, PublishProjection.

## What Doesn't Use V2 Yet (Master Side)

```
Heartbeat stream lost → failoverBlockVolumes(deadServer)
  → lease wait (F2 timer)
  → PromoteBestReplica(): heartbeat-stale health/LSN gates
  → epoch bump in registry
  → assignment enqueue
```

Weaknesses: stale heartbeat data as promotion oracle, ad-hoc mode, no
fail-closed activation gate.

## Tasks

### T1: EngineProjectionMode in Heartbeat

**What**: The VS already runs V2 core and caches `PublicationProjection`.
Make the heartbeat carry the engine-derived local projection mode as a new
distinct field.

**Where**:
- `weed/storage/blockvol/block_heartbeat.go` — add `EngineProjectionMode
  string` field (NOT `V2Mode`, NOT reusing `VolumeMode`)
- `weed/server/volume_server_block.go` — populate from `bs.coreProj[path]`
- `weed/server/master_block_registry.go` — store as
  `entry.EngineProjectionMode` (separate field from existing `VolumeMode`)

**Truth rule**: `EngineProjectionMode` is the VS-local V2 engine projection.
`VolumeMode` remains the existing ad-hoc field until explicitly removed.
Both exist during transition; only `EngineProjectionMode` is V2-authoritative.

**Test**: One new test: VS with V2 core → heartbeat →
`EngineProjectionMode == "publish_healthy"` arrives at master registry.

### T2: Promotion Evidence Query RPC

**What**: Add an RPC on the volume server that returns fresh promotion
evidence on demand.

**Where**:
- `weed/pb/master.proto` — add message pair:
  - `QueryBlockPromotionEvidenceRequest { volume_name, epoch }`
  - `QueryBlockPromotionEvidenceResponse { committed_lsn, wal_head_lsn,
    engine_projection_mode, eligible, reason }`
- `weed/server/master_grpc_server_block.go` — handler reads live
  `blockvol.Status()` + `bs.coreProj[path]`
- Master calls this RPC during promotion, not during heartbeat

**Truth rule**: This is the V2 three-channel separation. Heartbeat =
liveness. Evidence query = fresh facts at decision time. Assignment =
authorization.

**Test**: Unit test for query handler returning live Status() values.

### T3: Durability-First Promotion Selection

**What**: Replace `PromoteBestReplica()` with V2-style selection.

**Where**:
- `weed/server/master_block_failover.go` — new function
  `promoteReplicaV2(volumeName string)`:
  1. Collect candidate replica addresses from registry
  2. Query each via T2 RPC for fresh evidence
  3. Filter: only `eligible == true` candidates
  4. Select: highest `CommittedLSN`, tie-break by `WALHeadLSN`, then
     `HealthScore`
  5. If zero eligible candidates → **fail closed, do not promote**
  6. Bump epoch, enqueue assignment to selected candidate

**Legacy fallback policy**:
- Add `--block.v2Promotion` flag (default `false` — safe rollout default
  until proto regen enables the evidence RPC; once RPC is live, flip to
  default `true`)
- When `true`: `promoteReplicaV2()` with fail-closed on evidence failure
- When `false`: existing `promoteReplicaV1()` (V1 path)
- The flag is observable via `/vol/status` and metrics
- The flag is intended to be removed once V2 is validated, not permanent

**What is NOT allowed**: silently falling back to V1 when evidence query
fails. If the flag is `true` and evidence cannot be obtained → fail closed.
Operator sees the failure and can either fix the network or toggle the flag.

**Test**: CommittedLSN ordering test. All-ineligible → no promotion test.
Flag-off → V1 path test.

### T4: Local Activation Gate on Promoted Primary

**What**: After a primary assignment is applied through V2 core, the VS
checks the resulting projection locally and gates activation before serving.

**Where**:
- `weed/server/volume_server_block.go` — in the assignment application path
  (`applyCoreAssignmentEvent` or `ApplyAssignments`), after V2 core emits
  commands and they execute:
  1. Read resulting `EngineProjectionMode` from core projection
  2. If mode is `needs_rebuild` or `degraded`:
     - Set local `activationGated = true`
     - Do NOT publish as serving primary
     - Do NOT accept frontend (iSCSI) connections for this volume
     - Log: `"activation gated: mode=%s reason=%s"`
  3. If mode is `publish_healthy` or `replica_ready`:
     - Clear gate, allow serving

**Enforcement**: The gate is LOCAL on the VS. It does not wait for the next
heartbeat. It does not rely on the master to tell it to stop. The heartbeat
then carries the already-gated state (`EngineProjectionMode == "degraded"`)
so the master can observe it.

**Truth rule**: Assignment delivery is not permission to serve. The promoted
node decides locally whether reconstruction quality allows activation.
Heartbeat is the report path, not the enforcement path.

**Test**: Promote a node whose reconstruction shows `degraded` → verify
volume is not exported via iSCSI. Fix state → verify activation proceeds.

### T5: ClusterReplicationMode on Master

**What**: The master evaluates RF2 set health from heartbeat data as a
separate cluster-level concept.

**Where**:
- `weed/server/master_block_registry.go` — new function
  `evaluateClusterReplicationMode(entry *BlockVolumeEntry) string`:
  - All replicas `EngineProjectionMode == "publish_healthy"` + LSN within
    tolerance → `"keepup"`
  - Any replica catching up (LSN gap > threshold, recovery in progress)
    → `"catching_up"`
  - Any replica barrier-failed or mode degraded → `"degraded"`
  - Any replica `needs_rebuild` → `"needs_rebuild"`
  - Monotonic: worst replica state dominates
- Store as `entry.ClusterReplicationMode` (NOT `entry.VolumeMode`)
- Expose in `/vol/status` API and heartbeat diagnostics

**Truth rule**: `ClusterReplicationMode` is the master's cluster-level
replication health judgment. It is distinct from `EngineProjectionMode`
(VS-local). They answer different questions. They live in different fields.
They have different names.

**Test**: Unit test matrix:
- All replicas healthy → `keepup`
- One replica behind → `catching_up`
- One replica barrier-failed → `degraded`
- One replica needs rebuild → `needs_rebuild`

### T6: Hardware Validation on m01/M02

**What**: Run full sw-test-runner suite + one new V2-specific scenario.

**Where**:
- All 11 existing scenarios must pass with V2 brain active
  (`--block.v2-promotion=true`)
- New scenario `v2-failover-gate.yaml`:
  1. Create RF=2 volume, write data
  2. Corrupt replica state (force needs_rebuild via WAL gap)
  3. Kill primary
  4. Verify promotion is gated (new primary does not serve)
  5. Repair replica (rebuild)
  6. Verify activation proceeds after rebuild
  7. Read data back — matches

**Test**: `sw-test-runner run --all` on m01/M02.

### T7: Full Regression Suite

**Verification**:
```bash
go test ./weed/storage/blockvol/ -count=1 -timeout 120s
go test ./weed/server/ -count=1 -timeout 120s
go test ./weed/storage/blockvol/csi/ -count=1 -timeout 60s
go test ./sw-block/engine/replication/ -count=1 -timeout 60s
go test ./sw-block/runtime/masterv2/ ./sw-block/runtime/volumev2/ -count=1
sw-test-runner run --all  # on m01/M02
```

## Dependency Order

```
T1 (EngineProjectionMode in heartbeat)  — no deps, additive field
T2 (promotion evidence query RPC)       — no deps, new RPC
T3 (durability-first promotion)         — depends on T2
T4 (local activation gate)              — depends on T1 (reads projection)
T5 (ClusterReplicationMode on master)   — depends on T1 (reads projection)
T6 (hardware validation)                — depends on T1-T5
T7 (regression suite)                   — depends on T1-T5
```

T1 and T2 can run in parallel. T3, T4, T5 can partially overlap.

## File-Level Responsibility Map

### `weed/server/master_block_failover.go`
- Allowed: trigger detection (heartbeat loss), lease wait (F2), candidate
  discovery, evidence query dispatch, promotion authorization, epoch bump,
  assignment enqueue, deferred timer management, pending rebuild recording
- NOT allowed: reconstruction judgment, mode evaluation, activation
  enforcement, recovery choreography

### `weed/server/master_block_registry.go`
- Allowed: store heartbeat-observed facts, compute
  `ClusterReplicationMode` from multi-replica facts, serve registry
  lookups, manage assignment queue, expose operator diagnostics
- NOT allowed: override VS-emitted `EngineProjectionMode`, decide
  activation for a volume, own replication truth beyond cluster-level
  observation

### `weed/server/volume_server_block.go`
- Allowed: apply assignments through V2 core, execute commands through
  dispatcher, gate activation locally based on core projection, emit
  `EngineProjectionMode` in heartbeat, run catch-up/rebuild through
  existing execution path
- NOT allowed: override master's promotion authority, override master's
  epoch/lease, define alternative mode semantics

### `weed/storage/blockvol/block_heartbeat.go`
- Allowed: carry `EngineProjectionMode` as a distinct field alongside
  existing fields
- NOT allowed: merge `EngineProjectionMode` into `VolumeMode`, carry
  cluster-level mode (that belongs on the master)

## What This Phase Does NOT Do

1. Replace heartbeat gRPC transport — it stays
2. Replace WAL shipper — it stays (V1 execution, V2 orchestrated)
3. Replace assignment queue — it stays
4. Change blockvol engine internals — WAL/flusher/rebuild untouched
5. Build new simulation tests — sw-test-runner is the oracle
6. Add new files to `sw-block/runtime/volumev2/`

## Exit Criteria

1. `EngineProjectionMode` flows VS → master as a distinct field
2. `ClusterReplicationMode` is computed on master as a distinct field
3. Failover uses fresh evidence RPC, not stale heartbeat
4. Promotion selects by CommittedLSN, fail-closed on zero eligible
5. Promoted primary gates activation locally before serving
6. Legacy fallback is explicit flag, not silent default
7. All 11 existing sw-test-runner scenarios pass on m01/M02
8. One new V2-specific scenario passes on m01/M02
9. All unit test suites remain green

## What This Proves

After Phase 20, the production binary has V2 correctness:
- Durability-first promotion (not health-score-first)
- Fail-closed activation gating (not silent serve)
- Engine-derived local mode + cluster-level replication mode (not ad-hoc)
- Fresh evidence at decision time (not stale heartbeat)
- V2 promise preserved: master is identity authority, primary owns
  data-control truth, transport carries facts, surfaces are projections

And it runs on real hardware with real network, real iSCSI clients, and
real WAL shipping — not in a simulation harness.

## Reviewer Packs

### T2: Promotion Evidence Query RPC

**Allowed**: Dedicated RPC for promotion evidence. Return fresh local facts
from queried VS at call time. Read from local blockvol status and V2 core
projection. Return explicit eligibility plus reason. Keep heartbeat and
evidence query as separate channels.

**Not allowed**: Reuse heartbeat payload as promotion decision source.
Reconstruct evidence from master registry caches. Let master guess
engine_projection_mode. Hide evidence failure behind silent fallback when
V2 path is enabled. Mix assignment authorization into the evidence RPC.

**Truth owner**: Local storage/runtime facts = blockvol. Local semantic
projection and eligibility = V2 engine on queried VS. Master only consumes
evidence; it does not own or synthesize it.

**Required tests**: (1) Handler returns live committed_lsn / wal_head_lsn.
(2) Handler returns current engine_projection_mode from core projection.
(3) Handler returns eligible=false with explicit reason for gated states.
(4) Query against stale/unknown/missing volume fails cleanly.
(5) Proto/wire field presence/absence handled correctly.

**Pitfalls**: Using cached registry state instead of querying fresh VS-local
facts. Smuggling promotion policy into handler. Making RPC look like
"mini assignment" instead of evidence-only observation.

### T3: Durability-First Promotion Selection

**Allowed**: Master collects candidates from registry membership. Queries
each via T2 at failover time. Filter on eligible==true. Rank by
CommittedLSN then WALHeadLSN then health. Fail closed when no eligible
candidate. Explicit feature flag for temporary V1 fallback.

**Not allowed**: Promote based only on last heartbeat. Rank health before
durability. Silently fall back to V1 when evidence query fails in V2 mode.
Treat "best-looking candidate" as sufficient when durability is ambiguous.
Let master override a node's ineligibility reason.

**Truth owner**: Candidate durability/eligibility = queried VS local state +
engine. Promotion authorization = master. Registry = cluster membership/index
state, not evidence truth owner.

**Required tests**: (1) Higher CommittedLSN wins even if health lower.
(2) Equal CommittedLSN, higher WALHeadLSN wins. (3) All ineligible => no
promotion. (4) Evidence query failure in V2 mode => fail-closed. (5)
Flag-off uses legacy. (6) Epoch bump + assignment enqueue only after
successful selection.

**Pitfalls**: Leaving old PromoteBestReplica() heuristics in decision path.
Making flag a silent rescue. Letting registry-side stale WALHeadLSN
participate in final ordering.

### T4: Local Activation Gate

**Allowed**: After assignment through V2 core, read resulting local
projection. Gate local activation based on mode/reason. Refuse serving
while gated. Clear gate only when projection reaches allowed serving state.
Heartbeat may report gated state after enforcement.

**Not allowed**: Treat assignment delivery as permission to serve. Wait for
master/heartbeat to enforce no-serve. Allow frontend/iSCSI publish while
degraded or needs_rebuild. Reinterpret bad reconstruction as "good enough."
Put gate only in operator surfaces while serving still proceeds.

**Truth owner**: Reconstruction judgment and activation gate = promoted
primary local V2 engine/runtime. Master may observe, does not enforce.
Frontend/export = execution surface only.

**Required tests**: (1) Degraded projection does not export/serve.
(2) needs_rebuild does not export/serve. (3) Healthy projection clears
gate. (4) Gate enforced before heartbeat round-trip. (5) Recovery from
gated to healthy re-enables serving.

**Pitfalls**: Gate enforced too late (after export). Checking VolumeMode
instead of local engine projection. Gate advisory in logs but not in
serving paths.

### T5: ClusterReplicationMode on Master

**Allowed**: Compute new master-owned field from multi-replica facts. Keep
separate from EngineProjectionMode. Use for cluster/operator judgment.
Derive from replica set facts, freshness, lag. Distinct field on registry
entry and surfaces.

**Not allowed**: Reuse/rename VolumeMode. Copy primary's
EngineProjectionMode into ClusterReplicationMode. Collapse local and
cluster concepts. Expose two ambiguous generic mode fields. Override local
VS truth with master-computed local semantics.

**Truth owner**: EngineProjectionMode = VS-local engine truth.
ClusterReplicationMode = master-owned cluster judgment. VolumeMode = legacy
transitional, not new semantic source.

**Required tests**: (1) All healthy => keepup. (2) Replica behind =>
catching_up. (3) Missing/failed => degraded. (4) Unrecoverable gap =>
needs_rebuild. (5) Explicit proof the two modes can differ without
conflict. (6) Surface/API shows distinct naming.

**Pitfalls**: Computing from only primary-local projection. Reusing old
VolumeMode semantics. Exposing field ambiguously.

### Cross-Task Guardrails

**Allowed**: Additive migration with explicit flags. Reuse V1 execution
while replacing decision ownership. Projection/cache layers carrying truth
from actual owner. Fail-closed when critical evidence unavailable.

**Not allowed**: Silent fallback. Dual-truth mode handling with unclear
authority. Master-side invention of local semantic truth. New
simulation-only seams bypassing production binary path.

**Global required tests**: (1) End-to-end failover exercising T2+T3+T4.
(2) No serve-after-promotion when activation gated. (3) Operator surface
proves local vs cluster modes distinct. (4) Legacy-flag test proves
fallback is explicit and observable.

**Recurring failure pattern to watch**: Heartbeat becomes overloaded into
liveness + evidence + decision. Master starts "helpfully" reconstructing
local semantics. Local gate exists in logs/surfaces but actual serving path
still open.
