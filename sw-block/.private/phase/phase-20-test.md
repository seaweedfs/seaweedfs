# Phase 20 Test Matrix

Date: 2026-04-06
Status: active

## Purpose

This document defines the test coverage matrix for `Phase 20`.

It exists to answer five practical questions:

1. which `Phase 20` capability is being proven
2. which proof tier should carry that proof
3. which test layer is the cheapest valid place to prove it
4. which named scenarios should exist
5. what remains for `T6` / `T7` hardware validation and regression closure

This document is derived from:

1. `sw-block/.private/phase/phase-20.md`
2. `sw-block/design/v2-capability-map.md`

It should be read as the concrete test-execution companion for `Phase 20`.

## Why This Exists

`Phase 20` moved the system from a simpler `struct + function` path toward a
more explicit `event -> engine -> command -> adapter -> blockvol` execution
chain inside the real binary.

That raises two risks:

1. semantic drift across layers
2. apparent correctness from local tests without real whole-chain proof

So `Phase 20` testing must be tightened around capability proof, not around
"which file changed."

## Mapping To Capability Map

`v2-capability-map.md` uses four proof tiers:

1. `Core semantic`
2. `Seam / adapter`
3. `Integrated runtime`
4. `Soak / benchmark / adversarial`

For `Phase 20`, we map them to practical test layers as follows:

| Capability-map proof tier | Phase 20 practical layer | Main purpose |
|---|---|---|
| Core semantic | Unit test | prove pure rules, ordering, fail-closed semantics |
| Seam / adapter | Component test | prove real `weed/server` wiring and local closure |
| Integrated runtime | Integration test | prove real multi-server binary path |
| Soak / benchmark / adversarial | Hardware / runner | prove disturbance, real transport, real clients |

## Test Layer Definitions

### Unit

Use when the thing being tested is primarily:

1. a pure rule
2. a state transition
3. a ranking/order decision
4. a fail-closed invariant
5. a conversion or mapping

Goals:

1. cheapest feedback
2. broad negative coverage
3. precise invariant locking

Typical homes:

1. `sw-block/engine/replication/*`
2. pure helper logic inside `weed/server/*`
3. proto / wire conversion logic

### Component

Use when the thing being tested is:

1. a real production module
2. a real `weed/server` path
3. a local closure chain inside one process
4. a real enforcement or API surface using real structs and real adapters

Goals:

1. prove production wiring is correct
2. catch adapter drift cheaply
3. prove local enforcement is real, not just reported

Typical homes:

1. `weed/server/*_test.go`
2. `weed/storage/blockvol/*_test.go`

### Integration

Use when the thing being tested depends on:

1. more than one server or role
2. multiple channels (`heartbeat`, `query`, `assignment`)
3. real failover timing
4. true master/volume interaction

Goals:

1. prove whole-chain correctness
2. prove no cross-layer semantic collapse
3. prove failover and recovery actually close

Typical homes:

1. `weed/server/qa_*`
2. multi-server tests under `weed/server`

### Hardware / Runner

Use when correctness depends on:

1. real m01/M02 transport
2. real iSCSI / NVMe reconnect behavior
3. disturbance timing
4. sw-test-runner scenario closure

Goals:

1. prove integrated runtime on real hosts
2. prove no timing-only failure classes remain hidden
3. support `Phase 20` exit criteria and rollout judgment

Typical homes:

1. `sw-test-runner`
2. m01/M02 scenario pack
3. full regression runs

## Coverage Policy

Every `Phase 20` capability should have:

1. at least one `Unit` proof for the rule itself
2. at least one `Component` proof for the real production path
3. at least one `Integration` proof if the capability crosses server or channel boundaries

`Hardware / Runner` is mandatory for any capability whose closure claim includes:

1. failover
2. continued IO
3. frontend reconnect behavior
4. real transport or disturbance

## Capability Coverage Matrix

Legend:

1. `Must`: required before the capability can be called closed
2. `Should`: strongly preferred, high-value regression protection
3. `Optional`: useful but not first-line closure proof

| Capability | Unit | Component | Integration | Hardware / Runner | Main invariant |
|---|---|---|---|---|---|
| `T1` local engine projection field is distinct | Must | Must | Optional | No | `EngineProjectionMode` is VS-local truth and never collapses into legacy/master mode |
| `T1` heartbeat propagation | Should | Must | Should | No | VS projection reaches master as observation, not reconstructed truth |
| `T2` fresh evidence semantics | Must | Must | Must | Should | promotion facts come from live local query, not heartbeat cache |
| `T2` fail-closed evidence rejection | Must | Must | Must | Should | missing projection, epoch mismatch, missing volume, query failure do not produce eligibility |
| `T3` durability-first promotion ordering | Must | Must | Must | Should | `CommittedLSN > WALHeadLSN > HealthScore` dominates |
| `T3` no silent fallback | Must | Must | Must | Should | V2-on path never silently falls back to V1 on missing/partial evidence |
| `T3` rollout observability | Optional | Must | Should | Should | operator can see disabled / placeholder / transport-ready state |
| `T4` local gate decision | Must | Must | Must | Should | degraded / needs_rebuild / missing projection => no serve |
| `T4` real frontend enforcement | Optional | Must | Must | Must | frontend target is actually disconnected and later restored |
| `T4` recovery ungate | Should | Must | Must | Must | recovery transitions clear gate and serving resumes |
| `T5` cluster mode computation | Must | Must | Should | No | cluster judgment stays master-owned and worst-case dominates |
| `T5` local vs cluster mode separation | Must | Must | Should | No | `EngineProjectionMode` and `ClusterReplicationMode` can differ and must differ in name |
| `T5` surface exposure | Optional | Must | Should | No | block API and diagnostics expose distinct fields |
| Cross-layer consistency | Should | Must | Must | Should | projection -> heartbeat -> registry -> API stay aligned |
| End-to-end failover closure | No | Should | Must | Must | failover, gate, recovery, and cluster mode close on real path |
| Negative fail-closed matrix | Must | Must | Must | Must | ambiguity always biases toward stop / reject / degrade |

## Phase 20 Current Coverage Reading

Post-Tier-1 component test implementation. 49 total tests.

| Capability | Unit | Component | Integration | Hardware / Runner | Current reading |
|---|---|---|---|---|---|
| `T1` | strong (2/2) | **strong (3/3)** | missing (0/1) | not yet | proto round-trip added; distinctness + preservation covered |
| `T2` | strong (3/3) | strong (2/2) | missing (0/1) | not yet | semantics and fail-closed proven; no integrated master→VS query test |
| `T3` | strong (3/3) | **strong (5/5)** | bounded (2/2) | not yet | V2PromotionMode tri-state diagnostic added |
| `T4` | strong (1/1) | **strong (7/7)** | missing (0/2) | not yet | missing projection fail-closed + iSCSI target removal enforcement + proto round-trip all proven |
| `T5` | strong (6/6) | **strong (3/3)** | missing (0/1) | not yet | diagnostic snapshot carries modes proven |

### Coverage Summary by Layer

| Layer | Strong | Bounded | Missing | Total |
|---|---|---|---|---|
| Unit | 15 | 0 | 0 | 15 |
| Component | 16 | 1 | 0 | 17 |
| Integration | 2 | 2 | 6 | 10 |
| Hardware | 0 | 0 | 4 | 4 |

The one remaining bounded component test is `P20-T4-C7` (NVMe gate
alongside iSCSI) — requires a test NVMe server which may not be available
in the current test infrastructure.

### Component Tests Implemented (Tier 1 — commit `6bf9a6c28`)

| Roster ID | Test function | File |
|---|---|---|
| P20-T4-C3 | `TestT4_MissingProjection_FailsClosed` | `volume_grpc_block_activation_gate_test.go` |
| P20-T4-C6 | `TestT4_GateRemovesISCSITarget` | `volume_grpc_block_activation_gate_test.go` |
| P20-T5-C3 | `TestT5_DiagnosticSnapshot_CarriesModes` | `master_block_cluster_mode_test.go` |
| P20-T3-C5 | `TestT3_V2PromotionMode_DiagnosticTriState` | `master_block_cluster_mode_test.go` |
| P20-T1-C3 | `TestP20_ProtoRoundTrip_EngineProjectionMode` | `block_heartbeat_proto_test.go` |
| P20-T4-C8 | `TestP20_ProtoRoundTrip_ActivationGated` | `block_heartbeat_proto_test.go` |

### Tester-Found Wiring Bugs (all caught by production path review)

Every tester finding during T1-T5 was a **wiring bug**, not a logic bug:
- T1: stale EngineProjectionMode survived primary turnover (registry wiring)
- T2: fail-open without core projection (evidence wiring)
- T3: silent V1 fallback when querier nil (failover wiring)
- T4: gate was bookkeeping, not enforcement (frontend wiring)
- T5: missing replica not degraded, transport signal ignored (registry wiring)

None were caught by unit tests. All were caught reviewing the production
path. The Tier 1 component tests now lock these wiring paths against
regression.

### Remaining Gaps

**Integration (Tier 2 — pre-hardware, medium cost)**:
- P20-X-I1: end-to-end V2 failover chain
- P20-T4-I1: promoted degraded node does not serve
- P20-T5-I1: cluster mode evolves across failover

**Hardware (Tier 3 — T6/T7 on m01/M02)**:
- P20-T2-H1: evidence RPC under network delay
- P20-T3-H1: failover under disturbance
- P20-T4-H1: v2-failover-gate scenario
- P20-T5-H1: real disturbance cluster judgment

## Detailed Test Roster

The roster below is the recommended named test inventory for `Phase 20`.

Naming convention:

1. `P20-Tn-Ux` = unit
2. `P20-Tn-Cx` = component
3. `P20-Tn-Ix` = integration
4. `P20-Tn-Hx` = hardware / runner

The names below are scenario descriptions, not necessarily final function names.

## T1: EngineProjectionMode In Heartbeat

### Unit

#### `P20-T1-U1 EngineProjectionMode Distinct From VolumeMode`

Scenario:

1. local engine projection is set to one value
2. legacy `VolumeMode` path is absent or different
3. registry/API must preserve both as distinct concepts

Must prove:

1. no value copying from `EngineProjectionMode` into `VolumeMode`
2. no value copying from `VolumeMode` into `EngineProjectionMode`

#### `P20-T1-U2 Missing Field Preservation Rules`

Scenario:

1. same primary omits `engine_projection_mode`
2. old primary value should be preserved for backward compatibility
3. new primary omits field after turnover
4. stale old-primary value must clear

Must prove:

1. same-primary omission preserves
2. new-primary omission clears

### Component

#### `P20-T1-C1 VS Projection Reaches Master Registry`

Scenario:

1. VS has local V2 core projection
2. heartbeat is built and consumed
3. master registry stores `EngineProjectionMode`

Must prove:

1. VS emits local truth only
2. master stores observation only

#### `P20-T1-C2 Registry Keeps EngineProjectionMode Separate From VolumeMode`

Scenario:

1. heartbeat carries `EngineProjectionMode`
2. `VolumeMode` is absent or computed differently

Must prove:

1. registry fields remain distinct

### Integration

#### `P20-T1-I1 Multi-Server Heartbeat Preserves Local Truth`

Scenario:

1. real master + real VS path
2. local engine projection changes
3. heartbeat updates master registry and API

Must prove:

1. real binary path preserves the distinction end-to-end

## T2: Promotion Evidence Query

### Unit

#### `P20-T2-U1 Evidence Uses Core CommittedLSN`

Scenario:

1. local status has WAL head
2. core boundary has committed LSN
3. evidence must use committed boundary unconditionally when core exists

Must prove:

1. no fallback from `CommittedLSN == 0` to `WALHeadLSN`

#### `P20-T2-U2 MissingEngineProjection Fails Closed`

Scenario:

1. VS block volume exists
2. no V2 projection exists

Must prove:

1. `Eligible=false`
2. explicit `missing_engine_projection`

#### `P20-T2-U3 EpochMismatch Fails Closed`

Scenario:

1. caller expects epoch X
2. local volume reports epoch Y

Must prove:

1. mismatch produces ineligible evidence

### Component

#### `P20-T2-C1 Query Returns Live Local Facts`

Scenario:

1. write to local volume
2. query evidence immediately

Must prove:

1. query reads current local state
2. not heartbeat cache

#### `P20-T2-C2 Gated Projection Returns Ineligible Evidence`

Scenario:

1. local projection is `degraded`, `needs_rebuild`, or `bootstrap_pending`

Must prove:

1. evidence stays ineligible with explicit reason

### Integration

#### `P20-T2-I1 Master Uses Query Channel, Not Heartbeat Cache`

Scenario:

1. primary/replica heartbeat is stale or behind
2. direct evidence query returns fresher facts

Must prove:

1. failover decision path uses query-time facts

### Hardware / Runner

#### `P20-T2-H1 Evidence RPC Survives Real Network Delay`

Scenario:

1. m01/M02 with real query path
2. inject moderate delay / packet disturbance

Must prove:

1. clear fail-closed behavior on timeout or transport loss

## T3: Durability-First Promotion

### Unit

#### `P20-T3-U1 HigherCommittedLSN Wins`

Scenario:

1. lower-health candidate has higher committed boundary

Must prove:

1. durability dominates health

#### `P20-T3-U2 WALHead Breaks Committed Tie`

Scenario:

1. committed boundaries equal
2. WAL head differs

Must prove:

1. WAL head tie-break is applied

#### `P20-T3-U3 Health Breaks Final Tie`

Scenario:

1. committed and WAL head equal
2. health differs

Must prove:

1. health is final tie-break only

### Component

#### `P20-T3-C1 AllIneligible NoPromotion`

Scenario:

1. all candidates return ineligible evidence

Must prove:

1. no promotion
2. epoch unchanged
3. no assignment queued

#### `P20-T3-C2 NilQuerier FailClosed`

Scenario:

1. V2 promotion enabled
2. no querier installed

Must prove:

1. no silent V1 fallback

#### `P20-T3-C3 PartialEvidence FailClosed`

Scenario:

1. one candidate responds
2. another candidate query fails

Must prove:

1. no promotion from incomplete evidence

#### `P20-T3-C4 RolloutFlag Explicit`

Scenario:

1. `block.v2Promotion=false`
2. `block.v2Promotion=true`

Must prove:

1. V1 and V2 paths are chosen explicitly
2. observability reflects active mode

### Integration

#### `P20-T3-I1 Real Failover Uses DurabilityFirst Winner`

Scenario:

1. master + primary + multiple replica candidates
2. candidates differ in committed/wal/health

Must prove:

1. real master path promotes the correct winner

#### `P20-T3-I2 EpochBumpAndAssignmentOnlyAfterSelection`

Scenario:

1. failover occurs
2. candidate winner selected

Must prove:

1. registry mutation and assignment happen only after valid selection

### Hardware / Runner

#### `P20-T3-H1 Real Failover Under Disturbance Remains FailClosed`

Scenario:

1. query loss or partial transport disturbance during failover

Must prove:

1. system stops rather than promoting on ambiguity

## T4: Local Activation Gate

### Unit

#### `P20-T4-U1 GateClassification`

Scenario:

1. local projection enters `degraded`, `needs_rebuild`, or missing projection

Must prove:

1. each state maps to gated locally

### Component

#### `P20-T4-C1 DegradedProjection DisconnectsServing`

Scenario:

1. promoted node projection becomes `degraded`

Must prove:

1. frontend registration is removed
2. heartbeat reports `activation_gated=true`

#### `P20-T4-C2 NeedsRebuild DisconnectsServing`

Scenario:

1. promoted node projection becomes `needs_rebuild`

Must prove:

1. no serve
2. explicit gate reason

#### `P20-T4-C3 MissingProjection FailClosed`

Scenario:

1. V2 core active
2. projection missing for the path

Must prove:

1. local frontend is gated
2. reason is `missing_engine_projection`

#### `P20-T4-C4 RecoveryClearsGateAndRestoresFrontends`

Scenario:

1. node starts gated
2. core event moves projection to `replica_ready` then `publish_healthy`

Must prove:

1. gate clears
2. iSCSI returns
3. NVMe returns when enabled

#### `P20-T4-C5 GateBeforeHeartbeat`

Scenario:

1. assignment is processed
2. gate is evaluated immediately
3. heartbeat is collected afterward

Must prove:

1. heartbeat reports already-enforced state

### Integration

#### `P20-T4-I1 PromotedDegradedNodeDoesNotServe`

Scenario:

1. failover selects a node
2. local projection is gated

Must prove:

1. node does not accept frontend IO

#### `P20-T4-I2 RecoveryAfterGateRestoresServing`

Scenario:

1. gated promoted node later recovers

Must prove:

1. serving resumes only after local recovery truth allows it

### Hardware / Runner

#### `P20-T4-H1 v2-failover-gate`

Scenario:

1. create RF=2 volume
2. write data
3. force degraded / rebuild-needed condition
4. kill primary
5. verify promotion stays gated
6. repair / rebuild
7. verify serving resumes
8. verify data continuity

Must prove:

1. real fail-closed gate under hardware conditions

## T5: ClusterReplicationMode

### Unit

#### `P20-T5-U1 AllHealthyKeepup`

Scenario:

1. all replicas healthy and current

Must prove:

1. `keepup`

#### `P20-T5-U2 ReplicaBehindCatchingUp`

Scenario:

1. one replica behind but recoverable

Must prove:

1. `catching_up`

#### `P20-T5-U3 StaleOrFailedReplicaDegraded`

Scenario:

1. stale heartbeat or transport degraded or RF2 missing replica

Must prove:

1. `degraded`

#### `P20-T5-U4 UnrecoverableGapNeedsRebuild`

Scenario:

1. lag beyond rebuild threshold or rebuilding role

Must prove:

1. `needs_rebuild`

#### `P20-T5-U5 WorstReplicaDominates`

Scenario:

1. multiple replicas in mixed states

Must prove:

1. worst state dominates cluster mode

#### `P20-T5-U6 DistinctFromEngineProjectionMode`

Scenario:

1. local primary reports healthy local projection
2. one replica is behind

Must prove:

1. `EngineProjectionMode != ClusterReplicationMode`

### Component

#### `P20-T5-C1 HeartbeatAdvanceChangesClusterMode`

Scenario:

1. initial keepup
2. primary WAL head advances
3. replica lags

Must prove:

1. registry recomputes mode to `catching_up`

#### `P20-T5-C2 BlockVolumeAPIShowsDistinctFields`

Scenario:

1. block volume lookup/list API response is generated

Must prove:

1. `volume_mode`
2. `engine_projection_mode`
3. `cluster_replication_mode`
4. names and values can differ

#### `P20-T5-C3 FailoverDiagnosticShowsDistinctFields`

Scenario:

1. failover diagnostic snapshot is produced

Must prove:

1. diagnosis entry carries both local and cluster modes

### Integration

#### `P20-T5-I1 ClusterModeEvolvesAcrossFailoverAndRecovery`

Scenario:

1. healthy RF2 set
2. replica falls behind
3. failover/rejoin/rebuild happens

Must prove:

1. cluster mode changes through the expected sequence

### Hardware / Runner

#### `P20-T5-H1 Real RF2 Disturbance Produces Conservative Cluster Judgment`

Scenario:

1. real hardware disturbance
2. lag / stale / partial failure

Must prove:

1. cluster mode never overstates health

## Cross-Task Scenarios

These are the highest-value multi-capability scenarios for `Phase 20`.

### `P20-X-I1 EndToEnd_FreshEvidence_Failover_Gate_Recovery`

Capabilities covered:

1. `T2`
2. `T3`
3. `T4`
4. `T5`

Scenario:

1. RF=2 volume is healthy
2. primary is lost
3. master queries fresh evidence
4. durability-first selection chooses candidate
5. promoted node initially gates or serves based on local truth
6. heartbeat and API surfaces reflect resulting local and cluster state
7. if recovery completes, serving resumes and cluster mode improves

Must prove:

1. full `query -> promote -> assign -> gate -> recover -> observe` chain

### `P20-X-I2 NoSilentFallbackOnAmbiguousFailover`

Capabilities covered:

1. `T2`
2. `T3`

Scenario:

1. V2 promotion flag enabled
2. evidence missing, partial, or stale

Must prove:

1. no promotion
2. no V1 fallback
3. explicit observability

### `P20-X-I3 LocalVsClusterModeDivergenceVisible`

Capabilities covered:

1. `T1`
2. `T4`
3. `T5`

Scenario:

1. primary local projection is healthy enough locally
2. cluster still has lagging or impaired replica

Must prove:

1. local mode and cluster mode can differ
2. surfaces preserve both without collapsing them

## Recommended Test File Placement

### Unit

Prefer:

1. `sw-block/engine/replication/*_test.go`
2. `weed/server/master_block_evidence_test.go`
3. `weed/server/master_block_cluster_mode_test.go`
4. `weed/storage/blockvol/block_heartbeat_proto_test.go`

### Component

Prefer:

1. `weed/server/master_block_registry_test.go`
2. `weed/server/master_block_failover_test.go`
3. `weed/server/volume_server_block_test.go`
4. `weed/server/master_server_handlers_block_test.go`

### Integration

Prefer:

1. focused `weed/server/qa_*` tests
2. real multi-server tests under `weed/server`

### Hardware / Runner

Prefer:

1. `sw-test-runner`
2. m01/M02 scenario pack

## Current Hardware Readout

Latest real-hardware observation after `V1` auto-failover:

| Field | Before | After |
|---|---|---|
| `primary` | `10.0.0.1:18480` | `10.0.0.3:18480` |
| `epoch` | `1` | `2` |
| `volume_mode` | `allocated_only` | `allocated_only` |
| `cluster_replication_mode` | `keepup` | `degraded` |
| `health_state` | `healthy` | `unsafe` |

This is an important signal, not noise.

It shows that the three surfaces are already telling a coherent post-failover
story:

1. `EngineProjectionMode` / local projection view:
   promoted VS is primary but still locally incomplete
2. `ClusterReplicationMode` / master cluster view:
   the replica set is degraded because the old primary is gone
3. legacy `health_state`:
   the old path also sees danger, but with less precise vocabulary

The key current hardware gap is:

1. promoted VS local core still shows `ReplicaIDs=[]`
2. `RoleApplied=true` but `ShipperConfigured=false`
3. the V2 brain therefore cannot reach the full `publish_healthy` closure path

This should currently be read as:

1. `T5` mode separation is behaving correctly on hardware
2. `T4` fail-closed logic is still valuable and necessary
3. the blocking issue for stronger `T6/T7` closure is still the bootstrap /
   replica-membership wiring gap, not the existence of V2 mode surfaces

## T6/T7 Staged Validation Plan

`T6` and `T7` should not be treated as one single pass/fail run.

They should execute in stages.

### Stage 0: Bootstrap Closure Prerequisite

Goal:

1. fix the existing primary-core visibility gap so the promoted primary learns
   its replica membership

Required hardware proof:

1. promoted primary no longer shows `ReplicaIDs=[]`
2. `ShipperConfigured=true` can be reached on the healthy path
3. healthy RF=2 path can reach `publish_healthy` on real hosts

Recommended scenario:

1. `P20-H0 RF2BootstrapReplicaMembershipCloses`

### Stage 1: V1 Failover + V2 Observation

Run with:

1. `--block.v2Promotion=false`

Goal:

1. keep failover authority on the old path
2. validate that the V2 brain, local projection, cluster mode, and operator
   surfaces observe the real system correctly

Must prove:

1. `cluster_replication_mode` changes conservatively after primary loss
2. local projection on the promoted node remains honest
3. legacy `health_state` and V2 surfaces are directionally consistent without
   collapsing into one field
4. no regression to existing `V1` failover behavior

Recommended scenario:

1. `P20-T6-H1 V1Failover_V2Observation_SurfacesCoherent`

### Stage 2: V2 Failover + V2 Decision

Run with:

1. proto regenerated
2. real evidence RPC wired
3. `--block.v2Promotion=true`

Goal:

1. move failover authority onto the V2 decision path

Must prove:

1. promotion uses fresh evidence, not heartbeat cache
2. durability-first candidate selection wins on real hosts
3. missing / partial / stale evidence fail closed
4. activation gate blocks serving on degraded or rebuild-needed reconstructions

Recommended scenarios:

1. `P20-T6-H2 V2Failover_DurabilityFirstSelection`
2. `P20-T6-H3 V2Failover_FailClosedOnAmbiguousEvidence`
3. `P20-T6-H4 V2Failover_GatedPromotionRequiresRecovery`

### Stage 3: Side-By-Side Behavioral Comparison

Goal:

1. compare the same data-verified scenarios under `V1` failover and `V2`
   failover

Must prove:

1. `V2` preserves all supported good outcomes
2. `V2` is stricter where ambiguity exists
3. any divergence is explainable as "V2 fails closed where V1 guessed"

Recommended scenario:

1. `P20-T7-H1 Compare_V1AndV2_OnSameFailoverMatrix`

## Minimum Phase 20 Closure Gate

Before `Phase 20` can be called fully closed, require:

1. `T1-T5` all have at least one passing unit proof and one passing component proof
2. at least one integrated scenario proves `T2 + T3 + T4` together
3. at least one integrated scenario proves `T1 + T5` surfaces remain distinct
4. real hardware shows the promoted primary can learn replica membership and
   reach non-empty `ReplicaIDs` on the healthy RF=2 path
5. `sw-test-runner run --all` passes on m01/M02 with V2 promotion active
6. one explicit `v2-failover-gate` or equivalent hardware scenario passes
7. full `weed/server`, `weed/storage/blockvol`, `weed/storage/blockvol/csi`, and `sw-block/engine/replication` regression suites are green

## Full Roster Status (post-Tier-1)

49 tests total. Each roster item marked with current status.

### T1

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-T1-U1 | unit | **strong** | `EngineProjectionModeDistinctFromVolumeMode` |
| P20-T1-U2 | unit | **strong** | `AbsentPreservesExisting` + `ClearsOnPrimaryTurnover` + `PreservedOnNewPrimaryWithField` |
| P20-T1-C1 | component | **strong** | `ConsumesEngineProjectionModeFromPrimaryHeartbeat` |
| P20-T1-C2 | component | **strong** | `EngineProjectionModeDistinctFromVolumeMode` |
| P20-T1-C3 | component | **strong** | `TestP20_ProtoRoundTrip_EngineProjectionMode` |
| P20-T1-I1 | integration | missing | — |

### T2

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-T2-U1 | unit | **strong** | `ReturnsCoreProjectionMode` |
| P20-T2-U2 | unit | **strong** | `NoCoreProjectionFailsClosed` |
| P20-T2-U3 | unit | **strong** | `EpochMismatchIneligible` |
| P20-T2-C1 | component | **strong** | `ReturnsLiveFacts` |
| P20-T2-C2 | component | **strong** | `IneligibleForGatedStates` |
| P20-T2-I1 | integration | missing | — |
| P20-T2-H1 | hardware | missing | — |

### T3

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-T3-U1 | unit | **strong** | `SelectDurabilityFirst_HighestCommittedLSNWins` |
| P20-T3-U2 | unit | **strong** | `SelectDurabilityFirst_WALHeadLSNBreaksTie` |
| P20-T3-U3 | unit | **strong** | `SelectDurabilityFirst_HealthScoreBreaksFinalTie` |
| P20-T3-C1 | component | **strong** | `FailoverV2_AllIneligible_NoPromotion` |
| P20-T3-C2 | component | **strong** | `FailoverV2_NilQuerier_FailsClosed` |
| P20-T3-C3 | component | **strong** | `FailoverV2_PartialEvidenceFailure_FailsClosed` |
| P20-T3-C4 | component | **strong** | `FailoverV2_FlagOff_UsesLegacy` |
| P20-T3-C5 | component | **strong** | `TestT3_V2PromotionMode_DiagnosticTriState` |
| P20-T3-I1 | integration | bounded | `HigherCommittedLSNWins` (real registry, injected querier) |
| P20-T3-I2 | integration | **strong** | `EpochBumpAndAssignmentOnlyAfterSelection` |
| P20-T3-H1 | hardware | missing | — |

### T4

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-T4-U1 | unit | **strong** | `DegradedProjection` + `NeedsRebuildProjection` |
| P20-T4-C1 | component | **strong** | `DegradedProjection_GatesActivation` |
| P20-T4-C2 | component | **strong** | `NeedsRebuildProjection_GatesActivation` |
| P20-T4-C3 | component | **strong** | `TestT4_MissingProjection_FailsClosed` |
| P20-T4-C4 | component | **strong** | `RecoveryFromGated_ReenablesServing` |
| P20-T4-C5 | component | **strong** | `GateEnforcedBeforeHeartbeat` |
| P20-T4-C6 | component | **strong** | `TestT4_GateRemovesISCSITarget` |
| P20-T4-C7 | component | bounded | — (needs test NVMe server) |
| P20-T4-C8 | component | **strong** | `TestP20_ProtoRoundTrip_ActivationGated` |
| P20-T4-I1 | integration | missing | — |
| P20-T4-I2 | integration | missing | — |
| P20-T4-H1 | hardware | missing | — |

### T5

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-T5-U1 | unit | **strong** | `AllReplicasHealthy_Keepup` |
| P20-T5-U2 | unit | **strong** | `ReplicaBehind_CatchingUp` |
| P20-T5-U3 | unit | **strong** | `StaleHeartbeat_Degraded` + `RF2_MissingReplica` + `TransportDegraded` |
| P20-T5-U4 | unit | **strong** | `UnrecoverableGap_NeedsRebuild` + `RebuildingRole_NeedsRebuild` |
| P20-T5-U5 | unit | **strong** | `WorstReplicaDominates` |
| P20-T5-U6 | unit | **strong** | `DistinctFromEngineProjectionMode` |
| P20-T5-C1 | component | **strong** | `HeartbeatUpdatesClusterReplicationMode` |
| P20-T5-C2 | component | **strong** | `APISurface_DistinctNaming` |
| P20-T5-C3 | component | **strong** | `TestT5_DiagnosticSnapshot_CarriesModes` |
| P20-T5-I1 | integration | missing | — |
| P20-T5-H1 | hardware | missing | — |

### Cross-Task

| ID | Layer | Status | Test function |
|---|---|---|---|
| P20-X-I1 | integration | missing | — |
| P20-X-I2 | integration | bounded | individual tests exist, no chain |
| P20-X-I3 | integration | **strong** | `DistinctFromEngineProjectionMode` + `APISurface` |

## Current Practical Next Step

Tier 1 component tests are implemented and passing. The current reading is:

1. **Unit + component coverage is strong.** 32 of 33 unit+component roster
   items are strong. The one bounded item (T4-C7 NVMe) requires test
   infrastructure that may not be available.

2. **Integration layer is the main remaining gap.** 6 of 10 integration
   roster items are missing. These require real `MasterServer` + `BlockService`
   interaction but can still run in `go test`.

3. **Hardware layer is deferred to T6/T7.** Requires m01/M02 and follows
   the staged validation plan above.

Recommended next actions:

1. implement Tier 2 integration tests (3 tests, one new `qa_*` file)
2. proceed to Stage 0 of T6/T7 (bootstrap closure prerequisite on hardware)
3. run full regression on hardware with V2 observation active
