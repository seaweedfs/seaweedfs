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

Post-T1-T5 implementation audit. Assessed by mapping every roster item
against actual test functions in the codebase.

| Capability | Unit | Component | Integration | Hardware / Runner | Current reading |
|---|---|---|---|---|---|
| `T1` | strong (2/2) | strong (2/2) | missing (0/1) | not yet | unit+component cover distinctness and preservation; no multi-server chain |
| `T2` | strong (3/3) | strong (2/2) | missing (0/1) | not yet | semantics and fail-closed proven; no integrated master→VS query test |
| `T3` | strong (3/3) | strong (4/4) | bounded (2/2) | not yet | all selection + fail-closed rules proven; integration uses injected querier |
| `T4` | strong (1/1) | bounded (4/5) | missing (0/2) | not yet | gate state proven; **T4-C3 missing projection** untested; no frontend removal assertion |
| `T5` | strong (6/6) | bounded (2/3) | missing (0/1) | not yet | all modes proven; **T5-C3 diagnostic snapshot** untested; no evolution test |

### Coverage Gaps Found

Every tester finding during T1-T5 was a **wiring bug**, not a logic bug:
- T1: stale EngineProjectionMode survived primary turnover (registry wiring)
- T2: fail-open without core projection (evidence wiring)
- T3: silent V1 fallback when querier nil (failover wiring)
- T4: gate was bookkeeping, not enforcement (frontend wiring)
- T5: missing replica not degraded, transport signal ignored (registry wiring)

None were caught by unit tests. All were caught reviewing the production
path. This confirms that **component tests are the highest-value gap** for
CI/CD protection.

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

## Detailed Gap Inventory

### Existing Test → Roster Mapping (43 tests)

#### T1 (5 tests → 4 roster items covered)

| Roster ID | Status | Existing test |
|---|---|---|
| P20-T1-U1 | **strong** | `EngineProjectionModeDistinctFromVolumeMode` |
| P20-T1-U2 | **strong** | `AbsentPreservesExisting` + `ClearsOnPrimaryTurnover` + `PreservedOnNewPrimaryWithField` |
| P20-T1-C1 | **strong** | `ConsumesEngineProjectionModeFromPrimaryHeartbeat` |
| P20-T1-C2 | **strong** | `EngineProjectionModeDistinctFromVolumeMode` |
| P20-T1-I1 | **missing** | — |

#### T2 (12 tests → 5 roster items covered)

| Roster ID | Status | Existing test |
|---|---|---|
| P20-T2-U1 | **strong** | `ReturnsCoreProjectionMode` (CommittedLSN=42 assertion) |
| P20-T2-U2 | **strong** | `NoCoreProjectionFailsClosed` |
| P20-T2-U3 | **strong** | `EpochMismatchIneligible` |
| P20-T2-C1 | **strong** | `ReturnsLiveFacts` |
| P20-T2-C2 | **strong** | `IneligibleForGatedStates` (4 modes) |
| P20-T2-I1 | **missing** | — |
| P20-T2-H1 | **missing** | hardware only |

#### T3 (7 tests → 9 roster items covered)

| Roster ID | Status | Existing test |
|---|---|---|
| P20-T3-U1 | **strong** | `SelectDurabilityFirst_HighestCommittedLSNWins` |
| P20-T3-U2 | **strong** | `SelectDurabilityFirst_WALHeadLSNBreaksTie` |
| P20-T3-U3 | **strong** | `SelectDurabilityFirst_HealthScoreBreaksFinalTie` |
| P20-T3-C1 | **strong** | `FailoverV2_AllIneligible_NoPromotion` |
| P20-T3-C2 | **strong** | `FailoverV2_NilQuerier_FailsClosed` |
| P20-T3-C3 | **strong** | `FailoverV2_PartialEvidenceFailure_FailsClosed` |
| P20-T3-C4 | **strong** | `FailoverV2_FlagOff_UsesLegacy` |
| P20-T3-I1 | **bounded** | `HigherCommittedLSNWins` (real registry, injected querier) |
| P20-T3-I2 | **strong** | `EpochBumpAndAssignmentOnlyAfterSelection` |
| P20-T3-H1 | **missing** | hardware only |

#### T4 (6 tests → 5 roster items covered, 4 missing)

| Roster ID | Status | Existing test |
|---|---|---|
| P20-T4-U1 | **strong** | `DegradedProjection` + `NeedsRebuildProjection` |
| P20-T4-C1 | **bounded** | `DegradedProjection` (state, not target removal) |
| P20-T4-C2 | **bounded** | `NeedsRebuildProjection` (state, not target removal) |
| P20-T4-C3 | **missing** | — |
| P20-T4-C4 | **bounded** | `RecoveryFromGated` (state, not target re-add) |
| P20-T4-C5 | **strong** | `GateEnforcedBeforeHeartbeat` |
| P20-T4-I1 | **missing** | — |
| P20-T4-I2 | **missing** | — |
| P20-T4-H1 | **missing** | hardware only |

#### T5 (12 tests → 8 roster items covered)

| Roster ID | Status | Existing test |
|---|---|---|
| P20-T5-U1 | **strong** | `AllReplicasHealthy_Keepup` |
| P20-T5-U2 | **strong** | `ReplicaBehind_CatchingUp` |
| P20-T5-U3 | **strong** | `StaleHeartbeat_Degraded` + `RF2_MissingReplica` + `TransportDegraded` |
| P20-T5-U4 | **strong** | `UnrecoverableGap_NeedsRebuild` + `RebuildingRole_NeedsRebuild` |
| P20-T5-U5 | **strong** | `WorstReplicaDominates` |
| P20-T5-U6 | **strong** | `DistinctFromEngineProjectionMode` |
| P20-T5-C1 | **strong** | `HeartbeatUpdatesClusterReplicationMode` |
| P20-T5-C2 | **strong** | `APISurface_DistinctNaming` |
| P20-T5-C3 | **missing** | — |
| P20-T5-I1 | **missing** | — |
| P20-T5-H1 | **missing** | hardware only |

#### Cross-task

| Roster ID | Status | Existing test |
|---|---|---|
| P20-X-I1 | **missing** | — |
| P20-X-I2 | **bounded** | individual fail-closed tests exist, no integrated chain |
| P20-X-I3 | **strong** | `T5_DistinctFromEngineProjectionMode` + `APISurface` |

### Summary by layer

| Layer | Strong | Bounded | Missing | Total |
|---|---|---|---|---|
| Unit | 15 | 0 | 0 | 15 |
| Component | 10 | 3 | 2 | 15 |
| Integration | 2 | 2 | 6 | 10 |
| Hardware | 0 | 0 | 4 | 4 |

## Missing Component Tests (high priority for CI/CD)

These are the cheapest high-value tests to add before hardware validation.
Each can be implemented in existing `weed/server/*_test.go` files using
existing test helpers. No new infrastructure needed.

### `P20-T4-C3` Missing Projection Gate (fail-closed)

File: `weed/server/volume_grpc_block_activation_gate_test.go`

Scenario:
1. `BlockService` with `v2Core != nil` (production config)
2. volume exists but no projection cached for its path
3. call `evaluateActivationGate(path)`

Must prove:
1. `IsActivationGated(path) == true`
2. reason is `"missing_engine_projection"`

Why it matters: the fail-closed code path for missing projection was a
tester-found bug. Without a test, a refactor could silently revert to
fail-open.

### `P20-T5-C3` Diagnostic Snapshot Carries Modes

File: `weed/server/master_block_cluster_mode_test.go`

Scenario:
1. register volume with replicas and `EngineProjectionMode` set
2. trigger a pending rebuild (so volume appears in failover diagnostic)
3. call `FailoverDiagnosticSnapshot()`

Must prove:
1. `FailoverVolumeState.ClusterReplicationMode` is populated
2. `FailoverVolumeState.EngineProjectionMode` is populated
3. values match the registry entry

Why it matters: `FailoverDiagnosticSnapshot` was wired to enrich from
registry lookup but has zero test coverage. If the lookup path breaks,
operators lose mode visibility in the failover diagnostic.

### `P20-T4-C6` Gate Actually Removes iSCSI Target (enforcement)

File: `weed/server/volume_grpc_block_activation_gate_test.go`

Scenario:
1. `BlockService` with real `TargetServer` (use test iSCSI server)
2. register volume with target
3. inject degraded projection
4. call `evaluateActivationGate(path)`

Must prove:
1. `targetServer.HasTarget(iqn) == false` after gate
2. re-inject healthy projection
3. `targetServer.HasTarget(iqn) == true` after ungate

Why it matters: T4 tester finding was that gate was bookkeeping only. The
enforcement code exists now (`gateServing`/`ungateServing`), but no test
proves the target is actually removed/re-added at the iSCSI registry level.

### `P20-T4-C7` Gate Covers Both iSCSI and NVMe

File: `weed/server/volume_grpc_block_activation_gate_test.go`

Scenario:
1. `BlockService` with both `targetServer` and `nvmeServer`
2. register volume with both frontends
3. gate
4. verify both removed
5. ungate
6. verify both restored

Why it matters: NVMe gate was a tester-found bug. Tests should lock it.

### `P20-T1-C3` Heartbeat Proto Round-Trip Preserves EngineProjectionMode

File: `weed/storage/blockvol/block_heartbeat_proto_test.go`

Scenario:
1. build `BlockVolumeInfoMessage` with `EngineProjectionMode` set
2. convert to proto via `InfoMessageToProto`
3. convert back via `InfoMessageFromProto`

Must prove:
1. field survives round-trip
2. empty field stays empty (nil presence)

Why it matters: proto conversion was added manually (pending regen).
Round-trip test locks the wire format.

### `P20-T4-C8` Heartbeat Proto Round-Trip Preserves ActivationGated

File: `weed/storage/blockvol/block_heartbeat_proto_test.go`

Scenario:
1. build message with `ActivationGated=true` + reason
2. proto round-trip

Must prove:
1. both fields survive
2. `ActivationGated=false` does not produce spurious reason

### `P20-T3-C5` V2PromotionMode Diagnostic Reflects Flag State

File: `weed/server/master_block_failover_test.go`

Scenario:
1. `MasterServer` with `blockV2Promotion=false` → diagnostic shows `"disabled"`
2. `blockV2Promotion=true`, placeholder querier → `"placeholder_fail_closed"`
3. `blockV2Promotion=true`, `blockV2EvidenceTransport=true` → `"transport_ready"`

Must prove:
1. tri-state is accurate in all three configurations

## Missing Integration Tests (medium priority, pre-hardware)

These require real `MasterServer` + `BlockService` interaction but can
still run in `go test` without hardware.

### `P20-X-I1` End-to-End V2 Failover Chain

File: `weed/server/qa_block_v2_failover_test.go` (new)

Scenario:
1. create `MasterServer` + `BlockService` with V2 promotion enabled
2. register RF=2 volume (primary + replica)
3. inject evidence querier that returns fresh facts
4. trigger `failoverBlockVolumes(primaryServer)`
5. verify: promotion uses durability-first winner
6. verify: assignment enqueued for winner
7. verify: `ClusterReplicationMode` reflects post-failover state

Must prove:
1. full query → promote → assign → observe chain works together
2. no semantic collapse between layers

### `P20-T4-I1` Promoted Degraded Node Does Not Serve

File: `weed/server/qa_block_v2_failover_test.go`

Scenario:
1. after V2 failover, the promoted node's core projection is `degraded`
2. verify `IsActivationGated` is true
3. verify heartbeat carries `ActivationGated=true`
4. verify: no assignment confirmation from that node clears the gate

Must prove:
1. gate is enforced on the real failover path, not just manual injection

### `P20-T5-I1` Cluster Mode Evolves Across Failover

File: `weed/server/qa_block_v2_failover_test.go`

Scenario:
1. RF=2 healthy → `ClusterReplicationMode == "keepup"`
2. primary dies → replica promoted
3. old primary is pending rebuild → mode transitions
4. heartbeat from new primary updates mode

Must prove:
1. `ClusterReplicationMode` changes through expected sequence

## Minimum Phase 20 Closure Gate

Before `Phase 20` can be called fully closed, require:

1. `T1-T5` all have at least one passing unit proof and one passing component proof
2. all **missing component tests** listed above are implemented and passing
3. at least one integrated scenario proves `T2 + T3 + T4` together
4. at least one integrated scenario proves `T1 + T5` surfaces remain distinct
5. `sw-test-runner run --all` passes on m01/M02 with V2 promotion active
6. one explicit `v2-failover-gate` or equivalent hardware scenario passes
7. full `weed/server`, `weed/storage/blockvol`, `weed/storage/blockvol/csi`, and `sw-block/engine/replication` regression suites are green

## Priority Order for Implementation

### Tier 1: Missing component tests (do now, CI/CD protection)

1. `P20-T4-C3` missing projection gate
2. `P20-T5-C3` diagnostic snapshot modes
3. `P20-T1-C3` + `P20-T4-C8` proto round-trip
4. `P20-T3-C5` V2PromotionMode diagnostic
5. `P20-T4-C6` iSCSI target removal (if test TargetServer available)

### Tier 2: Integration tests (do before hardware, chain proof)

6. `P20-X-I1` end-to-end V2 failover chain
7. `P20-T4-I1` promoted degraded no serve
8. `P20-T5-I1` cluster mode evolution

### Tier 3: Hardware (T6/T7 on m01/M02)

9. `P20-T2-H1` evidence RPC under network delay
10. `P20-T3-H1` failover under disturbance
11. `P20-T4-H1` v2-failover-gate scenario
12. `P20-T5-H1` real disturbance cluster judgment

## Current Practical Next Step

1. implement Tier 1 component tests (7 tests, all in existing test files)
2. implement Tier 2 integration tests (3 tests, one new qa file)
3. run full regression to confirm no breakage
4. then proceed to T6/T7 on m01/M02 with confidence
