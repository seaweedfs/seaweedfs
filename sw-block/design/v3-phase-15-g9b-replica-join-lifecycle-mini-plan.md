# V3 Phase 15 G9B — Replica Join / Lifecycle Protocol Mini-Plan

**Date**: 2026-05-02
**Status**: CLOSED 2026-05-02
**Code branch**: `p15-g9b/replica-join-lifecycle`
**Predecessors**: G7 recovery data-plane close, G8 failover data continuity close, G9A ACK/reintegration policy close

---

## 1. Goal

Define and pin the product protocol for how a replica joins a volume:

```text
Observed process
  -> admitted placement candidate
  -> authority-minted assignment
  -> frontend primary or recovering replica role
  -> progress-proven replica_ready
```

The key rule is negative as much as positive:

```text
heartbeat / local role claim / first observed process != authority primary
heartbeat / local role claim / returned process != replica_ready
```

For a new empty volume, the first accepted replica MAY become primary, but only through `authority.Publisher` minting an `AssignmentInfo` from a bounded `AssignmentAsk`. It must not self-elect from heartbeat.

---

## 2. Scope

### 2.1 In scope

1. Pin the join vocabulary in code and tests:
   - `Observed`
   - `ReplicaCandidate`
   - `AuthorityRole=primary|superseded|unknown`
   - `FrontendPrimaryReady`
   - `ReplicationRole=not_ready|recovering|replica_ready`
2. Genesis/empty-volume bootstrap rule:
   - no prior authority line;
   - placement may emit `IntentBind`;
   - publisher mints epoch/endpoint version;
   - volume becomes primary only after assignment consumption.
3. Returned/late replica rule:
   - observation can produce candidate evidence;
   - candidate is not sync-ACK eligible until progress-ready evidence exists.
4. Tests guarding authority boundaries:
   - observation store and heartbeat path never construct `AssignmentInfo`;
   - topology/policy emits only `AssignmentAsk`;
   - publisher remains the sole minter of epoch/endpoint version.

### 2.2 Out of scope

1. New external control-plane RPC for create/delete volume.
2. Rack/AZ placement.
3. Dynamic capacity scheduling.
4. Full CSI lifecycle.
5. Product-daemon L2 explicit "replica in recovery" control seam.
6. Flow-control enforcement.

Those remain P15-P4/P15-P5+ work. G9B only makes the lifecycle/join protocol honest and test-pinned.

---

## 3. Current Code Leverage

| Area | Existing code | G9B use |
|---|---|---|
| Observation facts | `core/authority/observation_types.go`, `ObservationStore`, `BuildSnapshot` | Raw heartbeat/local role facts remain non-authority. |
| Placement-like controller | `core/authority.TopologyController` | Reuse for genesis placement and candidate selection. Do not rewrite. |
| Authority minter | `core/authority.Publisher` | Sole path that creates `adapter.AssignmentInfo` with `Epoch > 0`. |
| Master wire | `core/host/master/services.go` | Heartbeat ingests observation; assignment stream fans out publisher facts. |
| Volume status vocabulary | `core/host/volume/status_server.go` | Extend only when evidence exists; do not overclaim `replica_ready`. |
| ACK policy | G9A `Durability*` modes | Candidate/recovering replicas are not sync-ACK eligible. |

### 3.1 Progress so far

| Commit | Meaning |
|---|---|
| `1faeda4` | G9B-A: observation/local-role cannot mint authority; genesis placement emits `AssignmentAsk`; publisher `IntentBind` mints the first authority line; `/status` does not report frontend ready before assignment. |
| `759dd0d` | G9B-B: `/status` vocabulary maps returned/registered replica candidate -> `not_ready`, engine recovery -> `recovering`, completed same-lineage healthy support role -> `replica_ready`; supporting `replica_ready` does not become frontend primary. |
| `44aaae1` | G9B-B component proof: drives the same `not_ready -> recovering -> replica_ready` lifecycle through real `adapter.VolumeReplicaAdapter` events (`OnAssignment`, `OnProbeResult`, session close), not manual projection mutation. |
| `8db289b` | G9B-C L2 subprocess smoke: real `cmd/blockmaster` + two `cmd/blockvolume` processes prove r1 observation alone is not frontend-ready; after r2 joins and publisher assignment lands, r1 becomes frontend-ready and real iSCSI WRITE/READ is byte-equal. |

---

## 4. Protocol Rules

### R1 — Observation is never authority

A heartbeat can report:

```text
server reachable
slot exists
local role claim
addresses
progress-ish evidence score
```

It cannot mint:

```text
epoch
endpoint version
primary authority
replica_ready
sync-ACK eligibility
```

### R2 — Genesis primary requires publisher mint

For an empty/new volume, first observed candidate can become primary only through:

```text
BuildSnapshot supported
  -> TopologyController emits IntentBind
  -> Publisher.apply mints AssignmentInfo(Epoch=1, EndpointVersion=1)
  -> volume consumes AssignmentFact
  -> adapter/engine projection becomes frontend primary
```

### R3 — Returned replica is not ready by presence

A returned old primary or lagging secondary starts as observed/candidate. It may be reachable and eligible, but it is not `replica_ready` and not sync-ACK eligible until reintegration progress is complete.

### R4 — Assignment movement is not data continuity

Inherited from G8/G9A: publisher assignment movement alone does not prove data readiness. Data continuity requires recovery/progress evidence and, where relevant, byte-equal oracle.

### R5 — One owner mints, one owner feeds

Authority publisher mints identity. Feeder/replication owns data catch-up. Neither side may pretend to complete the other's contract.

---

## 5. TDD Plan

### G9B-A — Join protocol unit pins

1. `TestG9B_ObservationLocalPrimaryClaim_DoesNotMintAuthority`
   - Feed heartbeat with `LocalRoleClaim=Primary`.
   - Assert no `AssignmentInfo` exists before controller/publisher path.
2. `TestG9B_GenesisPlacement_EmitsBindButNotAssignmentInfo`
   - Supported no-line snapshot emits `AssignmentAsk{IntentBind}` only.
3. `TestG9B_PublisherBind_MintsGenesisPrimaryLine`
   - Applying `IntentBind` creates `Epoch=1, EndpointVersion=1` assignment.
4. `TestG9B_FirstObservedReplicaRequiresAssignmentBeforeFrontendReady`
   - Status/projection remains not frontend-ready before assignment.

### G9B-B — Candidate / ready split

1. `TestG9B_ReturnedReplica_ObservedCandidateNotReplicaReady`
2. `TestG9B_ReintegratedReplica_PublishesReplicaReadyOnlyAfterProgressEvidence`
3. `TestG9B_CandidateNotSyncAckEligible`

Initial G9B-B slice uses `/status` vocabulary as the component seam:

```text
registered returned replica, current authority line names another replica
  -> engine degraded     => ReplicationRole=not_ready
  -> engine recovering   => ReplicationRole=recovering
  -> engine healthy at same authority lineage
                      => ReplicationRole=replica_ready, FrontendPrimaryReady=false
```

This is intentionally status-only. It does not yet make `replica_ready` drive placement or ACK eligibility.

`44aaae1` adds the adapter-backed component variant: a returned registered replica consumes an assignment, dispatches probe, starts catch-up from progress facts, and only publishes `replica_ready` after the session close evidence reaches the adapter/engine. The test keeps the current frontend authority on a different replica, so `replica_ready` is explicitly a supporting-replica state, not a frontend primary claim.

### G9B-C — L2 subprocess lifecycle smoke

Shape:

```text
blockmaster + blockvolume r1
  -> r1 heartbeat observed
  -> no primary until assignment stream delivers publisher fact
  -> iSCSI attach/write/read succeeds after assignment
```

This is a smoke, not a full CSI lifecycle claim.

`8db289b` lands this exact smoke at `cmd/blockvolume` scope. The test intentionally withholds r2 at first under an RF=2 topology, so r1 can expose `/status` but must remain non-Healthy. Starting r2 lets the existing master topology/publisher path mint and deliver the assignment; only then does the iSCSI data path run.

---

## 6. Stop Rules

Stop and reopen design if any implementation requires:

1. heartbeat handler constructing `AssignmentAsk` or `AssignmentInfo`;
2. blockvolume self-promoting from local role;
3. `ReplicaReady` from heartbeat alone;
4. test-only production RPC;
5. assignment movement used as data continuity proof;
6. direct `AssignmentInfo` construction outside publisher / decode seam.

---

## 7. Closure Criteria

G9B can close when:

1. G9B-A tests pass and pin genesis authority flow.
2. G9B-B tests pass or any remaining `replica_ready` publication is explicitly forward-carried with no overclaim.
3. G9B-C L2 smoke passes with real product subprocesses and iSCSI byte-equal oracle.
4. Existing G7/G8/G9A scoped tests still pass.
5. Mini-plan records exact commits and non-claims.

---

## 8. Non-Claims

G9B will not claim:

1. user-facing volume create/delete API;
2. k8s CSI lifecycle;
3. automatic placement across racks/AZs;
4. returned replica driving placement or ACK eligibility from `replica_ready` status;
5. transparent OS initiator failover;
6. flow-control enforcement.

---

## 9. Close

**Close target**: `p15-g9b/replica-join-lifecycle` at `8db289b`

**Evidence commits**:

| Commit | Evidence |
|---|---|
| `1faeda4` | Authority boundary tests: observation/local role cannot mint authority; genesis bind goes through publisher. |
| `759dd0d` | Status vocabulary: returned/registered replica maps `not_ready -> recovering -> replica_ready` without becoming frontend primary. |
| `44aaae1` | Adapter-backed component proof: assignment/probe/session-close events drive the returned-replica lifecycle. |
| `8db289b` | L2 subprocess smoke: real `cmd/blockmaster` + two `cmd/blockvolume` daemons prove observation-alone is not primary; assignment unlocks iSCSI byte-equal write/read. |

**Close regression**:

```text
go test ./core/authority ./core/host/volume ./cmd/blockvolume -count=1
```

Result: PASS on 2026-05-02.

**Closed claim**:

G9B pins the replica join lifecycle vocabulary and the two key negative rules:

```text
heartbeat / local role claim / first observed process != authority primary
heartbeat / local role claim / returned process != replica_ready
```

The first primary for an RF=2 topology is minted only through the authority publisher after placement/topology conditions are satisfied. A returned/late replica can become `replica_ready` only after recovery progress evidence reaches adapter/engine state.

**Forward-carry**:

1. `replica_ready` does not yet drive placement membership.
2. `replica_ready` does not yet drive sync ACK voter eligibility.
3. Continuous WAL feeding after `replica_ready` remains governed by the G7/G9 feeder path, but the promotion of `replica_ready` into ACK/placement policy is a later gate.
4. External create/delete/scale API and CSI lifecycle remain later MVP work.
