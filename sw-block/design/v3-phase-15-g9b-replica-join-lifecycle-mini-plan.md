# V3 Phase 15 G9B — Replica Join / Lifecycle Protocol Mini-Plan

**Date**: 2026-05-02
**Status**: OPEN; starts after G9A ACK/reintegration policy close
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
| pending | G9B-B: returned/registered replica status lifecycle maps candidate -> `not_ready`, engine recovery -> `recovering`, completed same-lineage healthy support role -> `replica_ready`. |

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

### G9B-C — L2 subprocess lifecycle smoke

Shape:

```text
blockmaster + blockvolume r1
  -> r1 heartbeat observed
  -> no primary until assignment stream delivers publisher fact
  -> iSCSI attach/write/read succeeds after assignment
```

This is a smoke, not a full CSI lifecycle claim.

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
3. Existing G7/G8/G9A scoped tests still pass.
4. Mini-plan records exact commits and non-claims.

---

## 8. Non-Claims

G9B will not claim:

1. user-facing volume create/delete API;
2. k8s CSI lifecycle;
3. automatic placement across racks/AZs;
4. returned replica fully reintegrated unless G9B-B lands;
5. transparent OS initiator failover;
6. flow-control enforcement.
