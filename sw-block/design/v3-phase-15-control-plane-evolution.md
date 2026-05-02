# V3 Phase 15 — Control Plane Evolution Plan

**Date**: 2026-05-02
**Status**: ACTIVE design note for P15 MVP planning
**Code repo**: `seaweed_block`
**Docs repo**: `seaweedfs/sw-block/design`

---

## 0. Purpose

This note pins the control-plane shape we want while P15 is still small.

The product direction is a Kubernetes-usable block service that can later grow toward a Ceph-like cluster manager without rewriting the authority boundary. The immediate goal is not to build a full Ceph-class master. The goal is to keep the interfaces correct now, so later placement, repair, rebalance, and reintegration are not forced through ad-hoc shortcuts.

---

## 1. Core Rule

`blockmaster` may own placement as a product responsibility.

But code must express that ownership through a control-plane policy layer, not through daemon-local if/then assignment mutation.

Allowed shape:

```text
cmd/blockmaster
  -> ObservationHost
  -> ClusterState / TopologyController / PlacementPolicy / ReintegrationPolicy
  -> AssignmentAsk / PlacementPlan / RecoveryPlan
  -> authority.Publisher
  -> AssignmentFact
  -> blockvolume adapter.OnAssignment
```

Rejected shape:

```text
heartbeat handler
  -> if server timed out then publish r2 assignment
```

The rejected shape is the known anti-pattern: heartbeat timing becomes authority, `cmd` becomes policy owner, and assignment truth bypasses the testable authority controller seam.

---

## 2. Layering Contract

| Layer | Owns | Must not own |
|---|---|---|
| `cmd/blockmaster` | process lifecycle, flags, RPC servers, durable directories, wiring | placement policy, failover policy, direct `AssignmentInfo` construction |
| Observation ingestion | heartbeat/progress/durable-ack facts, freshness classification | authority minting, primary selection |
| Cluster state | current known servers, volumes, candidates, supportability, progress facts | direct frontend/data-plane mutation |
| Policy / planner | placement, failover, reintegration, repair, drain decisions as intent | epoch/endpoint minting, adapter calls |
| `authority.Publisher` | epoch/endpointVersion minting, assignment fan-out, durable authority line | deciding policy |
| `core/engine` | local replica recovery semantics: catch-up vs rebuild, session lifecycle, stale lineage | placement, failover candidate choice, rebalance |
| blockvolume runtime | execute assignment, feed WAL/base, report progress | self-promote, heartbeat-as-authority |

The short form:

```text
master owns cluster policy;
authority owns minting;
engine owns local recovery semantics;
runtime executes and reports facts.
```

---

## 3. Fact Types We Need To Keep Distinct

P15 should avoid overloading one heartbeat/status struct into every decision.

| Fact | Producer | Consumer | Meaning |
|---|---|---|---|
| `ObservationReport` | blockvolume | master observation layer | server/slot reachability and eligibility |
| `DurableProgressAck` | blockvolume / replica runtime | master cluster state | replica durable frontier / sync evidence |
| `BaseProgressAck` | recovery receiver | master/diagnostics | rebuild/base feed progress; useful for pin/retention and visibility |
| `PlacementIntent` | user/CSI/operator | master policy | desired volume/RF/topology input |
| `PlacementPlan` | planner | publisher/reconciler | bounded desired primary/replica set; not minted authority |
| `AssignmentFact` | publisher | blockvolumes | current authority line, epoch/EV minted by publisher |
| `RecoveryPlan` | planner/coordinator | feeder/runtime | who feeds whom from what base/WAL pin |
| `ReadinessFact` | runtime/feeder | master policy | candidate/syncing/ready, not frontend primary readiness |

Important distinction:

```text
FrontendHealthy != ReplicaReady
Observed != ReplicaCandidate
ReplicaCandidate != ReplicaReady
AuthorityMoved != DataContinuityProven
```

---

## 4. Returned Replica State Model

Old primary return is the first G8 pressure point where the distinction matters.

Correct sequence:

```text
old primary returns
  -> Observed
  -> FrontendClosed / Superseded
  -> ReplicaCandidate
  -> Syncing or Rebuilding
  -> ReplicaReady
```

Do not collapse this to:

```text
heartbeat arrived -> replica ready
```

For P15, the minimum future-facing interface should expose or internally model:

1. `FrontendPrimaryReady`: can serve frontend read/write.
2. `AuthorityRole`: current primary / non-primary / superseded.
3. `ReplicationRole`: none / candidate / syncing / ready.
4. `Progress`: durable ack, base progress, WAL frontier.

This does not require a production external API immediately. It does require tests and code to stop using `Healthy=false` as a catch-all for every non-primary state.

---

## 5. MVP Strengthening From This Design

The P15 MVP needs these additions before it can look like a smooth beta:

1. **Control-plane state vocabulary**
   - distinguish frontend readiness, authority role, replication role, and progress.
   - avoid interpreting one boolean `Healthy` as product state.

2. **Reintegration policy seam**
   - returned peer becomes candidate, not ready.
   - current primary/coordinator feeds it.
   - durable/progress facts promote it to ready.

3. **Placement intent seam**
   - user/CSI asks for volume + RF.
   - planner creates desired topology/placement.
   - publisher mints assignment only from bounded intent.

4. **Plan/result split**
   - planner returns `PlacementPlan` / `RecoveryPlan`.
   - reconciler executes and observes completion.
   - closure needs fact evidence, not just plan existence.

5. **Operator-visible explanation**
   - why selected primary?
   - why candidate rejected?
   - why replica is syncing instead of ready?
   - why recovery is stalled?

---

## 6. P15 Near-Term Application

### G8

G8 should remain narrow:

1. authority failover eligibility,
2. old primary stale fence,
3. new primary data continuity,
4. old primary return does not become double-primary.

G8 may add a prelude for returned peer becoming `ReplicaCandidate`, but `ReplicaReady` after reintegration can be a G8-followup/G9A item unless explicitly pulled into scope.

### G9 / G9A

G9/G9A should introduce the first product-shaped placement intent:

```text
CreateVolume(volume_id, size, rf)
  -> desired volume record
  -> placement plan
  -> publisher assignment
```

No CSI/API path should be allowed to submit raw `AssignmentInfo`.

### G17-lite

Observability should expose the split state:

```text
frontend_ready
authority_role
replication_role
durable_ack
recovery_phase
placement_reason
unsupported_reason
```

This is what makes a beta cluster understandable instead of log archaeology.

---

## 7. Anti-Pattern Checklist

Before any master/control-plane change lands, check:

1. Does a heartbeat timeout directly mint authority?
2. Does `cmd/blockmaster` construct or mutate `AssignmentInfo`?
3. Does a blockvolume local role claim become authority?
4. Does assignment movement alone claim data continuity?
5. Does returned process imply replica-ready without progress evidence?
6. Does engine receive placement/failover policy it should not own?
7. Does runtime callback return a semantic decision instead of an observation?

Any "yes" is a design stop unless explicitly ratified as a new product contract.

