# V3 Architecture Blueprint

**Date**: 2026-04-26
**Status**: Draft first-order architecture reference for P15+
**Owner**: V3 Architect
**Purpose**: describe how V3 fits together as a product system, so P15 gates and compact batch plans do not rediscover major boundaries late.

---

## 1. Why This Document Exists

P15 has several first-order references:

- `v3-phase-15-mvp-scope-gates.md` defines what must pass.
- `v3-block-behavior-contract-index.md` defines behavioral contracts.
- `protocol-anti-patterns.md` defines what must not re-enter.
- `v3-product-placement-authority-rationale.md` explains placement and authority philosophy.
- `v3-invariant-ledger.md` records tested claims.

This document fills the missing middle:

```text
product goal
  -> system architecture
  -> gates
  -> batch plans
  -> code and evidence
```

It is not a replacement for the gates. It is the blueprint that shows how each gate belongs to one coherent product architecture.

---

## 2. Architecture Thesis

V3 is a replicated block storage product with a strict separation between intent, authority, and execution.

```text
operator / CSI / API intent
  -> placement controller computes desired topology
  -> authority publisher mints epoch / endpoint version / assignment
  -> volume daemons execute frontend IO, durability, replication, recovery
  -> evidence surfaces report health, lag, rebuild, and gate status
```

The master is not just a rigid assignment server. It is the production control-plane interface through which operators and orchestrators express safe intent. Internally, it must keep policy and authority separate:

- policy/controller decides what topology is desired
- publisher mints what authority is current
- runtime executes without minting authority

This preserves V3's protocol discipline while still allowing V2-like product ergonomics.

---

## 3. Component Map

### 3.1 Product Control Plane

| Component | Responsibility | Must not do |
|---|---|---|
| Operator / CLI / API / CSI | Express user intent: create, attach, drain, replace, resize, snapshot, delete | Mint epoch, stuff assignment, choose local role |
| Product controller | Convert intent and policy into desired topology generation | Treat heartbeat as authority, mutate runtime state directly |
| Authority publisher / master | Validate observations against desired topology; mint epoch, endpoint version, assignment | Hide placement policy in heartbeat timing; silently reverse authority |
| Durable authority store | Persist current authority and topology generation | Depend on process memory as sole truth |
| Evidence/status surface | Explain current line, health, lag, rebuild, failures, unsupported decisions | Return green while data path is not usable |

### 3.2 Product Data Plane

| Component | Responsibility | Must not do |
|---|---|---|
| Volume daemon | Host one or more volume runtimes and report observations | Invent placement or authority |
| Frontend target | Serve iSCSI/NVMe/API IO only when projection authorizes | Re-derive authority or bypass stale-primary fence |
| Durable provider/backend | Persist local block data and WAL/checkpoint state | Let session lifecycle close shared persistent volume state incorrectly |
| Replication volume | Fan out primary writes to eligible peers under authority lineage | Ship writes without lineage or peer-set context |
| Replica listener | Accept incoming live/recovery traffic and apply through gates | Treat recovery traffic as live traffic or skip stale live writes silently |
| Engine/adapter | Own recovery state, session fate, and projection | Parse diagnostic strings as protocol decisions |

---

## 4. Truth Domains

Each fact has one owning domain.

| Truth | Owner | Consumers |
|---|---|---|
| Desired topology generation | Product controller | Publisher, diagnostics, placement plan API |
| Current epoch / endpoint version / assignment | Authority publisher | Volume daemon, frontend, replication, status |
| Local replica observation | Volume daemon | Master observation host |
| Frontend write/read readiness | Adapter projection | iSCSI/NVMe/API providers |
| Durability of local write | Durable backend | Replication coordinator, frontend ack path |
| Remote durability ack | Replication transport + peer | Durability coordinator |
| Recovery decision and retry budget | Engine | Adapter/executor |
| Per-LBA applied frontier | Replica apply gate / substrate contract | Recovery apply path |

Review rule: if a change creates a second owner for any row, it is architectural drift.

---

## 5. Control-Plane Flow

### 5.1 Create / Placement

```text
CreateVolume(size, rf, class)
  -> controller filters eligible nodes
  -> controller writes desired topology generation
  -> publisher validates observations against generation
  -> publisher mints assignment line
  -> volume daemons subscribe and self-determine role
```

P15 minimum: flat-topology placement is enough if G9A is accepted as MVP. Rack/AZ awareness and load-based rebalance can defer, but declared topology/manual placement must not be silently presented as dynamic assignment.

### 5.2 Assignment Subscription

Assignment facts are volume-scoped and master-minted. Every subscriber may see the same fact:

- `fact.ReplicaID == self.ReplicaID`: this daemon is primary for the line
- `fact.ReplicaID != self.ReplicaID`: this daemon is supporting replica or superseded

The volume daemon records and reacts to the fact; it does not reinterpret local role as authority.

### 5.3 Node Lifecycle

```text
node join
  -> observation appears
  -> controller may include node in future topology generation

node drain
  -> controller excludes node from new placements
  -> controller emits replacement topology changes
  -> runtime catches up / rebuilds replacements
  -> old replica is fenced or retired

node decommission
  -> no new assignment
  -> no remaining durable responsibility
```

This is why placement is part of product architecture, not a UI convenience.

---

## 6. Data-Plane Flow

### 6.1 Write Path

```text
frontend write
  -> projection/stale-primary check
  -> local durable append / fsync boundary
  -> replication fan-out to current peers
  -> durability mode evaluation
  -> host-visible ack or error
```

Durability modes must remain explicit:

- `best_effort`: local durable boundary plus best-effort shipping
- `sync_all`: all eligible peers satisfy barrier
- `sync_quorum`: quorum satisfies authority-aligned durability

No remote ack can count without authority lineage.

### 6.2 Read Path

```text
frontend read
  -> projection/stale-primary check
  -> local durable read
  -> protocol response
```

Reads are authority-gated because stale reads can poison filesystems and databases just as badly as stale writes.

### 6.3 Replication Path

```text
primary local write observed
  -> ReplicationVolume ships entry to ReplicaPeer
  -> BlockExecutor dials peer DataAddr
  -> ReplicaListener receives frame
  -> apply gate dispatches live or recovery lane
  -> substrate applies or rejects
```

`--data-addr` is the replication listener bind for the current G5 binary path because peer data traffic dials `AssignmentFact.Peers[*].DataAddr`.

---

## 7. Recovery Architecture

Recovery is engine-owned, not transport-owned.

```text
probe
  -> engine classifies catch-up vs rebuild
  -> adapter dispatches session via executor
  -> transport streams catch-up or rebuild
  -> replica apply gate prevents per-LBA regression
  -> session closes explicitly
  -> engine updates truth
```

Required rules:

- recovery starts from engine state, not raw probe payload
- catch-up scans from `replica flushed + 1`
- WAL recycled is a typed failure and escalates to rebuild
- catch-up exhaustion escalates to rebuild
- rebuild escalation is sticky until terminal
- session success is terminal close, not an intermediate ack

---

## 8. Failure Model

| Failure | Architectural behavior |
|---|---|
| Master process restart | Reload durable authority; volumes reconnect and replay observations; no new authority from heartbeat alone |
| Primary crash | Publisher mints new authority for eligible replica; old primary is stale-fenced |
| Supporting replica short disconnect | Engine-driven catch-up within retention |
| Supporting replica beyond retention | Rebuild escalation |
| Replica disk loss/corruption | Mark unsuitable; rebuild/recreate or explicit unsupported gate |
| Network partition | Old authority cannot count toward current writes or acks |
| Node drain | Controller removes node through desired topology generation, not manual assignment edits |
| Volume daemon restart | Reopens durable state, resubscribes, reconstructs runtime from assignment and probe facts |

P15 beta may choose single-master availability, but it must not blur "single master process" with "single primary per volume."

---

## 9. Operator Interface

The product should move toward this user model:

```text
seaweed block volume create --name db --size 100GiB --replicas 3 --class fast
seaweed block volume status db
seaweed block node drain nodeB
seaweed block replica replace db --from nodeB --to nodeD
seaweed block volume attach db --frontend nvme
```

The operator should see:

- desired topology generation
- current assignment line
- primary and supporting replicas
- peer set generation
- durability mode
- WAL pressure and checkpoint status
- replica lag
- recovery session state
- rebuild progress
- degraded reason
- unsupported/deferred feature explanations

If P15 cannot implement a surface, it must document the limitation rather than imply production support.

---

## 10. P15 Gate Alignment

| Gate family | Architecture responsibility |
|---|---|
| G0/G1 | Product processes host control plane and RPC without authority stuffing |
| G2/G3 | Frontends consume projection and enforce stale-path rejection |
| G4 | Durable local backend provides real persistence |
| G5 | Replicated write path and durability semantics |
| G6/G7/G12 | Catch-up, rebuild, and disk-failure handling |
| G8 | Failover with acknowledged-data continuity |
| G9/G9A | Lifecycle and placement intent become desired topology |
| G10/G11 | Snapshot and resize decisions align with product truth |
| G13 | Node lifecycle flows through placement/controller, not manual authority edits |
| G14/G15 | API/CSI expose safe intent, not authority internals |
| G16-G21 | Security, diagnostics, deployment, migration, deferred product policies, SLOs |
| G22 | End-to-end cluster evidence that the architecture holds |

`G9A` is the explicit placement-controller MVP decision point. It exists to prevent P15 lifecycle and CSI gates from overclaiming V2-like dynamic placement while topology is still manual.

---

## 11. P15 Non-Goals

Unless explicitly promoted by the gates, P15 does not claim:

- multi-master HA or distributed authority-store safety
- rack/AZ-aware placement
- automatic hot-volume rebalance
- advanced scheduler scoring
- ALUA/multipath parity beyond the accepted frontend gate
- snapshot-based long-lag catch-up
- wire protocol negotiation
- auth/encryption/mTLS beyond the accepted security gate
- multi-volume-per-binary if the product host remains single-volume in P15

Non-goals are acceptable only when documented with customer-visible implication.

---

## 12. How Mini-Plans Use This Document

Every P15 mini-plan should include an architecture reference in its scope section:

```text
Architecture touchpoints:
- v3-architecture.md §5.1 Create / Placement
- v3-architecture.md §6.1 Write Path
- v3-architecture.md §7 Recovery Architecture
```

The reviewer should ask:

1. Which architecture flow does this batch change?
2. Which truth domain owns the new fact?
3. Does this introduce a second authority path?
4. Does it preserve the product/control/data-plane split?
5. Which gate and invariant rows will prove the claim?

If the answer is unclear, update this document or the gate before coding.

---

## 13. Product Completion Ladder

Each close should state which product level it reaches. The level is not a precise percentage, but it prevents component completion from being reported as product completion.

| Level | Meaning | Typical proof |
|---|---|---|
| L0 Internal component | Logic works inside a package or fixture | Unit/component tests |
| L1 Product binary composition | Real product binary composes the needed components | Subprocess/L2 binary test |
| L2 Single-node usable IO | Real frontend IO works against durable local storage | OS or protocol harness attach/write/read |
| L3 Replicated IO | Real frontend writes move through replication to another node | Cross-node byte-equal test |
| L4 Failure/recovery under IO | Disconnect, crash, restart, catch-up/rebuild work under real IO | m01/L3 fault scenario |
| L5 Lifecycle/operator path | User/API/CSI can create, attach, drain, replace, delete without manual authority edits | API/CSI/admin scenario |
| L6 Beta-scope product | Accepted P15 gates pass or have explicit product-owner deferrals | G22 evidence bundle |

Example: G5-4 reached L1 for replicated write path. It did not reach L3, because byte movement under real frontend IO moved to G5-5.

---

## 14. Known Architecture Decisions Still Open

| Decision | Default for now | Bind point |
|---|---|---|
| `G5-DECISION-001`: primary restart recovery state | Path B runtime, Path A serializable seam open | G5-6 |
| `G5-DECISION-002`: walstore checkpoint cadence | Verify and document tuning policy | G5-2 / G5 close |
| Placement controller MVP | Add `G9A`; flat topology MVP recommended in P15 | G9/G13/G15 planning |
| Master HA | Single-node beta unless A1 changes | Post-P15 or explicit scope change |
| Rack-aware placement | Deferred; flat topology only | G20 or P16 |
| Real handler-context lane signal | Deferred hardening | Post-G5 |

---

## 15. Change Discipline

Update this document when:

- a gate adds/removes a component responsibility
- a batch changes a control-plane, data-plane, or recovery flow
- a new product operation is accepted
- a non-goal is promoted into P15 scope
- an invariant reveals that the architecture map is incomplete

Do not update this document for local implementation details that do not change system shape.

