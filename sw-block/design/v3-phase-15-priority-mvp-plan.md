# V3 Phase 15 — Priority MVP Plan

**Date**: 2026-05-02
**Status**: ACTIVE priority overlay; not a replacement for `v3-phase-15-mvp-scope-gates.md`
**Owner**: architect / sw / QA
**Code repo**: `seaweed_block`
**Docs repo**: `seaweedfs/sw-block/design`

---

## 0. Purpose

This document turns the P15 canonical gate list into an execution priority plan.

`v3-phase-15-mvp-scope-gates.md` remains the canonical definition of what each gate means. This plan answers a different question:

> Given current code reality after G7, what should we do first to reach a credible Kubernetes block-service MVP without over-claiming?

The north-star is a smooth, understandable beta:

1. A user can deploy the block service.
2. A user can create and attach a volume.
3. A pod can write/read real block data.
4. Replica loss, catch-up, and rebuild do not corrupt data.
5. Failover is explicitly tested before we imply availability.
6. Operators can see enough state to diagnose slow or stuck recovery.
7. Unsupported beta features are named, not silent.

---

## 1. Current Reality Snapshot

### 1.1 Closed or closure-ready capability

| Area | Current posture |
|---|---|
| G0/G1 product daemons + control RPC | Implemented via `cmd/blockmaster` / `cmd/blockvolume`, master-volume control route, status surfaces. |
| G2/G3 frontends | iSCSI + NVMe/TCP have substantial V2-port coverage and product-host wiring. Some original stale-OS critical-cell claims remain deferred to later failover evidence. |
| G4 local durability | smartwal path is the canonical durable path; walstore has known deferred issues. |
| G5/G6 replication + catch-up | Replicated write path, retention-aware catch-up, and WALRecycled -> rebuild dispatch are implemented and hardware-tested in prior gates. |
| G7 rebuild / replica re-creation | CLOSED 2026-05-02, pinned to `seaweed_block@d09fcc6`: #2 empty join, #5 concurrent writes during rebuild, #6 stale WAL -> rebuild all hardware GREEN. |
| Recovery debt cleanup | `targetLSN` no longer owns terminal close semantics; practical single feed ingress is in place; progress facts and flow-control diagnostics exist. |

### 1.2 Not yet product-complete

| Gap | Why it matters |
|---|---|
| G8 failover data continuity | CLOSED 2026-05-02 for first-close subprocess L2 + real iSCSI data-continuity scope. Follow-up owns m01/M02, G8b, and strict ACK policy. |
| ACK profile / quorum semantics | The current healthy-path continuity evidence must not be confused with strict RF=2 full-ack semantics under lag. |
| G9 lifecycle product verbs | Users still cannot naturally create/delete/attach/detach volumes without manual topology/assignment setup. |
| G9A placement intent | Current system has topology machinery, but not yet a full product path: intent -> placement -> publisher-minted assignment. |
| G15a CSI MVP | Kubernetes users need PVC -> pod -> block device workflow; CSI is the expected beta surface. |
| G17-lite observability | Hardware QA can inspect logs; ordinary users need status/metrics/diagnostics without grep archaeology. |
| G12/G13 failure/lifecycle policy | Disk failure, drain, decommission must either work or be explicitly unsupported with operator implications. |
| Flow-control enforcement | Diagnostics exist, but no product action yet. This is follow-up after observability and failover basics. |

### 1.3 ACK profile posture

P15 separates ACK policy from recovery policy.

`best-effort` is allowed as a beta profile only if it is explicitly named: frontend write/sync success is based on the primary path and does not wait for every replica to report a full durable ACK. A lagging replica is still a recovery concern. Progress/probe facts must drive catch-up or rebuild; best-effort is not permission to leave a replica silently behind.

`quorum` or `full-ack` is a separate future profile: frontend success waits for the configured replica durability condition. That profile is required before the product claims RF=2 no acknowledged-write loss when the secondary is lagging, down, or unable to durably ACK.

In RF=2 full-ack mode, a replica in recovery is not sync-ack eligible. The primary must therefore choose an explicit policy outcome: block/fail writes, transition the volume to a named degraded/best-effort mode, or make the volume read-only/unavailable. It must not silently return full-sync success while the only secondary is catching up or rebuilding.

---

## 2. Execution Principle

### 2.1 Gate discipline

Each gate gets a mini-plan before code:

1. **Scope**: exact claim, exact non-claims.
2. **Current code audit**: files and behavior already present.
3. **V2 port plan**: PORT-AS-IS / PORT-REBIND / SKIP / REWRITE-TINY.
4. **TDD plan**: red tests first for semantic risk.
5. **Hardware / integration evidence**: what scenario proves the gate.
6. **Closure report**: evidence, non-claims, forward-carry.

### 2.2 Priority rule

Prefer vertical product slices over deep internal polish.

Do not continue polishing recovery internals unless the work protects one of these beta-facing outcomes:

1. data correctness,
2. failover continuity,
3. lifecycle usability,
4. K8s attach/use,
5. operator diagnostics,
6. explicit unsupported-state handling.

### 2.3 Porting rule

Use WAL-style port-model for mature V2 mechanism files:

1. Port complete mechanism sections where the file is M or M*.
2. Rebind thin engine/storage calls to V3 contracts.
3. Reject V2 authority-minting permanently.
4. Do not ref-model protocol or CSI code unless V2 is authority-polluted or scope-deferred.

### 2.4 Control-plane interface rule

P15 must keep the master/control-plane interface future-compatible with a larger cluster manager.

`blockmaster` may own placement as a product responsibility, but placement, failover, rebalance, repair, and reintegration decisions must live behind a testable authority policy/planner seam. The daemon layer wires RPC, flags, stores, and components. It must not grow ad-hoc heartbeat-handler if/then assignment mutation.

Reference: `v3-phase-15-control-plane-evolution.md`.

Required vocabulary split:

1. `FrontendPrimaryReady` — can serve frontend read/write.
2. `AuthorityRole` — current primary / non-primary / superseded.
3. `ReplicationRole` — none / candidate / syncing / ready.
4. `Progress` — durable ack, base progress, WAL frontier.

Do not use one boolean `Healthy` as the product state for all four concepts. `Healthy=false` can mean old primary stale, supporting replica, syncing replica, unknown, or failed; those must become distinguishable before the MVP is user-friendly.

---

## 3. Priority Spine

### Immediate spine

```text
P15-P0: Freeze and close G7
  -> P15-P1: G7 follow-up hardening
  -> P15-P2: G8 failover data continuity [CLOSED 2026-05-02 for first-close scope]
  -> P15-P3: G9 lifecycle product verbs
  -> P15-P4: G9A flat placement / desired topology
  -> P15-P5: G17-lite observability + G15a CSI MVP in parallel
  -> P15-P6: internal K8s dogfood checkpoint
  -> P15-P7: remaining beta gates by dogfood feedback
  -> P15-P8: G22 final validation
```

This preserves the canonical gate graph while making the execution path concrete.

---

## 4. Gate Plan

### P15-P0 — Close G7 on pinned tree — ✅ done

**Goal**: close G7 honestly before adding more behavior.

**Closure target**: `seaweed_block@d09fcc6`.

**Explicitly excluded from canonical G7 evidence**: post-close `59ac82e` dry-run flow-control logging unless QA re-runs and architect chooses to move the close target.

**Why the remaining items stay out of G7**:

G7's closed product sentence is deliberately narrow: "dual-lane recovery data-plane correctness works for empty join, concurrent writes during rebuild, and stale-WAL rebuild." It is not "the recovery product is autonomous and operationally complete."

The remaining work is real, but it belongs to later gates because each item changes a different product contract:

| Remaining item | Why not G7 | Owning next place |
|---|---|---|
| Primary-kill / failover continuity | Changes authority-to-frontend serving semantics after primary loss; must prove new primary reads acknowledged data. | G8 |
| Autonomous probe-driven recover lifecycle | Changes coordinator policy: when to catch up, rebuild, degrade, or stay feeding. | G8/G9A follow-up or dedicated recover-policy batch |
| RF>=3 quorum / min-pin / slow-replica degrade | Changes write availability and quorum semantics, not just rebuild transport. | G8 or later RF policy batch |
| Strict RF=2 full-ack write contract | Changes frontend ACK behavior under replica lag; G8's current evidence is healthy-path data continuity, not full quorum semantics. | G8-followup / G9A |
| Flow-control enforcement | Changes write admission behavior under pressure. Dry-run diagnostics are safe; enforcement needs product tuning and observability. | G17-lite + flow-control track |
| External diagnostics endpoint | Product supportability surface, not data-plane closure. | G17-lite |
| Strict single-queue feeder refactor | Internal architecture hardening; G7 achieved practical single-ingress sufficient for its hardware gate. | G7-followup / pre-G8 hardening |

This boundary prevents G7 from becoming an unbounded recovery rewrite while still preserving every discovered debt in the forward plan.

**Done**:

1. Single-signed `v3-phase-15-g7-mini-plan.md` §close.
2. Updated roadmap state so it no longer claims P15 is closing G5.
3. Recorded G7 as closed with the precise non-claims:
   - no autonomous recovery orchestration,
   - no RF>=3 quorum/min-pin policy,
   - no full transient disconnect / primary crash matrix,
   - no flow-control enforcement,
   - no external diagnostics endpoint,
   - no strict single-queue feeder claim.

**Pass**: docs identify `d09fcc6` as G7 close evidence and no later commit is silently included.

---

### P15-P1 — G7 follow-up hardening

**Goal**: protect the G7 fixes from regression without expanding G7 scope.

**Why now**: the recovery work exposed structural risks. Small hardening is cheaper before G8 builds on it.

**Work items**:

1. Add invariant tests:
   - post-recovery first live write lazy-dials / uses valid session,
   - retained write is replayed by next recovery backlog,
   - live write during base transfer enters the same feeder path,
   - duplicate WAL/base apply remains idempotent.
2. Add anti-bypass test:
   - production live WAL path must go through `ReplicaPeer.ShipEntry -> FeedLiveWrite`, not direct `BlockExecutor.Ship`.
3. Clarify `LiveWriteRetained`:
   - stale epoch should log as dropped stale epoch, not "recovery will replay".
4. Gate, sample, or remove `g7-debug` logs from product default.
5. Document `ErrSinkSealed` invariant:
   - sealed fall-through is safe only after steady emit context has been restored.

**Pass**: scoped `go test` green; no new product behavior claim.

---

### P15-P2 — G8 Failover Data Continuity — ✅ closed for first-close scope

**Goal**: after primary failure, new primary serves tested acknowledged data; old primary cannot corrupt future state.

**Close status**: CLOSED 2026-05-02. Evidence is pinned in `v3-phase-15-g8-mini-plan.md` §13.

**ACK posture for first close**: G8 may close on healthy-path acknowledged data continuity. It must not claim strict RF=2 quorum/full-ack behavior unless an explicit ACK profile gate proves that writes/syncs fail or block when the replica cannot durably ACK.

**Scope**:

1. Kill primary after acknowledged writes.
2. Master publishes new assignment.
3. Client reconnects or reattaches.
4. New primary reads exact acknowledged data.
5. Old primary is fenced from future success.

**Non-scope**:

1. multi-master HA,
2. rack/AZ placement,
3. performance SLO,
4. full chaos matrix,
5. strict full-ack write contract under lagging or unavailable replicas.

**TDD first**:

1. component test: old primary stale write rejects after EV advance;
2. component test: new primary has acknowledged write after reassignment;
3. scenario test: kill primary -> wait assignment -> reconnect -> byte-equal;
4. negative test: failover cannot be declared by authority movement alone without data verification.

**Pass**: hardware or multi-process scenario proves data continuity, not just role movement.

**Control-plane strengthening needed before G8 close**:

1. G8 must explicitly state whether old-primary return is only `FrontendClosed/Superseded` or also `ReplicaCandidate`.
2. If G8 claims returned-peer reintegration, it must prove `ReplicationRole` transitions through candidate/syncing/ready using progress evidence.
3. Assignment movement alone remains a negative oracle; data continuity requires byte-equal proof.
4. `cmd/blockmaster` changes for failover must remain wiring/config only; policy stays in `core/authority`.
5. Best-effort ACK mode, if used, must still feed lagging replicas through catch-up/rebuild. It is an ACK latency/availability tradeoff, not a recovery exemption.

---

### P15-P3 — G9 Volume Lifecycle

**Goal**: users/tools can create, attach, detach, and delete volumes without hand-authoring internal authority state.

**Minimal product verbs**:

1. CreateVolume
2. DeleteVolume
3. Attach / Publish
4. Detach / Unpublish
5. Get/List status

**Implementation direction**:

1. Add V3-safe desired-volume model.
2. Reuse current master/volume RPC and authority publisher.
3. Keep mutating verbs intent-only; never expose direct `AssignmentInfo` mutation.
4. Use CSI/API semantics as the shape even if the first caller is a CLI/test client.

**TDD first**:

1. create volume intent persists;
2. attach cannot report ready before data path is usable;
3. delete leaves no orphan authority line;
4. all lifecycle verbs are idempotent.

**Pass**: external client or test drives create -> attach -> write/read -> detach -> delete through real product daemons.

---

### P15-P4 — G9A Flat Placement / Desired Topology

**Goal**: operator asks for replicated volume; system computes flat placement; publisher mints authority from desired topology, not heartbeat-as-authority.

**Current code advantage**: `core/authority.TopologyController` already contains placement-like machinery. This gate should productize it, not rewrite it.

**Must ship**:

1. flat RF=2/RF=3 placement from intent;
2. durable desired topology generation;
3. explain output: why selected / why rejected;
4. replacement placement for drain/disk-loss workflows if in scope;
5. no path constructs `AssignmentInfo` outside authority publisher.

**Explicit non-scope**:

1. rack/AZ awareness,
2. hot rebalance,
3. load-based auto movement,
4. multi-master HA,
5. V2 promote/demote semantics.

**Pass**: API/CSI/admin test creates RF volume from intent, observes desired topology generation, and volumes bind roles without manual topology stuffing.

**Interface strengthening**:

1. Introduce a product-shaped placement intent: `volume_id`, size, RF, and optional constraints.
2. Planner emits a `PlacementPlan` / bounded intent, never raw `AssignmentInfo`.
3. Reconciler observes plan progress and publisher facts separately.
4. Returned/stale replicas enter as `ReplicaCandidate`, not immediate `ReplicaReady`.
5. Candidate readiness requires durable/progress facts, not heartbeat presence.

---

### P15-P5A — G17-lite Observability

**Goal**: dogfood is not grep-only.

**Minimum endpoints / surfaces**:

1. volume status: role, epoch, endpoint version, mode;
2. recovery state: decision, R/S/H, session phase, durable ack known/R;
3. peer state: healthy/degraded/recovering, last probe/ack;
4. flow-control dry-run verdict;
5. structured log fields for recovery and failover events;
6. `/readyz` and `/healthz` or equivalent for deployment systems.

State vocabulary must separate:

1. frontend readiness;
2. authority role;
3. replication role;
4. durable/recovery progress;
5. placement or unsupported reason.

**TDD first**:

1. endpoint returns stable JSON shape;
2. flow-control verdict appears after observation;
3. stale/degraded/recovering states are distinguishable;
4. endpoint is loopback or authenticated until G16.

**Pass**: QA/user can diagnose stuck rebuild, lagging replica, and unavailable frontend from status output without reading engine internals.

---

### P15-P5B — G15a CSI MVP

**Goal**: Kubernetes PVC -> pod -> block device -> workload read/write -> clean teardown.

**Scheduling**: starts after G9A contracts are stable; can run in parallel with G17-lite.

**Port strategy from V2 CSI survey**:

| Module | Disposition |
|---|---|
| CSI gRPC service layer | PORT-AS-IS / light rebind |
| iSCSI/NVMe transport adapters | PORT-AS-IS |
| Volume backend bridge | PORT-REBIND to V3 lifecycle/placement APIs |
| Snapshot ID encoding | PORT-AS-IS, but snapshot RPCs deferred to G15b |
| deploy YAMLs | light rewrite |
| tests/scenarios | port with backend rewire |

**G15a must ship**:

1. `CreateVolume` / `DeleteVolume`;
2. `ControllerPublishVolume` / `ControllerUnpublishVolume`;
3. `NodeStageVolume` / `NodePublishVolume`;
4. `NodeUnpublishVolume` / `NodeUnstageVolume`;
5. `GetCapacity`;
6. controller/node capability truth.

**G15a must not claim**:

1. snapshots,
2. online resize,
3. clones,
4. rack-aware topology,
5. production security beyond declared beta posture.

**Pass**: k8s integration test provisions a volume, attaches to pod, runs fio/postgres-class workload, and tears down clean.

---

### P15-P6 — Internal K8s Dogfood Checkpoint

**Goal**: first complete user-visible slice.

**Required evidence**:

1. deploy blockmaster/blockvolume components on a small cluster;
2. create volume via CSI;
3. pod mounts/uses the volume;
4. workload runs for a declared contiguous duration;
5. replica failure recovers without indefinite dataplane stall;
6. status surfaces explain current state;
7. teardown cleans resources.

**Non-claims**:

1. snapshot/resize,
2. rack/AZ placement,
3. full node decommission,
4. performance SLO,
5. non-local production auth unless G16 lands.

---

### P15-P7 — Remaining Beta Gates

Prioritize by dogfood pain:

| Gate | Recommended disposition |
|---|---|
| G10 Snapshot | Implement minimal or explicit beta-defer. Do not expose CSI snapshot capability before engine truth exists. |
| G11 Resize | Implement minimal offline/online story or explicit beta-defer. Do not claim PVC expansion silently. |
| G12 Disk failure | Must either detect/evict/recreate, or document unsupported disk-failure path with operator action. |
| G13 Node lifecycle | At least join/drain/decommission story or explicit unsupported state. |
| G14 External API | Narrow because CSI covers many verbs; keep safe intent/status API. |
| G16 Security/Auth | Required before non-local beta; until then loopback/local-only claims must be explicit. |
| G18 Deployment | Helm/manifests/config validation after CSI shapes stabilize. |
| G19 Migration | Prefer explicit non-migration beta unless product owner accepts migration scope. |
| G20 QoS/rack/operator/GC | Mostly defer, but GC/retention implication cannot be silent. |
| G21 Performance SLO | Run after correctness/dogfood; define named hardware and V2 comparison where meaningful. |

---

### P15-P8 — G22 Final Cluster Validation

**Goal**: release evidence bundle, not local smoke.

**Must include**:

1. multi-process / multi-node bring-up;
2. lifecycle create/use/delete;
3. replicated write/read;
4. failover data continuity;
5. replica restart/catch-up;
6. rebuild/re-create;
7. disk failure or explicit unsupported evidence path;
8. node lifecycle or explicit limitation;
9. security negative tests if non-local beta;
10. metrics/log/artifact validation;
11. performance/soak report;
12. `manifest.json`, `result.json`, `result.xml`, logs, metrics, and claim matrix.

---

## 5. What Not To Do Now

1. Do not pursue strict physical single-queue shipper before G8 unless a failing test requires it.
2. Do not enforce flow control before observability and policy are visible.
3. Do not implement CSI snapshot/resize before G10/G11 truth exists.
4. Do not port V2 authority semantics: promotion/demotion, heartbeat-as-authority, local-role-as-authority.
5. Do not claim Kubernetes beta while lifecycle, placement, and diagnostics are still manual/opaque.
6. Do not update P15 close language to include post-`d09fcc6` commits unless QA re-runs the evidence and architect moves the pin.
7. Do not let `blockmaster` become an ad-hoc if/then assignment mutator. Placement belongs in the authority policy/planner seam even if the product says "master owns placement."
8. Do not claim a returned old primary is a ready replica just because it heartbeats. It is at most observed/candidate until progress evidence proves readiness.

---

## 6. Immediate Next Actions

### Action 1 — G7 close packet — ✅ done

1. Architect signed G7 §close on `d09fcc6`.
2. Roadmap state changed from “closing G5/G7” to “G7 closed; G8 next”.
3. Forward-carry register points to this document for prioritization.

### Action 2 — Start P15-P1

Open a small G7-followup hardening mini-plan:

1. list the invariant tests;
2. list log/disposition cleanups;
3. run scoped tests only;
4. no behavior expansion.

### Action 3 — Start G8 mini-plan

Open `v3-phase-15-g8-mini-plan.md`:

1. bind failover scenarios;
2. name hardware target;
3. define reconnect/reattach semantics;
4. define exact old-primary stale rejection;
5. define byte-equal data oracle.

---

## 7. Working Rule For Each Gate

Every gate should start with this checklist:

```text
1. What product sentence becomes true after this gate?
2. What product sentence remains false?
3. What V2 files are assets?
4. What V2 semantics are permanently rejected?
5. What is the first red test?
6. What is the hardware or integration evidence?
7. What is the forward-carry if we stop here?
```

If a gate cannot answer these seven questions, it is not ready for code.

---

## 8. MVP Replication Usage Rules

These rules define the beta-facing behavior until a later gate changes them with tests and docs.

### 8.1 Default ACK profile

MVP default is **best-effort replication ACK**:

1. Foreground write/sync success does not require every replica to return a full durable ACK.
2. Replica lag, miss, or recovery state is still monitored through progress/probe facts.
3. A lagging replica must be fed by catch-up or rebuild; best-effort is not permission to abandon recovery.
4. User-facing docs must name the RPO implication: if the primary dies before a secondary has durable progress, the acknowledged write may not survive unless a stricter profile is enabled.

### 8.2 Future full-ack / quorum profile

Full-ack/quorum mode is a separate policy gate.

For RF=2:

1. The only secondary must be `ReplicaReady` and sync-ack eligible.
2. If that secondary enters recovery, it no longer counts as the synchronous ACK peer.
3. Primary writes/syncs must fail or time out with a clear quorum error, unless an operator explicitly changes the volume to a named degraded/best-effort mode.
4. Reads from the current primary may continue if the authority line is valid and local data is healthy.
5. The system must not silently return "full sync" success while the only secondary is catching up or rebuilding.

### 8.3 Recovery policy

Recovery remains mandatory under both ACK profiles:

1. A new or returning replica starts as observed/candidate, not ready.
2. If retained WAL can cover the gap, the feeder catches it up.
3. If retained WAL cannot cover the gap, the coordinator starts rebuild.
4. Base/rebuild and WAL feeding are one ownership story: do not allow two independent feeders to advance the same peer's recovery truth.
5. Progress facts should eventually move pin/frontier forward; slow pin movement is a flow-control and degradation-policy input, not an ACK shortcut.

### 8.4 Operator-visible states

The MVP must make these states distinguishable in status/logs before claiming production-grade availability:

1. `best_effort`: writes may ACK on primary durability only.
2. `full_ack_unavailable`: configured full-ack mode cannot currently accept writes.
3. `replica_recovering`: replica is being fed and is not sync-ack eligible.
4. `replica_ready`: replica can participate in the configured ACK profile.
5. `degraded`: fewer healthy/ready replicas than desired.
