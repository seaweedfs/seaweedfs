# V3 Phase 15 — G5-5C (Peer Recovery Trigger After Replica Restart) Mini-Plan

**Date**: 2026-04-27 (v0.3 — revised per architect REVISE ruling on v0.2: V3 path correction + peer-set generation design + truth-domain wording)
**Status**: §1-§6 awaiting architect single-sign per `v3-batch-process.md §5` (Option A trigger source already bound; v0.3 absorbs three more REVISE items from architect's v0.2 review)
**Repo**: `seaweed_block` (V3) — **not** `seaweedfs` (V2)
**Owner**: sw (master observation-driven re-emit + primary-side recovery dispatch + tests); QA (m01 hardware re-run + scenario authoring)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR, one §close)
**Predecessors**: G5-5 closed at `seaweedfs@c78116fd2` (L3 Replicated IO on hardware; #4 carried to this batch); architect bindings 2026-04-27 (G5-5 round 14 close ruling + G5-5C v0.1 + v0.2 REVISE rulings)

---

## §1 Scope

G5-5 surfaced a **real recovery-path finding** during hardware verification step #4: when a replica process is killed mid-write, restarted against the same `--durable-root`, and rejoins the cluster, it never receives the LSNs the primary wrote during the down window. The replica reopens its durable storage, resubscribes to the master, and is observed back — but the primary's shipper for that peer remains in `ReplicaDegraded`, and `gate-degraded` rejects ships without retry. The barrier acks at the stale `achievedLSN` of the moment the peer went down. Engine-driven catch-up primitives (T4d-4) exist, but no runtime trigger fires them when a degraded peer becomes reachable again.

G5-5C closes that gap: define and implement the **trigger source** that re-arms a degraded peer's shipper after the peer comes back, and prove convergence on the exact failed hardware case from G5-5 #4.

### Architecture touchpoints

- `v3-architecture.md §6.3` Replication Path — primary → `ReplicaPeer` → shipper state machine (`ReplicaConnecting` → `ReplicaInSync` happy path; `ReplicaDegraded` failure latch)
- `v3-architecture.md §7` Recovery Architecture — engine-driven catch-up: probe → `ProbeReplica` outcome → if `ProbeCatchUpRequired`, engine plans + executes catch-up via T4d-4 primitives (lane-pure apply gate, R+1 boundary fences, per-LBA stale-skip)
- `v3-architecture.md §13` Product Completion Ladder — closing G5-5C completes **L4 Replicated IO with peer-restart resilience** (alongside L3 already reached)

### What G5-5C delivers

| # | Item | Verifier |
|---|---|---|
| 1 | **Trigger source** for re-arming a degraded peer's shipper after the peer becomes reachable again. **Architect binding (2026-04-27)**: must reuse engine-driven recovery primitives (T4d-4); no ad-hoc re-ship from the replication layer. **sw must propose trigger source first** — see §1.A "Trigger source options" below; one option is selected at architect ratification of this mini-plan. | Implementation lands the chosen path; #2 tests prove the wire-up |
| 2 | **m01 hardware re-run of G5-5 #4** — kill replica, write while down, restart same `--durable-root`, wait, verify `LBA[2]=0xef` byte-equal via `m01verify` within deadline. The script `iterate-m01-replicated-write.sh` already contains `verify_restart_catchup` (currently red); G5-5C closes when this step turns GREEN on hardware without script changes beyond optional deadline adjustment. | `iterate-m01-replicated-write.sh verify_restart_catchup` GREEN; artifacts under `/mnt/smb/work/share/g5-test/logs/artifacts-<timestamp>/` |
| 3 | **Component-scope test** — primary observes peer-up event after restart → shipper re-arms → engine plans catch-up → replica receives missed LSNs → barrier ack at current frontier. Tests the trigger path without the iSCSI/iptables harness. | New unit/component test in `weed/server/` (exact name + file determined at impl) |
| 4 | **Failure-mode test** — peer-up event fires but engine catch-up cannot complete (e.g., gap exceeds retention → rebuild required). Verifies the trigger correctly hands off to the rebuild path (`StartRebuildFromProbe`) instead of looping. | New unit/component test |

### §1.A Trigger source — bound

**Architect ruling 2026-04-27 (REVISE before code, v0.1 → v0.2)**: trigger source is **Option A — master observation-driven re-emission**, with **both halves of the causal chain in this batch's scope** (no split into a separate precursor batch). Options B and C are explicitly rejected for this batch.

Rationale (architect): "Master already owns peer-set/observation truth in V3. Periodic probe (Option B) is viable but less disciplined. Transport reconnect (Option C) is rejected for this batch. Splitting Option A into a separate G5-5B precursor would add process overhead for one causal chain — keep both halves together."

Option A has two parts, both required:

| Part | Side | What it does |
|---|---|---|
| **A1 — Master-side observation-driven re-emission** | master | Today, master's `SubscribeAssignments` only re-emits when the publisher mints a *new* fact (e.g., `IntentRefreshEndpoint`). Observation-freshness changes alone do not trigger re-emission (sw's G5-5 round-7 finding). A1 wires master so that when a previously-stale observation for an existing slot becomes fresh again (replica restart against same `--durable-root` keeps `server_id`/slot identity, only the observation reanimates), the publisher mints a re-emission of the assignment fact for that slot. |
| **A2 — Primary-side recovery dispatch** | primary | Primary's host loop already consumes assignment-fact updates from master via `SubscribeAssignments` (G5-5A code path). On consuming a re-emitted fact for a slot whose shipper is in `ReplicaDegraded`, the host emits a "peer-reappeared" event into the recovery manager, which calls `ProbeReplica`. On `ProbeCatchUpRequired` → engine-driven catch-up via T4d-4 primitives. On `ProbeRebuildRequired` → hand off to existing `StartRebuildFromProbe`. |

**Pros (recap)**: Authoritative source (master is the single source of peer-set truth in V3); aligns with G5-5A peer-set publication path; no new transport paths; no polling goroutine; observation freshness gating already exists in `ObservationStore.SlotFact` from G5-5A round 54.

**Cons accepted**: Recovery latency couples to master heartbeat cadence (~seconds, dominated by observation freshness window). For G5-5C #5 hardware deadline this is well within the 30s budget.

**Options B and C — recorded for future reference**:

- **Option B (periodic probe loop)** — rejected: introduces a polling goroutine and an independent timing source when the same event already flows through the master subscription. Less disciplined per V3 truth-domain split.
- **Option C (transport reconnect signal)** — rejected for this batch: `Ship()` short-circuits in `ReplicaDegraded`, so the shipper does not attempt a reconnect that could carry the signal. C in practice folds back into A or B.

### §1.B Peer-set generation scheme — design (architect REVISE v0.2 #2)

**Architect REVISE ruling on v0.2**: "v0.2 says master re-emits assignment facts, but does not define how `PeerSetGeneration` changes. A re-emitted fact with the same generation can be dropped by `ReplicationVolume.UpdateReplicaSet`. The mini-plan needs an explicit design: master-maintained peer-set revision, observation revision folded into generation, or another monotonic scheme."

**Today's encoding** (`core/host/master/services.go:215`):
```go
fact.PeerSetGeneration = (info.Epoch << 32) | info.EndpointVersion
```
With epoch and EV each constrained to fit in uint32 (asserted just above the encoding line). Generation is purely a function of the authority line `(epoch, ev)`. The publisher comment frames this as "monotonic across process lifetimes because Epoch/EV are durable authority facts preserved across master restart."

**Stale-drop hazard** (`core/replication/volume.go:194-209`):
```go
if generation > 0 && generation <= v.lastAppliedGeneration {
    v.replayedGens.Add(1)
    // ...stale-replay log + return nil...
}
```
A re-emit with unchanged `(epoch, ev)` produces an identical generation → silent stale-drop in `UpdateReplicaSet`. The peer set is NOT mutated, peer state is NOT recomputed, and any "peer reappeared" signal A1 wants to deliver is lost.

**Design space — three options the architect named, mapped to V3 mechanics:**

| Option | Wire shape | Master-side state | Cross-restart monotonicity | Aliasing risk |
|---|---|---|---|---|
| **α — Master-maintained per-volume monotonic counter** | Unchanged (single `PeerSetGeneration` uint64) | New durable (or quasi-durable) per-volume rev counter; bumps on every emission cause | Requires master to persist the counter (new authority-store entry), or accept reset on master restart with a forced subscriber bootstrap | None |
| **β — Observation revision folded into existing generation packing** | Unchanged (single uint64) | `obsRev` per (volume, replica) maintained by `ObservationStore`; packed as `(epoch << 48) \| (ev << 32) \| obsRev`, narrowing epoch + ev to 16 bits each | Preserved if epoch/ev fit in 16 bits | High — current code asserts epoch + ev fit in uint32; narrowing to uint16 is an invariant change |
| **γ — Add a separate `PeerSetRevision` field alongside `PeerSetGeneration`** | Wire change (additive proto field) — `(generation, revision)` compared lexicographically | `obsRev` per slot in `ObservationStore` (G5-5A round 54 already gates freshness; extend to count fresh-stale-fresh transitions) | Preserved — `PeerSetGeneration` keeps `(epoch<<32)\|ev` semantics; `PeerSetRevision` orders re-emits within a fixed lineage | None within the wire shape; lex compare in `UpdateReplicaSet` |

**sw recommendation: Option γ** — for these reasons:
1. **Discipline-coherent** — `PeerSetGeneration` continues to mean "the lineage stamp" (semantically aligned with `(epoch, ev)`); `PeerSetRevision` newly means "the re-emit ordinal within this lineage". Two distinct trust signals carried by two distinct fields.
2. **No bit-arithmetic gymnastics** — Option β narrows epoch/ev which violates an existing uint32 invariant; not worth the wire saving.
3. **No new master-side durable state** — unlike α, no need to add an authority-store entry just for the counter; `obsRev` lives where freshness already lives (`ObservationStore`).
4. **Backward-additive proto change** — V3-only, single field, default-zero is a safe "no revision yet" sentinel; existing tests with `PeerSetGeneration: N` continue to compile and pass with `PeerSetRevision: 0`.
5. **`UpdateReplicaSet` change is mechanical** — replace the `generation <= lastAppliedGeneration` stale-drop check with `(generation, revision) <= (lastAppliedGeneration, lastAppliedRevision)` lex compare. Existing replay-counter forensics extend cleanly.

**Reject α**: master restart problem and new durable state both add scope outside G5-5C's stated minimal-change posture.
**Reject β**: narrowing epoch + ev to 16 bits silently weakens an existing invariant; one uint32 epoch overflow in any future cluster year erases the option's benefit. Do not retire a stronger invariant for a weaker one.

**Wire change (γ) — proto field**:
```proto
// core/rpc/proto/control.proto — AssignmentFact message
message AssignmentFact {
  // ... existing fields ...
  uint64 peer_set_generation = N;   // existing — (epoch<<32)|ev lineage stamp
  uint64 peer_set_revision   = N+1; // NEW — re-emit ordinal within lineage; 0 = unrevised
}
```

**Generation rule update** (`UpdateReplicaSet`):
```
Compare (incoming_gen, incoming_rev) vs (lastApplied_gen, lastApplied_rev):
  - lex-greater          → apply + advance both
  - lex-equal-or-less    → stale-replay (existing log + replayedGens counter)
  - generation == 0      → unversioned apply (existing semantics, unchanged;
                           rev is ignored when gen==0)
```

**A1 emission trigger**:
- On lineage change (`(epoch, ev)` bump) → `PeerSetRevision = 0` (new lineage; rev resets)
- On observation freshness transition (stale → fresh) within unchanged lineage → `PeerSetRevision = obsRev_for_slot++` (or, for multi-slot facts, max across the slots that transitioned)
- `obsRev` lives in `ObservationStore` per (volume, replica), incremented on the fresh-stale-fresh transition (gated by `FreshnessConfig` from G5-5A round 54).

**Open architect choice within γ**: per-slot vs per-volume rev. sw proposes **per-volume max** — simpler, and re-emission is a volume-fact event regardless of which slot transitioned. Architect may bind otherwise at single-sign.

### §1.C Truth-domain wording correction (architect REVISE v0.2 #3)

**Architect REVISE ruling on v0.2**: "Adjust the truth-domain line: A1 is not just a 'read' of master truth if it causes subscription re-emission and peer-set revision movement. It can remain authority-safe, but document it as master observation-driven publication/re-emission, not pure read."

**Corrected truth-domain check** (replaces v0.2's §1 closing line):

| Truth domain | A1 (master) | A2 (primary) |
|---|---|---|
| Master peer-set + observation freshness | **Publication / re-emission** — A1 reads observation freshness from `ObservationStore` AND publishes a fact re-emission via the publisher; `PeerSetRevision` advances; existing `(epoch, ev)`-derived `PeerSetGeneration` does not change unless lineage itself changed | (no master writes from primary side) |
| Primary shipper state + recovery decisions | (no primary writes from master side) | **Write** — recovery manager dispatches, shipper state advances via existing transitions |
| Replica durable storage + acks | (no change) | (no change — replica already reopens correctly post-restart; G5-5 #4 confirmed restart side works) |
| Engine | (no change) | (no change — T4d-4 primitives reused as-is) |

A1 is **authority-safe**: it does not invent a new lineage, it does not advance `(epoch, ev)`, it does not change which replica is authoritative. It re-emits a fact that already represents authoritative truth, with a fresh `PeerSetRevision` so subscribers re-apply.

### What G5-5C does NOT deliver (explicit non-claims)

- **No rebuild-path-only batch.** This batch handles the catch-up case (gap within retention). Rebuild path (`StartRebuildFromProbe`) is already wired (T4d-4 part B/C); G5-5C #4 only verifies the trigger correctly *hands off* to rebuild, not the rebuild path itself.
- **No new engine primitives.** Architect binding: reuse T4d-4. The gap is at the runtime-trigger layer (which fires when), not the recovery substrate (which already works once fired).
- **No primary failover.** Primary stays primary throughout. G8 scope.
- **No NVMe / multi-replica / placement scope.** Same exclusions as G5-5; this batch is a targeted recovery-trigger fix on the 2-node iSCSI configuration.
- **No script harness rework.** `iterate-m01-replicated-write.sh verify_restart_catchup` already has the right shape; G5-5C must turn it GREEN, not rewrite it.

### Files (preliminary — exact set bound at code-start)

All paths are in **`seaweed_block` (V3)**. The v0.2 file map mistakenly listed V2 `weed/server/...` and `weed/storage/blockvol/...` paths; v0.3 corrects this per architect REVISE v0.2 #1.

| File | Side | Likely change | LOC est |
|---|---|---|---|
| `core/rpc/proto/control.proto` | wire (A1) | Add `uint64 peer_set_revision` to `AssignmentFact` (§1.B Option γ). Regenerate `core/rpc/control/control.pb.go` + `control_grpc.pb.go`. | ~5 (proto) + generated |
| `core/host/master/services.go` | master (A1) | In `SubscribeAssignments`, on observation freshness transition (stale → fresh) for a slot in the volume's topology, mint a re-emission of the current assignment fact with bumped `PeerSetRevision`. Reuse `Publisher.LastPublished` + `ObservationStore.SlotFact` (G5-5A round 54). | ~80 |
| `core/authority/observation_store.go` | master (A1) | Track per-slot `obsRev` (uint64) — increment on fresh-stale-fresh transition, expose `SlotObsRev(volumeID, replicaID)` and a freshness-transition callback (or polling comparison helper used by services.go). | ~40 |
| `core/replication/volume.go` | primary (A2) | `UpdateReplicaSet` signature: replace `generation uint64` with `(generation, revision uint64)` OR add a sibling method that accepts both. Stale-drop guard becomes lex compare on `(generation, revision)`. Existing `lastAppliedGeneration` joined by `lastAppliedRevision`. Existing `replayedGens` counter still applies; consider `replayedRevs` counter for forensics. | ~30 |
| `core/host/volume/host.go` | primary (A2) | In `applyFact`, decode `PeerSetRevision` from the fact (via `decodeReplicaTargets` or a parallel helper), pass to `UpdateReplicaSet`. After `UpdateReplicaSet` returns nil-with-mutation, detect peers that transitioned out of `ReplicaDegraded` (or were degraded at entry and now have a fresh reapply) and dispatch the peer-reappeared event into the recovery dispatcher. | ~60 |
| `core/replication/peer.go` | primary (A2) | Add an entry point on `ReplicaPeer` that the host's peer-reappeared handler calls — e.g., `OnReappeared(...)` — which probes the peer and dispatches to engine-driven catch-up via existing T4d-4 primitives, or hands off to rebuild on `ProbeRebuildRequired`. Includes per-peer cooldown (§6 risk #1). | ~70 |
| `core/host/volume/apply_fact_test.go` | primary tests | New test cases for revision-driven reapply + peer-reappeared dispatch. | ~70 |
| `core/host/master/services_test.go` (existing or new) | master tests | New test cases for observation-freshness-driven re-emission with bumped `PeerSetRevision`. | ~60 |
| `core/replication/volume_test.go` | replication tests | New cases for `(generation, revision)` lex compare in `UpdateReplicaSet`. | ~40 |
| `core/replication/component/...` (one of the existing component-test files) | end-to-end tests | Component test #4 — failure-mode rebuild hand-off without re-trigger loop. | ~50 |
| `sw-block/design/v3-phase-15-g5-5c-mini-plan.md` (this doc) | — | §close appended at batch close. | + §close |

Total estimate: ~285 LOC production + ~220 LOC tests + ~5 LOC proto + generated, split master-side ~125 / replication ~30 / primary-side ~130 / tests ~220.

### Architecture truth-domain check (`v3-architecture.md §4`)

See §1.C below for the corrected per-domain matrix (v0.3 fixes the v0.2 wording: A1 is **publication / re-emission** of master truth, not pure read; remains authority-safe because no new lineage is invented).

No truth-domain crossings introduced; A1 remains within the master truth domain (publication is master's own write surface), A2 remains within the primary truth domain.

---

## §2 Acceptance criteria

Numbered, verifier-named, single source of truth.

| # | Criterion | Verifier |
|---|---|---|
| 1 | Trigger source = Option A (master observation-driven re-emission), with both A1 (master-side re-emit on observation freshness transition) and A2 (primary-side recovery dispatch on consuming the re-emitted fact) implemented in this batch. | Architect §7 sign + §1.A binding |
| 2 | A1 — Master mints a re-emission of the existing assignment fact for a slot when its observation transitions stale → fresh (slot identity unchanged). The re-emission carries the unchanged `PeerSetGeneration` and a bumped `PeerSetRevision` per §1.B Option γ. | Component test #3a (master-side; `core/host/master/services_test.go` — exact test name pinned at code-start) |
| 3 | A2 — On consuming a re-emitted assignment fact whose `(PeerSetGeneration, PeerSetRevision)` is lex-greater than `(lastAppliedGeneration, lastAppliedRevision)`, `UpdateReplicaSet` re-applies, the host detects peers that were in `ReplicaDegraded`, and the recovery dispatcher calls `ProbeReplica` and dispatches to engine-driven catch-up (T4d-4 primitives) when outcome is `ProbeCatchUpRequired`. | Component test #3b (primary-side; `core/host/volume/apply_fact_test.go` + `core/replication/volume_test.go` — exact test names pinned at code-start) |
| 4 | When the gap exceeds retention and probe outcome is `ProbeRebuildRequired`, recovery manager hands off to `StartRebuildFromProbe` (already wired) without entering a re-trigger loop. Per-peer cooldown prevents flapping. | Component test #4 |
| 5 | `iterate-m01-replicated-write.sh verify_restart_catchup` step turns GREEN on m01/M02 hardware: `LBA[2]=0xef` byte-equal under `m01verify` within 30s deadline (same as G5-5 #3 network-catchup; budget = 1× master heartbeat for re-emit + catch-up time + safety margin, comfortably inside 30s). | Hardware re-run; artifacts archived under `g5-test/logs/artifacts-<timestamp>/` |
| 6 | No regression on G5-5 #1/#2/#3 — all three remain GREEN in the same hardware run. | Same script, same run |

**File + test names**: §2 #3a/#3b/#4 list (file: TBD at impl) is acceptable for v0.2 ratification per QA review; sw concretizes file path + Go test method name at code-start so QA can grep them in CI later. To be appended to this §2 as a code-start addendum (not requiring re-ratification — it's the same tests, just named).

### Architect review checklist (`v3-batch-process.md §12`) coverage

| Check | Where addressed |
|---|---|
| Scope truth | §1 + §1.A explicit non-claims; §1.A trigger options laid out without pre-deciding |
| V2 / new-build decision | New build; no V2 muscle PORT; G-1 N/A (§4) |
| Engine / adapter impact | Zero engine logic change (binding from architect); replication-layer + recovery-manager wiring only |
| Product usability level | Closing this batch reaches **L4 Replicated IO with peer-restart resilience** (replica process restart now self-heals via engine-driven catch-up) |

---

## §3 Invariants to inscribe at close

| INV ID (proposed) | What it claims | Test pointer (proposed) |
|---|---|---|
| `INV-REPL-PEER-RECOVERY-TRIGGER-001` | When a peer in `ReplicaDegraded` becomes observable again (per Option A trigger), the primary's recovery dispatcher re-probes and dispatches to engine-driven catch-up or rebuild without operator intervention. | Component test #3a/#3b + hardware step #5 |
| `INV-REPL-PEER-RECOVERY-NO-RETRIGGER-LOOP` | Hand-off from catch-up trigger to rebuild path is one-way; the trigger does not re-fire while a rebuild session is active. | Component test #4 |
| `INV-MASTER-PEER-SET-GEN-REV-MONOTONIC` | `(PeerSetGeneration, PeerSetRevision)` advances lex-monotonically across all assignment-fact emissions for a (volume, replica). Re-emission on observation freshness transition bumps `PeerSetRevision` while leaving `PeerSetGeneration` (= `(epoch<<32)\|ev`) unchanged. Lineage change resets `PeerSetRevision` to 0 and bumps `PeerSetGeneration`. | `core/host/master/services_test.go` (revision bump + lineage reset) + `core/replication/volume_test.go` (lex-compare stale-drop) |

**Forward-carry from G5-5 §close (deferred ledger pointers)**:
- `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` — G5-5C hardware re-run exercises this path; ledger pointer added at G5-5C §close (per G5-5 §close binding).
- `INV-REPL-LSN-ORDER-FANOUT-001` (T4a-4) — same; G5-5C close packages G5-5 #2 + #4 evidence as one Integration row update.

INVs **rejected / deferred**:
- "Engine catch-up auto-completes any retention-bounded gap" — already covered by T4d-4 invariants; G5-5C only adds the trigger, not the substrate.

---

## §4 G-1 V2 read

**N/A** — this batch is new build (recovery-trigger wiring), not a V2 muscle PORT. No V2 source to read against. Per `v3-batch-process.md §6.1`, G-1 is skipped.

---

## §5 Forward-carry consumed (from G5-5 §close)

| Carry item | Disposition in G5-5C |
|---|---|
| **Peer recovery trigger after replica restart** (architect-bound 2026-04-27) | **Primary scope of this batch** — §1 + §1.A + §2 #1-#5 directly address. |
| Reuse engine-driven primitives (T4d-4) | Binding adopted in §1 + §3; no new engine primitives introduced. |
| Define trigger source first | §1.A enumerates three options with tradeoffs; architect picks at §7 ratification. |
| Pass criterion = G5-5 #4 hardware case | §2 #5 uses the exact existing `verify_restart_catchup` step with no harness changes. |
| Seed evidence: `seaweed_block@5c4718f` primary-fail.log | **Pinned**: `V:\share\g5-test\logs\artifacts-20260427T092858Z\primary-fail.log` (G5-5 §close evidence run, surfaces the `gate-degraded + stale-barrier-ack` pattern). No new collection required. |
| Two ledger-pointer additions (`INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1`, `INV-REPL-LSN-ORDER-FANOUT-001`) | Inscribed at G5-5C §close per §3. |

Opportunistic carry items from G5-5 §close (no specific gate, not in G5-5C scope):
- `EnsureStorage → first-Open` Identity-latch component test — opportunistic, not blocking G5-5C.
- `start_cluster()` pre-flight cleanup discipline — already in `iterate-m01-replicated-write.sh` from G5-5 round 14; future hardware harnesses inherit by reference.

---

## §6 Risks + mitigations

| Risk | Mitigation |
|---|---|
| Chosen trigger source flaps (e.g., Option A: master observation freshness margin too tight → repeated peer-reappeared events) | Add a "last-trigger-time" gate per peer (cooldown of 1 master heartbeat or 5s, whichever larger); covered by component test #4 |
| Trigger fires but probe sees the peer hasn't actually finished durable reopen → false `ProbeCatchUpRequired` | Replica already exposes Healthy via `/status`; primary's probe path uses the same lineage check that G5-5 #2 proved correct. No new mitigation needed beyond reusing the existing probe contract. |
| Gap exceeds retention mid-trigger (race between ship retention pressure and trigger arming) | Probe outcome `ProbeRebuildRequired` already handles this; #4 component test pins the hand-off |
| Hardware re-run on m01 reveals secondary issues (residual iptables, leftover sessions, etc.) | `iterate-m01-replicated-write.sh start_cluster` already has pre-flight cleanup from G5-5 round 14; no new infra work |
| Component tests pass but hardware doesn't converge (timing/state-of-world differences) | Same closure pattern as G5-5: component tests are necessary but not sufficient; hardware GREEN is the §close gate |
| `obsRev` overflow over long-lived clusters (uint64 — practically infinite, but listed for completeness) | uint64 with rare bumps (only on freshness transitions) is effectively unbounded; no mitigation required |
| Subscriber on master restart sees `PeerSetRevision` decrease (master's `obsRev` resets to 0) | Existing master-restart semantics already invalidate subscriber state model; first post-restart fact carries `(generation = current (epoch, ev), revision = 0)`. Subscriber's `lastAppliedRevision` may be > 0 from before restart → fact appears stale in lex compare. Mitigation: subscriber bootstrap path (host's first attach) explicitly clears `lastAppliedGeneration`/`lastAppliedRevision` so first post-restart fact applies. To be made explicit in `applyFact` first-attach handling; covered by component test #3a master-restart sub-case. |

---

## §7 Sign table

| Item | Owner | When | State |
|---|---|---|---|
| §1.A trigger source binding (Option A, A1+A2 in same batch, B/C rejected) | architect | v0.1 → v0.2 REVISE ruling | ✅ done 2026-04-27 |
| §1.B peer-set generation scheme (γ recommended; per-slot vs per-volume rev open at single-sign) | architect | v0.2 → v0.3 REVISE ruling | ⏳ awaiting single-sign |
| §1.C truth-domain wording (publication / re-emission, not pure read) | architect | v0.2 → v0.3 REVISE ruling | ✅ absorbed in v0.3 |
| §1 Files V3 path correction (`core/...` not `weed/...`) | architect | v0.2 → v0.3 REVISE ruling | ✅ absorbed in v0.3 |
| §1-§6 architect single-sign of v0.3 | architect | Before code start | ⏳ pending |
| Code (proto field add + master-side A1 re-emit + replication lex-compare + primary-side A2 dispatch + tests) | sw | After §1-§6 single-sign | ⏳ blocked on single-sign |
| m01 hardware re-run of `verify_restart_catchup` (+ #1/#2/#3 regression check) | QA | After sw lands trigger + component tests | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*Appended at batch close per `v3-batch-process.md §2`.*
