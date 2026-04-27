# V3 Phase 15 — G5-5C (Peer Recovery Trigger After Replica Restart) Mini-Plan

**Date**: 2026-04-27 (v0.2 — revised per architect REVISE ruling + QA review of v0.1)
**Status**: §1-§6 awaiting architect single-sign per `v3-batch-process.md §5` (architect already ratified trigger source = Option A with A1 + A2 in same batch; v0.2 absorbs that ruling and QA's two pin requests)
**Owner**: sw (master observation-driven re-emit + primary-side recovery dispatch + tests); QA (m01 hardware re-run + scenario authoring)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR, one §close)
**Predecessors**: G5-5 closed at `seaweedfs@c78116fd2` (L3 Replicated IO on hardware; #4 carried to this batch); architect bindings 2026-04-27 (round 14 close ruling + v0.1 REVISE ruling)

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

### What G5-5C does NOT deliver (explicit non-claims)

- **No rebuild-path-only batch.** This batch handles the catch-up case (gap within retention). Rebuild path (`StartRebuildFromProbe`) is already wired (T4d-4 part B/C); G5-5C #4 only verifies the trigger correctly *hands off* to rebuild, not the rebuild path itself.
- **No new engine primitives.** Architect binding: reuse T4d-4. The gap is at the runtime-trigger layer (which fires when), not the recovery substrate (which already works once fired).
- **No primary failover.** Primary stays primary throughout. G8 scope.
- **No NVMe / multi-replica / placement scope.** Same exclusions as G5-5; this batch is a targeted recovery-trigger fix on the 2-node iSCSI configuration.
- **No script harness rework.** `iterate-m01-replicated-write.sh verify_restart_catchup` already has the right shape; G5-5C must turn it GREEN, not rewrite it.

### Files (preliminary — exact set bound at code-start)

| File | Side | Likely change | LOC est |
|---|---|---|---|
| `core/host/master/services.go` (or sibling: publisher/observation hookup) | master (A1) | When a slot's observation transitions stale → fresh and the slot identity is unchanged, mint a re-emission of the existing assignment fact for that slot. Reuse `Publisher.LastPublished` + `ObservationStore.SlotFact` freshness gate from G5-5A round 54. | ~70 |
| `core/authority/observation_store.go` (possibly) | master (A1) | If freshness-transition detection isn't already accessible, expose a minimal callback or comparison helper. Avoid new public surface where `SlotFact` already suffices. | ~20 |
| `weed/server/volume_server_block.go` (assignment subscription handler) | primary (A2) | On consuming an assignment-fact update for a slot whose shipper is in `ReplicaDegraded`, emit peer-reappeared event into recovery manager. | ~40 |
| `weed/server/block_recovery.go` | primary (A2) | New entry point (e.g., `OnPeerObservedAfterDegraded(replicaID)`) — probes peer, dispatches to engine catch-up or rebuild. Includes per-peer cooldown to prevent flapping (§6 risk #1). | ~60 |
| `weed/storage/blockvol/wal_shipper.go` | primary (A2) | Possibly: explicit `RearmFromDegraded()` method for the recovery manager to call (avoid touching shipper internals from server layer). | ~20 |
| `weed/server/block_recovery_test.go` + master-side tests | both | Component tests #3 + #4 (covers A1 freshness-transition + A2 dispatch); failure-mode test for rebuild hand-off. | ~150 |
| `sw-block/design/v3-phase-15-g5-5c-mini-plan.md` (this doc) | — | §close appended at batch close. | + §close |

Total estimate: ~360 LOC production + ~150 LOC tests, split master-side ~90 / primary-side ~120 / tests ~150.

### Architecture truth-domain check (`v3-architecture.md §4`)

- Master truth domain: peer-set + observation freshness — **read** (Option A consumes; no master mutation).
- Primary truth domain: shipper state + recovery decisions — **write** (the trigger fires recovery here).
- Replica truth domain: durable storage + acks — **no change** (replica already reopens correctly; G5-5 #4 confirmed restart side works).
- Engine: **no change** to recovery primitives (T4d-4 reused as-is).

No truth-domain crossings introduced.

---

## §2 Acceptance criteria

Numbered, verifier-named, single source of truth.

| # | Criterion | Verifier |
|---|---|---|
| 1 | Trigger source = Option A (master observation-driven re-emission), with both A1 (master-side re-emit on observation freshness transition) and A2 (primary-side recovery dispatch on consuming the re-emitted fact) implemented in this batch. | Architect §7 sign + §1.A binding |
| 2 | A1 — Master mints a re-emission of the existing assignment fact for a slot when its observation transitions stale → fresh (slot identity unchanged). | Component test #3a (master-side; file + test name pinned at code-start) |
| 3 | A2 — On consuming a re-emitted assignment fact for a slot whose shipper is in `ReplicaDegraded`, primary's recovery manager calls `ProbeReplica` and dispatches to engine-driven catch-up (T4d-4 primitives) when outcome is `ProbeCatchUpRequired`. | Component test #3b (primary-side; file + test name pinned at code-start) |
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
| `INV-REPL-PEER-RECOVERY-TRIGGER-001` | When a peer in `ReplicaDegraded` becomes observable again (per the chosen trigger source), the primary's recovery manager re-probes and dispatches to engine-driven catch-up or rebuild without operator intervention. | Component test #3 + hardware step #5 |
| `INV-REPL-PEER-RECOVERY-NO-RETRIGGER-LOOP` | Hand-off from catch-up trigger to rebuild path is one-way; the trigger does not re-fire while a rebuild session is active. | Component test #4 |

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

---

## §7 Sign table

| Item | Owner | When | State |
|---|---|---|---|
| §1.A trigger source binding (Option A, A1+A2 in same batch, B/C rejected) | architect | v0.1 → v0.2 REVISE ruling | ✅ done 2026-04-27 |
| §1-§6 architect single-sign of v0.2 (after this revision) | architect | Before code start | ⏳ pending |
| Code (master-side A1 re-emit + primary-side A2 dispatch + tests) | sw | After §1-§6 single-sign | ⏳ blocked on single-sign |
| m01 hardware re-run of `verify_restart_catchup` (+ #1/#2/#3 regression check) | QA | After sw lands trigger + component tests | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*Appended at batch close per `v3-batch-process.md §2`.*
