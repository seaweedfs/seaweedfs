# V3 Phase 15 — G5-5C (Peer Recovery Trigger After Replica Restart) Mini-Plan

**Date**: 2026-04-27 (v0.4.1 — adds §1.D two-feedback-loop ordering-independence statement per architect framing 2026-04-27; design unchanged from v0.4)
**Status**: §1-§6 awaiting architect single-sign per `v3-batch-process.md §5`
**Repo**: `seaweed_block` (V3) — **not** `seaweedfs` (V2)
**Owner**: sw (primary-side probe loop + recovery dispatch + tests); QA (m01 hardware re-run + scenario authoring)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR, one §close)
**Predecessors**: G5-5 closed at `seaweedfs@c78116fd2` (L3 Replicated IO on hardware; #4 carried to this batch)
**Architect bindings 2026-04-27**:
- G5-5 round 14 close ruling: #4 carries forward as real recovery-path finding
- G5-5C v0.1 ruling: define trigger source first
- G5-5C v0.2 ruling: bind Option A with A1+A2 in same batch (later retired — see below)
- G5-5C v0.3 ruling: V3 paths + PeerSetGeneration design + truth-domain wording (the design items become moot under v0.4)
- **G5-5C v0.4 ruling (control-plane / data-plane layering correction)**: retire Option A. Master must NOT take on observation-driven re-emission as a recovery scheduling mechanism — that overloads the control plane with runtime recovery cadence and forces `PeerSetGeneration` to carry two semantics (authority version + peer-set-view version). Bind **Option B (primary-side degraded-peer probe loop)** with explicit constraints (see §1.A). Master remains identity / topology / address / RF-health source; primary owns runtime recovery decisions. No master protocol change in this batch.

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

### §1.A Trigger source — bound (v0.4)

**Architect re-ruling 2026-04-27 (v0.3 → v0.4)**: bind trigger source to **Option B — primary-side degraded-peer probe loop**, with the explicit constraints below. Options A and C are now rejected.

**Rationale (architect, v0.4)**: control-plane / data-plane layering. Master must own *identity / topology / address / RF-health*; it must NOT own *runtime recovery scheduling*. v0.2/v0.3's Option A path forced master into recovery-scheduling territory and forced `PeerSetGeneration` to carry two distinct semantics (authority version + peer-set-view version). That is the wrong shape: master gets heavier, control-plane heartbeat cadence couples to data-plane recovery cadence, and the protocol cleanliness of "Epoch / EndpointVersion = authority line, full stop" erodes. Option B keeps master clean and puts runtime recovery decisions where they belong: on primary, next to the engine-driven primitives that already exist (T4d-4).

#### Option B — bound shape

| Aspect | Binding |
|---|---|
| **Loop owner** | Primary's per-volume runtime (host- or replication-layer goroutine; exact placement bound at code-start within §1 Files). One loop per primary serving the volume; no master-side participation. |
| **Scope** | Iterates ONLY over peers in `ReplicaDegraded` state. `ReplicaHealthy`, `ReplicaCatchingUp`, `ReplicaNeedsRebuild` are skipped (catching-up has its own recovery in flight; needs-rebuild dispatches via the existing rebuild path, not this loop). |
| **Cadence** | Low frequency. Initial value: **5 s** between iterations, configurable via a new flag (default 5 s). Each iteration is a single pass over the degraded set; no per-peer parallelism within the loop iteration (cap on concurrent probes prevents stampedes). |
| **Per-peer cooldown** | After a probe completes (success or fail), a per-peer cooldown of **5 s** (= one loop tick) is enforced before the same peer is probed again. Prevents retrigger storms on flapping peers. |
| **In-flight guard** | A peer with an active probe / catch-up / rebuild session is NOT re-probed by the loop. The loop reads the peer's state under its existing lock; if the state is no longer `ReplicaDegraded` at probe-dispatch time, the dispatch is skipped (TOCTOU-safe). |
| **Probe action** | Calls existing `ProbeReplica` (or V3 equivalent on the peer / executor). On `ProbeCatchUpRequired` → engine-driven catch-up via T4d-4 primitives (already wired). On `ProbeRebuildRequired` → hand off to the existing rebuild path. On any other outcome (peer still unreachable, transport error) → log + leave peer in `ReplicaDegraded`, next loop iteration retries after cooldown. |
| **Lifecycle** | Loop starts when the volume's primary role is admitted; stops on volume close / role change. Start/Stop discipline mirrors CP4B-2 lessons (BUG-CP4B2-1 stop-before-run deadlock; BUG-CP4B2-2 zero-interval panic; BUG-CP4B2-3 callback panic isolation). All three patterns explicitly covered by tests. |
| **Master interaction** | Loop does NOT consult master, NOT mutate master state, NOT re-trigger master publication. The peer's address came from the most recent `AssignmentFact` already in primary memory (existing `ReplicationVolume.peers` map); the loop just probes the existing peer handle. |

#### What this batch deliberately does NOT change

- **No master protocol change.** `AssignmentFact`, `PeerSetGeneration`, `SubscribeAssignments`, `Publisher`, `ObservationStore` all unchanged. Master continues to mint assignment facts on lineage events only.
- **No proto field add.** v0.3's proposed `PeerSetRevision` is dropped. `PeerSetGeneration = (epoch<<32)|ev` semantics preserved.
- **No `UpdateReplicaSet` semantic change.** The existing `(generation, lastAppliedGeneration)` stale-replay rule stays exactly as today.
- **No `ObservationStore` change.** `SlotFact` freshness gate (G5-5A round 54) stays as-is.
- **No engine primitive change.** T4d-4 catch-up + rebuild primitives reused unchanged.

#### Layering note

| Layer | Owns in V3 (after this batch) |
|---|---|
| Master / control plane | Mint epoch; publish assignment + declared peer set + addresses; observe slot freshness; (future) report RF health (e.g., desired RF=2, current effective RF=1) — observability only, NOT recovery scheduling |
| Primary / data-control plane | Live ship; mark peers degraded; **periodically probe degraded peers (this batch)**; dispatch engine-driven catch-up / rebuild on probe outcome; durability mode policy (current write / degraded write / fail closed) |
| Replica | Receive ship + barrier; reopen durable storage on restart; resubscribe to master |
| Engine | Recovery primitives (T4d-4) — invoked by primary's recovery dispatcher |

The `gate-degraded` symptom from G5-5 #4 closes by giving primary a runtime mechanism to probe degraded peers and dispatch the existing engine recovery — without dragging master into recovery scheduling.

#### Why Option A is retired

- **Layering**: master would have to take on observation-driven re-emission, which is recovery-scheduling work in disguise. Master's heartbeat cadence becomes the primary's recovery cadence — control plane and data plane no longer separable.
- **Protocol cost**: forces `PeerSetGeneration` to carry two semantics OR forces a new `PeerSetRevision` proto field (v0.3 Option γ). Either way, master assignment publication semantics expand.
- **Failure modes multiply**: master restart now resets recovery cadence; observation-store rev counter becomes a critical control-plane fact requiring durability or careful bootstrap. None of this is needed for runtime peer recovery.

#### Why Option C is rejected

`Ship()` short-circuits in `ReplicaDegraded`, so the shipper does not attempt a reconnect that could carry a transport-reconnect signal. C requires an out-of-band reconnect attempt, which is what Option B's probe loop already does — C reduces to B.

### §1.D Two parallel feedback loops — ordering-independence (protocol invariant)

A replica going down (or recovering) triggers feedback in **both** the control plane and the data-control plane. The protocol must treat these as **two independent loops that proceed in parallel without ordering dependencies**:

| Loop | Owner | Inputs | Action |
|---|---|---|---|
| **Identity / topology / health loop** | master (control plane) | replica heartbeat / observation freshness | Update observed health, effective RF, peer availability; (future) placement / failover / operator alerts. **Does NOT execute recovery.** |
| **Data governance loop** | primary (data-control plane) | ship / barrier failure → mark `ReplicaDegraded`; degraded-peer probe loop (this batch) | Probe degraded peer; on success dispatch engine-driven catch-up / rebuild (T4d-4). **Does NOT wait for master re-emit.** |

**Ordering-independence rule**: any of the following five orderings must be safe (no missed recovery, no double recovery, no protocol violation):

1. **Primary detects first** (ship / barrier fails before master sees observation drop) — primary marks degraded, applies durability-mode policy (continue / degraded-write / fail-closed). Master observation later confirms RF reduction.
2. **Master observes missing first** (observation expires before primary's next ship) — master records reduced health; **does NOT execute recovery**; primary's next ship / probe handles the data path.
3. **Replica recovers and heartbeats first** (master sees fresh observation before primary probes) — master health updates; primary's probe loop will connect on its next tick and catch up. No master re-emit required.
4. **Primary probe connects first** (primary's degraded-peer probe succeeds before master's next observation) — catch-up completes; subsequent master observation just confirms healthy.
5. **Both fire simultaneously** (concurrent observation freshening + concurrent probe success) — primary's recovery manager is **idempotent** under in-flight guard: a peer with an active catch-up / rebuild session is not re-dispatched; a peer in cooldown is skipped. Duplicate triggers are absorbed.

**Alignment surface**: the two loops align via durable identity facts already on the wire — `replicaID`, `epoch`, `endpointVersion`, peer address. Neither loop manufactures new identity; both consume the existing authoritative line.

**Anti-requirements** (what the protocol explicitly does NOT impose):

- Master re-emission of an assignment fact is **NOT a prerequisite** for primary-side recovery to fire.
- Primary-side recovery is **NOT blocked** on master health update.
- Neither loop requires the other to have fired first; neither requires the other to fire at all (e.g., a brief network blip might be resolved by primary's probe loop without master ever observing the freshness drop, which is fine).

**Idempotency guarantees** (load-bearing for "both fire simultaneously"):

- `ReplicaPeer.ProbeIfDegraded()` checks state under lock and returns early if not `ReplicaDegraded` or in cooldown.
- `peer.go` in-flight guard prevents re-dispatch on a peer with an active catch-up / rebuild session.
- Existing `UpdateReplicaSet` stale-replay rule (unchanged from today) absorbs duplicate assignment-fact replays without disturbing the peer set.

**Future RF-health observability path** (different batch, not G5-5C): master MAY surface "desired RF=N, current effective RF=M" to operators / placement controller. That is observability, not recovery scheduling — it does not change the ordering-independence rule above.

`INV-G5-5C-TWO-LOOPS-ORDERING-INDEPENDENT` (proposed, see §3): no run-order dependency between the two loops; idempotent primary-side dispatch under in-flight guard.

### §1.B Master protocol — explicitly unchanged

v0.3 proposed a `PeerSetRevision` proto field, an `ObservationStore.obsRev` counter, and a lex-compare upgrade to `UpdateReplicaSet`. **All three are retired in v0.4.** Master assignment publication semantics, `PeerSetGeneration = (epoch<<32)|ev`, and the existing stale-replay rule in `UpdateReplicaSet` (`core/replication/volume.go:194-209`) are preserved exactly as-is.

The stale-drop hazard that motivated v0.3's design is no longer relevant: under Option B, master is **not** asked to re-emit on observation freshness changes. There is no in-flight assignment fact that needs to bypass the existing stale-replay guard.

### §1.C Truth-domain check (v0.4)

| Truth domain | This batch (v0.4) |
|---|---|
| Master / control plane (epoch, EV, declared peer set, addresses, observation freshness) | **No change.** Master code and protocol untouched. |
| Primary / data-control plane (shipper state, recovery decisions, probe scheduling) | **Write** — new probe loop, peer-state-driven dispatch into existing engine recovery primitives. |
| Replica durable storage + acks | **No change.** Replica already reopens correctly post-restart (G5-5 #4 confirmed). |
| Engine recovery primitives (T4d-4) | **No change.** Reused as-is. |

This is the cleanest possible truth-domain shape: one truth domain (primary data-control) writes, all others are read-only or untouched.

### What G5-5C does NOT deliver (explicit non-claims)

- **No rebuild-path-only batch.** This batch handles the catch-up case (gap within retention). Rebuild path (`StartRebuildFromProbe`) is already wired (T4d-4 part B/C); G5-5C #4 only verifies the trigger correctly *hands off* to rebuild, not the rebuild path itself.
- **No new engine primitives.** Architect binding: reuse T4d-4. The gap is at the runtime-trigger layer (which fires when), not the recovery substrate (which already works once fired).
- **No primary failover.** Primary stays primary throughout. G8 scope.
- **No NVMe / multi-replica / placement scope.** Same exclusions as G5-5; this batch is a targeted recovery-trigger fix on the 2-node iSCSI configuration.
- **No script harness rework.** `iterate-m01-replicated-write.sh verify_restart_catchup` already has the right shape; G5-5C must turn it GREEN, not rewrite it.

### Files (preliminary — exact set bound at code-start)

All paths are in **`seaweed_block` (V3)**. v0.4 retires v0.3's master-side rows entirely (no master code change) and replaces them with primary-side probe loop infrastructure.

| File | Likely change | LOC est |
|---|---|---|
| `core/replication/peer.go` | New per-peer probe entry point (e.g., `ProbeIfDegraded()`) — checks state, returns early if not `ReplicaDegraded` or in cooldown, otherwise probes via existing transport / executor path; on `ProbeCatchUpRequired` dispatches to engine catch-up (T4d-4); on `ProbeRebuildRequired` hands off to existing rebuild path. Per-peer cooldown timestamp + in-flight guard. | ~90 |
| `core/replication/volume.go` (or a new sibling, e.g., `core/replication/probe_loop.go`) | New `degradedProbeLoop` goroutine started by `ReplicationVolume` lifecycle. Iterates over `peers` map under existing lock discipline, calls each peer's `ProbeIfDegraded()`. Configurable interval (default 5 s) and max-concurrent-probes (default 1). Start/Stop methods integrated with `ReplicationVolume.Close`. | ~120 |
| `cmd/blockvolume/main.go` | New flags: `--degraded-probe-interval` (default 5 s; 0 disables), `--degraded-probe-max-concurrent` (default 1). Threaded into `ReplicationVolume` construction. | ~15 |
| `core/replication/probe_loop_test.go` (new) | Loop lifecycle tests — start before run, stop while running, zero-interval guard (CP4B-2 lessons); cooldown enforcement; in-flight guard; only-degraded-peers iteration. | ~150 |
| `core/replication/peer_state_machine_test.go` (existing) or `peer_test.go` (new) | `ProbeIfDegraded` unit tests — non-degraded skip, degraded-with-cooldown skip, dispatch to catch-up vs rebuild branches. | ~80 |
| `core/replication/component/...` (extend an existing component test file or add `g5_5c_probe_loop_test.go`) | Component test for restart-catch-up: kill peer (substrate-level), write while down, restart, observe convergence via primary's probe loop. | ~80 |
| `sw-block/design/v3-phase-15-g5-5c-mini-plan.md` (this doc) | §close appended at batch close. | + §close |

Total estimate: ~225 LOC production + ~310 LOC tests, all primary-side. Zero LOC master / proto / observation_store changes (deliberately).

### Architecture truth-domain check (`v3-architecture.md §4`)

See §1.C below for the corrected per-domain matrix (v0.3 fixes the v0.2 wording: A1 is **publication / re-emission** of master truth, not pure read; remains authority-safe because no new lineage is invented).

No truth-domain crossings introduced; A1 remains within the master truth domain (publication is master's own write surface), A2 remains within the primary truth domain.

---

## §2 Acceptance criteria

Numbered, verifier-named, single source of truth.

| # | Criterion | Verifier |
|---|---|---|
| 1 | Trigger source = Option B (primary-side degraded-peer probe loop) with the constraints in §1.A: only-on-degraded, 5 s interval, per-peer cooldown, in-flight guard, max-concurrent-probes=1. No master protocol change. | Architect §7 sign + §1.A binding |
| 2 | Probe loop lifecycle is correct: starts on volume primary admit, stops on volume close / role change, no goroutine leak, no zero-interval panic, no callback panic propagation (CP4B-2 lessons). | `core/replication/probe_loop_test.go` lifecycle tests |
| 3 | Per-peer cooldown + in-flight guard hold under concurrent state changes: a peer that transitions out of `ReplicaDegraded` mid-loop is not double-dispatched; a peer in cooldown is skipped. | `peer_test.go` / `peer_state_machine_test.go` cooldown + TOCTOU tests |
| 4 | On `ProbeCatchUpRequired` the loop dispatches to engine-driven catch-up (T4d-4 primitives, unchanged). On `ProbeRebuildRequired` the loop hands off to the existing rebuild path. The loop does NOT re-fire on a peer with an active catch-up or rebuild session. | `peer_test.go` dispatch-branch tests |
| 5 | `iterate-m01-replicated-write.sh verify_restart_catchup` step turns GREEN on m01/M02 hardware: `LBA[2]=0xef` byte-equal under `m01verify` within 30 s deadline (same as G5-5 #3 network-catchup; budget = ≤1× probe interval (5 s) + catch-up time, comfortably inside 30 s). | Hardware re-run; artifacts archived under `g5-test/logs/artifacts-<timestamp>/` |
| 6 | No regression on G5-5 #1/#2/#3 — all three remain GREEN in the same hardware run. | Same script, same run |
| 7 | No master code touched. `git diff --stat` for the close PR shows zero changes under `core/host/master/`, `core/authority/`, `core/rpc/proto/`, `core/rpc/control/`. | Diff inspection at PR review |
| 8 | **Two-loop ordering independence** (§1.D): a unit test exercises the "simultaneous-fire" case — concurrent assignment-fact replay (master loop) + concurrent `ProbeIfDegraded` dispatch (primary loop) on the same `ReplicaDegraded` peer. Outcome: at most one catch-up / rebuild session installed; no panic; no goroutine leak; final state converges. Cooldown / in-flight guard absorbs the duplicate. | `core/replication/peer_test.go` — simultaneous-fire test (exact name pinned at code-start) |

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
| `INV-REPL-PEER-RECOVERY-PROBE-LOOP-001` | Primary runs a per-volume background loop that iterates only over peers in `ReplicaDegraded`, at a bounded interval, with per-peer cooldown and in-flight guard. The loop dispatches probe outcomes into existing engine recovery primitives without inventing new recovery substrate. | `core/replication/probe_loop_test.go` lifecycle + cooldown + only-degraded + dispatch tests |
| `INV-REPL-PEER-RECOVERY-NO-RETRIGGER-LOOP` | The probe loop does not re-fire on a peer with an active catch-up or rebuild session. Hand-off from catch-up to rebuild is one-way (loop ignores `NeedsRebuild` peers because the existing rebuild path owns them). | `peer_test.go` dispatch-branch tests + component test |
| `INV-G5-5C-NO-MASTER-PROTOCOL-CHANGE` | G5-5C closes without modifying master assignment publication, `PeerSetGeneration` semantics, `ObservationStore`, `UpdateReplicaSet` stale-replay rule, or any control-plane protocol surface. Runtime peer recovery is a primary-only concern. | §2 #7 diff-inspection at PR review |
| `INV-G5-5C-TWO-LOOPS-ORDERING-INDEPENDENT` | The control-plane identity/health loop and the data-plane governance loop run in parallel without ordering dependency. All five orderings in §1.D (primary-first, master-first, replica-recovers-first, primary-probe-first, simultaneous) terminate in correct convergence. Idempotency held by `ProbeIfDegraded` early-returns + in-flight guard + existing `UpdateReplicaSet` stale-replay. | `peer_test.go` simultaneous-fire test (concurrent fact-replay + probe dispatch on same peer); `probe_loop_test.go` cooldown / in-flight guard tests cover the simpler orderings. |

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
| **Peer recovery trigger after replica restart** (architect-bound 2026-04-27) | **Primary scope of this batch** — §1 + §1.A + §2 #1-#5 directly address via Option B (primary-side probe loop). |
| Reuse engine-driven primitives (T4d-4) | Binding adopted in §1 + §3; no new engine primitives introduced; probe loop only dispatches to existing primitives. |
| Define trigger source first | Bound at v0.4 to **Option B (primary-side probe loop)** per architect's control-plane / data-plane layering ruling 2026-04-27 — runtime peer recovery is a primary concern, not a master concern. |
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
| Probe loop polls when there's nothing to do (no degraded peers) → wasted goroutine wakeups | Bounded by 5 s interval; one cheap state check per peer; per-volume cost flat regardless of cluster size |
| Probe loop goroutine lifecycle bugs (CP4B-2 lessons: stop-before-run deadlock, zero-interval panic, callback panic propagation) | Explicit lifecycle tests as §2 #2 acceptance criterion; cover all three CP4B-2 patterns |
| Per-peer cooldown too short → retrigger storm; too long → slow convergence | Default 5 s = one loop interval; test §2 #3 covers cooldown enforcement; configurable via flag for ops tuning |
| In-flight guard race: peer transitions out of `ReplicaDegraded` between loop's state-read and dispatch | Loop re-checks state under peer's lock at dispatch site (TOCTOU-safe); test §2 #3 covers concurrent state change |
| Probe sees peer hasn't finished durable reopen → false `ProbeCatchUpRequired` | Replica `/status` exposes Healthy; primary's probe path uses the same lineage check that G5-5 #2 proved correct. No new mitigation needed. |
| Gap exceeds retention while loop is mid-iteration (race between ship retention pressure and probe arming) | Probe outcome `ProbeRebuildRequired` handles this; loop hands off to existing rebuild path; §2 #4 covers hand-off |
| Hardware re-run on m01 reveals secondary issues (residual iptables, leftover sessions, etc.) | `iterate-m01-replicated-write.sh start_cluster` already has pre-flight cleanup from G5-5 round 14; no new infra work |
| Component tests pass but hardware doesn't converge | Same closure pattern as G5-5: component tests are necessary but not sufficient; hardware GREEN is the §close gate |
| Future scope creep: someone wants to add observation re-emission "while we're at it" | Explicit `INV-G5-5C-NO-MASTER-PROTOCOL-CHANGE` (§3) + §2 #7 diff inspection make this hard to merge silently. Future master-side observability of RF health (desired RF=N, current effective RF=M) is a different batch with a different rationale (operator-facing observability, not recovery scheduling). |

---

## §7 Sign table

| Item | Owner | When | State |
|---|---|---|---|
| §1.A v0.1 trigger options enumerated (A/B/C) | sw | v0.1 | ✅ done 2026-04-27 |
| §1.A v0.2 binding: Option A with A1+A2 | architect | v0.1 → v0.2 REVISE | ✅ then **retired** in v0.4 |
| §1 V3 path correction + §1.B PeerSetGeneration design + §1.C truth-domain wording | architect | v0.2 → v0.3 REVISE | ✅ absorbed in v0.3, **then PeerSetGeneration scope retired in v0.4** (V3 path correction kept) |
| §1.A v0.4 re-binding: **Option B** (primary-side probe loop) per layering correction; master protocol unchanged | architect | v0.3 → v0.4 REVISE | ✅ ruled 2026-04-27 |
| §1-§6 architect single-sign of **v0.4** | architect | Before code start | ⏳ pending |
| Code (primary-side probe loop + peer probe entry + lifecycle tests + component test) | sw | After §1-§6 single-sign | ⏳ blocked on single-sign |
| m01 hardware re-run of `verify_restart_catchup` (+ #1/#2/#3 regression check) | QA | After sw lands trigger + component tests | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*Appended at batch close per `v3-batch-process.md §2`.*
