# V3 Phase 15 — G5-5C (Peer Recovery Trigger After Replica Restart) Mini-Plan

**Date**: 2026-04-27 (v0.1 — kickoff draft for architect ratification)
**Status**: §1-§6 awaiting architect ratification per `v3-batch-process.md §5`
**Owner**: sw (engine + replication-layer wiring); QA (m01 hardware re-run + scenario authoring)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR, one §close)
**Predecessors**: G5-5 closed at `seaweedfs@c78116fd2` (L3 Replicated IO on hardware; #4 carried to this batch); architect bindings 2026-04-27 (round 14 close ruling)

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

### §1.A Trigger source options (sw proposal — architect picks one)

The architect bound the constraint: "Define trigger source first: observation reappearance, periodic probe loop, or stream/transport reconnect signal." Three options, with tradeoffs:

| Option | Source of trigger | Pros | Cons |
|---|---|---|---|
| **A. Master observation reappearance** | Primary's host loop subscribes to assignment-fact updates from master. When a replica that was previously absent (or stale) reappears with a fresh observation, the host emits a "peer-reappeared" event into the recovery manager, which probes that peer. | Authoritative source (master is the single source of peer-set truth in V3); aligns with G5-5A peer-set publication path; no new transport paths. | Couples recovery cadence to master heartbeat cadence (~seconds); requires master observation freshness gate to avoid flapping. |
| **B. Periodic probe loop on Degraded peers** | Primary's recovery manager runs a slow background loop (e.g., 5s) over peers in `ReplicaDegraded` and calls `ProbeReplica` on each. On `ProbeCatchUpRequired`, engine plans catch-up; on `ProbeRebuildRequired`, hand to rebuild path. | Self-contained on primary; doesn't depend on master timing; existing `StartRebuildFromProbe` shows the pattern. | Adds a new background goroutine per primary; needs careful start/stop lifecycle (existing concern from BUG-CP4B2-1 deadlock pattern); polling cost grows with cluster size. |
| **C. Transport reconnect signal** | When the replica's `ReplicaListener` accepts a new control-channel connection from the primary's shipper (post-restart), the listener emits a "peer-up" event back through the assignment publisher; primary's shipper reconnect path (`ReplicaConnecting → ReplicaInSync`) is the trigger. | Lowest latency (event-driven, no polling); reuses existing transport path. | Requires the shipper to actually attempt a reconnect — but `Ship()` short-circuits in `ReplicaDegraded` (the very symptom). Would need an out-of-band "try-reconnect" path that doesn't ride on `Ship()` (i.e., timer or master event). Folds back into A or B in practice. |

**sw recommendation**: **Option A (master observation reappearance)**, because (a) V3 already routes peer-set authority through master observation (G5-5A), so this is the discipline-coherent path; (b) C reduces to A or B once you account for `ReplicaDegraded` not driving reconnects; (c) B works but introduces a polling goroutine when the same information already flows through the master subscription. Architect picks at §1-§6 ratification — sw will not start coding until the binding is explicit.

### What G5-5C does NOT deliver (explicit non-claims)

- **No rebuild-path-only batch.** This batch handles the catch-up case (gap within retention). Rebuild path (`StartRebuildFromProbe`) is already wired (T4d-4 part B/C); G5-5C #4 only verifies the trigger correctly *hands off* to rebuild, not the rebuild path itself.
- **No new engine primitives.** Architect binding: reuse T4d-4. The gap is at the runtime-trigger layer (which fires when), not the recovery substrate (which already works once fired).
- **No primary failover.** Primary stays primary throughout. G8 scope.
- **No NVMe / multi-replica / placement scope.** Same exclusions as G5-5; this batch is a targeted recovery-trigger fix on the 2-node iSCSI configuration.
- **No script harness rework.** `iterate-m01-replicated-write.sh verify_restart_catchup` already has the right shape; G5-5C must turn it GREEN, not rewrite it.

### Files (preliminary — exact set bound at code-start)

| File | Likely change | LOC est |
|---|---|---|
| `weed/server/block_recovery.go` | New "peer-up" entry point (e.g., `OnPeerObservedAfterDegraded(replicaID)`) — probes peer, dispatches to engine catch-up or rebuild | ~60 |
| `weed/server/volume_server_block.go` (or master-subscription handler) | Wire master observation update → emit peer-up event when a Degraded peer reappears with a fresh slot fact | ~40 (Option A); ~60 incl. timer (Option B) |
| `weed/storage/blockvol/wal_shipper.go` | Possibly: explicit `RearmFromDegraded()` method for the recovery manager to call (avoid touching shipper internals from server layer) | ~20 |
| `weed/server/block_recovery_test.go` | #3 + #4 component tests | ~120 |
| `sw-block/design/v3-phase-15-g5-5c-mini-plan.md` (this doc) | §close appended at batch close | + §close |

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
| 1 | Trigger source defined and ratified by architect (Option A / B / C from §1.A) before code start. | Architect mark on §7 sign table |
| 2 | After replica restart against same `--durable-root`, primary's recovery manager observes the peer-reappeared event within the bound (Option A: 1× master heartbeat; Option B: ≤ probe interval; Option C: 1× transport reconnect attempt). | Component test #3 (file: TBD at impl) |
| 3 | On peer-reappeared, recovery manager calls `ProbeReplica` and dispatches to engine-driven catch-up (T4d-4 primitives) when outcome is `ProbeCatchUpRequired`. | Component test #3 |
| 4 | When the gap exceeds retention and probe outcome is `ProbeRebuildRequired`, recovery manager hands off to `StartRebuildFromProbe` (already wired) without entering a re-trigger loop. | Component test #4 |
| 5 | `iterate-m01-replicated-write.sh verify_restart_catchup` step turns GREEN on m01/M02 hardware: `LBA[2]=0xef` byte-equal under `m01verify` within deadline (suggest 30s, same as #3 network-catchup; final value bound at code-start). | Hardware re-run; artifacts archived under `g5-test/logs/artifacts-<timestamp>/` |
| 6 | No regression on G5-5 #1/#2/#3 — all three remain GREEN in the same hardware run. | Same script, same run |

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
| Seed evidence: `seaweed_block@5c4718f` primary-fail.log | Referenced in §1 to ground the symptom; QA may re-collect a clean `gate-degraded + stale-barrier-ack` log to confirm the pre-fix baseline before the trigger lands. |
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
| §1-§6 architect ratification (including §1.A trigger source pick) | architect | Before code start | ⏳ pending |
| Code (recovery manager + master subscription wiring + tests) | sw | After §1-§6 ratification with bound trigger source | ⏳ blocked on ratification |
| m01 hardware re-run of `verify_restart_catchup` (+ #1/#2/#3 regression check) | QA | After sw lands trigger + component tests | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*Appended at batch close per `v3-batch-process.md §2`.*
