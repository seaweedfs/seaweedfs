# V3 Phase 15 — G5-4 (Binary T4 Replication Wiring) Mini-Plan

**Date**: 2026-04-26 (v0.5 — close-lock corrections per architect round 51)
**Status**: ✅ **CLOSED** — code at `seaweed_block@c820e17`; ledger 5 INV-BIN-WIRING-* rows ACTIVE at `seaweedfs@36ba7b44e`; architect close-sign 2026-04-26 (round 51). Acceptance criteria 1/2/5/6/7 satisfied; 3/4 explicitly relocated to G5-5 hardware first-light (need real iSCSI/NVMe frontend driver). `--ctrl-addr` reuse REJECTED during code; correct binding is `--data-addr` (executor dials `peer.DataAddr`).
**Owner**: sw (implementation); QA (acceptance)
**Authority sources**:
- `v3-phase-15-g5-kickoff.md` v0.3 (architect-ratified 2026-04-26 by pingqiu) — G5-4 row in §3 batch table; §7 governance loop
- `v3-phase-15-g5-m0102-bringup-handoff.md` (QA cross-node smoke surfacing the gap)
- `v3-phase-15-g5-m0102-bringup-answer.md` (sw root-cause diagnosis: binary lacks T4 wiring)
- T4d-4 part B `WithEngineDrivenRecovery()` framework binding at `core/replication/component/cluster.go:357-369` — the V3-native PORT reference
**Discipline**: same as T4 (kickoff → architect ratify → mini-plan → architect ratify → G-1 → code); §8C.2 single-sign at batch close
**Timeline**: ~1 week (single batch, 200-300 prod LOC + ~150 test LOC; G-1 V3-native PORT read serializes code start)
**Predecessors**: T4d closed (`e642ae8`); blockmaster `--expected-slots-per-volume` flag landed (`f5de7c5`); G5 kickoff v0.3 ratified

---

## §0 Why this is one batch, not two

G5-4 is a single semantic unit: **the production blockvolume binary instantiates the T4 replication stack per volume and binds it to the lifecycle host**. Splitting "primary-side" vs "replica-side" wiring into separate batches would create an interim state where either:
- The binary supports only primary (no replica) — same as today, no progress observable
- The binary supports only replica listener (no engine-driven outbound recovery) — incoherent

Both halves only become testable together. Single batch matches the "semantic whole" criterion architect used for T4 batches.

---

## §1 Scope

### §1.1 Semantic whole

After G5-4: `cmd/blockvolume` constructs a per-volume `engine.ReplicaState` + `adapter.VolumeReplicaAdapter` + `transport.BlockExecutor` per remote peer + `transport.ReplicaListener` for incoming traffic, all driven by assignment facts arriving via the master subscription. A 2-node cluster (m01-primary + m02-replica) reaches Healthy on both sides within ~3s of both binaries connecting to the master, and live writes ship from primary to replica through the production code path — not the component fixture path.

### §1.2 What's NOT in G5-4 (deferred per kickoff §2.2 + this mini-plan)

| Item | Defers to | Rationale |
|---|---|---|
| Multi-volume per-binary | post-G5 | Binary today is `--volume-id` single-volume; multi-volume needs separate config/lifecycle work |
| `--repl-data-addr` / `--repl-ctrl-addr` separate ports | **CORRECTED at close (v0.5)**: this batch binds `ReplicaListener` on `--data-addr` (NOT `--ctrl-addr`). Executor dials `peer.DataAddr` (`core/transport/executor.go:303`); listener must bind the address master mints into `AssignmentFact.peers[*].DataAddr`. `--ctrl-addr` stays reserved for future control-plane split (no current binder; verified no NVMe/iSCSI conflict). | Bandwidth/QoS-driven port split is a tuning question, not correctness |
| Per-volume RF derived from topology | post-G5 (already named in `--expected-slots-per-volume` followup) | G5-4 keeps the global flag; per-volume RF is independent refactor |
| ALUA / multipath at binary level | post-G5 | V2 had it; V3 has not picked up the equivalent yet |
| Snapshot / CoW at binary level | post-G5 | Phase 5 V2 work; V3 hasn't ported |

### §1.3 Role inference (kickoff §3 G5-4 design decision (a))

**Decision: assignment-driven, no new CLI flag, master-authoritative.**

The volume daemon does not declare its role. Role is per-volume per-assignment-fact, read directly from a master-minted field. Per `core/rpc/proto/control.proto:128-148` (verified against `core/host/master/services.go:198-205` mint site):

> `AssignmentFact { volume_id, replica_id, epoch, endpoint_version, data_addr, ctrl_addr, peers, peer_set_generation }`

`fact.replica_id` is the **bound primary** for this volume's current line — minted by master. SubscribeAssignments is volume-scoped (`services.go:65`); ALL replicas subscribed to the volume see the SAME stream of facts. Each subscriber self-determines its role:

- `fact.ReplicaID == self.ReplicaID` → **I am the primary for this line**; consume `fact.Peers` as my outbound replication targets
- `fact.ReplicaID != self.ReplicaID` → **I am a supporting replica**; the primary is at `fact.ReplicaID`. Stay listening; ignore `fact.Peers` (peers list is intended for the primary's use, but volume-scoped fan-out delivers it to all subscribers).

This matches V3's "master is sole authority for assignment truth" rule (T4d §B + INV-AUTH-*). No binary-side tie-breaker, no `lex-smallest` inference — master's `fact.ReplicaID` IS the truth.

(QA Note 1 round 1: the v0.1 "lex-smallest of the active set" phrasing was wrong; corrected here. There is no lex-smallest fallback because master always names exactly one bound replica per volume per line.)

Both code paths (outbound peer manager + inbound listener) are constructed at startup. Whichever role the assignment selects is the active path for that volume; the other stays idle. Role can flip across assignment lines (failover bumps epoch + binds a new replica) without restart.

### §1.4 Peer discovery (design decision (b))

`AssignmentFact.Peers` already populated by master (per T4a-5 P-refined wiring at `core/host/master/services.go:206-215`). G5-4 adds a per-binary peer-set updater: on each fresh `AssignmentFact`, diff `Peers` against the current set and:
- Add new peers → construct fresh `BlockExecutor(primaryStore, peerAddr)` + register with engine via `adapter`
- Remove dropped peers → stop their executors (LIFO with active session drain)
- Address change for existing peer → tear down old executor, construct new

Peer set updates flow through one updater goroutine to serialize against the lifecycle host's `ProcessBlockVolumeAssignments` analog.

### §1.5 Listener lifecycle (design decision (c))

`ReplicaListener` binds on **`--data-addr`** (corrected at close v0.5; v0.2 said `--ctrl-addr` — wrong). The executor's Probe + Ship paths dial `peer.DataAddr` (`core/transport/executor.go:303`); the listener MUST bind the address master mints into `AssignmentFact.peers[*].DataAddr`. `--ctrl-addr` stays reserved for future control-plane split — verified no current binder + no conflict with NVMe (`--nvme-listen`) / iSCSI (`--iscsi-listen`) / status (`--status-addr`) which all have dedicated flags. Bandwidth/QoS port split is a post-G5 tuning question.

Listener Start at binary startup; Stop on `host.Close()` via the existing host cleanup chain. New binding into `volume.Host` cleanup so listener teardown happens BEFORE engine teardown (LIFO).

### §1.6 Engine instantiation (design decision (d))

One engine per volume, one volume per binary process today. Per-volume `engine.ReplicaState` + `adapter.VolumeReplicaAdapter` constructed at binary startup with `--volume-id` + `--replica-id`. Engine state seeded as zero-value (no MemberPresent until first assignment lands). The adapter is the existing `T4d-4 part B` adapter — no new construction, just a binary-level call site instead of the component framework one.

### §1.7 Location

| File | Change |
|---|---|
| `cmd/blockvolume/main.go` | NEW: construct adapter + listener + peer manager; pass `volume.Config{ReplicationVolume: ...}` |
| `cmd/blockvolume/peer_manager.go` (new, ~120 LOC) | Peer-set diff/add/remove logic; serialized updater goroutine |
| `cmd/blockvolume/replication_wire.go` (new, ~80 LOC) | Construction helpers (newAdapter, newListener, newPrimaryStore wiring) |
| `core/host/volume/host.go` | Wire `ReplicationVolume` field into Start/Stop lifecycle (today the field exists but isn't consumed; `Healthy` projection check needs it) |
| `cmd/blockvolume/main_integration_test.go` (new, ~150 LOC) | Binary-level integration test: spin up 2 in-process binaries (or subprocess-driven via `--t1-readiness` analog), drive an assignment, verify both reach Healthy + a write ships |

Estimate total: ~250 prod + ~150 tests.

---

## §2 Tasks

Single batch, ordered subtasks (each landable on its own commit but reviewed together at batch close):

### G5-4.1 — Replication-wire construction helpers (~80 LOC)

`cmd/blockvolume/replication_wire.go`: helper functions for constructing the `BlockExecutor` per peer, the `ReplicaListener`, and the `VolumeReplicaAdapter`. Pure construction; no lifecycle. Mirrors `cluster.go:357-369` line-for-line but with binary-relevant parameter sources (CLI flags, durable provider, host accessors).

**Acceptance**: helpers compile + have a unit test asserting they produce non-nil with valid inputs and return errors with invalid inputs (nil store, empty addr).

### G5-4.2 — Peer manager (~120 LOC)

`cmd/blockvolume/peer_manager.go`: receives `AssignmentFact` updates, diffs the peer set, calls G5-4.1 helpers to add/remove `BlockExecutor` instances, registers/unregisters with the adapter. Single goroutine; channel-driven; LIFO teardown of removed peers.

**Acceptance**: unit test simulates a sequence of assignment facts (initial, peer added, peer removed, peer addr changed) and asserts the resulting executor set + adapter peer registry matches expected at each step.

### G5-4.3 — Host lifecycle binding (~30 LOC + host-side glue)

`core/host/volume/host.go`: consume `volume.Config.ReplicationVolume` in `Start()` (start listener + peer manager); in `Close()` (stop them in LIFO order). The field exists today but is unused.

`AdapterProjectionView` already reads from the adapter projection; G5-4.3 ensures the adapter is the one constructed by the peer manager (not a stub).

**Acceptance**: existing host tests still green; new test that constructs a host with a real adapter, calls Start, asserts listener is bound + peer manager goroutine is running; Close tears down cleanly with no leaked goroutines.

### G5-4.4 — Binary main.go wiring (~30 LOC)

`cmd/blockvolume/main.go`: construct adapter + listener + peer manager; pass into `volume.Config{ReplicationVolume: ...}`. Wire the assignment subscription channel from `volume.Host` to the peer manager's input.

**Acceptance**: binary builds; existing `cmd/blockvolume` tests still green.

### G5-4.5 — Binary-level integration test (~150 LOC)

`cmd/blockvolume/main_integration_test.go`: spin up an in-process master + 2 in-process blockvolume processes (using `cmd/blockmaster` / `cmd/blockvolume` `run()` functions directly, not via os.Exec, to keep the test fast and debuggable). Drive an assignment via topology + heartbeat ingest. Assert:
- Both volumes reach Healthy projection within 5s
- A primary-side write lands on the replica's store
- Stop primary, restart, write again, assert convergence

**Acceptance**: test passes deterministically (no t.Skip flake markers); 10× stress under `-race` on m01.

---

## §3 Predicates (must be true before G5-4.1 starts)

1. ✅ T4d closed (`e642ae8` part C lands; closure report committed)
2. ✅ G5 kickoff v0.3 ratified (architect round-49)
3. ✅ `--expected-slots-per-volume` flag landed (`f5de7c5`); cross-node smoke can reach the assignment-mint stage
4. ✅ This mini-plan ratified by QA (round 1) + architect (round 50, with 2 binding clarifications baked into v0.3 §4 #2 + #7 + §7.1)
5. ~~G-1 V3-native PORT read~~ — **DROPPED at v0.4** per round 51 process call (V3-native batch, no V2 PORT source; mini-plan + PR description sufficient). Architect's 2 binding questions answered inline in PR description per §7.1 v0.4.

Predicates 1-4 satisfied. Predicate 5 dropped. **Sw cleared to start G5-4.1 code immediately.**

---

## §4 Acceptance (per kickoff §3 G5-4 row + §4)

| # | Criterion | Verifier |
|---|---|---|
| 1 | `cmd/blockvolume` binary constructs `ReplicationVolume` and passes it via `volume.Config` | ✅ DONE — verified at `c820e17` `cmd/blockvolume/main.go` |
| 2 | In-process 2-volume cluster reaches role-appropriate ready state on both sides within 5s of bring-up via the production binary code path (NOT component framework fixture). **Architect binding clarification #1 (round 50)**: "Healthy" must split by role. **Primary**: `Healthy=true` per existing frontend/write-ready projection. **Replica**: listener-bound + role-aware Healthy=false. **Resolution at close (v0.5)**: existing `frontend.Projection.Healthy` reused with role-aware semantics — Primary reaches `Healthy=true` via engine.ModeHealthy; Replica stays `Healthy=false` because OnAssignment is skipped on supersede records (engine ModeIdle). Test pins both directions explicitly. | ✅ DONE — `TestG54_BinaryWiring_RoleSplit_2NodeSmoke` PASS (Windows 16.6s; m01 13.2s) |
| 3 | Primary-side live write lands byte-equal on replica's store | ⏸ **RELOCATED to G5-5** at close (v0.5) per architect round 51: in-process subprocess test cannot drive a real iSCSI/NVMe write without a kernel client; G5-5 m01 hardware first-light has the kernel iSCSI/NVMe tooling. G5-4 certifies binary wiring, not byte-movement. |
| 4 | Stop replica mid-flight + restart + replica catches up via T4 engine-driven recovery (NOT framework fixture) | ⏸ **RELOCATED to G5-5** at close (v0.5) per architect round 51: depends on #3. |
| 5 | All existing tests green; no regressions in `core/`, `cmd/`, `core/replication/component` | ✅ DONE — 23 packages green Windows |
| 6 | 10× stress under `-race` on m01 for the new integration test | ✅ DONE at close (v0.5) — `CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring_RoleSplit_2NodeSmoke ./cmd/blockvolume/` PASS in 13.2s on m01 |
| 7 | New invariants pinned in catalogue (see §6) **AND landed in `v3-invariant-ledger.md`** with at least one test pointer or batch-close evidence per row. **Architect binding clarification #2 (round 50)**: catalogue inscription alone is insufficient at G5-4 close; ledger discipline applies (per `v3-quality-system.md` §6 rule "an invariant without a test is a wish"). | ✅ DONE — 5 INV-BIN-WIRING-* rows ACTIVE in `v3-invariant-ledger.md` at `seaweedfs@36ba7b44e` with concrete test pointers |

---

## §5 Non-claims (explicit deferrals within G5-4)

- **G5-DECISION-001 — G5-4 ships Path B runtime, keeps Path A serializable seam open.** G5-4's runtime engine state is in-memory; on primary restart, the binary reconstructs state from master assignment + probe (Path B). T4d-4 part B's `TestG5Decision001_ReplicaState_RoundTripJSON` already pins that the engine state struct is JSON-serializable. G5-4 preserves that property — it does NOT add a persistence layer, but it does NOT introduce any non-serializable runtime state either. G5-6 architect ratification can promote to Path A by adding a persistence layer on top of the existing struct, with no engine-state-shape change required. This is an architect-promotable seam, not a position on which path wins. (QA Note 3 round 1 contradiction resolved: G5-4 ships Path B *runtime* + keeps Path A *upgrade path* open via serializability.)
- **No multi-volume-per-binary** — single `--volume-id`; per-binary multi-volume is post-G5
- **No ALUA / snapshots / multipath** — Phase 5 V2 surface; V3 hasn't picked up
- **No new metrics** — G5-3 owns metrics/backpressure assessment
- **No graceful peer-set-shrink during in-flight session** — initial peer-set updater serializes adds/removes but doesn't drain mid-session for an aggressive removal; carry to post-G5 if real workloads hit it

---

## §6 Invariants to preserve + new ones to inscribe

**Forward-carry baseline (must NOT regress):**
- All T4d-2 apply-gate invariants (lane purity, per-LBA stale-skip, no per-LBA regression)
- All T4d-3 R+1 threading invariants
- All T4d-4 part A/B/C invariants (RecoveryMode lifecycle, RebuildPinned stickiness, catch-up exhaustion → Rebuild)
- INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY (G5-4 stays in this discipline; binary doesn't reach into transport internals)

**New invariants to inscribe at G5-4 close:**
- INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT — binary doesn't declare role via CLI; role is per-volume per-assignment from master via `fact.ReplicaID == self.ReplicaID`
- INV-BIN-WIRING-PEER-SET-FROM-ASSIGNMENT-FACT — peer addresses come from `AssignmentFact.Peers`, not CLI; binary never accumulates peers from local observation (option R rejected per T4a-5.0)
- INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO — `ReplicaListener` Stop runs BEFORE engine Stop in `host.Close()` (LIFO with construction)
- INV-BIN-WIRING-ASSIGNMENT-DRIVES-MEMBERPRESENT — the binary doesn't fake MemberPresent; it waits for first `AssignmentFact` to set it via the engine apply path
- INV-BIN-WIRING-SESSIONID-VIA-ADAPTER — the binary path mints sessionIDs ONLY through `core/adapter` (which uses the process-wide `sessionIDCounter atomic.Uint64` at `adapter.go:70`); binary MUST NOT bypass the adapter with hardcoded sessionIDs the way component-framework shortcuts (`WithLiveShip`, `CatchUpReplica`) do. Adapter-routed dispatch is the production path; framework shortcuts are test conveniences with a known sessionID-collision gap (T4c §I carry; QA G5-1 round 1 SKIP). Pinning this invariant ensures the gap stays test-side and never propagates into the binary. (QA round 1 clarification ask answered: adapter mints unique sessionIDs across all volumes/replicas in the process; binary inherits this for free as long as it always dispatches via the adapter, never via framework shortcuts.)

---

## §7 Review gates

### §7.1 Pre-merge gates (mandatory; PR blocked until satisfied)

1. ~~**G-1 V3-native PORT read deliverable**~~ — **DROPPED at v0.4 (round 51 process call).** G5-4 is V3-native binary integration (not V2 muscle PORT); G-1 ceremony doesn't earn its keep this batch. Mini-plan v0.3 already covers scope + 7 acceptance criteria + 5 inscribed invariants + file map. Sw codes directly per the mini-plan; the 2 architect-binding questions (round 50) are answered **inline in the PR description** rather than as a separate ratified deliverable:
   - **Replica readiness semantics** — sw cites in PR description: which existing `volume.Status` / `ProjectionView` field is used for "replication-ready" (vs frontend-primary-Healthy). If none exist, sw introduces a new field with explicit name + godoc; if existing field is reused with role-aware semantics, sw cites the field + how the role-split is enforced
   - **`--ctrl-addr` reuse confirmation** — sw cites in PR description: verification result that ReplicaListener bind on `--ctrl-addr` does NOT conflict with NVMe/iSCSI control-plane traffic. If conflict found, sw introduces `--repl-addr` flag (small scope expansion, contained in this batch)
   - **Process lesson**: G-1 ceremony earned its keep on T4 V2-PORT batches (caught 3 real architectural pins pre-code); for V3-native batches with already-ratified mini-plan, normal PR review + G-2 godoc + G-3 cumulative-green is sufficient. Future V2-PORT batches still get G-1 ceremony.
   - V2-LESSON items: items learned from V2 that don't map 1:1 but inform implementation choices
2. **No `core/replication/component` import from `cmd/blockvolume`** — the binary mirrors the component framework's wiring but doesn't import it (component framework is test-only)
3. **Binary integration test (G5-4.5) passes deterministically + 10× under `-race` on m01**
4. **Full V3 suite green** including all T4 surfaces
5. **Architect single-sign at batch close per §8C.2**

### §7.2 Risks + procedural mitigations

| Risk | Mitigation |
|---|---|
| Binary integration test flakes (in-process master + volumes have bring-up timing edge cases) | Use deterministic scheduling primitives; explicit assignment.WaitForHealthy(ctx) helper rather than time.Sleep |
| Peer-set updater races against adapter's existing session callbacks | G5-4.2 unit test must include concurrent assignment-fact-arrival + session-callback scenarios |
| Listener lifecycle order wrong → leaked goroutines | t.Cleanup chain explicit; tests assert no leak via goleak.VerifyNone or similar |
| `--ctrl-addr` reuse for replica listener conflicts with NVMe/iSCSI control plane on the same port | G-1 deliverable confirms the reuse is safe; if not, G5-4 introduces `--repl-addr` flag (small scope expansion) |

---

## §8 Sign table (mid-batch, §8C.2)

| Stage | Signer | When | Status |
|---|---|---|---|
| Mini-plan ratification | architect (pingqiu) + QA | This submission | ✅ DONE 2026-04-26 (QA round 1 + architect round 50 with 2 bindings) |
| ~~G-1 PORT read deliverable~~ | ~~architect~~ | ~~After §7.1 #1 deliverable~~ | ~~⏳ pending~~ → **DROPPED v0.4** (round 51 process call) |
| Code start | sw | **Cleared at v0.4** — sw codes per mini-plan; addresses 2 architect bindings inline in PR description | ▶️ unblocked |
| G5-4 close (single sign per §8C.2) | architect (pingqiu) | At final commit + integration test pass + 5 INV-BIN-WIRING-* invariants landed in `v3-invariant-ledger.md` + PR description cites resolution of 2 architect bindings | ⏳ pending |

---

## §9 Change log

| Date | Version | Change |
|---|---|---|
| 2026-04-26 | v0.1 | Initial draft. Submitted for QA + architect ratification. |
| 2026-04-26 | v0.2 | QA round 1 review responses: §1.3 role inference rewritten to read `fact.ReplicaID == self.ReplicaID` from master-minted field (proto verified at `control.proto:128-148` + master mint at `services.go:198-205`); no lex-smallest fallback. §4 #2 verifier reframed to G5-4.5 in-process test (m01 hardware = G5-5). §5 G5-DECISION-001 contradiction resolved: G5-4 ships Path B runtime + keeps Path A serializability seam open, architect-promotable at G5-6 with no engine-state-shape change. §6 added INV-BIN-WIRING-SESSIONID-VIA-ADAPTER (clarification ask: adapter mints unique sessionIDs via process-wide counter at `adapter.go:70`; binary inherits for free; framework shortcuts that hardcode sessionID=1 are the known gap, must not propagate to binary). |
| 2026-04-26 | v0.3 | **Architect round 50 RATIFY with 2 binding clarifications.** Architect verbatim: "Role inference, in-process acceptance, G5-DECISION-001 seam, and sessionID discipline are architecturally correct. G-1 must clarify replica readiness semantics and confirm ctrl-addr reuse or introduce repl-addr before code." Bindings baked: (#1) §4 #2 acceptance criterion split by role — Primary `Healthy=true` per existing frontend/write-ready projection; Replica MUST NOT report `Healthy=true` if existing field implies frontend-primary-write-ready; G5-4.5 uses precise assertion names (`assertReplicaReplicationReady` vs `assertPrimaryFrontendReady`) if existing status field is too coarse. (#2) §4 #7 acceptance criterion strengthened: catalogue inscription alone insufficient at close; 5 INV-BIN-WIRING-* invariants MUST land in `v3-invariant-ledger.md` per `v3-quality-system.md` §6 rule "an invariant without a test is a wish"; ledger updated as PR atomic with code. §7.1 G-1 deliverable extended: G-1 MUST resolve replica-readiness-field question + ctrl-addr-reuse-vs-repl-addr question BEFORE code starts. Architect-pre-baked: ratification stays valid; no further mini-plan revisions needed before G-1. | architect |
| 2026-04-26 | v0.4 | **Round 51 process call: G-1 ceremony DROPPED for G5-4.** User question surfaced the overhead-vs-value of G-1 for V3-native batches. Honest assessment: G-1 ceremony earned its keep on T4 V2-PORT batches (caught 3 real architectural pins pre-code: T4b-4 5 hidden invariants; T4c-2 probe non-mutation; T4d-3 3 placement decisions). For G5-4 (V3-native binary integration, not V2 PORT), G-1 doesn't earn its keep — mini-plan v0.3 already has scope + 7 acceptance criteria + 5 inscribed invariants + file map. Architect's 2 binding questions (round 50) become PR-description deliverables instead of separate ratified G-1 doc. Sw cleared to code immediately. **Process lesson**: don't auto-port T4 governance template to every batch; ask "does this step earn its keep" each time. Future V2-PORT batches still get G-1 ceremony. | QA + sw + user process call |
| 2026-04-26 | v0.5 | **Architect round 51 close-sign + 4 doc-lock corrections applied.** Architect verbatim: "APPROVED — G5-4 close. Binary T4 replication wiring is complete at commit `seaweed_block@c820e17`; criteria 1/2/5/6/7 satisfied; criteria 3/4 explicitly relocated to G5-5 hardware first-light; --data-addr correction accepted; 5 INV-BIN-WIRING-* rows ACTIVE. Close claim is wiring-ready, not byte-movement-ready." Doc-lock fixes: (1) header status v0.2 → v0.5 CLOSED; (2) §1.2 + §1.5 `--ctrl-addr` → `--data-addr` correction inscribed (executor dials `peer.DataAddr` per `core/transport/executor.go:303`; `--ctrl-addr` reserved for future control-plane split, no current binder, no NVMe/iSCSI/status conflict); (3) §4 #3 + #4 marked RELOCATED to G5-5 with rationale (need real iSCSI/NVMe kernel client); (4) §4 #6 marked DONE (m01 -race ×10 PASS in 13.2s, was pending in v0.4); §4 #1/#2/#5/#7 marked DONE with evidence pointers. **Status**: CLOSED. | architect |
