# V3 Phase 15 — G5-4 (Binary T4 Replication Wiring) Mini-Plan

**Date**: 2026-04-26
**Status**: DRAFT v0.1 — submitted for QA + architect sign per parent kickoff §7 ("G5-4 mini-plan ratified before code")
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
| `--repl-data-addr` / `--repl-ctrl-addr` separate ports | This batch reuses `--ctrl-addr` for ReplicaListener bind | Keep flag surface minimal; bandwidth/QoS-driven port split is a tuning question, not correctness |
| Per-volume RF derived from topology | post-G5 (already named in `--expected-slots-per-volume` followup) | G5-4 keeps the global flag; per-volume RF is independent refactor |
| ALUA / multipath at binary level | post-G5 | V2 had it; V3 has not picked up the equivalent yet |
| Snapshot / CoW at binary level | post-G5 | Phase 5 V2 work; V3 hasn't ported |

### §1.3 Role inference (kickoff §3 G5-4 design decision (a))

**Decision: assignment-driven, no new CLI flag.**

The volume daemon does not declare its role. Role is per-volume per-assignment-fact. The binary listens for `AssignmentFact` from the master subscription; each fact carries `ReplicaID` + `Peers` (per T4a-5 P-refined). The engine's `applyAssignment` path consumes this and decides:
- If `assignment.ReplicaID == self.ReplicaID` AND `Peers` non-empty AND topology lex-smallest of the active set → **primary** (ships WAL to peers via outbound `BlockExecutor` instances)
- Otherwise → **replica** (accepts incoming traffic via local `ReplicaListener`)

Both code paths are constructed at startup. Whichever role the assignment selects is the active path; the other stays idle. This matches V3's "master is sole authority" architectural choice — the binary doesn't second-guess role assignment.

### §1.4 Peer discovery (design decision (b))

`AssignmentFact.Peers` already populated by master (per T4a-5 P-refined wiring at `core/host/master/services.go:206-215`). G5-4 adds a per-binary peer-set updater: on each fresh `AssignmentFact`, diff `Peers` against the current set and:
- Add new peers → construct fresh `BlockExecutor(primaryStore, peerAddr)` + register with engine via `adapter`
- Remove dropped peers → stop their executors (LIFO with active session drain)
- Address change for existing peer → tear down old executor, construct new

Peer set updates flow through one updater goroutine to serialize against the lifecycle host's `ProcessBlockVolumeAssignments` analog.

### §1.5 Listener lifecycle (design decision (c))

`ReplicaListener` binds on `--ctrl-addr` (reuse). Reusing `--ctrl-addr` rather than introducing a dedicated `--repl-addr` keeps the smoke-test command surface small and matches the architectural fact that V3 control + replica traffic both go through the same host. Bandwidth/QoS port split is a post-G5 tuning question.

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
4. ⏳ This mini-plan ratified by architect + QA
5. ⏳ G-1 V3-native PORT read of `cluster.go:357-369` + V2 comparison (see §7.1)

Predicates 1-3 are met. Predicates 4-5 are this submission + the next sw step.

---

## §4 Acceptance (per kickoff §3 G5-4 row + §4)

| # | Criterion | Verifier |
|---|---|---|
| 1 | `cmd/blockvolume` binary constructs `ReplicationVolume` and passes it via `volume.Config` | code review + sw confirms via `grep ReplicationVolume cmd/blockvolume/` |
| 2 | 2-node cluster (m01-primary + m02-replica) reaches Healthy on both sides within 5s of bring-up | QA m01 hardware run (G5-5 will exercise this; G5-4 unblocks it) |
| 3 | Primary-side live write lands byte-equal on replica's store | binary-level integration test G5-4.5 |
| 4 | Stop replica mid-flight + restart + replica catches up via T4 engine-driven recovery (NOT framework fixture) | binary-level integration test G5-4.5 |
| 5 | All existing tests green; no regressions in `core/`, `cmd/`, `core/replication/component` | full V3 suite |
| 6 | 10× stress under `-race` on m01 for the new integration test | QA m01 verification |
| 7 | New invariants pinned in catalogue (see §6) | sw + QA at batch close |

---

## §5 Non-claims (explicit deferrals within G5-4)

- **G5-DECISION-001 unaffected** — engine state persistence across primary restart stays open (G5-6 closes per architect ruling); G5-4 doesn't take a position
- **No multi-volume-per-binary** — single `--volume-id`; per-binary multi-volume is post-G5
- **No ALUA / snapshots / multipath** — Phase 5 V2 surface; V3 hasn't picked up
- **No new metrics** — G5-3 owns metrics/backpressure assessment
- **No assignment-store persistence at the binary** — engine state is in-memory; reconstructed from master on restart (G5-DECISION-001 Path B)
- **No graceful peer-set-shrink during in-flight session** — initial peer-set updater serializes adds/removes but doesn't drain mid-session for an aggressive removal; carry to post-G5 if real workloads hit it

---

## §6 Invariants to preserve + new ones to inscribe

**Forward-carry baseline (must NOT regress):**
- All T4d-2 apply-gate invariants (lane purity, per-LBA stale-skip, no per-LBA regression)
- All T4d-3 R+1 threading invariants
- All T4d-4 part A/B/C invariants (RecoveryMode lifecycle, RebuildPinned stickiness, catch-up exhaustion → Rebuild)
- INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY (G5-4 stays in this discipline; binary doesn't reach into transport internals)

**New invariants to inscribe at G5-4 close:**
- INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT — binary doesn't declare role via CLI; role is per-volume per-assignment from master
- INV-BIN-WIRING-PEER-SET-FROM-ASSIGNMENT-FACT — peer addresses come from `AssignmentFact.Peers`, not CLI
- INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO — `ReplicaListener` Stop runs BEFORE engine Stop in `host.Close()` (LIFO with construction)
- INV-BIN-WIRING-ASSIGNMENT-DRIVES-MEMBERPRESENT — the binary doesn't fake MemberPresent; it waits for first `AssignmentFact` to set it via the engine apply path

---

## §7 Review gates

### §7.1 Pre-merge gates (mandatory; PR blocked until satisfied)

1. **G-1 V3-native PORT read deliverable** — sw reads `core/replication/component/cluster.go:357-369` line-by-line and produces a §-by-§ port map for binary integration. Also reads `weed/storage/blockvol/blockvol.go` (V2) for any binary-level lessons (esp. lifecycle ordering and peer-set update race conditions). G-1 deliverable surfaces:
   - PORT items: items the binary mirrors verbatim from cluster.go
   - V3-NATIVE items: items the binary adds on top of cluster.go (CLI/lifecycle/error reporting/log channels)
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
| Mini-plan ratification | architect (pingqiu) + QA | This submission | ⏳ pending |
| G-1 PORT read deliverable | architect | After §7.1 #1 deliverable | ⏳ pending |
| Code start | sw | After mini-plan + G-1 ratify | ⏳ pending |
| G5-4 close (single sign per §8C.2) | architect (pingqiu) | At final commit + integration test pass | ⏳ pending |

---

## §9 Change log

| Date | Version | Change |
|---|---|---|
| 2026-04-26 | v0.1 | Initial draft. Submitted for QA + architect ratification. |
