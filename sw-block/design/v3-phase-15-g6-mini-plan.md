# V3 Phase 15 — G6 (Incremental WAL Catch-Up + Recycle Escalation) Mini-Plan

**Date**: 2026-04-28 (v0.1 — kickoff draft for architect ratification)
**Status**: §1-§6 awaiting architect ratification per `v3-batch-process.md §5`
**Repo**: `seaweed_block` (V3) — code; `seaweedfs` — design docs / mini-plans
**Owner**: sw (audit + tests; possibly small engine/host patches per §1.H); QA (harness extension + hardware re-run)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR per repo, one §close)
**Predecessors**:
- T4c muscle (catch-up sender + state machine + R+1 fence + integration matrix) closed at `seaweed_block@c910464a9` series
- G5-5C closed at `seaweedfs@1207fc544` — peer recovery trigger via probe loop; carry-forward `G6-T-WALRECYCLE-ESCALATE` from QA scenario D
**Architect bindings 2026-04-28**:
- Fold `G6-T-WALRECYCLE-ESCALATE` into G6 main acceptance (one closed-loop AC, not a separate sub-batch).
- §1 AC covers BOTH retention-OK catch-up AND recycle-triggered escalation in **one hardware scenario** (sustained write that crosses recycle boundary).
- §1.A is WAL retention policy (config-only knob / pin-window / replica-watermark-driven).
- §1.H follows G5-5C precedent: don't pre-declare "zero code"; audit first, then decide if code work is needed.

---

## §1 Scope

> **Scope-rule (carries from G5-5C §1)**: *master owns identity / topology; primary + engine own data recovery; the protocol aligns the two via `(PeerSetGeneration, epoch, EndpointVersion)` fences.* G6 lives entirely on the primary/engine side under this rule.

G6 closes the **catch-up under retention** + **escalation when retention exhausted** loop. T4c muscle (catch-up sender, state machine, R+1 fence) and T4d-4 muscle (engine-driven recovery dispatch) are done; G6 verifies the integrated behavior under realistic load and binds the WAL retention policy.

The G5-5C QA scenario D run surfaced a real boundary: under sustained 5000-LBA write (~56 MB/s), primary's WAL recycled past the LSN range a lagging replica needed for catch-up. Engine reported `storage: WALRecycled: fromLSN=602 checkpointLSN=700 headLSN=701`, peer marked `Degraded`, but rebuild dispatch was not observed in the 5s log scrape window. G6 closes whether that's:
- **(a)** existing-and-correct (rebuild WAS dispatched, just outside the 5s window — verification work + larger window), OR
- **(b)** a real gap in the runtime escalation chain (engine sees WALRecycled but doesn't translate it into a probe outcome that triggers `StartRebuildFromProbe` — small implementation work).

### Architecture touchpoints

- `v3-architecture.md §6.3` Replication Path — live ship + barrier under steady state
- `v3-architecture.md §7` Recovery Architecture — engine-driven catch-up (T4d-4) + retry budget (T4c-3) + rebuild path (T4d-4 part B)
- `v3-architecture.md §13` Product Completion Ladder — closing G6 keeps L4 + adds **L5 Replicated IO with retention-aware recovery** (catch-up bounded by retention; recycle-class failures escalate cleanly).

### What G6 delivers

| # | Item | Verifier |
|---|---|---|
| 1 | **Retention-OK catch-up** — replica falls behind within WAL retention window → engine dispatches `StartCatchUp` → catch-up sender streams missing entries → byte-equal converges. (T4c-3 already pins this at engine layer; G6 verifies the END-TO-END runtime path under sustained-write load.) | Hardware: `iterate-scale.sh` D-shape scenario tuned so replica gap stays under retention |
| 2 | **Recycle escalation** — sustained write pushes WAL past replica's R+1 → catch-up session emits `WALRecycled` → next probe outcome dispatches rebuild (or operator-visible failure per §1 product decision) within bounded deadline. | Hardware: scenario D extended with `wait_until_rebuild_dispatched` (or `wait_until_operator_failure_logged`) helper; deadline TBD at code-start (likely 30 s — same as G5-5C #4) |
| 3 | **WAL retention policy** bound (§1.A) — explicit operator-visible knob OR replica-aware pinning OR documented behavior. The current "checkpoint-driven recycle" is the baseline; G6 either ratifies it as-is + documents tradeoffs, or replaces with one of the §1.A options. | Hardware acceptance; operator-doc / changelog row |
| 4 | **§1.H code-start audit** result — written audit findings (one of three verdicts: verify-only / minor-patch / engine-evolution-batch). | Audit commit on the G6 branch before any production code change |

### §1.A WAL retention policy — three options for ratification

The current behavior (per G5-5C scenario D evidence) is **checkpoint-driven recycle**: WAL is recycled when checkpoint advances past `fromLSN`, regardless of whether any replica still needs that range. This is the simplest model but produces the surfaced gap when replicas lag.

Three policy options for G6:

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| **α — Config knob (status quo + tunable)** | Add `--wal-retention-bytes` (or `--wal-retention-lsns`) flag; operator sizes WAL relative to expected replica lag tolerance. Recycle still checkpoint-driven, but operator can size away from default to push recycle threshold. | Simplest. No new feedback path. Operator owns the tradeoff explicitly. Recycle WHEN it hits is the same as today, just at a configurable point. | Requires operator knowledge of workload characteristics. Doesn't adapt to actual replica lag — a slow replica can still trigger recycle if WAL fills before catch-up keeps up. |
| **β — Pin-window** | Primary tracks `minReplicaLSN` (lowest committed LSN across active replicas); WAL retention is pinned to `[minReplicaLSN, head]`; checkpoint can advance, but WAL frames `< minReplicaLSN` are NOT recycled until all replicas pass. Requires operator-defined dead-replica timeout to prevent unbounded disk growth on a permanently-dead replica. | Recycle never strands a live replica that's just slow. Bounds catch-up to "always succeeds within retention window for live replicas." | New feedback path (replica → primary `flushedLSN` reporting; partially exists for barrier ack but needs to drive retention). Dead-replica timeout adds operator policy surface. Disk growth risk if timeout misconfigured. |
| **γ — Replica-watermark-driven (subset of β)** | Same as β but: dead-replica detection via existing probe-loop `Degraded` state instead of new timeout. If a peer is `Degraded` past N probe cycles, retention drops it from the watermark calc — primary stops pinning for it (the next time replica returns, it's a rebuild, not catch-up — and it knows). | No new operator timeout; reuses G5-5C infrastructure. Naturally drives the "permanently-dead replica → rebuild" semantics. | Couples retention to probe-loop policy decisions. If probe-loop tunables change, retention behavior changes implicitly. |

**sw recommendation**: **Option α (config knob)** for G6. Reasoning:
1. **Smallest diff, fastest to ratify + test.** Under architect's "fold into G6 main AC" ruling, the policy ratification + hardware verification need to land together; α minimizes the work envelope.
2. **Honest about tradeoffs.** Operators sizing WAL is a known pattern (PostgreSQL `wal_keep_size`, MySQL `binlog_expire_logs_seconds`). Documents G6's contract clearly: *catch-up succeeds when replica lags <= retention window; otherwise rebuild dispatches*.
3. **β / γ are valuable but bigger.** They introduce a new replica → primary feedback loop (or repurpose probe-loop). That's a richer batch, more naturally a G6 follow-up (G6b?) once the escalation path is verified GREEN.
4. **G5-5C precedent.** G5-5C avoided protocol expansion (rejected Option A re-emission) and bounded itself to runtime mechanics. Same discipline here: G6 first proves the existing dispatch chain, THEN earns the right to add new feedback paths.

Architect picks at §1-§6 ratification. If α is bound, G6 acceptance #3 reduces to "flag landed, default value documented, recycle behavior documented." If β / γ is bound, G6 grows by ~150 LOC in the replication layer + pin-tracking tests.

### What G6 does NOT deliver (explicit non-claims)

- **No new engine recovery primitive.** T4d-4 muscle is done; G6 reuses unchanged. If §1.H finds the dispatch chain is broken, G6 includes a small patch — NOT a new primitive.
- **No rebuild path semantic changes.** Rebuild itself is G7 territory. G6 only verifies that *escalation TO rebuild* dispatches correctly.
- **No multi-replica RF coverage.** Same 1-replica / 2-node hardware as G5-5C. RF≥2 retention semantics are post-G6.
- **No durability mode coverage.** Still BestEffort (G5-2 / G5-6 territory).
- **No metrics / observability surface beyond logs.** G5-3 owns metrics; G6 verifies via existing log markers.

### Files (preliminary — exact set bound at code-start audit §1.H)

Audit-first per architect ruling. Likely files if §1.H verdict is "verify-only" vs "minor-patch":

| File | Likely change | Verdict-dependent |
|---|---|---|
| `core/engine/apply.go` | (read-only) — verify `applyProbeFailed` / `applySessionClosedFailed` translate `WALRecycled` failure kind → next decide() emits StartRebuild | verify-only OR minor-patch |
| `core/engine/state.go` | (read-only) — verify `RecoveryFailureWALRecycled` → `RebuildPinned=true` path is wired | verify-only |
| `core/transport/catchup_sender.go` | (read-only) — verify `WALRecycled` substrate error maps to `RecoveryFailureWALRecycled` engine kind via `OnSessionClose` callback | verify-only |
| `core/host/volume/probe_loop_wiring.go` (or new helper) | If §1.A binds Option α: minor patch to plumb `--wal-retention-*` flag | depends on §1.A pick |
| `cmd/blockvolume/main.go` | Add `--wal-retention-*` flag (Option α) OR no change (β / γ) | depends on §1.A pick |
| `core/engine/g6_*_test.go` (new) OR extend `g5_5c_boundary_test.go` | Engine-layer test pinning the WALRecycled → StartRebuild dispatch path (table-driven; covers `RecoveryFailureWALRecycled` failure-kind branch) | always |
| `scripts/iterate-m01-scale.sh` (extend the QA D harness) | `wait_until_rebuild_dispatched` helper + scenario tuned to cross retention boundary deterministically | always (QA-owned harness) |

Total estimate: **verify-only verdict ~80 LOC (mostly tests)**; **minor-patch verdict ~200 LOC** (config flag + plumb + tests).

### Architecture truth-domain check (`v3-architecture.md §4`)

| Truth domain | This batch (G6) |
|---|---|
| Master / control plane | **No change.** Master code untouched. |
| Primary / data-control plane | **Possible write (depends on §1.A pick)**: Option α adds operator-visible flag; β / γ add new replica-aware retention logic. |
| Replica durable storage | **No change.** |
| Engine recovery primitives | **Possible write (depends on §1.H audit verdict)**: minor patch if dispatch chain has gap; otherwise read-only. |

No truth-domain crossings introduced; G6 stays inside the G5-5C scope-rule.

---

## §2 Acceptance criteria

Numbered, verifier-named, single source of truth.

| # | Criterion | Verifier |
|---|---|---|
| 1 | **§1.H code-start audit** completed and findings published as commit note. Audit covers: (a) `WALRecycled` substrate error → `RecoveryFailureWALRecycled` engine kind mapping; (b) engine `applySessionClosedFailed` with `RecoveryFailureWALRecycled` → `RebuildPinned=true` + next probe → `StartRebuild` dispatch; (c) runtime probe-loop interaction (degraded peer post-WALRecycled stays in probe loop until rebuild lands). Verdict: verify-only / minor-patch / engine-evolution-batch. | sw audit commit before any production code |
| 2 | **§1.A WAL retention policy** bound + landed: Option α (config knob with documented default) OR Option β (pin-window with operator timeout) OR Option γ (replica-watermark via probe-loop). | Architect §7 ratification + (if α) flag landed in `cmd/blockvolume/main.go` + (if β/γ) replication-layer changes |
| 3 | **Engine-layer dispatch test**: table-driven test pins that `RecoveryFailureWALRecycled` failure kind on a running session leads to `RebuildPinned=true` and next decide() emits `StartRebuild`. Negative case: other failure kinds (Transport, SubstrateIO) do NOT pin Rebuild. | New `g6_*_test.go` in `core/engine/` (or extension of existing `g5_5c_boundary_test.go`) |
| 4 | **Hardware retention-OK catch-up** (closed-loop part 1): D-shape scenario with replica killed mid-write, restarted, gap STAYS within WAL retention → byte-equal converges within deadline (suggest 30 s same as G5-5C #4). | `iterate-m01-scale.sh` extension run on m01 |
| 5 | **Hardware recycle-escalation** (closed-loop part 2): D-shape scenario with sustained write that DELIBERATELY crosses retention boundary → engine logs WALRecycled → primary log shows rebuild-session-start within deadline (or, if §1 binds rebuild-as-NON-GOAL, operator-visible failure log within deadline). | `iterate-m01-scale.sh` extension with `wait_until_rebuild_dispatched` helper (or `wait_until_operator_failure_logged`) |
| 6 | **Both #4 and #5 pass in the SAME hardware run** — single closed-loop AC per architect binding 2026-04-28. NOT two separate hardware runs. | One m01 run, one log artifact, both phases GREEN |
| 7 | **No regression on G5-5C #1-#4 hardware steps** — `iterate-m01-replicated-write.sh` 6-step suite stays GREEN at the G6-fix tree. | Same m01 run as G5-5C regression suite, post-fix |
| 8 | **No master code touched** — `git diff --stat` for G6 PR shows zero changes under `core/host/master/`, `core/authority/`, `core/rpc/proto/`, `core/rpc/control/`. (Same fence as `INV-G5-5C-NO-MASTER-PROTOCOL-CHANGE` — recovery is primary/engine concern.) | Diff inspection at PR review |

### Architect review checklist (`v3-batch-process.md §12`) coverage

| Check | Where addressed |
|---|---|
| Scope truth | §1 + §1.A retention options + explicit non-claims |
| V2 / new-build decision | New build (V3 verification + possibly small new code per §1.H); G-1 N/A per `v3-batch-process.md §6.1`; §1.H pre-code audit substitutes per G5-5C precedent |
| Engine / adapter impact | §1.H audit determines; default expectation: read-only on engine; possible minor patch if audit surfaces a dispatch-chain gap |
| Product usability level | Closing G6 reaches **L5 Replicated IO with retention-aware recovery** — operator gets a documented WAL retention contract, and recycle-class failures escalate cleanly to rebuild without human intervention |

---

## §3 Invariants to inscribe at close

| INV ID (proposed) | What it claims | Test pointer (proposed) |
|---|---|---|
| `INV-G6-WALRECYCLE-DISPATCHES-REBUILD` | When a recovery session fails with `RecoveryFailureWALRecycled`, the engine pins `RebuildPinned=true` and the next probe-driven decide() emits `StartRebuild` (NOT `StartCatchUp`). The lagging replica is moved off the catch-up path onto the rebuild path automatically; operator never sees a permanently-degraded peer when the cause is recycle. | `core/engine/g6_*_test.go` (or `g5_5c_boundary_test.go` extension) — table-driven dispatch test + hardware step #5 |
| `INV-G6-CATCHUP-CONVERGES-WITHIN-RETENTION` | When replica's gap stays within configured WAL retention window, catch-up always converges to byte-equal within deadline; rebuild never dispatches in this regime. | `core/engine/apply_test.go` (existing T4c-3 retry-budget tests pin the engine side); hardware step #4 pins the runtime side |
| `INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE` (only if §1.A picks α) | WAL retention is operator-tunable via `--wal-retention-*` flag; default value is documented; the operator-tunable boundary is the contract for "catch-up succeeds vs rebuild dispatches." | `cmd/blockvolume/main.go` flag definition + help text + operator doc |
| `INV-G6-RETENTION-POLICY-REPLICA-AWARE` (only if §1.A picks β or γ) | WAL is NOT recycled past `minReplicaLSN` for live replicas; dead-replica eviction from the retention watermark is bounded by [timeout / probe-degraded-N-cycles]. | `core/replication/retention_test.go` (new) + hardware step #5 |
| `INV-G6-ENGINE-NO-REBUILD-PINNED-ON-OTHER-FAILURES` | `RebuildPinned=true` fires ONLY for `RecoveryFailureWALRecycled` (and the existing T4d-4 retry-budget-exhaustion path, unchanged). Transport / SubstrateIO failures stay on catch-up retry path. | Engine table-driven test negative case |

INVs **rejected / deferred**:
- "Rebuild path semantics correct" — that's G7, not G6.
- "Multi-replica retention coordination" — RF≥2 is post-G6.

---

## §4 G-1 V2 read

**N/A** — this batch is verification + possibly small patch on V3 code (T4c muscle was already V2-PORTed at T4c-2 close). No fresh V2 muscle PORT; no V2 source to read against. Per `v3-batch-process.md §6.1`, G-1 is skipped. §1.H pre-code audit substitutes per G5-5C precedent.

---

## §5 Forward-carry consumed (from G5-5C §close)

| Carry item | Disposition in G6 |
|---|---|
| **`G6-T-WALRECYCLE-ESCALATE`** (architect-bound 2026-04-28: fold into G6 main AC) | **Primary scope of this batch** — §1 + §2 #5 (recycle-escalation hardware step) directly address. NOT a sub-batch; one closed-loop AC. |
| Evidence: `V:\share\g5-test\logs\bcd-20260428T072539Z.log` D-section showing `WALRecycled fromLSN=602 checkpointLSN=700 headLSN=701` + no rebuild dispatch in 5s window | Cited in §1; G6 hardware step #5 reproduces the scenario AND verifies escalation in a wider deadline window. |
| QA prep: `wait_until_rebuild_dispatched` helper in `iterate-scale.sh` | Held until G6 §1-§6 ratified; QA implements after architect single-sign per §2 #5 acceptance |
| Cross-reference `INV-G5-5C-PROBE-BEFORE-CATCHUP` | G6 strengthens this — adds `INV-G6-WALRECYCLE-DISPATCHES-REBUILD` showing the dispatch path under WAL boundary |

---

## §6 Risks + mitigations

| Risk | Mitigation |
|---|---|
| §1.H audit surfaces engine-evolution-batch verdict (dispatch chain truly broken, requires new state machine work) | Stop and re-scope per G5-5C §1.H halt-condition discipline. G6 pauses; sw drafts engine-evolution mini-plan. (Architect already accepted this as legitimate possibility in 2026-04-28 ruling.) |
| §1.A pick is α (config knob) but operator default value is wrong for G5-5C-class workloads → recycle still hits sooner than tests expect | G6 hardware step #4 (retention-OK case) deliberately tunes the workload INSIDE the configured window; if default is too small, scenario fails fast and we tune. |
| §1.A pick is β/γ but new feedback path has bugs (e.g., dead replica's LSN wrongly pins retention indefinitely) | β/γ INV explicitly covers eviction; tests pin both "live replica pins retention" AND "dead replica gets evicted within bounded window." |
| Hardware test wallclock too short to observe escalation (G5-5C scenario D had 5 s window; not enough) | §2 #5 deadline matches G5-5C #4 (30 s suggested). `wait_until_rebuild_dispatched` polls primary log every 1 s. |
| Component test passes but hardware doesn't converge | Same closure pattern as G5-5C: component tests are necessary but not sufficient; hardware GREEN is the §close gate. Both #4 and #5 must run on m01. |
| Forward-carry creep: someone adds metrics / observability to G6 "while we're at it" | `INV-G6-…` claims are tightly scoped to retention + dispatch. Status-surface fields (recovery reason, last-recycle-time) stay in G5-3. §2 #8 diff-inspection guards against master-side touch. |

---

## §7 Sign table

| Item | Owner | When | State |
|---|---|---|---|
| §1.A WAL retention policy binding (α / β / γ) | architect | §1-§6 ratification | ⏳ pending |
| §1-§6 architect ratification of v0.1 | architect | Before code-start audit | ⏳ pending |
| §1.H code-start audit (verdict: verify-only / minor-patch / engine-evolution-batch) | sw | After §1-§6 ratification, before any production code | ⏳ blocked on ratification |
| Code (audit-dependent) — engine table-driven test + (if α) flag wiring + (if β/γ) replication-layer pin-window | sw | After §1.H verdict | ⏳ blocked on audit |
| Harness extension — `wait_until_rebuild_dispatched` helper + scenario tuning | QA | After §1-§6 ratified | ⏳ blocked on ratification |
| m01 hardware re-run (#1-#7 per §2) | QA | After sw lands code + harness extension | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*Appended at batch close per `v3-batch-process.md §2`.*
