# V3 Phase 15 — G5-5 (m01 Hardware First-Light + L3 Integration) Mini-Plan

**Date**: 2026-04-26
**Status**: DRAFT v0.1 — submitted for QA review (§12 architect review checklist) + architect §1-§6 ratification per `v3-batch-process.md`
**Owner**: sw (script + Go driver); QA (m01 verification + scenario authoring)
**Process**: First trial of compressed `v3-batch-process.md` (one doc, §close appended at batch close)
**Predecessors**: G5-4 closed (`seaweed_block@c820e17` + `seaweedfs@36ba7b44e`); 5 INV-BIN-WIRING-* invariants ACTIVE in ledger; `--expected-slots-per-volume` flag landed (`f5de7c5`)

---

## §1 Scope

G5-5 promotes G5-4's L1 (binary composition) result to L3 (Replicated IO) on real hardware via m01+M02. This is the first batch where a real frontend write moves through the production binary code path, lands on a remote replica via TCP, and is verified byte-equal.

### Architecture touchpoints

- `v3-architecture.md §6.1` Write Path — frontend write → projection check → local durable append → replication fan-out → host ack
- `v3-architecture.md §6.3` Replication Path — primary local write → `ReplicationVolume.Observe` → `ReplicaPeer` → `BlockExecutor` dials peer DataAddr → `ReplicaListener` accepts → apply gate dispatches → substrate applies
- `v3-architecture.md §7` Recovery Architecture — short disconnect → engine-driven catch-up within retention
- `v3-architecture.md §13` Product Completion Ladder — G5-4 reached L1; G5-5 targets L3 (and edges into L4 for the disconnect scenario)

### What G5-5 delivers

| # | Item | Verifier |
|---|---|---|
| 1 | `scripts/iterate-m01-replicated-write.sh` — orchestration script that builds binaries on m01, deploys via SMB share, starts blockmaster + 2× blockvolume cluster, drives a kernel iSCSI write workload from m01, verifies replica's underlying storage matches primary | Manual run on m01 + log artifacts |
| 2 | Real frontend write byte-equal verification — primary's iSCSI target accepts a kernel write from m01; the same bytes land in replica's walstore on M02; verified by reading replica's storage directly post-write | Step in #1 script + Go assertion helper |
| 3 | Mid-stream disconnect + catch-up scenario — iptables drops primary↔replica WAL data port mid-write; replica falls behind; iptables restored; verify replica catches up via T4d engine-driven recovery (catch-up path, NOT rebuild — gap stays within walstore retention) | Step in #1 script |
| 4 | m01 `-race ×10` verification of G5-4's integration test (`TestG54_BinaryWiring_RoleSplit_2NodeSmoke`) — Windows can't run `-race` (CGO/gcc); m01 is the verification environment | Step in #1 script |

### What G5-5 does NOT deliver (explicit non-claims)

- **No rebuild path on m01.** Catch-up within retention only (#3). Rebuild scenario (replica beyond retention → full extent transfer) defers to a separate batch — needs scrub state + WAL truncation timing that adds scope.
- **No NVMe target verification.** iSCSI is the kernel-target choice for #2 (existing m01 tooling per memory). NVMe-over-TCP target exists in V3 but kernel client setup on m01 isn't verified; defers to a follow-up batch.
- **No durability mode coverage.** ReplicationVolume defaults to `BestEffort`; this batch verifies the WIRE PATH, not the durability semantics. `sync_all` / `sync_quorum` mode coverage defers to G5-2 / G5-6.
- **No failover.** Primary stays primary throughout. Failover (kill primary → replica promotes) is G8 scope.
- **No failure-injection at backend layer.** All faults are network-layer (iptables) — backend-injection (disk full, walstore corruption) defers to fault-scenario batches.

### Files

| File | Change | LOC |
|---|---|---|
| `scripts/iterate-m01-replicated-write.sh` (new, in `seaweed_block`) | Orchestration script: build/deploy/start cluster/drive workload/verify/cleanup | ~200 |
| `cmd/blockvolume/m01_verify_helper.go` (new, build-tag `m01verify`) | Go helper for direct replica-store byte-equal assertion (reads replica's walstore .extent, computes hash, prints to stdout) | ~80 |
| `sw-block/design/v3-phase-15-g5-5-mini-plan.md` (this doc) | §close appended at batch close | ~250 + §close |

### Architecture truth-domain check (v3-architecture.md §4)

No new truth-domain owner. G5-5 verifies the existing truth-domain map (§4) at L3 scope — same owners as G5-4, just exercised end-to-end through real hardware.

---

## §2 Acceptance criteria

| # | Criterion | Verifier |
|---|---|---|
| 1 | `iterate-m01-replicated-write.sh` runs end-to-end on m01 cleanly: blockmaster + 2× blockvolume start, both reach role-appropriate ready state | Script step "verify_cluster_ready" exits 0 |
| 2 | Kernel iSCSI write from m01 → primary's iSCSI target → replication fan-out → byte-equal on replica's walstore extent | Script step "verify_byte_equal" computes SHA-256 of primary write payload + replica's stored block; equality required |
| 3 | iptables drops primary→replica WAL port during a write burst → replica lags → iptables restored → replica catches up to primary's H within 30s via T4d engine-driven recovery | Script step "verify_catchup" polls replica's status until R == primary's H within deadline |
| 4 | `TestG54_BinaryWiring_RoleSplit_2NodeSmoke` runs 10× under `-race` on m01 with no flake | Script step "verify_race_stress" runs `CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring` |
| 5 | No regressions in V3 suite | Full V3 suite green from m01 (`go test ./...` clean) |
| 6 | `v3-dev-roadmap.md` updated to reflect G5-5 close + L3 reached | sw + QA at §close |

### Architect review checklist (§12) coverage

| Check | This batch's answer |
|---|---|
| Scope truth | Done: end-to-end byte-equal write + short-disconnect catch-up at L3 on m01 hardware. Not done: rebuild scenario, NVMe target, durability modes, failover. Product risk: catch-up within retention is verified; gap-beyond-retention rebuild path stays unverified at L3 (verified at L1 component scope only). |
| V2/new-build decision | New build — script + Go helper. NO V2 muscle PORT (orchestration is shell + Go test). G-1 NOT required (per `v3-batch-process.md §6.1`). |
| Engine/adapter impact | Zero. G5-5 is verification-only. No engine, adapter, transport, or replication code changes. |
| Product usability level | Operator can run a real iSCSI volume across 2 nodes; writes survive a transient network blip via catch-up. Reaches L3 (Replicated IO) per `v3-architecture.md §13`. Falls short of L4 (rebuild path) and L5 (CSI/API operator surface). |

---

## §3 Invariants to inscribe at close

G5-5 verifies existing invariants on real hardware; it does NOT inscribe new INVs (verification batches don't add invariants — they upgrade existing ones from "component-only" to "Integration-verified" status in the ledger).

**Invariants whose ledger row updates at G5-5 close:**

| INV ID | Ledger row update |
|---|---|
| `INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT` | Add `iterate-m01-replicated-write.sh` evidence pointer; `Last verified` → 2026-04-DD |
| `INV-BIN-WIRING-PEER-SET-FROM-ASSIGNMENT-FACT` | Same pattern |
| `INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO` | Same pattern |
| `INV-BIN-WIRING-ASSIGNMENT-DRIVES-MEMBERPRESENT` | Same pattern |
| `INV-BIN-WIRING-SESSIONID-VIA-ADAPTER` | Same pattern |
| `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` | Add m01 catch-up scenario as Integration evidence |
| `INV-REPL-LSN-ORDER-FANOUT-001` (T4a-4) | Add m01 byte-equal evidence as Integration |
| (any other relevant T4 invariants the catch-up scenario exercises) | sw + QA enumerate at §close |

**New INV considered + rejected:** none — verification batches don't introduce new claims.

---

## §4 G-1 V2 read

**N/A.** Per `v3-batch-process.md §6.1`: G-1 required only for V2 muscle PORT batches. G5-5 is verification + bash + Go helper — no V2 muscle source. Skipping G-1.

---

## §5 Forward-carry consumed (from G5-4 §close)

G5-4 explicitly deferred to G5-5:

| G5-4 criterion | G5-5 absorption |
|---|---|
| #3 Byte-equal primary→replica via real frontend write | G5-5 §2 #2 |
| #4 Stop-restart catch-up convergence | G5-5 §2 #3 (variant: network disconnect via iptables, not process restart — same engine-driven catch-up path) |
| #6 10× `-race` stress on G5-4 integration test | G5-5 §2 #4 |

Plus the broader G5-4 wiring claim — that the binary-level T4 replication stack composes — gets m01 hardware verification at G5-5 §2 #1 (cluster reaches role-appropriate ready state on real hardware, not subprocess-only).

---

## §6 Risks + mitigations

| Risk | Mitigation |
|---|---|
| iSCSI kernel target setup on m01 is fragile (existing memory: `--iscsi-listen` + open-iscsi initiator on m01) | Reuse m01's existing iSCSI tooling per memory; `iterate-m01-replicated-write.sh` documents the exact `iscsiadm` invocations. If setup fails, script exits with named error; doesn't continue silently. |
| iptables disconnect window timing is racy (too short → no observable lag; too long → exceeds walstore retention → escalates to rebuild instead of catch-up) | Hard-coded conservative window (3s drop with 256-block walstore + low-rate write should keep gap within retention). Script asserts catch-up (NOT rebuild) via engine projection field. If rebuild emits, script fails fast with clear message — surfaces as a script-tuning fix, not a bug claim. |
| SMB share binary deployment race (already-running binary holds .exe open; rebuild fails) | Script kills processes BEFORE rebuild. Idempotent: re-run from any state. |
| `-race` on m01 needs `CGO_ENABLED=1` + `gcc` installed | Memory: m01 has gcc per RDMA build chain. Script asserts `which gcc` succeeds before -race step. |
| Test-only `m01_verify_helper.go` accidentally compiles into production binary | Guarded by `//go:build m01verify` build tag; default builds exclude it. Production binary unchanged. |

---

## §7 Sign table

| Stage | Signer | When | Status |
|---|---|---|---|
| §1-§6 ratification | architect (pingqiu) + QA review (§12 checklist) | This submission | ⏳ pending |
| Code start (script + Go helper) | sw | After §1-§6 ratification | ⏳ blocked on ratification |
| m01 hardware verification run | QA | After sw lands script + helper | ⏳ blocked |
| §close append + close sign | sw drafts §close; QA verifies evidence; architect single-sign per `v3-batch-process.md §5` | After m01 verification | ⏳ blocked |

---

## §close

*(Appended at batch close. Per `v3-batch-process.md §12`, the §close summary MUST include:)*

```
Done:
Not done:
Product level reached:
Next gate that makes it usable:
```

*(Plus deltas vs §1-§6, evidence pointers (commit hashes, m01 log artifacts, ledger row updates), and forward-carries to G5-6.)*

---

## Next actions

| Agent | Action |
|---|---|
| **architect** | Review + ratify §1-§6 of this mini-plan per `v3-batch-process.md §5` (compressed sign cycle: one ratification at start, one at close). Specifically check §1 architecture touchpoints, §2 acceptance criteria coverage, §3 invariant update plan, §6 risks. If §6.3 hotfix-class scope (1-line fix), architect can defer ratification — but G5-5 is ~280 LOC, not hotfix-class, so full §1-§6 ratification expected. |
| **sw** | After architect ratifies §1-§6: write `scripts/iterate-m01-replicated-write.sh` + `cmd/blockvolume/m01_verify_helper.go` (build-tag `m01verify`). Estimate: ~280 LOC + 1-2 commits. Hand off to QA for m01 run. |
| **QA** | Run §12 architect review checklist on this mini-plan now (BEFORE forwarding to architect per `v3-batch-process.md §14`); flag any scope/V2/engine/usability concerns. After sw lands script + helper: run on m01, capture artifacts, draft §close evidence pointers (sw + QA co-author per §14). |
