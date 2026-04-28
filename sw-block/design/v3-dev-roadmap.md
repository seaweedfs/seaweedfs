# V3 Dev Roadmap (Entry Point)

**Date**: 2026-04-26 (kept current; update on every gate close)
**Status**: ACTIVE — single navigation point for "where are we, what's next"
**Purpose**: 1-page roadmap for new readers + returning collaborators. Points to canonical docs for detail.

---

## 1. The 3 doc layers

| Layer | Doc | Purpose |
|---|---|---|
| **Methodology** | [`v3-phase-development-model.md`](./v3-phase-development-model.md) | How V3 phases work (closed-loop discipline, target/proof/closure). Apr 2026 draft, methodology-stable. |
| **Roadmap (this doc)** | `v3-dev-roadmap.md` | What's done, what's next. Entry point. |
| **Canonical scope** | [`v3-phase-15-mvp-scope-gates.md`](./v3-phase-15-mvp-scope-gates.md) | THE 22 gates of P15 + dependency graph + closure rules + P0/P1/P2 priority |

If you only read one doc beyond this one, read `v3-phase-15-mvp-scope-gates.md`.

---

## 2. Current state

| Phase | State |
|---|---|
| **P14** | ✅ CLOSED (authority + topology layer) |
| **P15** | 🟡 IN PROGRESS — closing gate **G5** of 22 |
| **P16** | ⏳ Not designed yet — only hinted as "in-place V2→V3 migration" |

P15 = the MVP. 22 gates G0–G22. Currently at gate 5/22. Long way to G22 final cluster validation.

---

## 3. P15 gates — visual progress

```
G0  Product hosting               ✅ closed (T0 implementation)
G1  Master-volume RPC             ✅ closed (T0/T1 implementation)
G2  Frontend smoke                ✅ closed (T1/T2)
G3  Real frontends iSCSI+NVMe     ✅ closed (T2)
G4  Local durable data path       ✅ closed (T3)
G5  Replicated write path         🟡 closing (T4 done; G5-1..6 close work; G5-5 just closed at L3, G5-5C carry-forward)
G6  Incremental WAL catch-up      ✅ closed 2026-04-28 (retention-aware recovery; α config knob + escalation pin)
G7  Rebuild / replica re-creation ⏳ (T4d-4 part B/C scaffolding done)
G8  Failover data continuity      ⏳
G9  Volume lifecycle              ⏳
G9A Placement Controller MVP      ⏳ NEW (added 2026-04-26 per architect; flat-topology only, no rebalance)
G10 Snapshot                      ⏳
G11 Resize                        ⏳
G12 Disk failure handling         ⏳
G13 Node lifecycle                ⏳
G14 External API                  ⏳
G15 CSI lifecycle                 ⏳
G16 Security / Auth               ⏳
G17 Diagnostics / Metrics / Logs  ⏳
G18 Configuration / Deployment    ⏳
G19 Migration / Coexistence       ⏳ (V2→V3 in-place is P16+)
G20 QoS / rack / operator / GC    ⏳ (P2 — defer-allowed)
G21 Performance SLO               ⏳
G22 Final cluster validation      ⏳ (release gate; P15 closes here)
```

---

## 4. Where we actually are right now (granular)

| Item | State |
|---|---|
| T4d (replicated write path implementation) | ✅ CLOSED at `seaweedfs@2ee12b2c1` (closure report) |
| G5 collective close (production-readiness) | 🟡 IN PROGRESS — 6 sub-batches: |
| ↳ G5-1 multi-replica QA scenarios | 🟡 2 landed; rest on hold |
| ↳ G5-2 walstore cadence smoke | ⏳ sw cleared, not started |
| ↳ G5-3 metrics/backpressure | ⏳ sw cleared, not started |
| ↳ G5-4 binary T4 wiring | ✅ **CLOSED** at `seaweedfs@daafc8e25` (mini-plan v0.5) |
| ↳ G5-5 m01 hardware first-light | ✅ **CLOSED** at `seaweedfs@c78116fd2` — Product level **L3 Replicated IO** reached on m01/M02 hardware (#1/#2/#3 GREEN; #4 carried to G5-5C) |
| ↳ G5-5C peer recovery trigger after replica restart | ▶️ **next** — pass criterion bound: G5-5 #4 hardware case (kill replica + write while down + restart same `--durable-root` + byte-equal converges within deadline) |
| ↳ G5-6 G5-DECISION-001 + closure report | ⏳ at G5 collective close |

---

## 5. Naming convention (decoder)

| Term | Meaning |
|---|---|
| **P** | Phase. P14, P15, P16. P15 = current MVP. |
| **G** | Gate. 22 gates within P15. Product-level milestones. |
| **T** | Implementation track. T0–T4 historical (each closed one or more gates). T5+ may appear for gates that need substantial new code. |
| **G_x_-N** | Sub-batch within a gate-close (e.g., G5-1..G5-6). Used when most implementation is done; close work is verification + missing pieces. |

**T-tracks vs G-N batches**: contextual choice per gate. T-tracks for substantial new implementation; G-N for gate-close verification when implementation is mostly done. Architect picks at each kickoff.

---

## 6. Source of truth pointers

For any specific question, go to the canonical doc:

| Question | Doc |
|---|---|
| What does P15 promise? | `v3-phase-15-mvp-scope-gates.md` (22 gates + closure rules) |
| What's the product goal + execution model? | `v3-phase-15-product-plan.md` |
| Why pre-declared topology + authority discipline (not V2-style heartbeat-as-authority)? | `v3-product-placement-authority-rationale.md` |
| What positive behavior contracts must V3 block preserve (vs object/KV)? | **[`v3-block-behavior-contract-index.md`](./v3-block-behavior-contract-index.md)** — first-order architect ref, positive counterpart to anti-patterns |
| What's the methodology / closed-loop discipline? | `v3-phase-development-model.md` |
| What invariants does V3 promise + which tests prove them? | `v3-invariant-ledger.md` |
| What's the V2→V3 contract bridge state? | `v2-v3-contract-bridge-catalogue.md` |
| What's the QA system (G-1, G-2, G-3 gates, kickoff/mini-plan/G-1 cadence)? | `v3-phase-15-qa-system.md` + `v3-quality-system.md` |

---

## 7. Recently closed batches (for context window)

| Batch | Closed | Commit | Highlights |
|---|---|---|---|
| BlockStore walHead hotfix | 2026-04-25 | `seaweed_block@f6084ee` | substrate-internal one-liner; closes round-43 storage-contract violation |
| T4d-1 typed RecoveryFailureKind | 2026-04-25 | `1edeb36` + `d6b1890` | 2-enum split; engine zero-imports storage |
| T4d-2 lane-pure apply gate | 2026-04-25 | `bd2de99` + `01f4ab9` + `a63ae9b` | round-43/44 architectural locks; per-LBA stale-skip |
| T4d-3 R+1 + boundary fences | 2026-04-25 | `44c60dd` | engine emit R+1; CARRY-T4D-LANE-CONTEXT-001 Option B |
| T4d-4 part A RecoveryMode + Stop | 2026-04-25 | `f88d097` | substrate method; BUG-005 non-repeat |
| T4d-4 part B engine wiring + round-47 | 2026-04-25 | `812d3fa` | WithEngineDrivenRecovery REAL; rebuild engine-driven |
| T4d-4 part C full L2 + 2 bug fixes | 2026-04-25 | `e642ae8` | 2 engine bugs surfaced + fixed during HARD GATE #3 |
| T4d closure report | 2026-04-25 | `seaweedfs@2ee12b2c1` | T4 batch series CLOSED |
| T2A NVMe race fix | 2026-04-25 | `seaweed_block@a0be6d5` | atomic.Pointer test fixture; -race ×50 PASS |
| G5-4 binary T4 wiring | 2026-04-26 | `seaweed_block@c820e17` + ledger `seaweedfs@36ba7b44e` + close-lock `daafc8e25` | binary now wires T4 stack; criteria 3+4 relocated to G5-5 |
| G5-5 m01 hardware first-light | 2026-04-27 | `seaweed_block@5c4718f` + close-doc `seaweedfs@c78116fd2` | L3 Replicated IO on m01/M02 hardware: #1 cluster role split GREEN, #2 live iSCSI replicated write byte-equal GREEN, #3 network partition + heal catch-up GREEN (8s); #4 replica-restart catch-up carried to G5-5C as real recovery-path finding |
| G6 Incremental WAL catch-up + recycle escalation | 2026-04-28 | `seaweed_block@96c51b4` (preceded by `85475cd` rebuild-START marker + `420ad1f` audit) + close-doc forthcoming | L5 retention-aware recovery: §1.A α config knob (`--wal-retention-lsns`) + walstore retention gate relaxation + engine table-driven WALRecycled→Rebuild dispatch test (3 tests, 6 cases) + substrate retention test (4 cases) + m01 single-run GREEN at 71s covering both #4 retention-OK catch-up and #5 recycle→rebuild escalation. 5 INVs inscribed including new harness-discipline INV-G6-HARNESS-DATA-AND-STATE-CONVERGENCE (chained scenarios need both byte-equal AND peer-healthy sync). G5-5C carry-forward G6-T-WALRECYCLE-ESCALATE closed at this gate. |

---

## 8. After G5 closes

Per `v3-phase-15-mvp-scope-gates.md` §4.5 dependency graph:
- G6 Incremental Catch-Up (T4c muscle done; mostly verification)
- G7 Rebuild (T4d-4 part B/C scaffolding done)
- G8 Failover continuity (substantial new work likely)
- G9 Volume lifecycle (new code)
- **G9A Placement Controller MVP** (new addition 2026-04-26 — flat-topology only, sits between G9 and G10)
- G10–G22 (mix of new code + verification + integration)

P15 closes at **G22 final cluster validation**. After P15 → P16 (in-place migration is the only hinted scope).

### Closed backlog tickets

| Ticket | Source | Resolution |
|---|---|---|
| **G6-T-WALRECYCLE-ESCALATE** | G5-5C QA scenario D 2026-04-28 (architect-bound carry) | ✅ **Closed at G6 §close 2026-04-28** — verdict (a) existing-and-correct + observability gap. Engine `apply.go:519-547` already wired `RecoveryFailureWALRecycled` → `RebuildPinned` + `StartRebuild` emit at T4d-1; G6 added the rebuild-START log marker + operator retention knob + 14 test cases (10 unit + 4 substrate) + m01 hardware single-run GREEN at 71s. INV-G6-WALRECYCLE-DISPATCHES-REBUILD inscribed. Original 5s observation was scrape-window-too-short, NOT a missing dispatch. |

---

## 9. Doc hygiene — outdated/historical docs

The `sw-block/design/` directory has accumulated 60+ `v3-phase-15-*` docs. Most are historical artifacts from closed T-tracks. To find current work:

- **Active**: any doc dated 2026-04-25 or later, OR referenced from this roadmap
- **Historical**: T0/T1/T2/T3 assignments + sketches + closure reports — kept for reference, not part of active work

For a full doc archive plan see §10 of this roadmap (when committed); meanwhile, use this roadmap's pointers + commit dates to navigate.

---

## 10. Update protocol

This doc gets updated at every gate-close:
1. Move closed gate from "🟡 closing" → "✅ closed" in §3
2. Move next gate to "🟡 closing" or "▶️ next"
3. Update §4 "where we actually are" with the new active batch
4. Append the gate-close commit to §7 recent-closed table
5. Re-check §8 "after G_x_ closes" prediction against actual scope

QA owns this doc; sw + architect review at each gate-close report sign.
