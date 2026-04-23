# V3 Phase 15 T3c — Mini Port Plan (LOCKED)

**Date**: 2026-04-22
**Status**: QA single-signed per §8C.2; sw cleared to start
**Owner**: QA drafts + signs; sw implements
**Predecessor**: T3b CLOSED (commit `72d0d40`)
**Successor**: T3 CLOSED → Gate G4 pass → P15 advances
**Skeleton→LOCK note**: post-T3b scope review found no shift needed. `--durable-root` + selector shape lets scenarios parameterize via existing `logicalStorageFactories()` helper without new infra. Original §0-§6 body retained below.

---

## 0. (Original skeleton preamble)
Owner
**Purpose**: Preliminary scope frame so sw sees what T3c will look like while working T3b. Do NOT start T3c work from this doc. Final LOCKED mini-plan replaces this file after T3b close.

---

## 0. Why a skeleton

§8C mid-T autonomy means QA can prep next-batch framing while sw works current. This doc captures the **shape** only:
- Files to expect
- Acceptance criteria headings (body TBD)
- Ledger row IDs to queue
- Stop rules (all inherited)

Skeleton DOES NOT:
- Commit file-level LOC budgets
- Lock test names
- Sign-off (QA does NOT sign a skeleton; sign lives on the real mini-plan post T3b-close)

If T3b delivery uncovers scope shift (e.g., a Provider detail that changes T3c scenario shape), QA edits this skeleton before locking.

---

## 1. T3c scope frame

T3c is the closing batch — scenarios + continuity + perf + T3-end three-sign.

### 1.1 Scenarios (6 YAMLs + Go replay, expanded per V2-parity design 2026-04-22)

**Original 4 (from audit §11 G-int.6 + §3.3)** + **2 additional (V2-parity additions for crash variant coverage)**.

| ID | Scenario | Variant matrix | Purpose |
|---|---|---|---|
| `t3c-durable-crash-recovery` | Write → ack → SIGKILL → restart → Read byte-exact | walstore × smartwal | **Canonical G4 pass gate** |
| `t3c-durable-fsync-boundary` | Unflushed Write → kill → restart → Read pre-or-post bytes, never corrupt | walstore × smartwal | fsync semantics |
| `t3c-durable-disk-fill` | Fill backing store → Write → graceful error | walstore × smartwal | fail-hard |
| `t3c-durable-wal-replay` | N acked writes + crash → N recoverable | walstore × smartwal | WAL replay correctness |
| ~~`t3c-durable-crash-during-sync`~~ (**DEFERRED** to inventory) | ~~Sync atomicity boundary; V2 parity (CP13)~~ | — | Post-sw-commit addition; shipped 4-scenario set covers canonical G4 + adjacent; inventoried as B-tier |
| ~~`t3c-durable-restart-loop`~~ (**DEFERRED** to inventory) | ~~Multi-recover drift; V2 parity (t0-hosting-smoke)~~ | — | Same — inventoried as B-tier post-T3-closed follow-up |

Each scenario lands as:
- YAML spec in `testrunner/scenarios/testdata/` (shape documentation)
- Go replay in `core/frontend/durable/scenario_*_test.go` — matrix-parameterized over both impls via `logicalStorageFactories()` (shared helper from T3a)

### 1.2 Frontend continuity smoke

Existing T2 iSCSI + NVMe smoke tests re-run with `DurableProvider` replacing `testback.StaticProvider` wiring in `cmd/blockvolume` test harness. No new tests — regression against durable backend.

### 1.3 Perf baseline (characterization only, NO threshold)

- Single fio workload: 4 KiB random write, QD=8, 60s
- Run against both walstore + smartwal (matrix per Addendum A #1)
- Output: `testrunner/perf/t3c-durable-baseline.md` with throughput + p50/p99 latency
- NO pass/fail gate; just published numbers per audit §11 G-int.6 and T3 sketch §3.5

### 1.4 Closure report for T3

New doc: `v3-phase-15-t3-closure-report.md`. Structure mirrors `v3-phase-15-t2-closure-report-draft.md`:
- §A batch history (T3.0 / T3a / T3b / T3c commits + status)
- §B delivery summary + non-claims + governance transition
- §C V2 port depth audit (7 muscle files — MATCHES / MATCHES-BETTER / GAP verdicts from T3.0 §10.3)
- §D new ledger rows (6 `INV-DURABLE-*` + supporting)
- §E T3-end three-sign signatures (architect + PM + QA)

Plus: update `v3-invariant-ledger.md` change log with T3c closure batch row.

---

## 2. Acceptance criteria (headings — bodies filled post T3b-close)

~10 criteria expected:
1. All 4 scenarios pass on both impls (walstore + smartwal)
2. Frontend continuity: T2 iSCSI smoke + T2 NVMe smoke pass against `DurableProvider`
3. m01 fs-workload smoke — `iterate-m01-nvme.sh` + equivalent iSCSI run green against durable backend (wall-clock bound: 10 cycles each, not 24h soak)
4. Perf baseline artifact filed; matrix run both impls
5. Full regression including T3a+T3b+T3c suite green
6. Boundary guard clean
7. All 6 new T3 ledger rows ACTIVE: `INV-DURABLE-001 / -WAL-REPLAY-001 / -FSYNC-BOUNDARY-001 / PCDD-DURABLE-DISK-FULL-001 / -IMPL-IDENTITY-001 (T3a) / -OPGATE-001 (T3a) / -PROVIDER-SELECT-001 (T3b) / -RECOVERY-READSIDE-001 (T3b) / -SYNC-WIRED-001 (T3b)`
8. T2's 16 existing ACTIVE rows still ACTIVE
9. Closure report `v3-phase-15-t3-closure-report.md` filled end-to-end
10. **T3-end three-sign** (architect + PM + QA per §8C.1)

---

## 3. Stop rules (all inherited — no T3c-specific additions expected)

- Port-model default (T3 sketch §6 + audit §5 + T3a §3 + T3b §3)
- No scope creep into T4 (replication, failover, rebuild) — §8C.3 trigger #3
- No new production surface beyond T3a+T3b deliverables (perf harness + scenario replays are test infra only)

---

## 4. Ledger row plan (queued ACTIVE at T3c close)

| ID | Where tested |
|---|---|
| `INV-DURABLE-001` | Scenario `t3c-durable-crash-recovery` |
| `INV-DURABLE-WAL-REPLAY-001` | Scenario `t3c-durable-wal-replay` |
| `INV-DURABLE-FSYNC-BOUNDARY-001` | Scenario `t3c-durable-fsync-boundary` |
| `PCDD-DURABLE-DISK-FULL-001` | Scenario `t3c-durable-disk-fill` |

Plus continuity validation: T2 frontend rows (NVMe + iSCSI) proven to survive backend swap from memback to durable.

---

## 5. Sign-off

### 5.1 QA single-sign (T3c open)

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ **SIGNED — T3c OPENED** per §8C.2 |

### 5.1.1 T3c CLOSE (§8C.2 single-sign)

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ **T3c CLOSED**. Sw delivered commit `829c6a9` (4 scenarios × 2 impls + perf first-light + 4 YAML shape docs) + `5c33460` (T3b follow-up integration matrix) meeting mini-plan §2 acceptance 7/7 sw-side. QA completed #3 m01 fs-workload 4-matrix green + #9 closure report review. 2 scenarios (crash-during-sync + restart-loop) deferred to inventory per §1.1 revision. Ready for T3-end three-sign. |

Sw may now begin T3c code commit.

### 5.2 T3c-end three-sign (post-T3c delivery)

| Role | Signer | Date | Decision |
|---|---|---|---|
| Architect | _________ | _________ | ⏸ |
| PM | _________ | _________ | ⏸ |
| QA Owner | _________ | _________ | ⏸ |

**Effect upon three-sign**: T3 CLOSED, Gate G4 passes, P15 advances to next gate (G5 Replicated Write Path per canonical roadmap OR whichever canonical sequence defines).

---

## 6. Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial skeleton drafted while sw works T3b; detail LOCKED post-T3b-close | QA Owner |
| 2026-04-22 | Post-T3b LOCK + QA single-sign | QA Owner |
| 2026-04-22 | V2-parity scope addition: §1.1 expanded from 4→6 scenarios (`t3c-durable-crash-during-sync` + `t3c-durable-restart-loop`). Rationale: V2 CP13 + t0-hosting-smoke 20-cycle parity. Wall-clock +half-day; zero impact on T3b close. QA L1 addendum (3 tests in `t3b_qa_l1_addendum_test.go`) landed separately in T3b. L3 `iterate-m01-nvme.sh` extended with Matrix C (1000 small files, group_commit batching) + Matrix D (5 remount cycles, cross-session consistency). | QA Owner |
