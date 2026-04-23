# V3 Phase 15 T3 — Local Durable Data Path (G4) — Port Plan Sketch

**Date**: 2026-04-22 (rev-2 post PM/architect review)
**Status**: **DRAFT v2 — external review** (PM / architect). Major rewrite from v1 to align with canonical P15 roadmap.
**Author**: QA Owner (rev-2)
**Predecessor**: T2 closed (Gate G3) 2026-04-22
**Gate**: **G4 Local Durable Data Path**, per `v3-phase-15-mvp-scope-gates.md` §G4 + `v3-phase-15-product-plan.md` §T3
**Governing methodology**: §8B PCDD + §8C accelerated-cadence governance

---

## 0. For the reviewer (read first)

**What this doc is**: T3 scope + port plan. T3 = **G4 Local Durable Data Path** — V3 frontend I/O reaches real local block storage with durable write/recovery semantics.

**What's already decided (no review needed)**:
- T2 closed 2026-04-22 (frontend protocols + product attach proven end-to-end)
- §8C Accelerated-Cadence Governance active (mid-T QA-solo; T-boundary three-sign)

**What needs reviewer decision** (only these):
- §3 IN-list aligns with canonical 7-file V2 port list + 2 new components
- §4 OUT-list preserves P15 track separation (no replication / no failover / no security spill-in)
- §7 A-tier invariants (5 new ledger rows, named under `INV-DURABLE-*` / `PCDD-DURABLE-*` per canonical ledger taxonomy)
- §11 open questions + QA defaults
- §12 signatures

**v1 → v2 rewrite summary (what changed vs previous sketch)**:
- v1 framed T3 as "Perf Gate + Mechanism Debt Paydown" — this was scope drift; the canonical P15 roadmap defines T3/G4 as Local Durable Data Path. v2 corrects this.
- Perf content moved from "gate" to "characterization smoke only, NOT a pass/fail gate" (per review: formal perf SLO belongs at G21/T8 or Final Gate)
- `L1B-2 FailoverMidWrite` removed from IN-list (canonical: G8/T6)
- `Prometheus metrics export` removed from production deliverable (canonical: G17/T8 diagnostics)
- Ledger row names corrected: `PCDD-PERF-*` → `INV-DURABLE-*` / `PCDD-DURABLE-*`

See §13 Change log + `v3-phase-15-t3-sketch-v1-retraction.md` for the v1 audit trail.

---

## 1. T3 Purpose — One Sentence

Deliver real local durable storage behind the V3 frontend — Write / Read / Flush / Close survive a crash and restart, with ack'd data recoverable byte-exact.

## 2. Gate Name

**G4 — Local Durable Data Path**. Exact name from `v3-phase-15-mvp-scope-gates.md` §G4. No invention.

## 3. Scope (IN-list)

### 3.1 V2 port (canonical 7 files per `v3-phase-15-mvp-scope-gates.md` §G4 + `v3-phase-15-product-plan.md` §T3)

| # | V2 file | Category | Scope |
|---|---|---|---|
| 1 | `weed/storage/blockvol/logical_storage*.go` | M | Port — V3's per-volume durable storage abstraction |
| 2 | `weed/storage/blockvol/smartwal*.go` | M | Port — WAL format + append + replay |
| 3 | `weed/storage/blockvol/wal_*.go` | M | Port — WAL writer, admission, pressure handling |
| 4 | `weed/storage/blockvol/dirty_map.go` | M | Port — page-dirty tracking for recovery |
| 5 | `weed/storage/blockvol/group_commit.go` | M | Port — batched fsync coordinator |
| 6 | `weed/storage/blockvol/write_gate.go` | M | Port — admission gate tied to fencing |
| 7 | `weed/storage/blockvol/superblock.go` | M | Port — volume metadata + epoch persistence |

Port discipline: **port-model faithful** per `v3-phase-15-t2-port-model-decision.md` §D2. No pragmatic simplification. V2 behaviors + invariants preserved; rebind storage IO calls to V3's backend interface.

**Pre-code T3.0 function-level port audit (mandatory before any T3 code commit)**:

Each of the 7 M-category files contains a mix of pure-mechanism code and V2-specific authority/storage assumptions. `write_gate.go`, WAL-pressure handling paths, and `superblock.go` epoch fields in particular may carry V2 behaviors that don't rebind 1:1 to V3 (e.g., V2 epoch was sometimes locally advanced; V3 authority is master-published only — `PCDD-STUFFING-001`). Before sw writes the first line of T3 production code, sw produces `sw-block/design/v3-phase-15-t3-port-audit.md` classifying each function in the 7 files as one of:

| Verdict | Meaning | Required annotation |
|---|---|---|
| `PORT-AS-IS` | Pure mechanism; copy verbatim, adjust imports only | — |
| `PORT-REBIND` | Mechanism + V2-specific call-site; port logic, rebind call-site to V3 equivalent | Explicit rebind target (V3 type / method / config) |
| `DEFER` | Mechanism not required for G4 correctness; postpone | Target track (T4 / G21 / etc.) |
| `NEVER` | Contains authority-as-storage mixing, retry-as-authority, or other E-category pattern | Citation of stop rule violated |

QA reviews the audit before signing T3a mini port plan. This is the dual-line port discipline (§8B.2 port-model + §8C.3 trigger #4 no pragmatic evolution) applied at function granularity.

### 3.2 New V3 components (per canonical §G4 "Make new")

1. **V3 local storage adapter with epoch/session/fence awareness** — connects ported V2 durable engine to V3 `frontend.Backend` + authority lineage (Epoch / EndpointVersion / ReplicaID / Healthy). Stale-lineage writes/reads return `ErrStalePrimary` (invariant already pinned by `INV-FRONTEND-002.*`; this adapter must preserve the pin under durable backend). **Adapter consumes published authority facts; it does not mint, advance, or publish Epoch / EndpointVersion / assignment — those remain master/authority's sole responsibility (`PCDD-STUFFING-001` boundary).**
2. **Recovery integration (read-side only)** — on restart: reload superblock → replay WAL → resync dirty_map → expose **recovered readiness** to the existing volume host / adapter projection path (same surface T1 built). Volume host then goes through its normal `HealthyPathExecutor` flow; master authority / publisher remains the ONLY source of Epoch / assignment publication. If recovered local state disagrees with the master-assigned lineage (e.g., superblock epoch > assigned epoch, or persisted writes beyond assigned EV), the volume reports **NotReady** with diagnostic evidence and refuses to serve I/O until master-side reconciliation. Storage adapter does NOT "publish epoch advance" — it observes, recovers, and reports.

### 3.3 Scenario coverage (canonical pass-gate translation)

Canonical pass-gate quoted: *"crash/restart test writes acknowledged data through the real frontend, kills/restarts the local process, and reads the acknowledged data back"*.

Materialized as scenario YAML + Go replay:

| ID | Scenario | Purpose |
|---|---|---|
| `t3-durable-crash-recovery.yaml` | Write 32 KiB at LBA 0 → ack → `SIGKILL` volume → restart → Read LBA 0 returns same bytes | Canonical G4 pass gate |
| `t3-durable-fsync-boundary.yaml` | Write unflushed → kill before sync → restart → Read may return pre-write bytes OR post-write bytes (either spec-legal), never corrupted | fsync semantics |
| `t3-durable-disk-fill.yaml` | Fill backing store → write → graceful error (no silent success, no corrupt) | fail-hard semantics |
| `t3-durable-wal-replay.yaml` | Write N times → kill between writes → restart → first N acked writes recoverable | WAL replay correctness |

### 3.4 Frontend continuity (from T2)

iSCSI + NVMe frontends from T2 MUST continue working, now backed by the durable storage (not memback). A T2-style smoke (attach + mkfs + mount + write + sync + umount + disconnect) MUST pass against the durable backend. This is the frontend-to-durable-backend integration, not new frontend surface.

### 3.5 Perf characterization (smoke only, NOT a gate)

Per review: T3 is NOT a perf gate. Perf numbers **published as baseline artifact**; no pass/fail threshold. Formal perf SLO lives at G21 / Final Gate.

Content:
- fio harness runs one fixed workload (e.g., 4 KiB random write × 60s) against T3 durable backend
- Numbers captured to `t3-perf-baseline.md` as characterization
- Optional: same run against T2 memback for comparison context
- **No `PCDD-PERF-*` ledger row**; **no V2-vs-V3 threshold**; only "published measurements exist"

## 4. Scope (OUT-list — explicit)

| Item | Reason | Where it belongs |
|---|---|---|
| Replication (WAL shipping, shipper group, replica apply, barrier) | Canonical T4/G5 | T4 per `v3-phase-15-product-plan.md` §T4 |
| Rebuild / catch-up | Canonical T5/G6 or G7 | T5 per product plan |
| Failover primary move (incl. L1B-2 FailoverMidWrite) | Canonical G8/T6 | G8/T6 |
| ALUA / CHAP / multi-path / auth | S-category | T7/T8 per product plan |
| Prometheus metrics production export | G17/T8 diagnostics surface | T8 per canonical |
| Formal perf SLO / V2 pass/fail threshold | Not a G4 concern; data-path correctness first | G21 / Final Gate |
| 24h soak / chaos / endurance | G22 Final Gate | G22 |
| CSI driver | Depends on G4 passed | per product plan, CSI follows G3+G4 |
| RF-1 bufpool (NVMe) | M\* perf-adjacent; not required for G4 correctness | T3 MAY include if it's blocking a test harness; otherwise defer to T3 perf-smoke follow-up or G21 |

## 5. V2 port-debt map (M/M\*/E/S per §4A port-migration plan)

Post-T2, durable-side gap analysis:

| V2 file | Category | V3 status after T2 | T3 action |
|---|---|---|---|
| `logical_storage*.go` | M | MISSING | **Port in T3** |
| `smartwal*.go` | M | MISSING | **Port in T3** |
| `wal_*.go` | M | MISSING | **Port in T3** |
| `dirty_map.go` | M | MISSING | **Port in T3** |
| `group_commit.go` | M | MISSING | **Port in T3** |
| `write_gate.go` | M | MISSING | **Port in T3** |
| `superblock.go` | M | MISSING | **Port in T3** |
| `adapter.go` (V2 storage glue) | E | Correctly NOT ported in T2 | NOT ported; replaced by new V3 storage adapter (§3.2 item 1) |
| `write_retry.go` | E | Correctly NOT ported; pinned by `PCDD-NVME-IO-NO-TARGET-RETRY-001` | NOT ported; confirm `PCDD-NVME-IO-NO-TARGET-RETRY-001` still ACTIVE post-T3 |

## 6. Stop rules (mechanical)

1. Boundary guard hard — frontend + storage MUST NOT import `core/authority` or `core/adapter` (PCDD-STUFFING-001 scope extends to new storage package)
2. Port-model default for the 7 M files — pragmatic evolution forbidden; any deviation requires written rationale + §8C.3 trigger #4 escalation
3. No retry-as-authority (`write_retry.go` stays out) — pinned by existing `PCDD-NVME-IO-NO-TARGET-RETRY-001`
4. Advertised ≡ implemented: any new frontend-visible capability advertising durability MUST have an ACTIVE ledger row pinning the durability behavior
5. A-tier ledger rows stay GREEN — T2's 16 rows (iSCSI VPD + NVMe Identify/CNTLID/IO-no-retry/etc.) must remain ACTIVE after T3 lands
6. No scope creep into replication / failover / security — violations trigger §8C.3 #3 (cross-T pollution)

## 7. A-tier invariants to add in T3

Per canonical ledger taxonomy (`INV-*` for positive invariants, `PCDD-*` for named bad-state families):

| ID (provisional) | Invariant | Layer |
|---|---|---|
| `INV-DURABLE-001` | Acknowledged Write survives process kill + restart; Read after restart returns byte-exact what was ack'd | Unit + Scenario |
| `INV-DURABLE-WAL-REPLAY-001` | N acknowledged writes followed by crash → all N recoverable via WAL replay; no ack'd write lost | Unit + Scenario |
| `INV-DURABLE-FSYNC-BOUNDARY-001` | Unflushed Write may be lost on crash (spec-legal); flushed Write MUST be preserved. No partial / corrupted reads on either side of boundary | Unit + Scenario |
| `PCDD-DURABLE-DISK-FULL-001` | Backing store exhaustion returns hard error to frontend; no silent success; no corrupted state (dirty_map consistent post-error) | Unit + Scenario |
| `INV-DURABLE-EPOCH-PERSISTED-001` | Epoch advances persisted through superblock; restart reloads current epoch, rejects stale-lineage I/O (preserves `INV-FRONTEND-002.EPOCH` under durable backend) | Unit + Component |

## 8. Acceptance sign checklist (for T3-end)

| # | Gate | Owner |
|---|---|---|
| 1 | 5 new A-tier ledger rows ACTIVE (§7) | QA |
| 2 | Canonical G4 pass-gate scenario (write / kill / restart / read-back byte-exact) green | sw + QA |
| 3 | 4 scenario YAMLs (§3.3) green with Go replay | sw + QA |
| 4 | T2 frontend smoke (iSCSI + NVMe) still green against durable backend (§3.4) | QA |
| 5 | Full regression green: `go test ./core/...` | both |
| 6 | Boundary guard green (incl. new storage package) | both |
| 7 | T2's 16 existing ACTIVE ledger rows still ACTIVE | QA |
| 8 | Perf baseline published as artifact (`t3-perf-baseline.md`); no threshold | QA |
| 9 | Closure report for T3 with ledger rows + commit hashes | QA |
| 10 | **T3-end three-sign** per §8C.1 | architect + PM + QA |

## 9. Estimated size

| Metric | Estimate |
|---|---|
| Production LOC delta (ported V2 + new adapter + recovery integration) | ~1500-2500 |
| Test LOC delta (unit + scenario YAMLs + replay + frontend continuity) | ~1500-2500 |
| Wall-clock | 5-8 work days |
| Main risks | (a) V2 durability code has subtle correctness invariants not apparent from top-level shape — faithful port with V2 test suite re-run is load-bearing; (b) V3 adapter must preserve `INV-FRONTEND-002.*` facet rows under durable backend (regression risk) |

## 10. Inventory items NOT carried into T3

From `sw-block/design/bugs/inventory/nvme-test-coverage-deferred.md`, the following remain inventoried and are NOT T3 scope:

- L1B-2 FailoverMidWrite → G8/T6 (failover data continuity track)
- L1B-5 MultiVolumeConcurrent → depends on harness change in T3 but the test itself is perf-adjacent; defer to T3-end's optional perf-smoke or G21
- All L2-B scenarios except the 4 durability ones in §3.3 → T4+ or G22
- L3B / L4B endurance → G22 Final Gate
- Replication-related (L1B-2 failover, any HA scenario) → T4+ / G8

Quarterly rotation per inventory doc §Rotation audit continues.

## 11. Open questions (QA-default answers below)

| # | Question | Reviewer decision needed? |
|---|---|---|
| 1 | Perf characterization workload — single fio pattern or small matrix? | Yes |
| 2 | Baseline hardware for perf smoke — m01 + M02 only? | Yes |
| 3 | V2 durability behavior vs V3: where can V3 legitimately diverge? (e.g., V2 had specific retry-within-WAL behavior that overlaps with `write_retry.go` — must confirm that part is NOT ported) | Yes |
| 4 | Recovery integration with epoch advance — who publishes post-recovery epoch? (new adapter's responsibility vs an existing publication path) | Yes |

### 11.5 QA default answers (accept by not editing; override per-line if you disagree)

| # | Question | QA-default |
|---|---|---|
| 1 | Perf workload | Single fio pattern: 4 KiB random write, 60s, 8 IO depth. One number to publish. Matrix is G21 concern. |
| 2 | Baseline hardware | m01 + M02 for T3; second-class hardware at G22. |
| 3 | V2 durability / `write_retry.go` boundary | `write_retry.go` stays NOT ported (E-category, pinned by `PCDD-NVME-IO-NO-TARGET-RETRY-001`). Any retry-looking code inside the 7 M files (e.g., WAL admission-retry-on-pressure) is port-model-literal: ported if it's part of the ack path; rejected if it's authority-layer retry-as-authority. Concrete file-by-file triage done during port audit (sw produces during T3 kickoff). |
| 4 | Recovery → readiness path (NOT epoch publish) | **Storage adapter does NOT publish epoch.** On restart, durable storage reloads superblock/WAL and exposes **recovered readiness** (a local-state signal, not an authority fact) to the existing volume host / adapter path built in T1. Epoch / EndpointVersion remain master-published authority facts (`PCDD-STUFFING-001`). If recovered local state disagrees with assigned lineage (e.g., superblock epoch > assigned epoch), volume reports `NotReady` with evidence; master-side reconciliation governs. Storage layer observes + recovers + reports; it does not mint, advance, or publish authority. |

## 12. Change log + signatures

### 12.1 T3-start signatures (required for sw to begin coding)

| Role | Signer | Date | Decision |
|---|---|---|---|
| Architect | pingqiu (acting) | 2026-04-22 | ✅ approved rev-2 pending 2 fixes (applied); fixes landed in rev-2.1 |
| PM | pingqiu (acting) | 2026-04-22 | ✅ approved rev-2 pending 2 fixes (applied); fixes landed in rev-2.1 |
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ signed rev-2.1 post-fixes |

Once all three sign, sw + QA execute T3 per §8C (mid-T QA-solo signing). T3-end returns to three-sign.

### 12.2 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial sketch (v1) — proposed "Perf Gate + Mechanism Debt Paydown" framing | QA Owner |
| 2026-04-22 | **v1 REJECTED** by PM/architect review: canonical T3/G4 is Local Durable Data Path, not perf. v1 was scope drift. Specific findings: perf-as-gate wrong position, missing 7 V2 durable files, L1B-2 FailoverMidWrite belongs to G8/T6 not T3, Prometheus metrics belongs to G17/T8, ledger row names `PCDD-PERF-*` incorrect taxonomy. | PM/architect |
| 2026-04-22 | v2 rewrite per review: gate identity restored to **G4 Local Durable Data Path**; 7 V2 files listed per canonical `v3-phase-15-mvp-scope-gates.md` §G4; perf demoted from gate to smoke-only characterization (no threshold); L1B-2 + Prometheus removed; ledger rows renamed `INV-DURABLE-*` / `PCDD-DURABLE-*`; §11 open questions aligned with durable semantics | QA Owner |
| 2026-04-22 | **v2.1 fixes per PM/architect rev-2 review (signable)**: (a) §3.2 + §11.5 #4 corrected — storage adapter does NOT publish epoch; recovery exposes readiness only; master-published authority is sole source of Epoch/EV per `PCDD-STUFFING-001`. (b) §3.1 added mandatory **T3.0 function-level port audit** prerequisite — sw classifies each function in the 7 M files as PORT-AS-IS / PORT-REBIND / DEFER / NEVER before any code commit. T3-start three-sign complete. | PM/architect + QA Owner |

---

## 13. Governance lesson (for audit trail)

v1 is a case study for **§8C Rule**: *"accelerated cadence speeds up internal decisions (who reviews, how many signs), but the canonical delivery scope is immutable — review must enforce alignment with the roadmap."* QA Owner's first draft drifted T3 from Local Durable Data Path (canonical) to Perf Gate (invented). PM/architect review caught it. The lesson:

- QA Owner has mid-T single-sign authority under §8C.2 for **implementation decisions** (test shapes, bug fix bundling, port-model vs pragmatic-evolution calls inside the ported files)
- QA Owner does NOT have authority to redefine track identity / gate definition / canonical V2 port sources
- T-boundary three-sign exists specifically to catch this drift; it worked as designed

No rule change needed — §8C.1 three-sign-at-T-boundaries is already the enforcing mechanism. This doc (§13) is the audit record.
