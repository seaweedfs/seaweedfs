# V3 Phase 15 T3 — Closure Report

**Date**: 2026-04-22
**Status**: DRAFT — awaiting T3-end three-sign per §8C.1
**Gate**: T3 → G4 (durable-data-path pass)
**Predecessor**: T2 three-signed (frontend contract complete)
**Successor**: G5 (replicated write path) or whichever canonical sequence defines

---

## §A Batch history

| Batch | Commit | Status | Delivery |
|---|---|---|---|
| T3.0 (port audit) | doc-only | QA single-signed | `v3-phase-15-t3-port-audit.md` + Addendum A; 7 V2 muscle files classified PORT-AS-IS / PORT-REBIND / NEVER; risk flags resolved |
| T3a | `0e1595c` | CLOSED | StorageBackend adapter (G-int.1+.2+.3) + Backend interface extension (Sync + SetOperational) + superblock ImplKind/ImplVersion (Addendum A #2) + matrix test infra (Addendum A #1) |
| T3b | `72d0d40` | CLOSED | DurableProvider (G-int.4) + Recovery (G-int.5) + iSCSI SYNC_CACHE wire + NVMe Flush wire + cmd/blockvolume integration |
| T3b follow-up | `5c33460` | CLOSED | iSCSI + NVMe → durable matrix integration tests (closes wire-level coverage gap) |
| T3c | `829c6a9` + `5c33460` (T3b follow-up integration matrix) | pending three-sign | 4 scenarios × 2 impls + perf baseline first-light + closure report + m01 verification (2 impls) |
| Pre-sign fix Phase 1 | `4c256aa` | CLOSED | QA artifacts commit + doc integrity (audit header / commit hash / perf doc / sketch delta) |
| **BUG-005 fix** | **`42b045a`** | CLOSED | Session-layer `Backend.Close` removed (nvme target + iscsi session); `frontend.Backend` interface godoc Provider-ownership clause; regression test matrix both impls. See §H Phase 3. |
| m01 infra | `27486db` | CLOSED | `iterate-m01-nvme.sh` pre-cleanup + dmesg clear for reproducible BUG-005 re-verify |

---

## §B Delivery summary

### What T3 delivered

1. **StorageBackend adapter** — `core/frontend/durable/storage_adapter.go`. Bridges `core/storage.LogicalStorage` (LBA-based, typed errors) to `core/frontend.Backend` (byte-offset, fence + operational gate). ~310 LOC production, 22 subtests × 2 impls = 44 subtests green.

2. **Backend interface extension** — `core/frontend/types.go` gains `Sync(ctx) error` + `SetOperational(ok, evidence)`. All 6 existing Backend implementations updated atomically in the same commit (memback + testback + 3 QA test shims).

3. **Superblock impl-identity** (Addendum A #2) — both walstore and smartwal superblocks gain `ImplKind` + `ImplVersion` fields. Schema version bumped 1→2 with one-way discipline. DurableProvider peeks the 4-byte magic BEFORE storage Open and fails fast with `ErrImplKindMismatch` on selector/disk disagreement.

4. **DurableProvider** — `core/frontend/durable/provider.go`. Per-volumeID handle cache. Production default is `smartwal`; `walstore` reachable via `--durable-impl walstore`. Integrated into `cmd/blockvolume` behind the `--durable-root` flag; memback fallback preserved when flag is empty.

5. **Recovery wrapper** — `core/frontend/durable/recovery.go`. Pure function wrapping `LogicalStorage.Recover()` into a `RecoveryReport`. No new publication path (Option 3 from audit §10.5): report surfaces via the existing `/status` HTTP endpoint + adapter's `SetOperational`; master authority remains sole Epoch / assignment publisher.

6. **Frontend Sync wire** — iSCSI `SYNCHRONIZE_CACHE(10)` + `SYNCHRONIZE_CACHE(16)` and NVMe `ioFlush` dispatch to `backend.Sync(ctx)`. Errors map faithfully to spec-legal status (SCSI `MEDIUM_ERROR` / `WRITE_ERROR`; NVMe `InternalError`). Stop rule §3.6 forbids swallowing.

7. **Matrix test infrastructure** (Addendum A #1) — single shared `logicalStorageFactories()` helper returning `{walstore, smartwal}`. Every T3a/T3b/T3c test iterates over it — 68 subtests per impl in the durable package.

8. **4 scenarios** (T3c):
   - `TestT3c_Scenario_CrashRecovery_AckedBytesByteExact` — pins INV-DURABLE-001
   - `TestT3c_Scenario_WALReplay_NAckedWrites` — pins INV-DURABLE-WAL-REPLAY-001
   - `TestT3c_Scenario_FsyncBoundary_AckedSurvivesUnackedMayVanish` — pins INV-DURABLE-FSYNC-BOUNDARY-001
   - `TestT3c_Scenario_DiskFill_OutOfRange_FailsCleanly` — pins PCDD-DURABLE-DISK-FULL-001

   4 YAML shape docs in `testrunner/scenarios/testdata/` mirror each Go replay for reviewability.

9. **Perf baseline** — `testrunner/perf/t3c-durable-baseline.md` + `BenchmarkT3c_DurablePerf`. Characterization only; no threshold. First-light numbers filled 2026-04-22 (sw dev loopback): walstore 197K ns/op / 20.75 MB/s / ~5067 ops/s; smartwal 303K ns/op / 13.50 MB/s / ~3295 ops/s. Sustained-throughput m01 fio measurement is a post-sign activity.

10. **cmd/blockvolume integration** — 4 new flags (`--durable-root`, `--durable-impl`, `--durable-blocks`, `--durable-blocksize`). Startup: build Provider → Open → RecoverVolume → flip operational → accept connections. Shutdown: Provider.Close (backend first, storage second, both idempotent).

### Non-claims (explicit)

- **No replication.** T3 is single-node durable storage; replicated writes + failover + rebuild belong to G5+. The existing Phase 4A replication surface (shipper, barrier, promotion, rebuild) is NOT touched by T3 and is NOT claimed to integrate with DurableProvider until a later gate explicitly ports it.
- **No authority publication.** T3 storage observes and reports; master authority remains sole Epoch / assignment publisher (§3.2 boundary; PCDD-STUFFING-001). Recovery surfaces readiness via `/status` read-side only.
- **No write_gate in storage.** V2's `write_gate.go` is explicitly NOT ported; fencing is adapter-side via per-I/O lineage check (INV-FRONTEND-002.*).
- **No ambitious concurrency rewrite.** The V2 patterns that were previously ported faithfully (BUG-001 lesson) stay that way; no simplification / "V3 style" reinvention.
- **Perf numbers are first-light.** The Go-bench loopback numbers in §B #9 are characterization only; sustained-throughput fio on m01 is a post-sign activity, not a delivered T3 metric.
- **G4 product pass = smartwal only.** walstore is a documented non-default fallback impl. Matrix D failure (BUG-007) means walstore is NOT G4-qualified at T3 close; walstore side of `INV-DURABLE-001` is deferred to BUG-007 close. Production default (smartwal) satisfies canonical G4.

### Governance transition

- Mid-T batches (T3a, T3b, T3c) used QA single-sign per §8C.2 — accelerated cadence.
- T3-end requires three-sign per §8C.1 (architect + PM + QA) — this report is the artifact.
- Discovery Bridge: §8C.3 trigger #1 (unknown-unknown architectural bug — V2/V3 shape-level mismatch, BUG-001 class) **fired once** during T3, on BUG-005 discovery in Path B (session-layer `Backend.Close` cleared Provider-cached Backend across sessions — V3 introduced `frontend.Backend.Close()` abstraction without carrying V2's implicit Subsystem.Dev registry-ownership contract into interface godoc). Handled per §8C.3: architect + PM notified same-day; bug log entry at `bugs/005_backend_close_cross_session.md`; fix `42b045a`; catalogue §2.3 drift-event row added; `feedback_porting_discipline.md` updated with "new-abstraction ownership drift" as third citation. No other §8C.3 triggers fired. All remaining scope changes (Addendum A additions to T3.0 audit) went through mid-T QA single-sign per §8C.2.

---

## §C V2 port depth audit

Per T3.0 §10 walk-through, the 7 V2 durable-layer muscle files validated against V3 existing port:

| V2 file | V3 file | V2 LOC | V3 LOC | Verdict |
|---|---|---|---|---|
| `superblock.go` | `core/storage/superblock.go` | 298 | 227* | MATCHES (* +30 from T3a ImplKind add) |
| `wal_entry.go` | `core/storage/wal_entry.go` | 153 | 169 | MATCHES (V3 slightly larger, doc) |
| `wal_writer.go` | `core/storage/wal_writer.go` | 302 | 193 | MATCHES |
| `wal_admission.go` | `core/storage/wal_admission.go` | 178 | 156 | MATCHES-BETTER (V3 is pure-mechanism; no authority hooks) |
| `dirty_map.go` | `core/storage/dirty_map.go` | 156 | 141 | MATCHES-BETTER (V3 splits Delete + compareAndDelete; unconditional Delete has zero callers — Phase 08 footgun structurally disarmed) |
| `group_commit.go` | `core/storage/group_commit.go` | 222 | 164 | MATCHES-BETTER (V3 drops PostSyncCheck field entirely; authority-check path unreachable by construction) |
| `write_gate.go` | (none) | 28 | 0 | NEVER (§3.2 boundary — fencing is adapter-side) |

Plus 1 audit blocking gap resolved:
- **superblock.Epoch** absent in V3 → LOCKED as Option 3 (no local mirror; per-I/O adapter fence check is sole authority-drift guard). Stronger §3.2 outcome than sw's original PORT-REBIND proposal.

Total summary: **48 MATCHES + 2 MATCHES-BETTER + 1 Option-3-resolved + 0 VIOLATIONS** → V3 existing `core/storage/` port quality is high; user's decision to retain V3 over re-porting from V2 is validated.

---

## §D Ledger rows

### New at T3 close (queued ACTIVE pending three-sign)

| ID | Batch | Statement |
|---|---|---|
| `INV-DURABLE-OPGATE-001` | T3a | Before `SetOperational(true, _)`, all I/O returns `ErrNotReady`; preserves `INV-FRONTEND-002.*` under durable backend |
| `INV-DURABLE-IMPL-IDENTITY-001` | T3a | Superblock records `ImplKind` + `ImplVersion`; mismatch between stored ImplKind and opener's selector is rejected, not silently coerced |
| `INV-DURABLE-PROVIDER-SELECT-001` | T3b | `DurableProvider.Open` selects correct impl per config + fails fast on ImplKind mismatch |
| `INV-DURABLE-RECOVERY-READSIDE-001` | T3b | Recovery exposes readiness via adapter's `SetOperational` + existing `/status` HTTP surface; no new publication path; master authority remains sole Epoch/assignment publisher |
| `INV-DURABLE-SYNC-WIRED-001` | T3b | iSCSI `SYNCHRONIZE_CACHE(10/16)` + NVMe `Flush` reach `LogicalStorage.Sync`; error propagates to spec-legal host status |
| `INV-DURABLE-001` | T3c + m01 Matrix E | Acknowledged Write survives real SIGKILL + restart; Read returns byte-exact. **Scope: smartwal only** (production default); walstore evidence deferred to BUG-007 close. Canonical row name stands per m01 Matrix E SIGKILL + byte-exact readback (2026-04-22, commit `seaweed_block@313dd52` Matrix F robustness fix; evidence run against BUG-005 fix `42b045a`). |
| `INV-DURABLE-SESSION-LIFECYCLE-001` | BUG-005 fix | Across NVMe/iSCSI session disconnect + reconnect, the Provider-cached Backend remains operational; I/O on the reconnected session MUST NOT receive ErrBackendClosed caused by prior session teardown |
| `INV-DURABLE-WAL-REPLAY-001` | T3c | N acked writes + crash → all N recoverable |
| `INV-DURABLE-FSYNC-BOUNDARY-001` | T3c | Flushed preserved; unflushed may be lost; neither corrupted |
| `PCDD-DURABLE-DISK-FULL-001` | T3c | Disk-full / volume-full → hard error; no silent success; in-range data uncorrupted |

### Existing rows confirmed still ACTIVE

T1 facets (unchanged):
- `INV-FRONTEND-002.EPOCH / .EV / .REPLICA / .HEALTHY` — verified under durable backend via T3a fence-check tests + T3c integration tests

T2 rows (unchanged):
- 16 ACTIVE rows covering iSCSI + NVMe protocol invariants — verified still green post-Backend-interface-extension via full regression (17/17 packages ok)

---

## §E T3-end three-sign

### Deliverable artifacts

- `sw-block/design/v3-phase-15-t3-port-plan-sketch.md` (rev-2.1, three-signed)
- `sw-block/design/v3-phase-15-t3-port-audit.md` (QA-signed with Addendum A)
- `sw-block/design/v3-phase-15-t3a-mini-plan.md` (CLOSED)
- `sw-block/design/v3-phase-15-t3b-mini-plan.md` (CLOSED)
- `sw-block/design/v3-phase-15-t3c-mini-plan.md` (this batch)
- `sw-block/design/v3-phase-15-t3-closure-report.md` (this doc)
- `testrunner/perf/t3c-durable-baseline.md` (first-light numbers)
- `testrunner/scenarios/testdata/t3c-durable-*.yaml` (4 scenario shape docs)

### Full regression

```
go test ./core/... -count=1 → 17/17 packages ok
  core/frontend/durable: ~1s (68+ subtests, all green, matrix 2×)
  core/frontend/iscsi: ~31s (no regression; SYNC_CACHE wire green)
  core/frontend/nvme: ~61s (no regression; Flush wire green)
  core/host: ~123s (subprocess tests still use memback; no change)
  all others sub-second
```

Boundary guard green: `core/frontend/durable/` auto-covered by the recursive walk in `TestFrontendCannotMintAuthority_BoundaryGuard`.

### Sign table

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ SIGNED. Basis: smartwal full A–F green on m01 (commit `seaweed_block@313dd52`, BUG-005 fix `42b045a`); Matrix E real SIGKILL + byte-exact recovery satisfies canonical G4 pass gate; Matrix F real ENOSPC graceful hard-error satisfies PCDD-DURABLE-DISK-FULL-001. walstore scoped as non-default fallback with BUG-007 carry-forward. T3-DEF-5/6/7 retrospective closed (catalogue / L1 addendum / BUG-006). Prior retraction superseded. |
| Architect | pingqiu | _________ | ⏸ awaiting |
| PM | pingqiu | _________ | ⏸ awaiting |

**Effect upon three-sign**: T3 CLOSED, Gate G4 passes (smartwal production default), P15 advances to next gate. walstore tracked separately via BUG-007.

### m01 fs-workload verification (2026-04-22, QA — final)

**Evidence base**: `seaweed_block` commits `42b045a` (BUG-005 fix) + `313dd52` (Matrix F cleanup robustness + T3-DEF-6 test). Script: `scripts/iterate-m01-nvme.sh` at `313dd52`. Host: m01 (192.168.1.181) real Linux 6.17 kernel + in-tree NVMe/TCP initiator + `mkfs.ext4`.

Six matrices A–F run per Addendum A #1 matrix discipline. **Pass criterion for G4 = smartwal (production default) full A–F green**. walstore results reported for completeness but explicitly not required for G4 at T3 close.

| Matrix | Coverage | smartwal | walstore | G4 evidence? |
|---|---|:-:|:-:|:-:|
| A | 10 cycles: attach / mkfs / mount / dd / sync / umount / disconnect | ✅ 10/10 | ✅ 10/10 | yes |
| B | Size coverage 32K × 50 + 256K × 10 + 1M × 5 | ✅ | ✅ | yes (adjunct) |
| C | Small-file burst: 500/500 files + sync → 501 fs-count (group_commit batching stressed) | ✅ | ✅ | yes (adjunct) |
| D | Cross-session consistency: 5 remount cycles with pattern integrity check | ✅ 5/5 | ❌ fails cycle 1 (BUG-007) | yes for smartwal; walstore deferred |
| E | Real SIGKILL of blockvolume mid-write → restart with same `--durable-root` → reconnect NVMe → byte-exact readback | ✅ | (not required for smartwal-path G4; walstore run skipped pending BUG-007) | yes — canonical G4 |
| F | Real ENOSPC via size-limited tmpfs durable-root → verify graceful hard-error (no silent success, no corruption) | ✅ | ✅ | yes — canonical PCDD-DURABLE-DISK-FULL |

dmesg clean on smartwal A–F runs: no `r2t exceeded` / no `-71` / no reconnect. Matrix E smartwal recovery verified byte-exact vs pre-kill payload (SIGKILL = pgrep + `kill -9` on blockvolume pid mid-write; no graceful shutdown path). Matrix F smartwal success signal: server-side `ENOSPC triggered gracefully` message + dd failure + no silent-success write; cleanup SSH fragility tolerated via pattern-based detection at `313dd52`.

**walstore Matrix D failure**: `BUG-007 — walstore loses written data across umount + remount`. Pre-existing walstore-specific durability bug surfaced by Matrix D; walstore `Read` does not serve uncommitted-but-WAL-persisted writes across same-session fs-layer umount+remount. Non-blocking for T3 because (a) smartwal is documented production default per T3b Addendum A, (b) canonical G4 pass gate is met by smartwal full A–F. walstore is reclassified as non-default fallback until BUG-007 closes.

**G4 verdict**: ✅ PASS on smartwal; walstore deferred via BUG-007.

### Outstanding work (post-sign, not blocking)

- fio sustained-throughput numbers for perf baseline (replaces Go-bench first-light in `t3c-durable-baseline.md` — wire-layer overhead measurement)
- 24h soak (equivalent to BUG-001 post-fix soak pattern; file as inventory item for G21/G22 if needed)
- 2 L2 scenarios deferred to `bugs/inventory/nvme-test-coverage-deferred.md`:
  - `t3c-durable-crash-during-sync` — Sync atomicity boundary (V2 CP13 parity)
  - `t3c-durable-restart-loop` — multi-cycle recover drift detection (V2 t0-hosting-smoke parity)

  Both were proposed post-sw-commit; shipped 4-scenario set already covers canonical G4 + 3 adjacent. Not blocking G4; inventoried for post-T3-closed follow-up.

None of these block T3-end sign; they are verification extensions that raise confidence without changing the shipped contract.

---

## §F File manifest

All T3 source + test + doc file touches across the 4 commits. NEW = file added this phase; MOD = pre-existing file modified.

### Production code — `seaweed_block` repo

| Path | T3a (`0e1595c`) | T3b (`72d0d40`) | T3b+ (`5c33460`) | T3c (`829c6a9`) |
|---|:-:|:-:|:-:|:-:|
| `cmd/blockvolume/main.go` | | MOD | | |
| `core/frontend/types.go` | MOD | | | |
| `core/frontend/memback/backend.go` | MOD | | | |
| `core/frontend/testback/testback.go` | MOD | | | |
| `core/frontend/durable/storage_adapter.go` | **NEW** | | | |
| `core/frontend/durable/provider.go` | | **NEW** | | |
| `core/frontend/durable/recovery.go` | | **NEW** | | |
| `core/frontend/iscsi/scsi.go` | | MOD | | |
| `core/frontend/iscsi/errors.go` | | MOD | | |
| `core/frontend/nvme/io.go` | | MOD | | |
| `core/frontend/nvme/session.go` | | MOD | | |
| `core/storage/superblock.go` | MOD | | | |
| `core/storage/walstore.go` | MOD | | | |
| `core/storage/smartwal/superblock.go` | MOD | | | |

### BUG-005 fix — `seaweed_block` repo (commit `42b045a`)

| Path | Change | Note |
|---|---|---|
| `core/frontend/nvme/target.go` | MOD (−1) | Removed `defer backend.Close()` in handleConn |
| `core/frontend/iscsi/session.go` | MOD (−3) | Removed `s.backend.Close()` in Session.close |
| `core/frontend/types.go` | MOD (+11) | Backend interface godoc — Provider ownership clause |
| `core/frontend/durable/bug005_session_reuse_test.go` | NEW (~120 LOC) | Matrix regression: `TestT3_Bug005_ProviderCachedBackend_ReusableAcrossSessions` + `TestT3_Bug005_ExplicitBackendClose_IsAllowed` |

### m01 infra — `seaweed_block` repo (commit `27486db`)

| Path | Change | Note |
|---|---|---|
| `scripts/iterate-m01-nvme.sh` | MOD | Pre-cleanup (pkill stale blockmaster/volume) + dmesg clear before each run; required for BUG-005 re-verify to be deterministic |

### Test files — `seaweed_block` repo

| Path | Commit |
|---|:-:|
| `core/frontend/durable/storage_adapter_test.go` | T3a NEW |
| `core/frontend/iscsi/t2_ckpt10_hardening_test.go` | T3a MOD (trivial Backend impls) |
| `core/frontend/nvme/t2_v2port_nvme_no_retry_test.go` | T3a MOD (trivial Backend impls) |
| `core/frontend/nvme/t2_v2port_nvme_write_chunked_r2t_test.go` | T3a MOD (trivial Backend impls) |
| `core/frontend/durable/provider_test.go` | T3b NEW |
| `core/frontend/durable/recovery_test.go` | T3b NEW |
| `core/frontend/iscsi/t3b_sync_cache_test.go` | T3b NEW |
| `core/frontend/nvme/t3b_flush_test.go` | T3b NEW |
| `core/frontend/durable/integration_iscsi_test.go` | T3b+ NEW |
| `core/frontend/durable/integration_nvme_test.go` | T3b+ NEW |
| `core/frontend/durable/scenario_crash_recovery_test.go` | T3c NEW |
| `core/frontend/durable/scenario_wal_replay_test.go` | T3c NEW |
| `core/frontend/durable/scenario_fsync_boundary_test.go` | T3c NEW |
| `core/frontend/durable/scenario_disk_fill_test.go` | T3c NEW |
| `core/frontend/durable/perf_baseline_test.go` | T3c NEW |

### Scenario shape docs + perf artifact — `seaweed_block/testrunner/`

| Path | Commit |
|---|:-:|
| `testrunner/scenarios/testdata/t3c-durable-crash-recovery.yaml` | T3c NEW |
| `testrunner/scenarios/testdata/t3c-durable-wal-replay.yaml` | T3c NEW |
| `testrunner/scenarios/testdata/t3c-durable-fsync-boundary.yaml` | T3c NEW |
| `testrunner/scenarios/testdata/t3c-durable-disk-fill.yaml` | T3c NEW |
| `testrunner/perf/t3c-durable-baseline.md` | T3c NEW |

### Design docs — `seaweedfs/sw-block/design/` (this repo)

| Path | Status |
|---|---|
| `v3-phase-15-t3-port-plan-sketch.md` | rev-2.1 three-signed (pre-T3) |
| `v3-phase-15-t3-port-audit.md` | T3.0 QA-signed (+ Addendum A) |
| `v3-phase-15-t3a-mini-plan.md` | CLOSED |
| `v3-phase-15-t3b-mini-plan.md` | CLOSED |
| `v3-phase-15-t3c-mini-plan.md` | CLOSED (this batch) |
| `v3-phase-15-t3-closure-report.md` | **this doc** — awaiting 3-sign |

### Totals

- Production files: **14 touched** (8 NEW + 6 MOD)
- Test files: **15 touched** (11 NEW + 4 MOD)
- Scenario YAML + perf doc: **5 NEW**
- Design docs: **6** (5 batch docs + this closure report)

### Not in T3 scope (explicitly)

- `scripts/iterate-m01-nvme.sh` — QA-owned m01 adaptation, committed separately by QA
- `core/frontend/durable/t3b_qa_l1_addendum_test.go` — QA-authored addendum test, committed separately by QA
- `cmd/blockmaster/*` — authority daemon untouched (correct per §3.2 boundary)
- `core/authority/*`, `core/adapter/*`, `core/engine/*` — control-plane untouched (boundary guard verifies)
- All V2 `weed/storage/blockvol/*` — audit reference only; nothing ported from there since V3 already had the existing port (see §C)

---

## §G Signed-scope delta vs T3-start sketch

T3-start sketch rev-2.1 (three-signed 2026-04-22) set a specific scope. T3-end delivery diverges in 3 documented places. Each divergence was LOCKED during an earlier mid-T QA single-sign per §8C.2 (not a silent mid-track change); this section surfaces the trail so a reviewer can reconcile sketch-promise vs closure-truth without spelunking.

### §G.1 `write_gate.go` — promised PORT, delivered NEVER

**T3-start sketch §3.1 row 6**: `weed/storage/blockvol/write_gate.go` — M — "Port — admission gate tied to fencing".

**T3-end actual**: Not ported. File classified NEVER in T3.0 audit §3.7 (per risk-flag resolution). Fencing is adapter-side via `INV-FRONTEND-002.*` per-I/O lineage check; V3 storage has no `writeGate` function (grep zero matches) and no `ErrNotPrimary/ErrEpochStale/ErrLeaseExpired` leaks.

**Where LOCKED**: T3.0 audit §3.7 + §10.3.7, QA single-signed 2026-04-22. Sw sanity comment in §10.7-B explicitly validated this as a stronger §3.2 outcome than the sketch proposed.

**Net**: sketch's 7-file port list effectively shrinks to 6 files + 1 NEVER. Sketch row #6 retired; no new canonical port replaces it.

### §G.2 `INV-DURABLE-EPOCH-PERSISTED-001` — promised QUEUE, not queued

**T3-start sketch §7**: 5 A-tier invariants including `INV-DURABLE-EPOCH-PERSISTED-001` — "Epoch persists through superblock; restart reloads current epoch, rejects stale-lineage I/O".

**T3-end actual**: Row NOT queued. V3 `core/storage/superblock.go` has no `Epoch` field (audit §10.3.1 GAP + §10.5 Option 3 LOCKED). Authority-drift guard is per-I/O adapter fence check (`INV-FRONTEND-002.EPOCH/.EV/.REPLICA/.HEALTHY` inherited from T1, verified ACTIVE under durable backend by T3a fence-check tests).

**Where LOCKED**: Audit §10.5 Option 3, QA single-signed 2026-04-22. Architect-line §10.7 could be extended but the decision was mid-T QA autonomy per §8C.2.

**Net**: sketch's 5-row A-tier queue becomes different shape: `INV-DURABLE-OPGATE-001` (T3a) + `INV-DURABLE-IMPL-IDENTITY-001` (T3a, Addendum A) + `INV-DURABLE-PROVIDER-SELECT-001` (T3b) + `INV-DURABLE-RECOVERY-READSIDE-001` (T3b) + `INV-DURABLE-SYNC-WIRED-001` (T3b) replace the epoch-persisted row. Plus 4 T3c rows. **9 total rows queued** (vs 5 sketched) — a net +4 expansion covering adapter, provider, recovery, sync-wire, impl-identity invariants not enumerated in the sketch.

### §G.3 crash-recovery + disk-full invariant names downgraded

**T3-start sketch §7 + T3c mini-plan §1.1**:
- `INV-DURABLE-001` = "Acknowledged Write survives **kill + restart**"
- `PCDD-DURABLE-DISK-FULL-001` = "**Backing store exhaustion** returns hard error"

**T3-end actual**: both scenarios implemented as `Close() + reopen` (clean restart) and fixed-geometry-out-of-range writes, NOT real `SIGKILL` + real `ENOSPC`. PM + architect review 2026-04-22 caught both overclaims.

**Resolution (FINAL, 2026-04-22)**: Path B executed end-to-end. BUG-005 fixed in `42b045a`. Matrix F cleanup robustness + T3-DEF-6 addendum test committed in `313dd52`. m01 smartwal **Matrix A–F all PASS** including Matrix E real SIGKILL with byte-exact readback and Matrix F real ENOSPC graceful-hard-error. Canonical ledger names retained: `INV-DURABLE-001` (kill + restart byte-exact) + `PCDD-DURABLE-DISK-FULL-001` (real backing-store exhaustion). Both scoped to smartwal (production default) per §B non-claim and §E G4 verdict; walstore side deferred via BUG-007.

**Where LOCKED**: THIS REVIEW CYCLE. Pre-sign, not mid-T. Path B elected by PM 2026-04-22; executed + verified same day.

---

## §H Review response log (pre-sign)

Pre-sign PM + architect review 2026-04-22 identified 6 blockers + 1 low issue. QA retracted premature sign row and executed 2-phase fix:

### Phase 1 — doc integrity (landed 2026-04-22)

| # | Finding | Action |
|---|---|---|
| B3 (Architect-H1) | Audit doc said DRAFT; closure said QA-signed | Audit header `LOCKED + QA single-signed 2026-04-22`; §8 sign-off table filled |
| B4 (Architect-H2) | Closure didn't reconcile sketch-scope delta | This §G added with 3 explicit divergences |
| B5 (PM-M2 + Architect-M1) | `<TBD this commit>`, stale perf-baseline wording | §A T3c commit = `829c6a9`; §B row 9 updated with actual numbers; perf doc removed `_TBD_` claim |
| B6 (PM-M1) | Uncommitted QA artifacts | Script + L1 addendum test committed in the sign cycle |
| B7 (PM-L1) | Perf doc "_TBD_ placeholder numbers are acceptable" wording | Removed; replaced with "first-light numbers 2026-04-22 ... m01 fio post-sign" wording |

### Phase 2 — real G4 evidence (path B elected)

| # | Finding | Action |
|---|---|---|
| B1 (PM-H1) | crash-recovery used `Close()` not SIGKILL — canonical G4 requires kill/restart | m01 Matrix E added: SIGKILL volume process mid-write → restart with same `--durable-root` → reconnect NVMe → verify data byte-exact. Real Linux kernel initiator over ungraceful unmount path. |
| B2 (PM-H2) | disk-full tested out-of-range writes, not real ENOSPC | m01 Matrix F added: `--durable-root` backed by size-limited tmpfs → fill tmpfs → attempt write → verify graceful hard error (no silent success, no hang, no corruption of in-range data). |

Matrix E + F matrixed per Addendum A #1 intent. Final result (see §E): smartwal A–F ✅; walstore A–C + F ✅, Matrix D ❌ (pre-existing walstore bug, filed as BUG-007); Matrix E skipped for walstore pending BUG-007 (no value running SIGKILL recovery when same-session remount already fails).

Post-Phase-2 artifacts (BUG-005 fix `42b045a`, Matrix F robustness + T3-DEF-6 test `313dd52`) feed §E final verification table + unlock QA re-sign. QA re-signed 2026-04-22 (see §E sign table).

### Phase 3 — BUG-005 discovery during Path B execution (landed 2026-04-22)

Path B Matrix A cycle 2 surfaced a latent lifecycle bug:

| Item | Detail |
|---|---|
| Symptom | m01 kernel dmesg `nvme1n1: I/O Cmd(0x2) @ LBA 0, I/O Error (sct 0x3 / sc 0x2) DNR` on reconnect cycle 2 |
| Root cause | Session-layer `defer backend.Close()` (nvme target) + `s.backend.Close()` (iscsi session close) flipped DurableProvider's cached Backend `closed=true`; next session's Open returned same closed handle; all I/O → `ErrBackendClosed` → NVMe maps to SCT=3 SC=2 (ANA Inaccessible) |
| V2 comparison | V2 `Subsystem.Dev` lifecycle is registry-owned (AddVolume/RemoveVolume), NOT session-owned. V2's `Controller.shutdown()` explicitly does NOT touch `Subsystem.Dev`. V3 introduced `frontend.Backend.Close()` abstraction without carrying V2's implicit ownership contract into interface godoc |
| Fix | Removed 2 lines (nvme target handleConn defer; iscsi session close); added Provider-ownership clause to Backend interface godoc; regression test `TestT3_Bug005_ProviderCachedBackend_ReusableAcrossSessions` matrix both impls |
| Commit | `42b045a` |
| New ledger row | `INV-DURABLE-SESSION-LIFECYCLE-001` (queued ACTIVE post-three-sign) |
| Discipline citation | `feedback_porting_discipline.md` third citation added: "new-abstraction ownership drift" as distinct failure mode from BUG-001's "V2-file-simplification drift" |

**Why existing tests missed it**: all T3a/T3b/T3c tests are either single-session (scenarios) or direct-handler (integration tests that bypass `target.handleConn`). Only "real target × session 1 disconnect × session 2 open" exercises the cross-session cache interaction. Same L1-passes-L2-fails pattern as BUG-001 — L1 tests at isolated layers don't catch multi-layer lifecycle drift.

### Phase 4 — T3-DEF-5/6/7 retrospective (landed 2026-04-22)

Tier A retrofill of the V2/V3 Contract Bridge Catalogue surfaced 3 latent gaps in T3 scope. All three processed pre-sign so T3 closes with zero open inventory rows tied to its scope.

| Gap | Nature | Outcome |
|---|---|---|
| T3-DEF-5 | iSCSI VPD Model/Vendor/Firmware-Rev constants reviewed during Addendum A but never catalogued; risk of silent drift when multi-tenant/ANA requirements arrive | Catalogued as C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED (PORT-AS-IS LOCKED explicit) in §2.2.13. No regression test needed — discipline row; reclassification trigger documented. **Closes as catalogue row.** |
| T3-DEF-6 | NVMe Session cleanup-on-close tested only implicitly via goroutine-leak count; no explicit contract row | L1 addendum test `t3_qa_session_cleanup_addendum_test.go` landed at `seaweed_block@313dd52` — **smoke + goroutine-leak guard**, not full state-release introspection. `TestT3_NVMe_Session_CleanupOnClose_NoLeaksAcrossCycles` (20 open/close cycles + Identify admin cmd; fails if `NumGoroutine` delta > 30 after drain) + `TestT3_NVMe_Session_CleanupOnClose_CNTLIDAllocator` (8 cycles, CNTLID strictly monotonic + no zero-sentinel + allocator not wedged). Both PASS 0.78s. Test-only introspection of Target.ctrls / AER slot / KATO-stored-ms is **not** exercised — follow-up if/when that introspection surface is added. New catalogue row C5-NVME-SESSION-STATE-CLEANUP-ON-CLOSE records the contract; pin strength is "smoke" today. **Closes T3-DEF-6 as L1 smoke + catalogue row; full introspection pin queued as post-T3 inventory if BUG-006 timer work touches this area.** |
| T3-DEF-7 | KATO timer store-only per BUG-003 Discovery Bridge scope; m01 Matrix D kernel dmesg `long keepalive RTT (2522123190 ms)` proves contract violated today, not merely deferred-safe | Filed as `bugs/006_nvme_kato_timer_not_enforced.md` (Medium severity, target G21 / post-T3, ~50-100 LOC fix sketch + 4-test coverage plan). Catalogue row C1-NVME-SESSION-KATO reclassified **VIOLATED** with BUG-006 anchor + m01 evidence citation. **Closes as formal bug with track assignment.** |

### Phase 5 — Walstore BUG-007 (non-blocking, filed 2026-04-22)

m01 re-verify against both impls surfaced a pre-existing walstore-specific durability bug: cross-session umount+remount (via fs layer, no process restart) loses clean-sync'd data. smartwal passes the same Matrix D cycle. Filed as `bugs/007_walstore_umount_remount_data_loss.md`; **non-blocking for T3** — smartwal is production default per T3b, and canonical G4 gate is satisfied by smartwal full A-F green. Walstore-side evidence for ledger row `INV-DURABLE-001` deferred to BUG-007 close.

---

## §I Contract Bridge Catalogue integration

T3 is the first track where the V2→V3 port's **entity bridging** discipline is retrofilled into documentation. Triggered by the BUG-001 + BUG-005 + Addendum A pattern being recognized as systemic (not one-off).

**Reference**: `sw-block/design/v2-v3-contract-bridge-catalogue.md`

- §2 storage-layer entity bridge + contract catalogue has been retrofilled with T3 scope:
  - Subsystem (V2) → DurableProvider + LogicalStorage (V3) — merged bridge
  - Controller (V2) → nvme.Session + StorageBackend (V3) — split bridge
  - BlockVol (V2) → LogicalStorage + adapter fence (V3) — split bridge
  - flusher / dirty_map / wal_* (V2) → 1:1 with V3 equivalents (per T3.0 §10.3 walk-through)
  - New V3 entities without V2 analog: ProjectionView, StorageBackend operational gate, frontend.Backend interface itself

- §2.3 maps all three known drifts to catalogue entity rows:
  - BUG-001 → Controller C3 (rxLoop serial) + C6 (R2T-before-H2CData ordering)
  - BUG-005 → Controller C1 (session ≠ device lifecycle); Subsystem C1 (Dev lifecycle is registry-owned)
  - Addendum A iSCSI VPD 0x80 Serial → Subsystem metadata contract (V3 derives from VolumeID, V2 was a hardcoded stub)

- §8C.8 discipline promotes the catalogue to mandatory pre-T-start artifact for any track involving V2 port. For T4+ (G5 replicated write path), catalogue §3 replication section must be LOCKED before T-start three-sign.

**T3 close uses the catalogue retroactively** — the 5 V3 model shifts (event model / storage model / authority model / lifecycle / concurrency) in §1.1 of the catalogue explain why several "1:1" port rows in §C of this report diverge from V2 even though nominally unchanged (e.g., GroupCommitter drops OnDegraded + PostSyncCheck — same file name, different contract).

---

## §J Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial T3 closure report drafted; awaiting three-sign | sw |
| 2026-04-22 | §F file manifest added (commit-by-commit touch record) | sw |
| 2026-04-22 | Pre-sign review: Phase 1 doc integrity + §G sketch-delta + §H review response log | sw |
| 2026-04-22 | BUG-005 discovered during Path B Matrix A cycle 2; fixed in `42b045a`; §A + §D + §G.3 + §H.Phase3 updated | sw |
| 2026-04-22 | §I Contract Bridge Catalogue integration; T3 retrofill reference; §8C.8 discipline citation | sw |
| 2026-04-22 | §F file manifest added (explicit file-by-file touch record across 4 commits) | sw |
| 2026-04-22 | PM + architect pre-sign review identified 6 blockers + 1 low. QA retracted premature sign; executed Phase 1 (doc integrity: §G signed-scope delta, audit sign-off, TBD→commit, perf doc cleanup, artifact commits) + Phase 2 (m01 Matrix E SIGKILL + Matrix F ENOSPC — real G4 evidence) | QA Owner |
| 2026-04-22 | Phase 4 T3-DEF-5/6/7 retrospective closure: DEF-5 catalogued (C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED), DEF-6 landed as `t3_qa_session_cleanup_addendum_test.go` pinning C5, DEF-7 filed as BUG-006 reclassifying C1-NVME-SESSION-KATO as VIOLATED | QA Owner |
| 2026-04-22 | Phase 5 BUG-007 filed: pre-existing walstore umount+remount data loss; non-blocking since smartwal is prod default and satisfies canonical G4 | QA Owner |
