# V3 Phase 15 T3b — Mini Port Plan

**Date**: 2026-04-22
**Status**: QA single-signed per §8C.2; sw cleared to start
**Owner**: QA drafts + signs; sw implements
**Predecessor**: T3a CLOSED (commit `0e1595c`; mini-plan `v3-phase-15-t3a-mini-plan.md`)
**Gate**: T3b — Provider + Recovery + frontend Sync wiring
**Successor**: T3c mini-plan (scenarios + continuity smoke + perf baseline + T3-end three-sign)
**Governing methodology**: §8B PCDD + §8C accelerated-cadence governance

---

## 0. Scope (locked)

T3b delivers in one commit (atomic; splitting is a §8C.3 trigger):

1. **G-int.4 `DurableProvider`** — new `core/frontend/durable/provider.go` implementing `frontend.Provider`:
   - `Open(ctx, volumeID)` selects impl (`smartwal` or `walstore`) from config; opens the underlying `LogicalStorage` instance; wraps in T3a `StorageBackend`; returns
   - Enforces Addendum A #2: reads superblock, compares `ImplKind` vs selector; mismatch → fail-fast with named error (NOT silent coerce)
   - Production default: `smartwal` (per PM direction "prod 如果能用 smart 最好"); `walstore` remains reachable via config
   - `testback.StaticProvider` unchanged (per audit §11 G-int.4: tests keep injection path)

2. **G-int.5 Recovery integration** — new `core/frontend/durable/recovery.go`:
   - `Recover(storage LogicalStorage) (RecoveryReport, error)` — wraps `LogicalStorage.Recover()` + surfaces recovered LSN + any lineage-drift evidence
   - Wired from `cmd/blockvolume` startup via a new thin recovery coordinator
   - After `Recover` success: adapter's `SetOperational(true, evidence)` called → volume host's `HealthyPathExecutor.Probe` proceeds (T1 path, no new publication surface)
   - If `Recover` returns error OR recovered state disagrees with assigned lineage (per-I/O fence check will catch it anyway): `SetOperational(false, evidence)` + report `NotReady` via existing `status_server.go` `/status` (read-side only, no publication)
   - **Option 3 from audit §10.5 honored**: no local epoch mirror added; per-I/O adapter fence check remains sole authority-drift guard

3. **iSCSI `SYNCHRONIZE_CACHE` wire** — modify `core/frontend/iscsi/scsi.go`:
   - `SYNCHRONIZE_CACHE(10)` (0x35) and `SYNCHRONIZE_CACHE(16)` (0x91) handlers currently no-op; wire to `backend.Sync(ctx)`
   - Error from `Sync` maps to spec-legal SCSI status (sw picks; `GOOD` on success, `MEDIUM_ERROR` / `HARDWARE_ERROR` on fault per SPC-5)

4. **NVMe `Flush` wire** — modify `core/frontend/nvme/io.go`:
   - `ioFlush` opcode (0x00) handler currently no-op; wire to `backend.Sync(ctx)`
   - Error from `Sync` maps to spec-legal NVMe status (sw picks; generic success 0x00 / internal-error on fault per NVMe 1.4)

5. **Adapter lifecycle — sw design note honored**:
   - Per T3a sw close note: `Backend.Close()` does NOT tear down underlying `LogicalStorage`; Provider retains storage handle and closes it explicitly
   - Double-close of adapter is idempotent
   - Provider-level shutdown (`cmd/blockvolume` SIGTERM) closes both adapter and underlying storage in correct order

Out of T3b (per T3 sketch + audit §12):
- 4 G6 scenarios → T3c
- m01 iSCSI + NVMe fs-workload smoke against durable backend → T3c
- Perf smoke + baseline doc → T3c
- T3 closure report + T3-end three-sign → T3c
- Any reinstatement of `OnDegraded` on `GroupCommitterConfig` — confirmed NOT needed per T3.0 sw note A; storage stays pure-mechanism; degraded-signal lives at adapter/host layer in T3b Recovery

---

## 1. File layout

### 1.1 New files

| Path | Purpose | Est LOC |
|---|---|---|
| `core/frontend/durable/provider.go` | `DurableProvider` + config + impl selector + ImplKind mismatch detection | 200-250 |
| `core/frontend/durable/provider_test.go` | Unit tests (matrix over both impls; mismatch negative cases) | 200-250 |
| `core/frontend/durable/recovery.go` | `Recover` wrapper + `RecoveryReport` + evidence string builder | 100-150 |
| `core/frontend/durable/recovery_test.go` | Unit tests (clean recover / mismatch / storage error path) | 150-200 |

### 1.2 Modified files

| Path | Change | Est LOC delta |
|---|---|---|
| `core/frontend/iscsi/scsi.go` | Wire `SYNCHRONIZE_CACHE(10)` + `SYNCHRONIZE_CACHE(16)` handlers to `backend.Sync(ctx)`; map error to SCSI status | +30 |
| `core/frontend/nvme/io.go` | Wire `ioFlush` opcode handler to `backend.Sync(ctx)`; map error to NVMe status | +20 |
| `cmd/blockvolume/main.go` (or equivalent setup site) | Wire `DurableProvider` into volume host + invoke `Recover` at startup + flip adapter operational bit | +40-60 |
| `core/frontend/iscsi/scsi_test.go` + existing iSCSI tests | Add test for SYNC_CACHE handler wiring (one test asserts `backend.Sync` called) | +30 |
| `core/frontend/nvme/io_test.go` + existing NVMe tests | Add test for Flush opcode wiring | +20 |
| `core/host/volume/healthy_executor*` (if needed) | Only if Recover-to-Healthy wiring requires a new hook; otherwise untouched | +0-20 |

### 1.3 Touched but not modified

- `core/frontend/durable/storage_adapter.go` — T3a delivered; T3b consumes but doesn't change
- `core/frontend/types.go` — interface stable post-T3a
- `core/storage/*` — `LogicalStorage` stable; no schema or behavior change in T3b
- `core/frontend/testback/*` — unchanged; kept for tests per audit §11 G-int.4

### 1.4 Total T3b LOC budget

| Line class | Budget |
|---|---|
| Production | 350-450 (provider + recovery + 2 frontend wires + cmd wire) |
| Test | 250-300 (provider + recovery + frontend handler tests) |
| Total | 600-750 |

Wall-clock: 1-1.5 day (per audit §12).

---

## 2. Acceptance criteria (gate — QA single-sign per §8C.2)

| # | Criterion | Evidence |
|---|---|---|
| 1 | `DurableProvider` implements `frontend.Provider` | Compile + `TestT3b_DurableProvider_ImplementsProvider` |
| 2 | `Provider.Open` selects impl by config (`smartwal` default in prod; `walstore` reachable) | `TestT3b_DurableProvider_Open_SelectsImpl_Matrix` (both impls) |
| 3 | Returned Backend starts `operational=false` until Recovery flips it | `TestT3b_DurableProvider_Open_StartsNotOperational` |
| 4 | Addendum A #2: ImplKind mismatch between selector and on-disk superblock → named error (not silent coerce) | `TestT3b_DurableProvider_Open_ImplKindMismatch_FailsFast` |
| 5 | `Recover(storage)` calls `LogicalStorage.Recover()`; on success returns RecoveryReport with `recoveredLSN`; on error returns wrapped error + NotReady evidence | `TestT3b_Recovery_CleanRecover_*` + `TestT3b_Recovery_StorageErrorPath_*` |
| 6 | iSCSI `SYNCHRONIZE_CACHE(10)` + `SYNCHRONIZE_CACHE(16)` handlers dispatch to `backend.Sync(ctx)` | `TestT3b_ISCSI_SyncCache_DispatchesToBackend` (both opcodes) |
| 7 | NVMe `ioFlush` opcode handler dispatches to `backend.Sync(ctx)` | `TestT3b_NVMe_Flush_DispatchesToBackend` |
| 8 | Backend Close does NOT tear down underlying storage; Provider.Close() tears down in correct order; double-close idempotent | `TestT3b_DurableProvider_Lifecycle_*` |
| 9 | Full regression including T3a + prior T0/T1/T2 suite green | `go test ./core/... -count=1` → all packages ok |
| 10 | Boundary guard green — `core/frontend/durable/` + `cmd/blockvolume` wire path still no `core/authority` / `core/adapter` imports from frontend | `TestFrontendCannotMintAuthority_BoundaryGuard` (existing recursive walk) |

All 10 green → QA single-signs T3b close → T3c mini-plan opens.

---

## 3. Stop rules (T3b-specific + inherited)

Inherited from T3 sketch §6 + audit §5 + T3a §3:
- Port-model default for anything ported from V2
- Storage NEVER publishes authority; adapter NEVER advances epoch
- `writeGate` NEVER reappears
- `LogicalStorage` interface signature NEVER changes
- `Backend` interface signature NEVER changes in T3b (T3a locked)

T3b-specific:

1. **Atomic commit** — all 5 items (Provider + Recovery + iSCSI wire + NVMe wire + cmd wire) ship in one commit. Splitting = §8C.3 trigger.
2. **No `OnDegraded` callback on `GroupCommitterConfig`** — confirmed NOT needed per T3.0 sw note A. If T3b implementation finds a case where it's needed, that's a §8C.3 trigger #1 (architectural issue — escalate).
3. **No local epoch mirror in superblock** — Option 3 from audit §10.5 locked. Adding a superblock `Epoch` field in T3b is a §8C.3 trigger #4 (port-model principle violation) and requires new Discovery Bridge.
4. **Recovery never publishes** — Recovery exposes readiness via `status_server.go` `/status` (read-side). Any new RPC / publication path in Recovery flow = §8C.3 trigger #1.
5. **Provider selector is config-driven, not code-branch** — production default is `smartwal`, but the selector is a single config field (string or enum), NOT a compile-time branch or hard-coded path. Enables Addendum A #1 matrix testing in T3c without rebuild.
6. **Frontend Sync wire is error-faithful** — iSCSI SYNC_CACHE / NVMe Flush handlers MUST propagate `Sync` errors to spec-legal status. Swallowing errors (returning GOOD regardless) = §8C.3 trigger #1 (data integrity hazard).
7. **No scenarios in T3b** — 4 G6 scenarios + continuity smoke + perf baseline stay T3c scope. Any scenario YAML or Go replay landing in T3b = §8C.3 trigger #3 (cross-batch pollution).

---

## 4. Ledger rows queued (ACTIVE at T3b close)

| ID | Statement |
|---|---|
| `INV-DURABLE-PROVIDER-SELECT-001` | `DurableProvider.Open` selects correct impl per config + fails fast on ImplKind mismatch with on-disk superblock |
| `INV-DURABLE-RECOVERY-READSIDE-001` | Recovery exposes readiness via adapter's `SetOperational` + existing `/status` HTTP surface; no new publication path; master authority remains sole Epoch/assignment publisher |
| `INV-DURABLE-SYNC-WIRED-001` | iSCSI `SYNCHRONIZE_CACHE(10/16)` + NVMe `Flush` reach `LogicalStorage.Sync`; error propagates to spec-legal host status |

Existing rows MUST remain ACTIVE post-T3b:
- `INV-DURABLE-OPGATE-001` (T3a)
- `INV-DURABLE-IMPL-IDENTITY-001` (T3a)
- `INV-FRONTEND-002.EPOCH / .EV / .REPLICA / .HEALTHY` (T1)

---

## 5. Sign-off

### 5.1 QA single-sign (T3b open)

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ **SIGNED — T3b open for sw implementation** per §8C.2 |

### 5.2 Sw delivery log (fill on commit)

| Date | Action | Commit |
|---|---|---|
| 2026-04-22 | T3b code delivered | `72d0d40` |
| 2026-04-22 | QA §2 acceptance walk-through complete: 10/10 green (incl. bonus Open_Caches test) + spot-checks confirmed: SYNC errors map to ASCWriteError (no swallow), Flush errors propagate via `backend.Sync`, zero publication paths in cmd/blockvolume wire, memback fallback preserved when `--durable-root` empty | — |
| 2026-04-22 | **QA T3b-close single-sign per §8C.2** — T3b CLOSED | — |
| 2026-04-22 (addendum) | **QA L1 addendum landed** — 3 tests in `core/frontend/durable/t3b_qa_l1_addendum_test.go` (V2-reference-driven coverage: ConcurrentIO_SyncBarrier + RecoverThenServe + LargeSpanningWrite). Matrix 6 subtests / 2 impls each PASS. Closes V2-parity gaps at L1 before T3c L2 scenarios. Zero impact on prior T3b close; §8C.2 QA single-sign holds | QA Owner |

### 5.3 Review + close

QA runs §2 acceptance 10-row walk-through after sw commit. All 10 green → QA single-signs T3b close → T3c mini-plan opens (scenarios + smoke + perf + closure + T3-end three-sign).

Minor fails: mid-batch iterate per §8C.2.
Major fails (architectural): §8C.3 trigger — escalate architect + PM.

---

## 6. Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial T3b mini plan; QA single-signed; sw cleared to code | QA Owner |
