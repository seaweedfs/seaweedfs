# BUG-005 — Backend.Close in session lifecycle breaks cached DurableProvider Backend across sessions

**Status**: FIX-LANDED — seaweed_block commit `<TBD>`; unit regression green; awaiting m01 re-verification
**Severity**: BLOCKER (G4 partial-pass gate — without this fix, cross-session reconnect fails 100%)
**Component**: `core/frontend/nvme/target.go` + `core/frontend/iscsi/session.go` (session-layer Backend close)
**Discovered-by**: QA Owner during Path B m01 Matrix A cycle-2 reconnect test
**Discovered-when**: 2026-04-22, during T3c three-sign prep
**Owner**: sw
**Blocks**: T3-end three-sign (G4 durable-data-path pass)

---

## 1. One-line symptom

After a full NVMe session (connect → mkfs → IO → disconnect) completes cleanly, the NEXT `nvme connect` on the same target returns `I/O Error (sct 0x3 / sc 0x2) DNR` on the very first Read. The kernel treats it as ANA "Asymmetric Access Inaccessible" and drops the namespace.

Not a data-integrity bug — the bytes on disk are intact. A lifecycle bookkeeping bug: the cached Backend handle gets marked closed by session 1's teardown, so session 2 receives the same (closed) handle from the Provider cache.

---

## 2. Environment

- Host: m01 (Linux 6.17, nvme-cli)
- Target: V3 `blockvolume` with `--durable-root` + `--durable-impl smartwal`
- Phase: Path B m01 reconnect matrix during T3c three-sign prep

---

## 3. Symptom — verbatim

### Kernel dmesg
```
nvme nvme1: creating 8 I/O queues.  (cycle 1)
... mkfs / mount / write / sync / umount / disconnect succeed ...
nvme nvme1: creating 8 I/O queues.  (cycle 2)
nvme1n1: I/O Cmd(0x2) @ LBA 0, 8 blocks, I/O Error (sct 0x3 / sc 0x2) DNR
```

### V3 status mapping
`core/frontend/nvme/errors.go`:
```go
SCTPathRelated                = 0x3
SCPathAsymAccessInaccessible  = 0x02
```
Result of mapping `frontend.ErrBackendClosed` through `NewBackendClosed()`.

---

## 4. Root cause

Two design elements combined into a bug:

**Element A — per-volumeID Backend cache in `DurableProvider`**. Required because `LogicalStorage` is an exclusive resource: cannot have two open WAL writers on the same file. So `Open` returns the same `StorageBackend` for repeated Opens of the same volumeID.

**Element B — session-layer calls to `Backend.Close()`**:
- `core/frontend/nvme/target.go` `handleConn`: `defer backend.Close()`
- `core/frontend/iscsi/session.go` `close()`: `_ = s.backend.Close()`

These were memback-era patterns that worked because memback's `Provider.Open` creates a NEW `backend` struct per call (fresh `closed=false`); only the underlying `volumeStore` is cached. Session-level Close marked just that session's handle closed.

When T3a `DurableProvider` introduced a per-volumeID cache, the two elements became incompatible: session 1's Close flipped the CACHED handle closed. Session 2's Open returned the same (closed) handle. Every I/O hit `StorageBackend.gate()`'s `closed=true` branch and returned `ErrBackendClosed`, which the NVMe layer faithfully reported as `SCT=3 SC=2`.

### Why V2 didn't have this

V2's architecture structurally avoided the conflict:
- `Subsystem` holds the `BlockDevice`; registered via `AddVolume` / unregistered via `RemoveVolume` (admin-level, not per-session)
- `Controller` (per-session) holds a `*Subsystem` POINTER — never closes the underlying `Dev`
- `Controller.shutdown()` closes conn, stops KATO, clears pendingCapsules, unregisters CNTLID from admin map — explicit list, no `Dev.Close()`

V3 introduced the `frontend.Backend` abstraction without carrying V2's implicit ownership contract ("device outlives sessions") into the new interface's godoc. The result: T2 target authors saw `Backend.Close()` and treated it as session-owned, matching memback's accidental compatibility. DurableProvider's arrival broke that.

---

## 5. Fix

**Minimal 2-line delete** + godoc formalization + regression test:

1. Remove `defer backend.Close()` in `core/frontend/nvme/target.go` `handleConn` (replaced with a BUG-005 comment).
2. Remove `_ = s.backend.Close()` in `core/frontend/iscsi/session.go` `close()` (replaced with a BUG-005 comment).
3. Add Provider-ownership clause to `frontend.Backend` interface godoc in `core/frontend/types.go`:
   > "Backend.Close() is OWNED BY THE PROVIDER. Session-layer consumers MUST NOT call Close on a Backend obtained from Provider.Open — treat the returned Backend as a BORROWED reference..."
4. Regression test `TestT3_Bug005_ProviderCachedBackend_ReusableAcrossSessions` matrix-parameterized over walstore + smartwal.

No changes to `StorageBackend` itself; the invariant is enforced at the call-site level. `Backend.Close()` remains a legal operation for callers who OWN the handle (Provider.Close, explicit tests).

---

## 6. Why existing tests missed it

All T3a/T3b/T3c tests either:
- Single-session (`scenario_crash_recovery_test.go` etc.) — no session-close → no cache poisoning
- Direct-handler (`integration_iscsi_test.go` etc.) — call `SCSIHandler.HandleCommand` / `IOHandler.Handle` directly, bypassing `target.handleConn`'s defer

**Only "real target + session 1 + disconnect + session 2" exercises the cross-session cache-poisoning path.** m01 is the first environment that drove this pattern end-to-end.

Same structural lesson as BUG-001: L1 coverage of individual layers (adapter, provider, handler) doesn't catch bugs that emerge only when the target's session-lifecycle × Provider's handle-cache interaction runs in a real multi-session workload.

---

## 7. Fix verification

- Unit regression: `TestT3_Bug005_ProviderCachedBackend_ReusableAcrossSessions` passes for both impls.
- Full tree: `go test ./core/... -count=1` — all packages green.
- m01 re-run post-fix: pending QA cycle with Path B (Matrix A / B / C / D already green pre-BUG-005; re-verify they stay green, then Matrix E attempted).

---

## 8. Exit criteria

- [x] Session-layer `Backend.Close` removed (nvme/target.go + iscsi/session.go)
- [x] Backend godoc documents Provider ownership
- [x] Regression test matrix pins the invariant
- [ ] m01 reconnect cycle green (QA re-run)
- [ ] Ledger row `INV-DURABLE-SESSION-LIFECYCLE-001` ACTIVE

## 9. New ledger row

`INV-DURABLE-SESSION-LIFECYCLE-001`: Across NVMe / iSCSI session disconnect + reconnect, the Provider-cached Backend remains operational. I/O on the reconnected session MUST NOT receive `ErrBackendClosed` caused by prior session teardown.

## 10. Cross-links

- V2 reference (correct pattern): `weed/storage/blockvol/nvme/controller.go` `shutdown()` + `weed/storage/blockvol/nvme/server.go` `AddVolume / RemoveVolume`
- BUG-001 (precedent): same L1-passes-L2-fails pattern; storage-level drift vs session-level drift
- T3a design note §3 (pre-existing contract): "Backend.Close marks closed but does NOT tear down storage; Provider retains storage handle"
- Memory: `feedback_porting_discipline.md` (update: new-abstraction-layer ownership discipline)

## 11. Log

| Date | Actor | Event |
|---|---|---|
| 2026-04-22 | QA | Path B cycle-2 reconnect fails; dmesg `sct 0x3/sc 0x2` captured |
| 2026-04-22 | sw | Root cause identified: session-layer `Backend.Close` × Provider cache interaction |
| 2026-04-22 | sw | Fix Option 1 landed (2-line delete + godoc + regression test) |
| 2026-04-22 | sw | Regression test green both impls; awaiting m01 re-verify |
