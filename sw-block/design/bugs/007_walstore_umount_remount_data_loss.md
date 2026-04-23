# BUG-007 — walstore loses written data across umount + remount (same session)

**Status**: OPEN — filed 2026-04-22 during T3-end m01 re-verify
**Severity**: Medium — walstore is non-default impl (smartwal is production default per T3b); but "fs cross-umount persistence" is a baseline durability expectation
**Component**: `core/storage/walstore.go` (walstore Read path OR Sync persistence)
**Discovered-by**: QA Owner during Matrix D (cross-session consistency) re-verify against 27486db
**Discovered-when**: 2026-04-22 (T3-end pre-sign)
**Owner**: sw (after T3 closes)
**Blocks**: not T3 close (smartwal full A-F green satisfies canonical G4; walstore explicitly non-default per T3b config); tracks for post-T3 walstore hardening

---

## 1. One-line symptom

Against a walstore-backed NVMe device, `mkfs.ext4 -F; mount; echo X > f; sync; umount; mount; cat f` returns "file not found" instead of "X". The clean umount + remount in same blockvolume process loses the sync'd write.

## 2. Environment

| Item | Value |
|---|---|
| Host | m01 (192.168.1.181) |
| Kernel | Linux 6.17.0-19-generic |
| V3 commit | phase-15 @ `27486db` |
| DurableProvider `--durable-impl` | `walstore` (fails) vs `smartwal` (passes same test) |

## 3. Symptom — verbatim

`scripts/iterate-m01-nvme.sh` Matrix D (5 remount cycles on shared ext4):

```
Matrix D — cross-session consistency (5 remount cycles)
cross-session cycle 1: got 'MISSING' want 'cycle-1-marker-1776918177'
[iterate-m01 FAIL] Matrix D cross-session consistency failed
```

Same test `DURABLE_IMPL=smartwal` → all 5 cycles PASS.

## 4. Reproduction

```bash
# Against a running blockvolume with --durable-impl walstore:
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n <nqn>
DEV=$(sudo nvme list | awk '/SeaweedFS/ {print $1; exit}')
sudo mkfs.ext4 -F -b 1024 -I 128 -N 2000 $DEV
sudo mkdir -p /mnt/t
sudo mount $DEV /mnt/t
echo "probe-$(date +%s)" | sudo tee /mnt/t/probe.txt
sudo sync
sudo umount /mnt/t

# Now remount — probe.txt should exist:
sudo mount $DEV /mnt/t
ls /mnt/t/probe.txt
# → ls: cannot access '/mnt/t/probe.txt': No such file or directory
```

Deterministic (first attempt fails).

## 5. Hypothesis

`walstore.Read(lba)` may not be pulling uncommitted-but-WAL-persisted writes back when the flusher hasn't yet applied them to the extent. Sequence:

- ext4 writes superblock / directory entries / file to various LBAs
- Each write → walstore.Write → appends to WAL
- kernel sync → walstore.Sync → fsync WAL (checkpoint NOT advanced if flusher async)
- kernel umount → kernel Flush → walstore.Sync (same)
- kernel mount (same device) → reads superblock from LBA 0
- walstore.Read(LBA=0) → if checks extent only, returns pre-write superblock (not the ext4-fresh one) → mount sees stale/invalid fs → files missing

`walstore.Read` at line 382 DOES split into `readFromWAL` (line 405) + `readFromExtent` (line 427) via dirtyMap lookup (line 390+). So the mechanism SHOULD exist. Something about the umount→remount sequence probably invalidates dirtyMap state, OR checkpoint advance happens unsafely, OR something else.

Needs `core/storage/walstore.go` inspection; not blocking T3 close.

## 6. Why smartwal doesn't have this

smartwal uses a ring-based WAL with per-slot validation; its read path may be more robust against the specific "recent Write + Sync but no flusher yet" window. Investigation required.

## 7. Why this wasn't caught earlier

- `core/storage/walstore_test.go` covers Write → Sync → **Close+Reopen** → Read. It does NOT cover Write → Sync → **Read** without reopen (the same-session cross-umount case).
- T3a adapter tests (`TestT3a_StorageBackend_ByteLBATranslation_Matrix`) write and read but don't do the umount/remount sequence.
- T3c scenarios do crash + reopen (which is fine) but not umount + remount.
- My QA L1 addendum `TestT3_Durable_RecoverThenServe` does reopen — different code path from same-session umount/remount.

Matrix D is the first test that exercises "fs layer flush + remount WITHOUT process restart" path against walstore.

## 8. Impact on T3

- T3 closes normally: smartwal is production default per T3b; walstore is fallback. smartwal Matrix A-F all green.
- walstore is explicitly a **non-primary impl**. Post-T3 walstore hardening addresses this.
- Closure report §C: walstore port quality "MATCHES" rating stands (functionality exists); §D update: "walstore post-T3 hardening tracked in BUG-007".
- Ledger row `INV-DURABLE-001` (crash recovery byte-exact) queues ACTIVE based on smartwal evidence; walstore-side evidence deferred to BUG-007 close.

## 9. Catalogue linkage

`v2-v3-contract-bridge-catalogue.md` §2.2.5 `Superblock` + §2.2.4 `GroupCommitter` both relate. New catalogue annotation to add:

> BUG-007 surfaces a **walstore-specific** contract gap: walstore's `Read` must return WAL-resident writes even before flusher advance. The contract is **same-session durability under umount-triggered sync**. PRESERVE verdict for walstore; implementation gap to close via BUG-007 fix.

## 10. Exit criteria

- Root cause identified in `core/storage/walstore.go`
- Fix lands; existing walstore tests still green
- `TestT3_Walstore_UmountRemount_Persistence` L0 regression added; both walstore + smartwal run it
- m01 `DURABLE_IMPL=walstore` Matrix A-F all green
- BUG moves to CLOSED

## 11. Log

| Date | Actor | Event |
|---|---|---|
| 2026-04-22 | QA | Filed after Matrix D walstore failure in T3-end re-verify; BUG-005 already fixed so this is NOT a regression — pre-existing walstore bug surfaced by Matrix D addition |
