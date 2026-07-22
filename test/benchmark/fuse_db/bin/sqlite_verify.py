#!/usr/bin/env python3
# Verify a SQLite DB after a crash/restart.
#  - PRAGMA integrity_check  (structural consistency of the b-tree / pages)
#  - row count / sum(chk) / sum(id) vs expected
#  - recompute crc32(payload) for every row vs stored chk (byte corruption)
#
# usage: sqlite_verify.py <db> <expected_rows> [mode]
#   mode=exact  (default): count must == expected_rows (normal/unexpected shutdown
#               of a fully-loaded DB -- nothing may be lost or extra).
#   mode=at least: expected_rows is a LOWER BOUND (durable committed progress at
#               crash time). count must be >= expected_rows and form a contiguous
#               prefix 0..count-1 (the in-flight txn may or may not have committed,
#               but no committed row may be lost and recovery must be consistent).
import sys, sqlite3, zlib, random

db_path = sys.argv[1]
expected_rows = int(sys.argv[2])
mode = sys.argv[3] if len(sys.argv) > 3 else "exact"
if mode not in ("exact", "at least"):   # a typo must not silently relax checks
    raise SystemExit(f"invalid mode: {mode!r} (want exact|at least)")

def payload(i: int) -> bytes:
    return random.Random(i).randbytes(4096)

con = sqlite3.connect(db_path)
ok = True

ic = con.execute("PRAGMA integrity_check;").fetchone()[0]
print(f"integrity_check: {ic}")
if ic != "ok": ok = False

cnt    = con.execute("SELECT COUNT(*) FROM t;").fetchone()[0]
minid  = con.execute("SELECT MIN(id) FROM t;").fetchone()[0]
maxid  = con.execute("SELECT MAX(id) FROM t;").fetchone()[0]
sum_chk= con.execute("SELECT COALESCE(SUM(chk),0) FROM t;").fetchone()[0]
sum_id = con.execute("SELECT COALESCE(SUM(id),0)  FROM t;").fetchone()[0]

if mode == "exact":
    target = expected_rows
    if cnt != expected_rows:
        print(f"rows: got={cnt} expected={expected_rows}  MISMATCH"); ok = False
    else:
        print(f"rows: got={cnt} expected={expected_rows}  OK")
else:  # at least
    print(f"rows: got={cnt} committed-lower-bound={expected_rows}  "
          f"{'OK' if cnt >= expected_rows else 'LOST COMMITTED DATA'}")
    if cnt < expected_rows: ok = False
    target = cnt
    # contiguous prefix 0..cnt-1 ?
    if cnt > 0 and (minid != 0 or maxid != cnt - 1):
        print(f"prefix: MIN={minid} MAX={maxid} expected 0..{cnt-1}  NON-CONTIGUOUS"); ok = False
    else:
        print(f"prefix: 0..{maxid} contiguous  OK")

exp_sum_chk = sum(zlib.crc32(payload(i)) for i in range(target))
exp_sum_id  = target * (target - 1) // 2
print(f"sum(chk): got={sum_chk} expected={exp_sum_chk}  {'OK' if sum_chk==exp_sum_chk else 'MISMATCH'}")
print(f"sum(id):  got={sum_id} expected={exp_sum_id}  {'OK' if sum_id==exp_sum_id else 'MISMATCH'}")
if sum_chk != exp_sum_chk or sum_id != exp_sum_id: ok = False

corrupt = 0; scanned = 0
for rid, p, c in con.execute("SELECT id,payload,chk FROM t;"):
    scanned += 1
    if zlib.crc32(p) != c: corrupt += 1
print(f"payload byte-check: scanned={scanned} corrupt={corrupt}  {'OK' if corrupt==0 else 'CORRUPT'}")
if corrupt != 0: ok = False

con.close()
print("VERIFY: " + ("PASS" if ok else "FAIL"))
sys.exit(0 if ok else 1)
