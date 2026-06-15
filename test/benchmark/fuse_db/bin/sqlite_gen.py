#!/usr/bin/env python3
# Deterministic ~1GB SQLite load on the FUSE mount.
# Each row: id, payload (4096 deterministic bytes derived from id), chk=crc32(payload).
# Commits in batches; after each commit, writes the committed row count to a
# progress file on LOCAL disk and fsyncs it -> a durable record of what was
# committed, used to verify "no committed data lost" after a crash.
import sys, os, sqlite3, zlib, random

db_path      = sys.argv[1]
total_rows   = int(sys.argv[2])
batch        = int(sys.argv[3])
progress_path= sys.argv[4]            # LOCAL disk
ref_path     = sys.argv[5] if len(sys.argv) > 5 else None
sync_mode    = sys.argv[6] if len(sys.argv) > 6 else "FULL"
journal_mode = sys.argv[7] if len(sys.argv) > 7 else "DELETE"

def payload(i: int) -> bytes:
    # deterministic but incompressible (PRNG seeded by id) -> 4096 bytes that
    # actually consume ~1GB on the volume servers (defeats gzip storage).
    return random.Random(i).randbytes(4096)

con = sqlite3.connect(db_path, isolation_level=None)
con.execute(f"PRAGMA journal_mode={journal_mode};")
con.execute(f"PRAGMA synchronous={sync_mode};")
con.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, payload BLOB, chk INTEGER);")

def write_progress(n):
    with open(progress_path, "w") as f:
        f.write(str(n)); f.flush(); os.fsync(f.fileno())

done = 0
sum_chk = 0
sum_id = 0
while done < total_rows:
    end = min(done + batch, total_rows)
    rows = []
    for i in range(done, end):
        p = payload(i); c = zlib.crc32(p)
        rows.append((i, p, c)); sum_chk += c; sum_id += i
    con.execute("BEGIN;")
    con.executemany("INSERT INTO t(id,payload,chk) VALUES(?,?,?);", rows)
    con.execute("COMMIT;")
    done = end
    write_progress(done)
    print(f"committed {done}/{total_rows}", flush=True)

if ref_path:
    with open(ref_path, "w") as f:
        f.write(f"{total_rows} {sum_chk} {sum_id}\n"); f.flush(); os.fsync(f.fileno())
con.close()
print("DONE", flush=True)
