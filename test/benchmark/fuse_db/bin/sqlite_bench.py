#!/usr/bin/env python3
# SQLite performance probe at one db path. Prints TSV: METRIC<TAB>secs<TAB>rate
#   LOAD  : bulk-load <rows> rows (4096B incompressible payload) in <batch>-row txns
#   OLTP  : <oltp> single-row INSERTs, each its own fsync'd autocommit txn -> tx/s
#   SCAN  : full table scan reading every payload (warm) -> MB/s
# journal=DELETE, synchronous=FULL (durable: fsync per commit).
import sys, os, sqlite3, time, random

db    = sys.argv[1]
rows  = int(sys.argv[2])
batch = int(sys.argv[3])
oltp  = int(sys.argv[4])

for ext in ("", "-journal", "-wal", "-shm"):
    try: os.remove(db + ext)
    except OSError: pass

con = sqlite3.connect(db, isolation_level=None)
con.execute("PRAGMA journal_mode=DELETE;")
con.execute("PRAGMA synchronous=FULL;")
con.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB, chk INTEGER);")

# LOAD
mb = rows * 4096 / 1048576
t = time.time(); done = 0
while done < rows:
    end = min(done + batch, rows)
    data = [(i, random.Random(i).randbytes(4096), 0) for i in range(done, end)]
    con.execute("BEGIN")
    con.executemany("INSERT INTO t VALUES(?,?,?)", data)
    con.execute("COMMIT")
    done = end
dt = time.time() - t
print(f"LOAD\t{dt:.1f}\t{mb/dt:.0f} MB/s")

# OLTP: each INSERT autocommits (isolation_level=None) -> 1 fsync'd txn each
con.execute("CREATE TABLE o(id INTEGER PRIMARY KEY, v INTEGER);")
t = time.time()
for i in range(oltp):
    con.execute("INSERT INTO o(v) VALUES(?)", (i,))
dt = time.time() - t
print(f"OLTP\t{dt:.2f}\t{oltp/dt:.0f} tx/s")

# SCAN (warm): read every payload
t = time.time(); nbytes = 0
for rid, p, c in con.execute("SELECT id,payload,chk FROM t"):
    nbytes += len(p)
dt = time.time() - t
print(f"SCAN\t{dt:.1f}\t{nbytes/1048576/dt:.0f} MB/s")
con.close()
