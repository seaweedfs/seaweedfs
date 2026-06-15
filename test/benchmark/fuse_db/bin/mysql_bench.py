#!/usr/bin/env python3
# MySQL performance probe via the mysql CLI (no driver needed). Times each phase
# in Python. Prints TSV: METRIC<TAB>secs<TAB>rate
#   LOAD : bulk-load <rows> rows (4096B incompressible AES payload), <batch>-row txns
#   OLTP : <oltp> single-row autocommit INSERTs (each fsyncs redo, trx_commit=1) -> tx/s
#   SCAN : SUM(CRC32(payload)) over the table (reads every payload) -> MB/s
import os, sys, subprocess, time

sock  = sys.argv[1]
db    = sys.argv[2]
rows  = int(sys.argv[3])
batch = int(sys.argv[4])
oltp  = int(sys.argv[5])
MYSQL = os.environ.get("MYSQL_BIN", "mysql")   # lib.sh exports MYSQL_BIN; else use PATH

def run(sql):
    r = subprocess.run([MYSQL, "--socket", sock, "-uroot"], input=sql.encode(),
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode()[:500]); raise SystemExit("mysql failed")

run(f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db};"
    f"CREATE TABLE {db}.t(id INT PRIMARY KEY,payload BLOB,chk BIGINT) ENGINE=InnoDB;"
    f"CREATE TABLE {db}.o(id INT AUTO_INCREMENT PRIMARY KEY,v INT) ENGINE=InnoDB;")

mb = rows * 4096 / 1048576
stmts = ["SET SESSION cte_max_recursion_depth=1000000;"]
o = 0
while o < rows:
    b = min(batch, rows - o)
    stmts.append(
        f"INSERT INTO {db}.t(id,payload,chk) "
        f"WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<{b-1}) "
        f"SELECT n+{o},AES_ENCRYPT(REPEAT('a',4080),SHA2(n+{o},256)),"
        f"CRC32(AES_ENCRYPT(REPEAT('a',4080),SHA2(n+{o},256))) FROM seq;")
    o += b
t = time.time(); run("\n".join(stmts)); dt = time.time() - t
print(f"LOAD\t{dt:.1f}\t{mb/dt:.0f} MB/s")

sql = "\n".join(f"INSERT INTO {db}.o(v) VALUES({i});" for i in range(oltp))
t = time.time(); run(sql); dt = time.time() - t
print(f"OLTP\t{dt:.2f}\t{oltp/dt:.0f} tx/s")

t = time.time(); run(f"SELECT SUM(CRC32(payload)) FROM {db}.t;"); dt = time.time() - t
print(f"SCAN\t{dt:.1f}\t{mb/dt:.0f} MB/s")
