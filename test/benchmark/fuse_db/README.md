# FUSE database load / durability / performance benchmark

Runs **MySQL (InnoDB)** and **SQLite** with their data files on a SeaweedFS **FUSE mount**,
with ~1 GB datasets, and answers two questions:

1. **Durability** — does any committed data get lost across normal shutdown, an unexpected
   `kill -9`, or a crash during active writes?
2. **Performance** — how much slower is the FUSE mount than the local disk for the same
   workload (bulk load, OLTP commits, scans, raw fsync)?

Each row carries a CRC32 of an incompressible (so ~1 GB actually hits the volumes)
deterministic payload; verification checks `integrity_check`/`CHECK TABLE`, exact row count,
contiguous-id prefix, and a per-row CRC recompute.

## Results (single node, macOS arm64 + macFUSE, weed 4.34, local NVMe, 2026-06-15)

### Durability — 6/6 PASS, no committed data lost, no corruption
| scenario | what | SQLite | MySQL |
|---|---|---|---|
| A normal shutdown | graceful DB stop, unmount, cluster stop, restart, verify | PASS | PASS |
| B unexpected (`kill -9`) | kill -9 db+mount+cluster, restart, recover, verify | PASS | PASS |
| C crash during writes | kill -9 mid-load, restart, recover, verify committed prefix | PASS | PASS |

Scenario C confirmed correct recovery: the in-flight transaction rolled back, every
fsync-committed row survived as a contiguous prefix (InnoDB redo recovery ~9 s).

### Performance — FUSE vs host (same disk, same durability settings)
Raw filesystem:
| metric | host | FUSE | FUSE slower |
|---|---|---|---|
| seq write +fsync | 2336 MB/s | 369 MB/s | 6.3x |
| seq read (warm) | 3435 MB/s | 1422 MB/s | 2.4x |
| fsync latency | 0.13 ms | 1.18 ms | 9x |

SQLite (1 GB, journal=DELETE, synchronous=FULL):
| metric | host | FUSE | FUSE slower |
|---|---|---|---|
| bulk load | 5.8 s (177 MB/s) | 11.6 s (88 MB/s) | 2.0x |
| OLTP 1-row txns | 1987 tx/s | 171 tx/s | 11.6x |
| full scan (warm) | 316 MB/s | 427 MB/s | ~equal (python-bound) |

MySQL/InnoDB (1 GB, trx_commit=1, flush_method=fsync):
| metric | host | FUSE | FUSE slower |
|---|---|---|---|
| bulk load | 24.4 s (42 MB/s) | 32.9 s (31 MB/s) | 1.35x |
| OLTP 1-row commits | 10542 tx/s | 1144 tx/s | 9.2x |
| full scan | 2419 MB/s | 430 MB/s | 5.6x |

**Takeaway:** the cost is dominated by fsync/commit latency (0.13 → 1.18 ms, ~9x) because
every durable commit uploads the chunk to the volume + persists the filer chunk-manifest over
loopback. Small fsync'd transactions are ~9-12x slower; bulk loads (commits amortized) are
1.3-2x slower; warm/sequential reads are close to host. These are single-node loopback
numbers — a real cluster (remote volumes, replication) adds network RTT + replica fsync per
commit.

## Durability caveat (test scope)
`kill -9` terminates the **processes** while the OS keeps running, so everything that reached
the OS page cache (all fsync'd data) survives — this faithfully tests process/daemon crashes
and is what passed 6/6. It does **not** simulate true **power loss** (page cache lost). The
FUSE mount uploads chunks **without** `fsync=true` (`weed/mount/weedfs_write.go` builds
`UploadOption` with no `Fsync`), so the volume does not fsync `.dat` per upload and the filer
leveldb likely does not sync per write; under a real power cut, recently-"committed" data that
only reached the page cache could be lost. A VM hard-reset / power-loss test on real hardware
is the follow-up to close that gap.

## Requirements
- `weed` on `$PATH` (or set `WEED=/path/to/weed`)
- macFUSE (macOS) or libfuse (Linux)
- `python3`, `sqlite3`
- MySQL/MariaDB install; set `MYSQL_BASE` to its prefix (default macOS Homebrew
  `/opt/homebrew/opt/mysql`; must contain `bin/mysqld`, `bin/mysql`, `bin/mysqladmin`).
  For `mysql_bench.py` on a non-default install, also set `MYSQL_BIN=/path/to/mysql`.

## Run
Runtime artifacts (cluster, mount, logs) go to `$SEAWEED_BENCH_WORK`
(default `/tmp/seaweedfs_fuse_db_bench`), kept out of the repo.

    cd test/benchmark/fuse_db
    bash bin/run_sqlite.sh   > results/sqlite.log   2>&1   # SQLite durability suite
    bash bin/run_mysql.sh    > results/mysql.log    2>&1   # MySQL durability suite
    bash bin/compare.sh      > results/compare.log  2>&1   # FUSE vs host performance

The harness manages only its own processes (via pidfiles) on non-default ports
(9555/9560/9565, mysqld 3308); it never runs `pkill weed`, so other SeaweedFS instances on
the box are untouched.

## Files
    bin/lib.sh           cluster/mount/mysqld lifecycle helpers (start/stop/kill, clean state)
    bin/run_sqlite.sh    SQLite 1GB load + A/B/C crash scenarios
    bin/run_mysql.sh     MySQL 1GB load + A/B/C crash scenarios
    bin/compare.sh       FUSE-vs-host driver
    bin/sqlite_gen.py    deterministic incompressible SQLite loader (progress/ref files)
    bin/sqlite_verify.py SQLite integrity + count + prefix + per-row CRC verifier
    bin/sqlite_bench.py  SQLite load/OLTP/scan timing probe
    bin/mysql_bench.py   MySQL load/OLTP/scan timing probe (via mysql CLI)
    bin/fsbench.py       raw seq-write / fsync-latency / seq-read microbenchmark

The numbers above are the reference baseline; rerun the scripts to reproduce. Run output is
written to results/*.log, which is gitignored (repo-wide *.log rule) -- not checked in.
