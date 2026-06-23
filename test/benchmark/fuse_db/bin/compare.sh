#!/bin/bash
# Identical SQLite + MySQL + raw-fs workloads on HOST (local APFS) vs the SeaweedFS
# FUSE mount. Same disk underneath, same DB durability settings -> the delta is the
# SeaweedFS+FUSE stack.
BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BIN/lib.sh"
HOSTDIR=$WORK/hostdir          # local disk, NOT under the FUSE mount
ROWS=262144; SBATCH=4096; MBATCH=8192; OLTP=2000

mysqld_init_at() { rm -rf "$1"; mkdir -p "$1"; "$MYSQLD" --initialize-insecure --datadir="$1" \
    --basedir="$MYSQL_BASE" --log-error="$LOGS/mysql_init_cmp.log"; }
mysqld_run() {
  "$MYSQLD" --datadir="$1" --basedir="$MYSQL_BASE" --socket="$MYSQL_SOCK" --port=$MYSQL_PORT \
    --bind-address=127.0.0.1 --innodb_flush_log_at_trx_commit=1 --innodb_flush_method=fsync \
    --innodb_doublewrite=ON --innodb_buffer_pool_size=256M --secure-file-priv="" \
    --log-error="$LOGS/mysqld_cmp.log" >> "$LOGS/mysqld_cmp.out" 2>&1 &
  echo $! > "$PIDS/mysql.pid"; mysql_wait; }

echo "###### FUSE vs HOST   $(date) ######"
echo "ROWS=$ROWS  payload=4096B incompressible  SQLite(journal=DELETE,sync=FULL)  InnoDB(trx_commit=1,flush=fsync)"

# ===================== HOST (local APFS, no SeaweedFS) =====================
clean_state >/dev/null 2>&1
rm -rf "$HOSTDIR"; mkdir -p "$HOSTDIR/sqlite" "$HOSTDIR/fsb"
echo; echo "===== HOST: raw fs ====="; python3 "$BIN/fsbench.py" "$HOSTDIR/fsb"
echo; echo "===== HOST: SQLite ====="; python3 "$BIN/sqlite_bench.py" "$HOSTDIR/sqlite/b.db" $ROWS $SBATCH $OLTP
echo; echo "===== HOST: MySQL ====="
mysqld_init_at "$HOSTDIR/mysql/data"; mysqld_run "$HOSTDIR/mysql/data"
python3 "$BIN/mysql_bench.py" "$MYSQL_SOCK" cmp $ROWS $MBATCH $OLTP
mysql_stop_graceful

# ===================== FUSE (SeaweedFS mount) =====================
cluster_start || exit 1; mount_start || exit 1
mkdir -p "$MNT/sqlite" "$MNT/fsb"
echo; echo "===== FUSE: raw fs ====="; python3 "$BIN/fsbench.py" "$MNT/fsb"
echo; echo "===== FUSE: SQLite ====="; python3 "$BIN/sqlite_bench.py" "$MNT/sqlite/b.db" $ROWS $SBATCH $OLTP
echo; echo "===== FUSE: MySQL ====="
mysqld_init_at "$MNT/mysql/data"; mysqld_run "$MNT/mysql/data"
python3 "$BIN/mysql_bench.py" "$MYSQL_SOCK" cmp $ROWS $MBATCH $OLTP
mysql_stop_graceful

stop_all
rm -rf "$HOSTDIR"
echo; echo "###### compare done ######"
