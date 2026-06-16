# Shared config + helpers for the SeaweedFS FUSE load/durability test.
# IMPORTANT: this harness manages ONLY its own processes via pidfiles and its own
# (non-default) ports. It never runs `pkill weed`, so any other SeaweedFS instances
# on the box (e.g. default ports 9333/8888) are safe.
#
# Config via env:
#   SEAWEED_BENCH_WORK  runtime dir for cluster/mount/logs (default /tmp/...; kept out of the repo)
#   WEED                path to the weed binary (default: from $PATH)
#   MYSQL_BASE          MySQL install prefix (default macOS Homebrew; on Linux e.g. /usr)

set -u

WORK="${SEAWEED_BENCH_WORK:-/tmp/seaweedfs_fuse_db_bench}"
WEED="${WEED:-$(command -v weed || echo weed)}"
RUN=$WORK
CLUSTER=$RUN/cluster        # weed server -dir (master+volume+filer state) -- local disk
MNT=$RUN/mnt                # FUSE mount point (the SeaweedFS filesystem under test)
LOGS=$RUN/logs
LOCAL=$RUN/local            # non-FUSE scratch: pidfiles, mysql socket, references
PIDS=$LOCAL/pids
RESULTS=$RUN/results

# Isolated, non-default ports (avoid colliding with any running cluster)
MASTER_PORT=9555
VOLUME_PORT=9560
FILER_PORT=9565
FILER_GRPC=19565

# MySQL (macOS Homebrew default; override MYSQL_BASE on Linux, e.g. /usr)
MYSQL_BASE="${MYSQL_BASE:-/opt/homebrew/opt/mysql}"
MYSQLD=$MYSQL_BASE/bin/mysqld
MYSQL=$MYSQL_BASE/bin/mysql
export MYSQL_BIN="${MYSQL_BIN:-$MYSQL}"   # propagated to mysql_bench.py
MYSQLADMIN=$MYSQL_BASE/bin/mysqladmin
MYSQL_PORT=3308
MYSQL_SOCK=$LOCAL/mysql.sock      # socket on LOCAL disk, datadir on FUSE
MYSQL_DATADIR=$MNT/mysql/data     # on the FUSE mount -- the thing under test

ts() { date +%H:%M:%S; }
say() { echo "[$(ts)] $*"; }

ensure_dirs() { mkdir -p "$CLUSTER" "$MNT" "$LOGS" "$LOCAL" "$PIDS" "$RESULTS" "$LOCAL/mountcache"; }

# Portable unmount: fusermount (Linux non-root) -> umount -> diskutil (macOS).
fuse_umount()       { fusermount -u  "$1" 2>/dev/null || fusermount3 -u  "$1" 2>/dev/null || umount    "$1" 2>/dev/null || diskutil unmount       "$1" 2>/dev/null; }
fuse_umount_force() { fusermount -uz "$1" 2>/dev/null || fusermount3 -uz "$1" 2>/dev/null || umount -f "$1" 2>/dev/null || diskutil unmount force "$1" 2>/dev/null; }

# My (and only my) ports. Used for the kill-by-port safety net in clean_state.
MY_PORTS="$MASTER_PORT $VOLUME_PORT $FILER_PORT 19555 19560 $FILER_GRPC $MYSQL_PORT"
port_busy() { lsof -ti TCP:"$1" >/dev/null 2>&1; }
wait_port_free() { # $1=port $2=max-halfsec
  local n=${2:-40}; for i in $(seq 1 "$n"); do port_busy "$1" || return 0; sleep 0.5; done; return 1; }

# ---- cluster (master+volume+filer in one process) ----
cluster_start() {
  ensure_dirs
  if ! wait_port_free "$MASTER_PORT" 30; then
    say "ERROR master port $MASTER_PORT still busy (pid $(lsof -ti TCP:$MASTER_PORT|tr '\n' ' ')); not starting"; return 1
  fi
  say "starting weed server (master:$MASTER_PORT volume:$VOLUME_PORT filer:$FILER_PORT)"
  "$WEED" server -ip=127.0.0.1 -dir="$CLUSTER" \
    -master.port=$MASTER_PORT -volume.port=$VOLUME_PORT \
    -filer -filer.port=$FILER_PORT \
    -volume.max=50 -master.volumeSizeLimitMB=1024 \
    >> "$LOGS/cluster.log" 2>&1 &
  echo $! > "$PIDS/cluster.pid"
  cluster_wait
}
cluster_wait() {
  local pid; pid=$(cat "$PIDS/cluster.pid" 2>/dev/null)
  for i in $(seq 1 120); do
    # the process we launched must still be alive -- guards against a foreign
    # process answering on the port after ours fataled (e.g. bind conflict)
    if ! kill -0 "$pid" 2>/dev/null; then
      say "ERROR cluster process $pid died during startup"; tail -15 "$LOGS/cluster.log"; return 1
    fi
    if curl -s "http://127.0.0.1:$MASTER_PORT/cluster/status" >/dev/null 2>&1 \
       && curl -s "http://127.0.0.1:$FILER_PORT/" >/dev/null 2>&1; then
      say "cluster ready (pid $pid)"; return 0
    fi
    sleep 0.5
  done
  say "ERROR cluster not ready"; tail -20 "$LOGS/cluster.log"; return 1
}
cluster_stop_graceful() {
  local pid; pid=$(cat "$PIDS/cluster.pid" 2>/dev/null) || return 0
  [ -n "${pid:-}" ] || return 0
  say "graceful stop cluster pid $pid (SIGTERM)"
  kill -TERM "$pid" 2>/dev/null
  for i in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || { rm -f "$PIDS/cluster.pid"; return 0; }; sleep 0.5; done
  say "cluster still alive after 30s; SIGKILL"; kill -9 "$pid" 2>/dev/null; rm -f "$PIDS/cluster.pid"
}
cluster_kill_hard() {
  local pid; pid=$(cat "$PIDS/cluster.pid" 2>/dev/null) || return 0
  [ -n "${pid:-}" ] && { say "HARD kill -9 cluster pid $pid"; kill -9 "$pid" 2>/dev/null; }
  rm -f "$PIDS/cluster.pid"
}

# ---- FUSE mount ----
mount_start() {
  ensure_dirs
  say "mounting FUSE at $MNT (filer 127.0.0.1:$FILER_PORT)"
  "$WEED" mount -filer=127.0.0.1:$FILER_PORT -dir="$MNT" \
    -dirAutoCreate -cacheDir="$LOCAL/mountcache" \
    >> "$LOGS/mount.log" 2>&1 &
  echo $! > "$PIDS/mount.pid"
  mount_wait
}
mount_wait() {
  local pid; pid=$(cat "$PIDS/mount.pid" 2>/dev/null)
  for i in $(seq 1 120); do
    if ! kill -0 "$pid" 2>/dev/null; then
      say "ERROR mount process $pid died during startup"; tail -15 "$LOGS/mount.log"; return 1
    fi
    if mount | grep -q "$MNT" && ls "$MNT" >/dev/null 2>&1; then say "mount ready (pid $pid)"; return 0; fi
    sleep 0.5
  done
  say "ERROR mount not ready"; tail -20 "$LOGS/mount.log"; return 1
}
mount_stop_graceful() {
  say "graceful unmount $MNT"
  fuse_umount "$MNT"
  local pid; pid=$(cat "$PIDS/mount.pid" 2>/dev/null)
  if [ -n "${pid:-}" ]; then
    for i in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && { kill -TERM "$pid" 2>/dev/null; sleep 1; }
  fi
  rm -f "$PIDS/mount.pid"
}
mount_kill_hard() {
  local pid; pid=$(cat "$PIDS/mount.pid" 2>/dev/null)
  [ -n "${pid:-}" ] && { say "HARD kill -9 mount pid $pid"; kill -9 "$pid" 2>/dev/null; }
  rm -f "$PIDS/mount.pid"
  # clean up the stale mountpoint
  fuse_umount_force "$MNT"
}

# ---- MySQL ----
mysql_init() {
  ensure_dirs
  rm -rf "$MNT/mysql"; mkdir -p "$MNT/mysql"
  say "initializing MySQL datadir on FUSE: $MYSQL_DATADIR"
  "$MYSQLD" --initialize-insecure --datadir="$MYSQL_DATADIR" --basedir="$MYSQL_BASE" \
    --log-error="$LOGS/mysql-init.log"
}
mysql_start() {
  say "starting mysqld (port $MYSQL_PORT, datadir on FUSE, flush_log_at_trx_commit=1, flush_method=fsync)"
  "$MYSQLD" --datadir="$MYSQL_DATADIR" --basedir="$MYSQL_BASE" \
    --socket="$MYSQL_SOCK" --port=$MYSQL_PORT --bind-address=127.0.0.1 \
    --innodb_flush_log_at_trx_commit=1 --innodb_flush_method=fsync \
    --innodb_doublewrite=ON --innodb_buffer_pool_size=256M \
    --secure-file-priv="" --log-error="$LOGS/mysqld.log" \
    >> "$LOGS/mysqld.out" 2>&1 &
  echo $! > "$PIDS/mysql.pid"
  mysql_wait
}
mysql_wait() {
  for i in $(seq 1 120); do
    "$MYSQLADMIN" --socket="$MYSQL_SOCK" -uroot ping >/dev/null 2>&1 && { say "mysqld ready"; return 0; }
    sleep 0.5
  done
  say "ERROR mysqld not ready"; tail -30 "$LOGS/mysqld.log"; return 1
}
mysql_stop_graceful() {
  say "graceful mysqld shutdown"
  "$MYSQLADMIN" --socket="$MYSQL_SOCK" -uroot shutdown 2>/dev/null
  local pid; pid=$(cat "$PIDS/mysql.pid" 2>/dev/null)
  if [ -n "${pid:-}" ]; then for i in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done; fi
  rm -f "$PIDS/mysql.pid"
}
mysql_kill() {
  local pid; pid=$(cat "$PIDS/mysql.pid" 2>/dev/null)
  [ -n "${pid:-}" ] && { say "HARD kill -9 mysqld pid $pid"; kill -9 "$pid" 2>/dev/null; }
  rm -f "$PIDS/mysql.pid"
}
mq() { "$MYSQL" --socket="$MYSQL_SOCK" -uroot "$@"; }

# ---- whole-node unexpected shutdown (simultaneous -9) ----
hard_kill_all() {
  say "=== UNEXPECTED SHUTDOWN: kill -9 db + mount + cluster ==="
  mysql_kill
  mount_kill_hard
  cluster_kill_hard
  kill_my_ports   # safety net in case any pidfile was stale
}

# stop everything I started (graceful, pidfile-based)
stop_all() { mysql_stop_graceful; mount_stop_graceful; cluster_stop_graceful; }

# safety net: kill anything still bound to MY (non-default) ports
kill_my_ports() {
  for p in $MY_PORTS; do
    local pids; pids=$(lsof -ti TCP:"$p" 2>/dev/null)
    [ -n "$pids" ] && { say "killing leftover pid(s) $pids on my port $p"; kill -9 $pids 2>/dev/null; }
  done
}

# ---- clean slate (wipe all test state; never touches user data) ----
clean_state() {
  say "cleaning test state"
  stop_all
  fuse_umount_force "$MNT"
  kill_my_ports
  for p in $MASTER_PORT $VOLUME_PORT $FILER_PORT; do wait_port_free "$p" 40 || say "WARN port $p still busy"; done
  rm -rf "$CLUSTER" "$MNT" "$LOCAL/mountcache" "$PIDS" 2>/dev/null
  ensure_dirs
}

status_report() {
  echo "---- status ----"
  echo "df $MNT:"; df -h "$MNT" 2>/dev/null | tail -1
  echo "cluster dir size:"; du -sh "$CLUSTER" 2>/dev/null
  echo "volumes:"; curl -s "http://127.0.0.1:$MASTER_PORT/dir/status" 2>/dev/null | python3 -c 'import sys,json;d=json.load(sys.stdin);print("  topology free/max:",d.get("Topology",{}).get("Free"),d.get("Topology",{}).get("Max"))' 2>/dev/null
}
