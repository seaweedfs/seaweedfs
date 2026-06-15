#!/bin/bash
# MySQL (InnoDB) ~1GB load test on the SeaweedFS FUSE mount + crash suite.
# Datadir lives on the FUSE mount; innodb_flush_log_at_trx_commit=1 +
# innodb_flush_method=fsync => every commit fsyncs the redo log through FUSE.
BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BIN/lib.sh"

TOTAL=262144          # 262144 * 4096B payload = ~1GiB row data (InnoDB on-disk larger)
BATCH=8192            # 32 committed transactions
PROG=$LOCAL/mysql_progress.txt
PASS=0; FAIL=0
chk() { if [ "$1" -eq 0 ]; then echo ">>> $2: PASS"; PASS=$((PASS+1)); else echo ">>> $2: FAIL"; FAIL=$((FAIL+1)); fi; }

mysql_schema() {
  mq -e "CREATE DATABASE IF NOT EXISTS loadtest;
         DROP TABLE IF EXISTS loadtest.t;
         CREATE TABLE loadtest.t(id INT PRIMARY KEY, payload BLOB, chk BIGINT) ENGINE=InnoDB;"
}
mysql_load() {  # loads TOTAL rows in BATCH-sized committed inserts; progress -> $PROG
  local O=0 B rem
  : > "$PROG"
  while [ $O -lt $TOTAL ]; do
    B=$BATCH; rem=$((TOTAL-O)); [ $rem -lt $B ] && B=$rem
    # incompressible + deterministic payload: AES-128-ECB (no IV -> deterministic)
    # of a fixed 4080B plaintext, keyed per-id -> 4096B ciphertext that defeats
    # gzip on the volumes. Identical expr for payload and chk, so CRC32(payload)
    # recomputed at verify must equal stored chk.
    mq -e "SET SESSION cte_max_recursion_depth=1000000;
           INSERT INTO loadtest.t(id,payload,chk)
           WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n < $((B-1)))
           SELECT n+$O,
                  AES_ENCRYPT(REPEAT('a',4080), SHA2(n+$O,256)),
                  CRC32(AES_ENCRYPT(REPEAT('a',4080), SHA2(n+$O,256)))
           FROM seq;" \
      || { echo "INSERT failed at offset $O"; return 1; }
    O=$((O+B)); echo $O > "$PROG"
  done
}
mysql_verify() {  # $1=expected $2=mode(exact|atleast) ; returns 0 on PASS
  local exp=$1 mode=$2 cnt sumchk corrupt minid maxid checktbl ok=0
  read -r cnt sumchk corrupt minid maxid <<EOF
$(mq -N -e "SELECT COUNT(*),COALESCE(SUM(chk),0),COALESCE(SUM(chk<>CRC32(payload)),0),COALESCE(MIN(id),-1),COALESCE(MAX(id),-1) FROM loadtest.t;")
EOF
  checktbl=$(mq -N -e "CHECK TABLE loadtest.t;" | awk '{print $NF}')
  echo "  rows=$cnt corrupt=$corrupt minid=$minid maxid=$maxid CHECK=$checktbl (want $mode $exp)"
  [ "$checktbl" = "OK" ] || { echo "  -> CHECK TABLE failed"; ok=1; }
  [ "$corrupt" = "0" ]   || { echo "  -> payload corruption in $corrupt rows"; ok=1; }
  if [ "$mode" = exact ]; then
    [ "$cnt" = "$exp" ] || { echo "  -> row count mismatch"; ok=1; }
    { [ "$minid" = 0 ] && [ "$maxid" = "$((exp-1))" ]; } || { echo "  -> not contiguous 0..$((exp-1))"; ok=1; }
  else
    [ "$cnt" -ge "$exp" ] 2>/dev/null || { echo "  -> LOST COMMITTED DATA (cnt<$exp)"; ok=1; }
    { [ "$minid" = 0 ] && [ "$maxid" = "$((cnt-1))" ]; } || { echo "  -> not contiguous prefix"; ok=1; }
  fi
  return $ok
}

echo "######## MySQL FUSE load test  $(date) ########"

# ---------- fresh cluster + mysql + 1GB load ----------
clean_state; cluster_start || exit 1; mount_start || exit 1
mysql_init || { echo "mysql_init failed"; tail -20 "$LOGS/mysql-init.log"; exit 1; }
mysql_start || exit 1
mysql_schema
say "loading ~1GB into InnoDB"
t0=$SECONDS
mysql_load || echo "load reported an error"
say "load done in $((SECONDS-t0))s"
mq -N -e "SELECT CONCAT('innodb rows=',COUNT(*),'  data_len=',ROUND(SUM(LENGTH(payload))/1048576),'MB') FROM loadtest.t;"
du -sh "$MYSQL_DATADIR"; status_report

# ---------- Scenario A: normal (graceful) shutdown + restart ----------
echo; echo "==================== SCENARIO A: NORMAL SHUTDOWN ===================="
mysql_stop_graceful
mount_stop_graceful
cluster_stop_graceful
sleep 1
cluster_start || exit 1; mount_start || exit 1; mysql_start || exit 1
mysql_verify $TOTAL exact; chk $? "A/normal-shutdown"

# ---------- Scenario B: unexpected shutdown (kill -9 everything) ----------
echo; echo "============== SCENARIO B: UNEXPECTED SHUTDOWN (kill -9) =============="
hard_kill_all
sleep 1
cluster_start || exit 1; mount_start || exit 1
say "restarting mysqld -> InnoDB crash recovery runs"
mysql_start || { echo "mysqld failed to recover"; tail -30 "$LOGS/mysqld.log"; chk 1 "B/unexpected-shutdown"; }
if mq -e "SELECT 1" >/dev/null 2>&1; then mysql_verify $TOTAL exact; chk $? "B/unexpected-shutdown"; fi

# ---------- Scenario C: crash DURING active writes ----------
echo; echo "=========== SCENARIO C: CRASH DURING ACTIVE WRITES (kill -9) ==========="
mysql_stop_graceful
clean_state; cluster_start || exit 1; mount_start || exit 1
mysql_init || exit 1; mysql_start || exit 1; mysql_schema
say "loading in background; will crash storage + mysqld mid-write"
mysql_load > "$LOGS/mysql_load_c.log" 2>&1 &
LPID=$!
while :; do
  p=$(cat "$PROG" 2>/dev/null || echo 0); p=${p:-0}
  [ "$p" -ge $((TOTAL/2)) ] && break
  kill -0 $LPID 2>/dev/null || { say "loader exited early"; break; }
  sleep 0.2
done
LB=$(cat "$PROG" 2>/dev/null || echo 0)
say "durable committed lower-bound at crash = $LB rows; crashing now"
hard_kill_all
kill -9 $LPID 2>/dev/null; wait $LPID 2>/dev/null
sleep 1
cluster_start || exit 1; mount_start || exit 1
say "restarting mysqld -> InnoDB recovery; in-flight txn must roll back, committed must survive"
mysql_start || { echo "recovery failed"; tail -30 "$LOGS/mysqld.log"; chk 1 "C/crash-during-write"; }
if mq -e "SELECT 1" >/dev/null 2>&1; then mysql_verify $LB atleast; chk $? "C/crash-during-write"; fi

echo; echo "######## MySQL RESULT: PASS=$PASS FAIL=$FAIL ########"
mysql_stop_graceful; mount_stop_graceful; cluster_stop_graceful
exit $FAIL
