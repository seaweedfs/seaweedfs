#!/bin/bash
# SQLite ~1GB load test on the SeaweedFS FUSE mount + crash suite.
BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BIN/lib.sh"

TOTAL=262144          # 262144 * 4096B payload = ~1GiB of row data
BATCH=4096            # 64 committed transactions (each fsync'd, synchronous=FULL)
DB=$MNT/sqlite/load.db
PROG=$LOCAL/sqlite_progress.txt
REF=$LOCAL/sqlite_ref.txt
PASS=0; FAIL=0
chk() { if [ "$1" -eq 0 ]; then echo ">>> $2: PASS"; PASS=$((PASS+1)); else echo ">>> $2: FAIL"; FAIL=$((FAIL+1)); fi; }

echo "######## SQLite FUSE load test  $(date) ########"

# ---------- fresh cluster + 1GB load ----------
clean_state; cluster_start || exit 1; mount_start || exit 1
mkdir -p "$MNT/sqlite"
say "loading ~1GB into SQLite (journal=DELETE synchronous=FULL)"
t0=$SECONDS
python3 "$BIN/sqlite_gen.py" "$DB" $TOTAL $BATCH "$PROG" "$REF" FULL DELETE | tail -3
say "load done in $((SECONDS-t0))s; file size:"; ls -lh "$DB"; du -sh "$MNT/sqlite"
status_report

# ---------- Scenario A: normal (graceful) shutdown + restart ----------
echo; echo "==================== SCENARIO A: NORMAL SHUTDOWN ===================="
mount_stop_graceful
cluster_stop_graceful
sleep 1
cluster_start || exit 1; mount_start || exit 1
say "verifying after graceful restart"
python3 "$BIN/sqlite_verify.py" "$DB" $TOTAL exact; chk $? "A/normal-shutdown"

# ---------- Scenario B: unexpected shutdown (kill -9) + restart ----------
echo; echo "============== SCENARIO B: UNEXPECTED SHUTDOWN (kill -9) =============="
hard_kill_all
sleep 1
cluster_start || exit 1; mount_start || exit 1
say "verifying after kill -9 + restart"
python3 "$BIN/sqlite_verify.py" "$DB" $TOTAL exact; chk $? "B/unexpected-shutdown"

# ---------- Scenario C: crash DURING active writes ----------
echo; echo "=========== SCENARIO C: CRASH DURING ACTIVE WRITES (kill -9) ==========="
clean_state; cluster_start || exit 1; mount_start || exit 1
mkdir -p "$MNT/sqlite"; : > "$PROG"
say "starting loader in background; will crash storage mid-write"
python3 "$BIN/sqlite_gen.py" "$DB" $TOTAL $BATCH "$PROG" "" FULL DELETE > "$LOGS/sqlite_gen_c.log" 2>&1 &
GENPID=$!
# wait until at least half is durably committed
while :; do
  p=$(cat "$PROG" 2>/dev/null || echo 0); p=${p:-0}
  [ "$p" -ge $((TOTAL/2)) ] && break
  kill -0 $GENPID 2>/dev/null || { say "loader exited early"; break; }
  sleep 0.2
done
LB=$(cat "$PROG" 2>/dev/null); LB=${LB:-0}   # empty file (killed mid-write) -> 0
say "durable committed lower-bound at crash = $LB rows; crashing storage now"
hard_kill_all                      # storage dies under the still-writing loader
kill -9 $GENPID 2>/dev/null; wait $GENPID 2>/dev/null
sleep 1
cluster_start || exit 1; mount_start || exit 1
say "verifying recovery (committed prefix must survive, integrity must hold)"
python3 "$BIN/sqlite_verify.py" "$DB" "$LB" atleast; chk $? "C/crash-during-write"

echo; echo "######## SQLite RESULT: PASS=$PASS FAIL=$FAIL ########"
mount_stop_graceful; cluster_stop_graceful
exit $FAIL
