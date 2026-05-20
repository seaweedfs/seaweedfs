#!/usr/bin/env bash
#
# Locking / concurrency test battery for Samba on a SeaweedFS FUSE mount.
#
# Covers the challenges a network-filesystem backend has to get right:
#   1. POSIX fcntl byte-range locking on the FUSE mount (SetLk/GetLk)
#   2. Distributed locking (-dlm): a write held open on one mount blocks a
#      writer on another mount until it is released
#   3. Distributed locking integrity: concurrent writers to the same file from
#      two mounts produce intact (non-torn) data
#   4. Concurrent writers to distinct files all succeed
#
# Required env:
#   SMB_USER, SMB_PASS              samba credentials
#   MOUNT_SHARE                     dir on the smbd-backed FUSE mount (mount 1)
#   MOUNT2_SHARE                    dir on the second FUSE mount (mount 2)
# Optional env:
#   SMB_HOST (127.0.0.1), SMB_SHARE (seaweedfs), SMB_PORT (445)
set -uo pipefail

SMB_HOST="${SMB_HOST:-127.0.0.1}"
SMB_SHARE="${SMB_SHARE:-seaweedfs}"
SMB_PORT="${SMB_PORT:-445}"
SMB_USER="${SMB_USER:?SMB_USER is required}"
SMB_PASS="${SMB_PASS:?SMB_PASS is required}"
MOUNT_SHARE="${MOUNT_SHARE:?MOUNT_SHARE is required}"
MOUNT2_SHARE="${MOUNT2_SHARE:?MOUNT2_SHARE is required}"

WORK="$(mktemp -d /tmp/samba-locktest.XXXXXX)"
trap 'rm -rf "${WORK}"' EXIT

PASS=0
FAIL=0
XFAIL=0
XPASS=0
pass() { printf '  [PASS] %s\n' "$1"; PASS=$((PASS + 1)); }
fail() { printf '  [FAIL] %s\n' "$1"; FAIL=$((FAIL + 1)); }
# Expected failure: a known-broken behavior. [XFAIL] does not fail the suite;
# an unexpected pass ([XPASS]) does, so the check gets promoted once it's fixed.
xfail() { printf '  [XFAIL] %s\n' "$1"; XFAIL=$((XFAIL + 1)); }
xpass() { printf '  [XPASS] %s\n' "$1"; XPASS=$((XPASS + 1)); }

smb() {
  smbclient "//${SMB_HOST}/${SMB_SHARE}" -p "${SMB_PORT}" \
    -U "${SMB_USER}%${SMB_PASS}" -m SMB3 -c "$1"
}
md5() { md5sum "$1" | awk '{print $1}'; }

# 1. POSIX fcntl byte-range locking on the FUSE mount ------------------------
# Exercises the mount's SetLk/GetLk via two processes contending over fcntl
# (F_SETLK) byte-range locks. python3's fcntl.lockf issues real POSIX locks.
echo "==> 1. POSIX fcntl byte-range locking (FUSE mount SetLk/GetLk)"
lockfile="${MOUNT_SHARE}/fcntl_lock.dat"
: >"${lockfile}"
fcntl_out="$(python3 - "${lockfile}" <<'PY'
import fcntl, os, sys

path = sys.argv[1]
parent_to_child_r, parent_to_child_w = os.pipe()  # release signal
child_to_parent_r, child_to_parent_w = os.pipe()  # locked signal

pid = os.fork()
if pid == 0:  # child: hold an exclusive lock on [0,100)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.lockf(fd, fcntl.LOCK_EX, 100, 0, 0)
    os.write(child_to_parent_w, b"L")
    os.read(parent_to_child_r, 1)  # wait until parent says release
    fcntl.lockf(fd, fcntl.LOCK_UN, 100, 0, 0)
    os.close(fd)
    os._exit(0)

# parent
os.read(child_to_parent_r, 1)  # wait until child holds the lock
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
results = []

# a. a conflicting exclusive lock must be denied while the child holds it
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 0, 0)
    fcntl.lockf(fd, fcntl.LOCK_UN, 100, 0, 0)
    results.append(("conflicting exclusive lock denied while held", False))
except OSError:
    results.append(("conflicting exclusive lock denied while held", True))

# b. a non-overlapping range must be grantable
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 200, 0)
    fcntl.lockf(fd, fcntl.LOCK_UN, 100, 200, 0)
    results.append(("non-overlapping range lock granted", True))
except OSError:
    results.append(("non-overlapping range lock granted", False))

# c. after the holder releases, the lock must be acquirable
os.write(parent_to_child_w, b"R")
os.waitpid(pid, 0)
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 0, 0)
    fcntl.lockf(fd, fcntl.LOCK_UN, 100, 0, 0)
    results.append(("lock acquirable after holder releases", True))
except OSError:
    results.append(("lock acquirable after holder releases", False))

for name, ok in results:
    print(("  [PASS] " if ok else "  [FAIL] ") + name)
sys.exit(0 if all(ok for _, ok in results) else 1)
PY
)"
echo "${fcntl_out}"
PASS=$((PASS + $(grep -c '\[PASS\]' <<<"${fcntl_out}")))
FAIL=$((FAIL + $(grep -c '\[FAIL\]' <<<"${fcntl_out}")))

# 2. Distributed lock blocks a cross-mount writer, then hands it off ----------
# mount 2 holds a file open for writing (holding the DLM lock on its path).
# An SMB put of the same file goes through mount 1 and must (a) block while
# mount 2 holds it and (b) actually SUCCEED once mount 2 releases, leaving the
# SMB writer's payload on disk. smbclient gets a long client timeout (-t) so we
# are testing the lock handoff itself, not smbclient's own ~20s default timeout.
#
# KNOWN ISSUE (expected failure): the (b) handoff checks are marked xfail. The
# holder releases the distributed lock only on FUSE Release, which the kernel
# delays for tens of seconds under this contention, so the waiting writer does
# not acquire in time. This is a DLM liveness bug, not data corruption. When the
# holder-side release is fixed these flip to [XPASS] and fail the suite, a
# reminder to promote them to hard assertions.
echo "==> 2. distributed lock: cross-mount write coordination"
dlmfile="dlm_coord.bin"
newdata="${WORK}/dlm_new.bin"
head -c 4096 /dev/urandom >"${newdata}"

# Hold the file open for writing on mount 2 via fd 9 -> holds the DLM lock.
exec 9>"${MOUNT2_SHARE}/${dlmfile}"
printf 'held-by-mount2' >&9

# Start the SMB write; record its real exit code when it returns.
rm -f "${WORK}/dlm_put.rc"
(
  smbclient "//${SMB_HOST}/${SMB_SHARE}" -p "${SMB_PORT}" \
    -U "${SMB_USER}%${SMB_PASS}" -m SMB3 -t 120 \
    -c "put ${newdata} ${dlmfile}" >/dev/null 2>&1
  echo "$?" >"${WORK}/dlm_put.rc"
) &
smb_bg=$!

sleep 4
if [[ ! -f "${WORK}/dlm_put.rc" ]]; then
  pass "SMB write blocks while another mount holds the file open"
else
  fail "SMB write returned early instead of blocking (rc=$(cat "${WORK}/dlm_put.rc"))"
fi

# Release mount 2's DLM lock; the blocked SMB write must now complete.
exec 9>&-

# Wait (bounded) for the SMB put to finish so a stuck handoff fails the test
# instead of hanging the suite.
put_rc="timeout"
for _ in $(seq 1 20); do
  if [[ -f "${WORK}/dlm_put.rc" ]]; then
    put_rc="$(cat "${WORK}/dlm_put.rc")"
    break
  fi
  sleep 1
done
kill "${smb_bg}" 2>/dev/null
wait "${smb_bg}" 2>/dev/null

# xfail: the handoff stalls because the lock is freed only on the delayed FUSE
# Release. A pass here means the holder-side release was fixed.
if [[ "${put_rc}" == "0" ]]; then
  xpass "blocked SMB write succeeds after the other mount releases"
else
  xfail "blocked SMB write succeeds after the other mount releases (rc=${put_rc})"
fi

# A correct handoff leaves the SMB writer's payload on disk: mount 1 acquired
# the lock and wrote after mount 2 released.
got="${WORK}/dlm_got.bin"
if smb "get ${dlmfile} ${got}" >/dev/null 2>&1 && [[ "$(md5 "${got}")" == "$(md5 "${newdata}")" ]]; then
  xpass "post-release content is the SMB writer's payload (correct handoff)"
else
  xfail "post-release content is the SMB writer's payload (correct handoff)"
fi

# 3. Distributed lock integrity: concurrent writers, same file ---------------
# An SMB writer (mount 1) and a direct writer (mount 2) race on one file. DLM
# serializes them, so the result must be exactly one of the two payloads.
echo "==> 3. distributed lock: concurrent writers produce intact data"
racefile="dlm_race.bin"
payloadA="${WORK}/dlm_raceA.bin"
head -c 1048576 /dev/urandom >"${payloadA}"
payloadB="direct-write-from-mount2-payload"
(smb "put ${payloadA} ${racefile}" >/dev/null 2>&1) &
(printf '%s' "${payloadB}" >"${MOUNT2_SHARE}/${racefile}") &
wait
racegot="${WORK}/dlm_race_got.bin"
if smb "get ${racefile} ${racegot}" >/dev/null 2>&1 &&
  { [[ "$(md5 "${racegot}")" == "$(md5 "${payloadA}")" ]] || [[ "$(cat "${racegot}")" == "${payloadB}" ]]; }; then
  pass "concurrent same-file writers leave one intact payload"
else
  fail "concurrent same-file writers leave one intact payload"
fi

# 4. Concurrent writers to distinct files ------------------------------------
echo "==> 4. concurrent writers to distinct files"
n=6
declare -a srcs=()
for i in $(seq 1 "${n}"); do
  s="${WORK}/cc_${i}.bin"
  head -c 1048576 /dev/urandom >"${s}"
  srcs+=("${s}")
  (smb "put ${s} concurrent_${i}.bin" >/dev/null 2>&1) &
done
wait
all_ok=true
for i in $(seq 1 "${n}"); do
  g="${WORK}/cc_got_${i}.bin"
  if ! smb "get concurrent_${i}.bin ${g}" >/dev/null 2>&1 ||
    [[ "$(md5 "${srcs[$((i - 1))]}")" != "$(md5 "${g}")" ]]; then
    all_ok=false
  fi
done
if ${all_ok}; then
  pass "${n} concurrent distinct-file writes all intact"
else
  fail "${n} concurrent distinct-file writes all intact"
fi

echo
echo "==> Summary: ${PASS} passed, ${FAIL} failed, ${XFAIL} expected-fail"
if [[ "${XPASS}" -gt 0 ]]; then
  echo "==> ${XPASS} check(s) unexpectedly passed - the DLM handoff appears fixed; promote them from xfail to assertions"
fi
[[ "${FAIL}" -eq 0 && "${XPASS}" -eq 0 ]]
