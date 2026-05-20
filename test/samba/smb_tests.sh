#!/usr/bin/env bash
#
# SMB protocol test battery against a Samba share backed by a SeaweedFS FUSE
# mount. Driven both by the local runner (test/samba/run.sh) and by the Docker
# harness (run_inside_container.sh).
#
# Required env:
#   SMB_USER   samba username
#   SMB_PASS   samba password
# Optional env:
#   SMB_HOST   samba host                (default 127.0.0.1)
#   SMB_SHARE  share name                (default seaweedfs)
#   SMB_PORT   smbd port                 (default 445)
#   SHARE_FS_PATH  directory on the FUSE mount that backs the share. When set,
#                  the suite also checks cross-protocol consistency: data written
#                  over SMB is visible on the FUSE mount, and vice versa.
set -uo pipefail

SMB_HOST="${SMB_HOST:-127.0.0.1}"
SMB_SHARE="${SMB_SHARE:-seaweedfs}"
SMB_PORT="${SMB_PORT:-445}"
SMB_USER="${SMB_USER:?SMB_USER is required}"
SMB_PASS="${SMB_PASS:?SMB_PASS is required}"
SHARE_FS_PATH="${SHARE_FS_PATH:-}"

WORK="$(mktemp -d /tmp/samba-smbtest.XXXXXX)"
trap 'rm -rf "${WORK}"' EXIT

PASS=0
FAIL=0
pass() { printf '  [PASS] %s\n' "$1"; PASS=$((PASS + 1)); }
fail() { printf '  [FAIL] %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Run one or more smbclient commands (separated by ';') against the share.
smb() {
  smbclient "//${SMB_HOST}/${SMB_SHARE}" -p "${SMB_PORT}" \
    -U "${SMB_USER}%${SMB_PASS}" -m SMB3 -c "$1"
}

md5() { md5sum "$1" | awk '{print $1}'; }

echo "==> Target //${SMB_HOST}/${SMB_SHARE} (port ${SMB_PORT}) as ${SMB_USER}"
[[ -n "${SHARE_FS_PATH}" ]] && echo "==> Cross-protocol checks against ${SHARE_FS_PATH}"

# 1. Connectivity ------------------------------------------------------------
echo "==> 1. connectivity"
if smb "ls" >/dev/null 2>&1; then
  pass "connect and list share root"
else
  fail "connect and list share root"
fi

# 2. Upload / download round-trip -------------------------------------------
echo "==> 2. upload / download round-trip"
src="${WORK}/src.bin"
head -c 1048576 /dev/urandom >"${src}" # 1 MiB
if smb "put ${src} roundtrip.bin" >/dev/null 2>&1; then
  pass "put 1 MiB file"
else
  fail "put 1 MiB file"
fi
got="${WORK}/got.bin"
if smb "get roundtrip.bin ${got}" >/dev/null 2>&1 && [[ "$(md5 "${src}")" == "$(md5 "${got}")" ]]; then
  pass "get returns identical content"
else
  fail "get returns identical content"
fi
if [[ -n "${SHARE_FS_PATH}" ]]; then
  if [[ -f "${SHARE_FS_PATH}/roundtrip.bin" ]] && [[ "$(md5 "${SHARE_FS_PATH}/roundtrip.bin")" == "$(md5 "${src}")" ]]; then
    pass "SMB-written file visible on FUSE mount with identical content"
  else
    fail "SMB-written file visible on FUSE mount with identical content"
  fi
fi

# 3. Directory operations ----------------------------------------------------
echo "==> 3. directory operations"
if smb "mkdir docs; cd docs; put ${src} nested.bin; ls" >/dev/null 2>&1; then
  pass "mkdir + put into subdirectory"
else
  fail "mkdir + put into subdirectory"
fi
if [[ -z "${SHARE_FS_PATH}" || -f "${SHARE_FS_PATH}/docs/nested.bin" ]]; then
  pass "nested file present"
else
  fail "nested file present"
fi

# 4. Rename ------------------------------------------------------------------
echo "==> 4. rename"
if smb "rename roundtrip.bin renamed.bin" >/dev/null 2>&1; then
  pass "rename file"
else
  fail "rename file"
fi
renback="${WORK}/renamed.bin"
if smb "get renamed.bin ${renback}" >/dev/null 2>&1 && [[ "$(md5 "${renback}")" == "$(md5 "${src}")" ]]; then
  pass "renamed file readable with original content"
else
  fail "renamed file readable with original content"
fi
if [[ -n "${SHARE_FS_PATH}" ]]; then
  if [[ -f "${SHARE_FS_PATH}/renamed.bin" && ! -e "${SHARE_FS_PATH}/roundtrip.bin" ]]; then
    pass "rename reflected on FUSE mount"
  else
    fail "rename reflected on FUSE mount"
  fi
fi

# 5. Large file (exercises SeaweedFS chunking) -------------------------------
echo "==> 5. large file (SeaweedFS chunking)"
big="${WORK}/big.bin"
head -c 67108864 /dev/urandom >"${big}" # 64 MiB
bigback="${WORK}/big.back"
if smb "put ${big} big.bin" >/dev/null 2>&1 &&
  smb "get big.bin ${bigback}" >/dev/null 2>&1 &&
  [[ "$(md5 "${big}")" == "$(md5 "${bigback}")" ]]; then
  pass "64 MiB put/get round-trip"
else
  fail "64 MiB put/get round-trip"
fi

# 6. Recursive upload --------------------------------------------------------
echo "==> 6. recursive upload"
tree="${WORK}/tree"
mkdir -p "${tree}/a/b"
echo one >"${tree}/f1.txt"
echo two >"${tree}/a/f2.txt"
echo three >"${tree}/a/b/f3.txt"
if (cd "${WORK}" && smb "recurse ON; prompt OFF; mput tree" >/dev/null 2>&1) &&
  { [[ -z "${SHARE_FS_PATH}" ]] || [[ -f "${SHARE_FS_PATH}/tree/a/b/f3.txt" ]]; }; then
  pass "recursive mput"
else
  fail "recursive mput"
fi

# 7. Cross-protocol read (FUSE writes, SMB reads) ----------------------------
if [[ -n "${SHARE_FS_PATH}" ]]; then
  echo "==> 7. cross-protocol read (FUSE write -> SMB read)"
  echo "written-via-fuse" >"${SHARE_FS_PATH}/from_fuse.txt"
  cpb="${WORK}/from_fuse.back"
  if smb "get from_fuse.txt ${cpb}" >/dev/null 2>&1 && grep -q written-via-fuse "${cpb}"; then
    pass "FUSE-written file readable over SMB"
  else
    fail "FUSE-written file readable over SMB"
  fi
fi

# 8. Delete ------------------------------------------------------------------
echo "==> 8. delete"
smb "del renamed.bin" >/dev/null 2>&1
smb "del big.bin" >/dev/null 2>&1
smb "deltree docs" >/dev/null 2>&1
smb "deltree tree" >/dev/null 2>&1
if [[ -n "${SHARE_FS_PATH}" ]]; then
  if [[ ! -e "${SHARE_FS_PATH}/renamed.bin" && ! -e "${SHARE_FS_PATH}/big.bin" &&
    ! -e "${SHARE_FS_PATH}/docs" && ! -e "${SHARE_FS_PATH}/tree" ]]; then
    pass "delete files and directory trees"
  else
    fail "delete files and directory trees"
  fi
else
  if ! smb "get renamed.bin /dev/null" >/dev/null 2>&1; then
    pass "deleted file no longer retrievable"
  else
    fail "deleted file no longer retrievable"
  fi
fi

echo
echo "==> Summary: ${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
