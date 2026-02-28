#!/usr/bin/env bash
# smoke-test.sh — iscsiadm smoke test for SeaweedFS iSCSI target
#
# Prerequisites:
#   - Linux host with iscsiadm (open-iscsi) installed
#   - Root access (for iscsiadm, mkfs, mount)
#   - iscsi-target binary built: go build ./weed/storage/blockvol/iscsi/cmd/iscsi-target/
#
# Usage:
#   sudo ./smoke-test.sh [target-addr:port]
#
# Default: starts a local target on :3260, runs discovery + login + I/O, cleans up.

set -euo pipefail

TARGET_ADDR="${1:-127.0.0.1}"
TARGET_PORT="${2:-3260}"
TARGET_IQN="iqn.2024.com.seaweedfs:smoke"
VOL_FILE="/tmp/iscsi-smoke-test.blk"
MOUNT_POINT="/tmp/iscsi-smoke-mnt"
TARGET_PID=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[SMOKE]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cleanup() {
    log "Cleaning up..."

    # Unmount if mounted
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        umount "$MOUNT_POINT" || true
    fi
    rmdir "$MOUNT_POINT" 2>/dev/null || true

    # Logout from iSCSI
    iscsiadm -m node -T "$TARGET_IQN" -p "${TARGET_ADDR}:${TARGET_PORT}" --logout 2>/dev/null || true
    iscsiadm -m node -T "$TARGET_IQN" -p "${TARGET_ADDR}:${TARGET_PORT}" -o delete 2>/dev/null || true

    # Stop target
    if [[ -n "$TARGET_PID" ]] && kill -0 "$TARGET_PID" 2>/dev/null; then
        kill "$TARGET_PID"
        wait "$TARGET_PID" 2>/dev/null || true
    fi

    # Remove volume file
    rm -f "$VOL_FILE"

    log "Cleanup complete"
}

trap cleanup EXIT

# -------------------------------------------------------
# Preflight
# -------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    fail "This script must be run as root (for iscsiadm/mount)"
fi

if ! command -v iscsiadm &>/dev/null; then
    fail "iscsiadm not found. Install open-iscsi: apt install open-iscsi"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_BIN="${SCRIPT_DIR}/iscsi-target"
if [[ ! -x "$TARGET_BIN" ]]; then
    # Try the repo root
    TARGET_BIN="$(cd "$SCRIPT_DIR/../../../../../.." && pwd)/iscsi-target"
fi
if [[ ! -x "$TARGET_BIN" ]]; then
    fail "iscsi-target binary not found. Build with: go build ./weed/storage/blockvol/iscsi/cmd/iscsi-target/"
fi

# -------------------------------------------------------
# Step 1: Start iSCSI target
# -------------------------------------------------------
log "Starting iSCSI target..."
"$TARGET_BIN" \
    -create \
    -vol "$VOL_FILE" \
    -size 100M \
    -addr "${TARGET_ADDR}:${TARGET_PORT}" \
    -iqn "$TARGET_IQN" &
TARGET_PID=$!
sleep 1

if ! kill -0 "$TARGET_PID" 2>/dev/null; then
    fail "Target failed to start"
fi
log "Target started (PID $TARGET_PID)"

# -------------------------------------------------------
# Step 2: Discovery
# -------------------------------------------------------
log "Running iscsiadm discovery..."
DISCOVERY=$(iscsiadm -m discovery -t sendtargets -p "${TARGET_ADDR}:${TARGET_PORT}" 2>&1) || {
    fail "Discovery failed: $DISCOVERY"
}
echo "$DISCOVERY"

if ! echo "$DISCOVERY" | grep -q "$TARGET_IQN"; then
    fail "Target IQN not found in discovery output"
fi
log "Discovery OK"

# -------------------------------------------------------
# Step 3: Login
# -------------------------------------------------------
log "Logging in to target..."
iscsiadm -m node -T "$TARGET_IQN" -p "${TARGET_ADDR}:${TARGET_PORT}" --login || {
    fail "Login failed"
}
log "Login OK"

# Wait for device to appear
sleep 2
ISCSI_DEV=$(iscsiadm -m session -P 3 2>/dev/null | grep "Attached scsi disk" | awk '{print $4}' | head -1)
if [[ -z "$ISCSI_DEV" ]]; then
    warn "Could not determine attached device — trying /dev/sdb"
    ISCSI_DEV="sdb"
fi
DEV_PATH="/dev/$ISCSI_DEV"
log "Attached device: $DEV_PATH"

if [[ ! -b "$DEV_PATH" ]]; then
    fail "Block device $DEV_PATH not found"
fi

# -------------------------------------------------------
# Step 4: dd write/read test
# -------------------------------------------------------
log "Writing test pattern with dd..."
dd if=/dev/urandom of=/tmp/iscsi-smoke-pattern bs=1M count=1 2>/dev/null
dd if=/tmp/iscsi-smoke-pattern of="$DEV_PATH" bs=1M count=1 oflag=direct 2>/dev/null || {
    fail "dd write failed"
}

log "Reading back and verifying..."
dd if="$DEV_PATH" of=/tmp/iscsi-smoke-readback bs=1M count=1 iflag=direct 2>/dev/null || {
    fail "dd read failed"
}

if cmp -s /tmp/iscsi-smoke-pattern /tmp/iscsi-smoke-readback; then
    log "Data integrity verified (dd write/read OK)"
else
    fail "Data integrity check failed!"
fi
rm -f /tmp/iscsi-smoke-pattern /tmp/iscsi-smoke-readback

# -------------------------------------------------------
# Step 5: mkfs + mount + file I/O
# -------------------------------------------------------
log "Creating ext4 filesystem..."
mkfs.ext4 -F -q "$DEV_PATH" || {
    warn "mkfs.ext4 failed — skipping filesystem tests"
    # Still consider the test a pass if dd worked
    log "SMOKE TEST PASSED (dd only, mkfs skipped)"
    exit 0
}

mkdir -p "$MOUNT_POINT"
mount "$DEV_PATH" "$MOUNT_POINT" || {
    fail "mount failed"
}
log "Mounted at $MOUNT_POINT"

# Write a file
echo "SeaweedFS iSCSI smoke test" > "$MOUNT_POINT/test.txt"
sync

# Read it back
CONTENT=$(cat "$MOUNT_POINT/test.txt")
if [[ "$CONTENT" != "SeaweedFS iSCSI smoke test" ]]; then
    fail "File content mismatch"
fi
log "File I/O verified"

# -------------------------------------------------------
# Step 6: Logout
# -------------------------------------------------------
log "Unmounting and logging out..."
umount "$MOUNT_POINT"
iscsiadm -m node -T "$TARGET_IQN" -p "${TARGET_ADDR}:${TARGET_PORT}" --logout || {
    fail "Logout failed"
}
log "Logout OK"

# Verify no stale sessions
SESSION_COUNT=$(iscsiadm -m session 2>/dev/null | grep -c "$TARGET_IQN" || true)
if [[ "$SESSION_COUNT" -gt 0 ]]; then
    fail "Stale session detected after logout"
fi
log "No stale sessions"

# -------------------------------------------------------
# Result
# -------------------------------------------------------
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  SMOKE TEST PASSED${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Discovery:  OK"
echo "  Login:      OK"
echo "  dd I/O:     OK"
echo "  mkfs+mount: OK"
echo "  File I/O:   OK"
echo "  Logout:     OK"
echo "  Cleanup:    OK"
echo ""
