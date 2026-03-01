#!/usr/bin/env bash
# sw-block-attach.sh — Discover and attach a SeaweedFS block volume via iSCSI
#
# Prerequisites:
#   - Linux host with iscsiadm (open-iscsi) installed
#   - Root access (for iscsiadm login)
#   - SeaweedFS volume server running with --block.dir set
#
# Usage:
#   sudo ./sw-block-attach.sh <target-addr> [volume-name]
#
# Examples:
#   sudo ./sw-block-attach.sh 10.0.0.1:3260           # discover all, attach first
#   sudo ./sw-block-attach.sh 10.0.0.1:3260 myvol     # attach specific volume
#   sudo ./sw-block-attach.sh 10.0.0.1:3260 --list     # list available targets
#   sudo ./sw-block-attach.sh --detach <target-iqn> <portal>  # detach a volume
#
# The script will:
#   1. Run iscsiadm discovery against the target portal
#   2. Find the matching volume IQN (or list all)
#   3. Login (attach) the iSCSI target
#   4. Print the attached block device path

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[sw-block]${NC} $*"; }
warn() { echo -e "${YELLOW}[sw-block]${NC} $*"; }
fail() { echo -e "${RED}[sw-block]${NC} $*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Usage:
  sw-block-attach.sh <target-addr:port> [volume-name]   Attach a block volume
  sw-block-attach.sh <target-addr:port> --list           List available targets
  sw-block-attach.sh --detach <target-iqn> <portal>      Detach a block volume
  sw-block-attach.sh --status                             Show active sessions

Examples:
  sudo ./sw-block-attach.sh 10.0.0.1:3260
  sudo ./sw-block-attach.sh 10.0.0.1:3260 myvol
  sudo ./sw-block-attach.sh --detach iqn.2024-01.com.seaweedfs:vol.myvol 10.0.0.1:3260
EOF
    exit 1
}

# -------------------------------------------------------
# Preflight
# -------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    fail "This script must be run as root (for iscsiadm)"
fi

if ! command -v iscsiadm &>/dev/null; then
    fail "iscsiadm not found. Install open-iscsi: apt install open-iscsi"
fi

if [[ $# -lt 1 ]]; then
    usage
fi

# -------------------------------------------------------
# --status: show active sessions
# -------------------------------------------------------
if [[ "$1" == "--status" ]]; then
    log "Active iSCSI sessions:"
    iscsiadm -m session 2>/dev/null || echo "  (none)"
    exit 0
fi

# -------------------------------------------------------
# --detach: logout from a target
# -------------------------------------------------------
if [[ "$1" == "--detach" ]]; then
    if [[ $# -lt 3 ]]; then
        fail "Usage: sw-block-attach.sh --detach <target-iqn> <portal>"
    fi
    TARGET_IQN="$2"
    PORTAL="$3"

    log "Logging out from $TARGET_IQN at $PORTAL..."
    iscsiadm -m node -T "$TARGET_IQN" -p "$PORTAL" --logout || {
        fail "Logout failed"
    }
    iscsiadm -m node -T "$TARGET_IQN" -p "$PORTAL" -o delete 2>/dev/null || true
    log "Detached successfully"
    exit 0
fi

# -------------------------------------------------------
# Attach mode
# -------------------------------------------------------
PORTAL="$1"
VOL_NAME="${2:-}"

# Add default port if not specified.
if [[ "$PORTAL" != *:* ]]; then
    PORTAL="${PORTAL}:3260"
fi

# -------------------------------------------------------
# Step 1: Discovery
# -------------------------------------------------------
log "Discovering targets at $PORTAL..."
DISCOVERY=$(iscsiadm -m discovery -t sendtargets -p "$PORTAL" 2>&1) || {
    fail "Discovery failed: $DISCOVERY"
}

if [[ -z "$DISCOVERY" ]]; then
    fail "No targets found at $PORTAL"
fi

# -------------------------------------------------------
# --list: just show targets
# -------------------------------------------------------
if [[ "$VOL_NAME" == "--list" ]]; then
    log "Available targets:"
    echo "$DISCOVERY"
    exit 0
fi

# -------------------------------------------------------
# Step 2: Find target IQN
# -------------------------------------------------------
if [[ -n "$VOL_NAME" ]]; then
    # Match by volume name suffix.
    TARGET_IQN=$(echo "$DISCOVERY" | awk '{print $2}' | grep -F "$VOL_NAME" | head -1)
    if [[ -z "$TARGET_IQN" ]]; then
        fail "No target found matching '$VOL_NAME'. Available targets:"
        echo "$DISCOVERY" >&2
    fi
else
    # Use the first target found.
    TARGET_IQN=$(echo "$DISCOVERY" | awk '{print $2}' | head -1)
    if [[ -z "$TARGET_IQN" ]]; then
        fail "No targets found"
    fi
fi

log "Target: $TARGET_IQN"

# -------------------------------------------------------
# Step 3: Check if already logged in
# -------------------------------------------------------
if iscsiadm -m session 2>/dev/null | grep -q "$TARGET_IQN"; then
    warn "Already logged in to $TARGET_IQN"
    # Show the device
    DEV=$(iscsiadm -m session -P 3 2>/dev/null | grep -A 20 "$TARGET_IQN" | grep "Attached scsi disk" | awk '{print $4}' | head -1)
    if [[ -n "$DEV" ]]; then
        log "Device: /dev/$DEV"
    fi
    exit 0
fi

# -------------------------------------------------------
# Step 4: Login
# -------------------------------------------------------
log "Logging in to $TARGET_IQN..."
iscsiadm -m node -T "$TARGET_IQN" -p "$PORTAL" --login || {
    fail "Login failed"
}

# Wait for device to appear.
sleep 2

# -------------------------------------------------------
# Step 5: Find attached device
# -------------------------------------------------------
DEV=$(iscsiadm -m session -P 3 2>/dev/null | grep -A 20 "$TARGET_IQN" | grep "Attached scsi disk" | awk '{print $4}' | head -1)
if [[ -z "$DEV" ]]; then
    warn "Login succeeded but could not determine device path"
    warn "Check: lsblk, or iscsiadm -m session -P 3"
else
    log "Attached: /dev/$DEV"
    echo ""
    echo -e "  Block device:  ${GREEN}/dev/$DEV${NC}"
    echo -e "  Target IQN:    $TARGET_IQN"
    echo -e "  Portal:        $PORTAL"
    echo ""
    echo "  To detach:  sudo $0 --detach $TARGET_IQN $PORTAL"
fi
