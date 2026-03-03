#!/usr/bin/env bash
# sw-block-multipath-setup.sh -- install SeaweedFS BlockVol multipath config.
# Usage: sudo ./sw-block-multipath-setup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONF_SRC="${SCRIPT_DIR}/sw-block-multipath.conf"
CONF_DIR="/etc/multipath/conf.d"
CONF_DST="${CONF_DIR}/sw-block.conf"

if [ ! -f "$CONF_SRC" ]; then
    echo "ERROR: $CONF_SRC not found" >&2
    exit 1
fi

# Ensure required packages are installed
for pkg in multipath-tools sg3-utils; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        echo "Installing $pkg..."
        apt-get install -y "$pkg"
    fi
done

# Install config
mkdir -p "$CONF_DIR"
cp "$CONF_SRC" "$CONF_DST"
echo "Installed $CONF_DST"

# Restart multipathd
systemctl restart multipathd
echo "multipathd restarted"

# Show current multipath state
echo ""
echo "=== multipath -ll ==="
multipath -ll || true
