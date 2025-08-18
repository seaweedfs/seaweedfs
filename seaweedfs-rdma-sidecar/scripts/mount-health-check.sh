#!/bin/bash

set -euo pipefail

MOUNT_POINT=${MOUNT_POINT:-"/mnt/seaweedfs"}

# Check if mount point exists and is mounted
if [[ ! -d "$MOUNT_POINT" ]]; then
    echo "Mount point $MOUNT_POINT does not exist"
    exit 1
fi

if ! mountpoint -q "$MOUNT_POINT"; then
    echo "Mount point $MOUNT_POINT is not mounted"
    exit 1
fi

# Try to list the mount point
if ! ls "$MOUNT_POINT" >/dev/null 2>&1; then
    echo "Cannot list mount point $MOUNT_POINT"
    exit 1
fi

echo "Mount point $MOUNT_POINT is healthy"
exit 0
