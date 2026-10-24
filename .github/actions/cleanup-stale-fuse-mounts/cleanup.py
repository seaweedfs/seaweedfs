#!/usr/bin/env python3
"""Detach disconnected SeaweedFS integration-test mounts without touching live ones."""

import errno
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


def test_mounts(mountinfo):
    """Read the kernel table directly: traversing a stale mount may block."""
    roots = {tempfile.gettempdir(), "/tmp", "/var/tmp"}
    for line in mountinfo.splitlines():
        fields, filesystem = line.split(" - ", 1)
        if filesystem.split()[0].split(".")[0] != "fuse":
            continue
        fields = fields.split()
        # mountinfo escapes whitespace and backslashes with octal sequences.
        target = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
        path = Path(target)
        # Test mounts are a mount* leaf under a seaweedfs_fuse_* scratch dir
        # (seaweedfs_fuse_test_*/mount, seaweedfs_fuse_failover_*/mount0, ...)
        # or the posix-compliance workflow's fixed /tmp/seaweedfs/mount.
        if (not path.name.startswith("mount")
                or str(path.parent.parent) not in roots
                or not (path.parent.name.startswith("seaweedfs_fuse")
                        or path.parent.name == "seaweedfs")):
            continue
        yield fields[0], target


def mounted(mount_id, target):
    return (mount_id, target) in test_mounts(Path("/proc/self/mountinfo").read_text())


def cleanup():
    failed = False
    for mount_id, target in test_mounts(Path("/proc/self/mountinfo").read_text()):
        try:
            # A separate, bounded probe prevents a wedged FUSE daemon from
            # hanging cleanup. Only ENOTCONN proves the daemon disconnected;
            # a timeout or another error is not enough to detach a live mount.
            probe = subprocess.run(
                [sys.executable, __file__, "--probe", target], timeout=5,
                capture_output=True, check=False,
            )
        except subprocess.TimeoutExpired:
            print(f"::warning::FUSE probe timed out; leaving {target} mounted")
            continue
        if probe.returncode != errno.ENOTCONN or not mounted(mount_id, target):
            continue

        print(f"Unmounting disconnected FUSE test mount: {target}", flush=True)
        # Lazy unmount detaches a dead mount even when an old process still
        # holds it open. Prefer the unprivileged FUSE helper; sudo is fallback.
        for command in (
            ["fusermount3", "-uz", "--", target],
            ["fusermount", "-uz", "--", target],
            ["sudo", "-n", "umount", "-l", "--", target],
        ):
            try:
                subprocess.run(command, timeout=10, check=False)
            except (OSError, subprocess.TimeoutExpired) as error:
                print(f"{command[0]}: {error}", file=sys.stderr)
            if not mounted(mount_id, target):
                break
        else:
            print(f"::error::Could not unmount disconnected FUSE test mount: {target}")
            failed = True
            continue
        # The detached mount's parent still holds the crashed run's data and
        # logs; test_mounts only yields targets under a SeaweedFS test dir in
        # a temp root, so the parent is safe to reclaim here.
        shutil.rmtree(Path(target).parent, ignore_errors=True)
    return int(failed)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        try:
            os.stat(sys.argv[2])
        except OSError as error:
            sys.exit(error.errno)
        sys.exit(0)
    sys.exit(cleanup())
