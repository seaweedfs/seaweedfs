import errno
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("cleanup", Path(__file__).with_name("cleanup.py"))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)

TARGET = "/tmp/seaweedfs_fuse_test_123/mount"
MOUNTINFO = f"42 1 0:9 / {TARGET} rw - fuse.seaweedfs seaweedfs rw\n"


class CleanupTests(unittest.TestCase):
    def run_cleanup(self, results, mounted=True):
        with patch.object(cleanup.Path, "read_text", return_value=MOUNTINFO), \
                patch.object(cleanup, "mounted", side_effect=mounted if isinstance(mounted, list) else None,
                             return_value=mounted) as check, \
                patch.object(cleanup.shutil, "rmtree") as rmtree, \
                patch.object(cleanup.subprocess, "run", side_effect=results) as run:
            status = cleanup.cleanup()
        return status, run, check, rmtree

    def result(self, status):
        return subprocess.CompletedProcess([], status)

    def test_only_fuse_integration_test_mounts_are_candidates(self):
        rows = MOUNTINFO
        rows += MOUNTINFO.replace("fuse.seaweedfs", "ext4")
        rows += MOUNTINFO.replace(TARGET, "/mnt/production")
        rows += MOUNTINFO.replace(TARGET, "/tmp/seaweedfs_fuse_test_123/data")
        rows += MOUNTINFO.replace(TARGET, "/home/user/seaweedfs_fuse_test_123/mount")
        rows += MOUNTINFO.replace(TARGET, "/tmp/seaweedfs/data")
        self.assertEqual(list(cleanup.test_mounts(rows)), [("42", TARGET)])

    def test_all_test_mount_layouts_are_candidates(self):
        targets = (
            "/tmp/seaweedfs_fuse_test_123/mount",
            "/tmp/seaweedfs_fuse_failover_456/mount0",
            "/tmp/seaweedfs_fuse_failover_456/mount1",
            "/var/tmp/seaweedfs_fuse_p2p_test_789/mount2",
            "/tmp/seaweedfs/mount",
        )
        rows = "".join(
            MOUNTINFO.replace("42", str(40 + i)).replace(TARGET, target)
            for i, target in enumerate(targets)
        )
        self.assertEqual(
            list(cleanup.test_mounts(rows)),
            [(str(40 + i), target) for i, target in enumerate(targets)],
        )

    def test_mountinfo_paths_are_unescaped_once(self):
        target = "/tmp/seaweedfs_fuse_test_space and\\slash/mount"
        encoded = target.replace("\\", r"\134").replace(" ", r"\040")
        rows = MOUNTINFO.replace(TARGET, encoded)
        self.assertEqual(list(cleanup.test_mounts(rows)), [("42", target)])

    def test_no_mounts_is_a_noop(self):
        with patch.object(cleanup.Path, "read_text", return_value=""), \
                patch.object(cleanup.subprocess, "run") as run:
            self.assertEqual(cleanup.cleanup(), 0)
        run.assert_not_called()

    def test_healthy_mount_is_untouched(self):
        status, run, _, _ = self.run_cleanup([self.result(0)])
        self.assertEqual(status, 0)
        self.assertEqual(run.call_count, 1)

    def test_other_probe_errors_are_untouched(self):
        for error in (errno.EACCES, errno.ENOENT, errno.EIO):
            with self.subTest(error=error):
                status, run, _, _ = self.run_cleanup([self.result(error)])
                self.assertEqual(status, 0)
                self.assertEqual(run.call_count, 1)

    def test_probe_timeout_does_not_detach_a_possibly_live_mount(self):
        status, run, _, _ = self.run_cleanup([subprocess.TimeoutExpired("probe", 5)])
        self.assertEqual(status, 0)
        self.assertEqual(run.call_count, 1)

    def test_disconnected_mount_is_lazily_unmounted(self):
        status, run, _, _ = self.run_cleanup([self.result(errno.ENOTCONN), self.result(0)], [True, False])
        self.assertEqual(status, 0)
        self.assertEqual(run.call_args_list[1].args[0], ["fusermount3", "-uz", "--", TARGET])
        self.assertEqual(run.call_args_list[0].kwargs["timeout"], 5)
        self.assertEqual(run.call_args_list[1].kwargs["timeout"], 10)

    def test_replaced_or_removed_mount_is_untouched(self):
        status, run, _, _ = self.run_cleanup([self.result(errno.ENOTCONN)], False)
        self.assertEqual(status, 0)
        self.assertEqual(run.call_count, 1)

    def test_mount_identity_is_checked_without_stat(self):
        with patch.object(cleanup.Path, "read_text", return_value=MOUNTINFO):
            self.assertTrue(cleanup.mounted("42", TARGET))
            self.assertFalse(cleanup.mounted("43", TARGET))

    def test_missing_or_timed_out_helper_uses_fallback(self):
        for error in (FileNotFoundError(), subprocess.TimeoutExpired("fusermount3", 10)):
            with self.subTest(error=error):
                status, run, _, _ = self.run_cleanup(
                    [self.result(errno.ENOTCONN), error, self.result(0)], [True, True, False])
                self.assertEqual(status, 0)
                self.assertEqual(run.call_args.args[0], ["fusermount", "-uz", "--", TARGET])

    def test_failed_helpers_fall_back_to_sudo(self):
        status, run, _, _ = self.run_cleanup(
            [self.result(errno.ENOTCONN), self.result(1), self.result(1), self.result(0)],
            [True, True, True, False])
        self.assertEqual(status, 0)
        self.assertEqual(run.call_args.args[0], ["sudo", "-n", "umount", "-l", "--", TARGET])

    def test_cleanup_fails_when_stale_mount_remains(self):
        status, run, _, rmtree = self.run_cleanup(
            [self.result(errno.ENOTCONN), self.result(1), self.result(1), self.result(1)])
        self.assertEqual(status, 1)
        self.assertEqual(run.call_count, 4)
        rmtree.assert_not_called()

    def test_unmounted_test_dir_is_reclaimed(self):
        status, _, _, rmtree = self.run_cleanup(
            [self.result(errno.ENOTCONN), self.result(0)], [True, False])
        self.assertEqual(status, 0)
        rmtree.assert_called_once_with(Path(TARGET).parent, ignore_errors=True)

    def test_probe_process_reports_errno(self):
        for target, expected in ((str(Path(__file__)), 0), (str(Path(__file__) / "absent"), errno.ENOTDIR)):
            with self.subTest(target=target):
                result = subprocess.run([cleanup.sys.executable, cleanup.__file__, "--probe", target], check=False)
                self.assertEqual(result.returncode, expected)


if __name__ == "__main__":
    unittest.main()
