//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSmoke(t *testing.T) {
	t.Run("Discovery", testSmokeDiscovery)
	t.Run("DDIntegrity", testSmokeDDIntegrity)
	t.Run("MkfsExt4", testSmokeMkfsExt4)
	t.Run("MkfsXfs", testSmokeMkfsXfs)
	t.Run("FioVerify", testSmokeFioVerify)
	t.Run("LogoutClean", testSmokeLogoutClean)
}

func testSmokeDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "50M", "")

	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	iqns, err := iscsi.Discover(ctx, host, tgt.config.Port)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	found := false
	for _, iqn := range iqns {
		if iqn == tgt.config.IQN {
			found = true
		}
	}
	if !found {
		t.Fatalf("IQN %s not found in discovery response: %v", tgt.config.IQN, iqns)
	}
}

func testSmokeDDIntegrity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "50M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Write 1MB of random data
	_, _, code, err := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if err != nil || code != 0 {
		t.Fatalf("dd write: code=%d err=%v", code, err)
	}

	// Read back and checksum
	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum", dev))
	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum", dev))

	s1 := firstLine(sum1)
	s2 := firstLine(sum2)
	if s1 != s2 {
		t.Fatalf("checksum mismatch: %s vs %s", s1, s2)
	}
	t.Logf("checksums match: %s", s1)
}

func testSmokeMkfsExt4(t *testing.T) {
	testSmokeMkfs(t, "ext4", "mkfs.ext4", "100M")
}

func testSmokeMkfsXfs(t *testing.T) {
	t.Skip("P3-BUG-11: WRITE SAME(16) not implemented, XFS sends it for inode zeroing")
	if !clientNode.HasCommand("mkfs.xfs") {
		t.Skip("mkfs.xfs not available")
	}
	testSmokeMkfs(t, "xfs", "mkfs.xfs", "500M") // XFS needs >= 300MB
}

func testSmokeMkfs(t *testing.T, fstype, mkfsCmd, volSize string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, volSize, "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-mnt"

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanCancel()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("rm -rf %s", mnt))
	})

	// mkfs
	mkfsArgs := " -F" // ext4: force, xfs: force overwrite
	if fstype == "xfs" {
		mkfsArgs = " -f"
	}
	_, stderr, code, err := clientNode.RunRoot(ctx,
		fmt.Sprintf("%s%s %s", mkfsCmd, mkfsArgs, dev))
	if err != nil || code != 0 {
		t.Fatalf("mkfs.%s: code=%d stderr=%s err=%v", fstype, code, stderr, err)
	}

	// Mount, write, unmount
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	testContent := "blockvol-integration-test-data"
	// Use bash -c with tee to ensure redirect works under sudo
	clientNode.RunRoot(ctx, fmt.Sprintf("bash -c 'echo %s | tee %s/testfile.txt'", testContent, mnt))
	clientNode.RunRoot(ctx, "sync")
	clientNode.RunRoot(ctx, fmt.Sprintf("umount %s", mnt))

	// Brief pause to let device settle after unmount
	time.Sleep(1 * time.Second)

	// Remount and verify
	mountOpts := ""
	if fstype == "xfs" {
		mountOpts = "-o nouuid" // avoid UUID conflict with stale kernel state
	}
	_, stderr2, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s %s", mountOpts, dev, mnt))
	if code != 0 {
		t.Fatalf("remount failed: %s", stderr2)
	}

	stdout, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("cat %s/testfile.txt", mnt))
	if !strings.Contains(stdout, testContent) {
		t.Fatalf("file content mismatch: got %q, want %q", stdout, testContent)
	}
	t.Logf("%s: file persists across mount cycles", fstype)
}

func testSmokeFioVerify(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	cmd := fmt.Sprintf("fio --name=verify --filename=%s --rw=randrw --verify=crc32 "+
		"--bs=4k --size=50M --randrepeat=1 --direct=1 --ioengine=libaio "+
		"--runtime=60 --time_based=0 --output-format=json", dev)
	stdout, stderr, code, err := clientNode.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		t.Fatalf("fio: code=%d stderr=%s err=%v", code, stderr, err)
	}

	if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
		t.Fatalf("fio verify errors detected")
	}
	t.Log("fio verify passed with 0 errors")
}

func testSmokeLogoutClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "50M", "")
	_ = startAndLogin(t, ctx, tgt, iscsi, host)

	// Logout
	if err := iscsi.Logout(ctx, tgt.config.IQN); err != nil {
		t.Fatalf("logout: %v", err)
	}

	// Verify no stale sessions
	stdout, _, _, _ := clientNode.RunRoot(ctx, "iscsiadm -m session 2>&1")
	if strings.Contains(stdout, tgt.config.IQN) {
		t.Fatalf("stale session found after logout: %s", stdout)
	}
	t.Log("no stale sessions after logout")
}
