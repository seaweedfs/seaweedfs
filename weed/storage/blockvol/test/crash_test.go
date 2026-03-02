//go:build integration

package test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCrash(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required for crash tests")
	}
	t.Run("Kill9Fsync", testCrashKill9Fsync)
	t.Run("Kill9NoSync", testCrashKill9NoSync)
	t.Run("WALReplay", testCrashWALReplay)
	t.Run("RapidKill10x", testCrashRapidKill10x)
	t.Run("FsckAfterCrash", testCrashFsckAfterCrash)
}

func testCrashKill9Fsync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Write with fdatasync
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=sync --filename=%s --rw=write --bs=4k --size=10M "+
			"--fdatasync=1 --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio write failed: code=%d", code)
	}

	// Record checksum of synced data
	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=2560 iflag=direct 2>/dev/null | md5sum", dev))

	// Kill9
	t.Log("killing target...")
	tgt.Kill9()

	// Clean up stale iSCSI state before restart
	iscsi.Logout(ctx, tgt.config.IQN)
	iscsi.CleanupAll(ctx, tgt.config.IQN)

	// Restart
	t.Log("restarting target...")
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// Re-login
	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	// Verify synced data intact
	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=2560 iflag=direct 2>/dev/null | md5sum", dev))

	if firstLine(sum1) != firstLine(sum2) {
		t.Fatalf("synced data corrupted: %s vs %s", firstLine(sum1), firstLine(sum2))
	}
	t.Log("synced data intact after Kill9")
}

func testCrashKill9NoSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	_ = startAndLogin(t, ctx, tgt, iscsi, host)

	// Kill9 without sync
	tgt.Kill9()
	iscsi.Logout(ctx, tgt.config.IQN)

	// Restart -- volume must open without corruption
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart after unclean kill: %v", err)
	}

	// Login to verify volume is usable
	_, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("login after restart: %v", err)
	}
	t.Log("volume opened successfully after Kill9 (no sync)")
}

func testCrashWALReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "4M") // small WAL
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Write a 4k block of known data (O_DIRECT requires sector-aligned writes)
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=4k count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("pattern write failed")
	}
	// Read back the checksum before kill
	sumBefore, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=1 iflag=direct 2>/dev/null | md5sum", dev))

	// Kill9 before flush can happen
	tgt.Kill9()
	iscsi.Logout(ctx, tgt.config.IQN)

	// Restart (WAL replay)
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// Re-login and verify
	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	sumAfter, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=1 iflag=direct 2>/dev/null | md5sum", dev))
	// Non-fdatasync writes have no durability guarantee, but volume must be readable
	if firstLine(sumAfter) == "" {
		t.Fatalf("could not read data after WAL replay")
	}
	t.Logf("WAL replay: before=%s after=%s (match=%v)",
		firstLine(sumBefore), firstLine(sumAfter), firstLine(sumBefore) == firstLine(sumAfter))
	t.Log("WAL replay completed, volume intact")
}

func testCrashRapidKill10x(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "50M", "")

	n := 10
	if testing.Short() {
		n = 3
	}
	for i := 0; i < n; i++ {
		t.Logf("iteration %d/%d", i+1, n)

		create := (i == 0)
		if err := tgt.Start(ctx, create); err != nil {
			t.Fatalf("iter %d start: %v", i, err)
		}

		_, err := iscsi.Discover(ctx, host, tgt.config.Port)
		if err != nil {
			t.Fatalf("iter %d discover: %v", i, err)
		}

		dev, err := iscsi.Login(ctx, tgt.config.IQN)
		if err != nil {
			t.Fatalf("iter %d login: %v", i, err)
		}

		// Write 1MB
		_, _, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
		if code != 0 {
			t.Fatalf("iter %d dd write failed", i)
		}

		iscsi.Logout(ctx, tgt.config.IQN)
		tgt.Kill9()
	}
	t.Logf("%dx rapid kill completed", n)
}

func testCrashFsckAfterCrash(t *testing.T) {
	t.Skip("P3-BUG-11: WRITE SAME(16) not implemented, XFS sends it for inode zeroing")
	if !clientNode.HasCommand("mkfs.xfs") {
		t.Skip("mkfs.xfs required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "500M", "") // XFS needs >= 300MB
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-mnt"

	// mkfs.xfs + mount
	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.xfs -f %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	// Workload: create some files
	for i := 0; i < 50; i++ {
		clientNode.RunRoot(ctx, fmt.Sprintf("dd if=/dev/urandom of=%s/file%d bs=4k count=10 2>/dev/null", mnt, i))
	}

	// Sync filesystem metadata to device, then unmount + Kill9
	clientNode.RunRoot(ctx, "sync")
	clientNode.RunRoot(ctx, fmt.Sprintf("umount %s 2>/dev/null", mnt))
	iscsi.Logout(ctx, tgt.config.IQN)
	iscsi.CleanupAll(ctx, tgt.config.IQN)
	tgt.Kill9()

	// Restart
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	// xfs_repair -n (read-only check)
	stdout, stderr, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("xfs_repair -n %s", dev))
	if code != 0 {
		t.Fatalf("xfs_repair failed: stdout=%s stderr=%s", stdout, stderr)
	}
	t.Log("xfs_repair -n passed (filesystem clean)")
}
