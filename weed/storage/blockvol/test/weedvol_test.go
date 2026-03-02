//go:build integration

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// weedBinary is built once in TestWeedVol and reused across subtests.
var weedBinary string

func TestWeedVol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Build weed binary once
	repoDir := *flagRepoDir
	t.Log("building weed binary...")
	wt := NewWeedTarget(targetNode, DefaultTargetConfig())
	if err := wt.Build(ctx, repoDir); err != nil {
		t.Fatalf("build weed: %v", err)
	}
	weedBinary = repoDir + "/weed-linux"
	if err := wt.Deploy(weedBinary); err != nil {
		t.Fatalf("deploy weed: %v", err)
	}
	t.Log("weed binary built and deployed")

	// 3B-1: Smoke
	t.Run("Discovery", testWeedVolDiscovery)
	t.Run("LoginIO", testWeedVolLoginIO)
	t.Run("MkfsExt4", testWeedVolMkfsExt4)
	t.Run("FioVerify", testWeedVolFioVerify)
	t.Run("Heartbeat", testWeedVolHeartbeat)
	t.Run("AttachScript", testWeedVolAttachScript)

	// 3B-2: WAL Pressure
	t.Run("PressureSustained", testWeedVolPressureSustained)
	t.Run("PressureSync", testWeedVolPressureSync)
	t.Run("PressureCrash", testWeedVolPressureCrash)
	t.Run("PressureBatch", testWeedVolPressureBatch)

	// 3B-3: Chaos
	t.Run("MonkeyReconnect", testWeedVolMonkeyReconnect)
	t.Run("MonkeyMultiVol", testWeedVolMonkeyMultiVol)
	t.Run("MonkeyConfigRestart", testWeedVolMonkeyConfigRestart)
	t.Run("MonkeyAttachDetach", testWeedVolMonkeyAttachDetach)
	t.Run("MonkeyWALFull", testWeedVolMonkeyWALFull)

	// 3B-4: Filesystem Stress
	t.Run("FsMkfsExt4Stress", testWeedVolFsMkfsStress)
	t.Run("FsTarExtract", testWeedVolFsTarExtract)
	t.Run("FsLongSoak", testWeedVolFsLongSoak)
	t.Run("FsPostgres", testWeedVolFsPostgres)
	t.Run("FsFsstress", testWeedVolFsFsstress)
}

// newWeedTestTarget creates a WeedTarget with test-specific config and cleanup.
func newWeedTestTarget(t *testing.T, volSize string) (*WeedTarget, *ISCSIClient, string) {
	cfg := DefaultTargetConfig()
	name := strings.ReplaceAll(t.Name(), "/", "-")
	cfg.IQN = "weedvol:" + strings.ToLower(name)
	if volSize != "" {
		cfg.VolSize = volSize
	}

	wt := NewWeedTarget(targetNode, cfg)
	iscsiC := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		iscsiC.Logout(ctx, wt.IQN())
		iscsiC.CleanupAll(ctx, wt.iqnPrefix)
		wt.Stop(ctx)
		wt.Cleanup(ctx)
	})
	t.Cleanup(func() { artifacts.Collect(t, wt) })

	return wt, iscsiC, host
}

// startAndLoginWeed creates vol, starts weed volume, discovers, logs in.
func startAndLoginWeed(t *testing.T, ctx context.Context, wt *WeedTarget, iscsiC *ISCSIClient, host string) string {
	t.Helper()
	if err := wt.Start(ctx, true); err != nil {
		t.Fatalf("start weed: %v", err)
	}
	if _, err := iscsiC.Discover(ctx, host, wt.config.Port); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsiC.Login(ctx, wt.IQN())
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	return dev
}

// ============================================================
// 3B-1: Smoke Tests
// ============================================================

func testWeedVolDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "50M")
	if err := wt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	iqns, err := iscsiC.Discover(ctx, host, wt.config.Port)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	found := false
	for _, iqn := range iqns {
		if iqn == wt.IQN() {
			found = true
		}
	}
	if !found {
		t.Fatalf("IQN %s not found in discovery: %v", wt.IQN(), iqns)
	}
	t.Logf("discovered IQN: %s", wt.IQN())
}

func testWeedVolLoginIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "50M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	// Write 1MB + read back + verify
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=4k count=1000 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("dd write failed")
	}

	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=1000 iflag=direct 2>/dev/null | md5sum", dev))
	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=1000 iflag=direct 2>/dev/null | md5sum", dev))

	if firstLine(sum1) != firstLine(sum2) {
		t.Fatalf("checksum mismatch: %s vs %s", firstLine(sum1), firstLine(sum2))
	}
	t.Logf("checksums match: %s", firstLine(sum1))
}

func testWeedVolMkfsExt4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)
	mnt := "/tmp/blockvol-mnt"

	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cc()
		clientNode.RunRoot(cctx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
	})

	// mkfs + mount + write + unmount + remount + verify
	_, stderr, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	if code != 0 {
		t.Fatalf("mkfs.ext4 failed: %s", stderr)
	}

	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	clientNode.RunRoot(ctx, fmt.Sprintf("bash -c 'echo weedvol-test-data | tee %s/testfile.txt'", mnt))
	clientNode.RunRoot(ctx, "sync")
	clientNode.RunRoot(ctx, fmt.Sprintf("umount %s", mnt))

	time.Sleep(1 * time.Second)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("remount failed")
	}

	stdout, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("cat %s/testfile.txt", mnt))
	if !strings.Contains(stdout, "weedvol-test-data") {
		t.Fatalf("file content mismatch: %q", stdout)
	}
	t.Log("ext4: file persists across mount cycles via weed volume")
}

func testWeedVolFioVerify(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-verify --filename=%s --rw=randrw --verify=crc32 "+
			"--bs=4k --size=50M --randrepeat=1 --direct=1 --ioengine=libaio "+
			"--output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
		t.Fatal("fio verify errors")
	}
	t.Log("fio verify passed via weed volume")
}

func testWeedVolHeartbeat(t *testing.T) {
	// Heartbeat requires weed master running. Skip for now -- would need
	// a full master+volume setup. Test that the volume starts and serves.
	t.Skip("requires weed master for heartbeat verification")
}

func testWeedVolAttachScript(t *testing.T) {
	// The attach script requires weed master to look up volumes.
	// Skip for now -- script works via master API.
	t.Skip("requires weed master for attach script")
}

// ============================================================
// 3B-2: WAL Pressure + Group Commit
// ============================================================

func testWeedVolPressureSustained(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	// Sustained write larger than default WAL
	_, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-sustained --filename=%s --rw=write --bs=64k "+
			"--size=80M --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	t.Log("sustained write pressure passed via weed volume")
}

func testWeedVolPressureSync(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	runtime := 60
	if testing.Short() {
		runtime = 15
	}
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-sync --filename=%s --rw=randwrite --bs=4k "+
			"--size=50M --fdatasync=1 --numjobs=16 --direct=1 --ioengine=libaio "+
			"--runtime=%d --time_based --group_reporting --output-format=json", dev, runtime))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}

	if idx := strings.Index(stdout, "\"iops\""); idx >= 0 {
		end := idx + 30
		if end > len(stdout) {
			end = len(stdout)
		}
		t.Logf("sync batch IOPS: %s...", stdout[idx:end])
	}
	t.Log("fdatasync pressure passed via weed volume")
}

func testWeedVolPressureCrash(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	// Write with fdatasync
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-crash --filename=%s --rw=write --bs=4k --size=10M "+
			"--fdatasync=1 --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio write failed")
	}

	// Record checksum
	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=2560 iflag=direct 2>/dev/null | md5sum", dev))

	// Kill
	t.Log("killing weed volume...")
	iscsiC.Logout(ctx, wt.IQN())
	iscsiC.CleanupAll(ctx, wt.iqnPrefix)
	wt.Kill9()

	// Restart
	t.Log("restarting weed volume...")
	if err := wt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	iscsiC.Discover(ctx, host, wt.config.Port)
	dev, err := iscsiC.Login(ctx, wt.IQN())
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=2560 iflag=direct 2>/dev/null | md5sum", dev))

	if firstLine(sum1) != firstLine(sum2) {
		t.Fatalf("synced data corrupted: %s vs %s", firstLine(sum1), firstLine(sum2))
	}
	t.Log("crash recovery: synced data intact via weed volume")
}

func testWeedVolPressureBatch(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	runtime := 30
	if testing.Short() {
		runtime = 10
	}
	// Heavy concurrent fdatasync -- should trigger group commit batching
	_, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-batch --filename=%s --rw=randwrite --bs=4k "+
			"--size=50M --fdatasync=1 --numjobs=32 --direct=1 --ioengine=libaio "+
			"--runtime=%d --time_based --group_reporting", dev, runtime))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	t.Log("group commit batch pressure passed via weed volume")
}

// ============================================================
// 3B-3: Chaos Monkey
// ============================================================

func testWeedVolMonkeyReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	if err := wt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	n := 10
	if testing.Short() {
		n = 3
	}
	for i := 0; i < n; i++ {
		t.Logf("reconnect %d/%d", i+1, n)

		iscsiC.Discover(ctx, host, wt.config.Port)
		dev, err := iscsiC.Login(ctx, wt.IQN())
		if err != nil {
			t.Fatalf("iter %d login: %v", i, err)
		}

		_, _, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
		if code != 0 {
			t.Fatalf("iter %d dd write failed", i)
		}

		if err := iscsiC.Logout(ctx, wt.IQN()); err != nil {
			t.Fatalf("iter %d logout: %v", i, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("%dx reconnect completed via weed volume", n)
}

func testWeedVolMonkeyMultiVol(t *testing.T) {
	// Multi-volume: create 2 .blk files in block dir, verify both discoverable
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt := NewWeedTarget(targetNode, DefaultTargetConfig())
	iscsiC := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		iscsiC.CleanupAll(cctx, wt.iqnPrefix)
		wt.Stop(cctx)
		wt.Cleanup(cctx)
	})
	t.Cleanup(func() { artifacts.Collect(t, wt) })

	// Create block dir with 2 volume files
	wt.node.Run(ctx, fmt.Sprintf("rm -rf %s && mkdir -p %s", wt.blockDir, wt.blockDir))
	wt.node.Run(ctx, fmt.Sprintf("truncate -s 50M %s/vol1.blk", wt.blockDir))
	wt.node.Run(ctx, fmt.Sprintf("truncate -s 50M %s/vol2.blk", wt.blockDir))

	if err := wt.Start(ctx, false); err != nil {
		t.Fatalf("start: %v", err)
	}

	iqns, err := iscsiC.Discover(ctx, host, wt.config.Port)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	iqn1 := wt.iqnPrefix + "vol1"
	iqn2 := wt.iqnPrefix + "vol2"
	found1, found2 := false, false
	for _, iqn := range iqns {
		if iqn == iqn1 {
			found1 = true
		}
		if iqn == iqn2 {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("expected both %s and %s in discovery, got: %v", iqn1, iqn2, iqns)
	}

	// Login to both and do I/O
	dev1, err := iscsiC.Login(ctx, iqn1)
	if err != nil {
		t.Fatalf("login vol1: %v", err)
	}
	dev2, err := iscsiC.Login(ctx, iqn2)
	if err != nil {
		t.Fatalf("login vol2: %v", err)
	}

	// Write different data to each
	clientNode.RunRoot(ctx, fmt.Sprintf("dd if=/dev/urandom of=%s bs=4k count=100 oflag=direct 2>/dev/null", dev1))
	clientNode.RunRoot(ctx, fmt.Sprintf("dd if=/dev/urandom of=%s bs=4k count=100 oflag=direct 2>/dev/null", dev2))

	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=100 iflag=direct 2>/dev/null | md5sum", dev1))
	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=4k count=100 iflag=direct 2>/dev/null | md5sum", dev2))

	if firstLine(sum1) == firstLine(sum2) {
		t.Fatalf("volumes should have different data")
	}

	iscsiC.Logout(ctx, iqn1)
	iscsiC.Logout(ctx, iqn2)
	t.Logf("2 volumes served independently: %s %s", dev1, dev2)
}

func testWeedVolMonkeyConfigRestart(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	// fio phase 1
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-cfg1 --filename=%s --rw=randrw --bs=4k "+
			"--size=10M --direct=1 --ioengine=libaio --randrepeat=1", dev))
	if code != 0 {
		t.Fatalf("fio phase 1 failed")
	}

	// Logout + stop + restart
	iscsiC.Logout(ctx, wt.IQN())
	iscsiC.CleanupAll(ctx, wt.iqnPrefix)
	wt.Stop(ctx)

	if err := wt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	iscsiC.Discover(ctx, host, wt.config.Port)
	dev, err := iscsiC.Login(ctx, wt.IQN())
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	// fio phase 2
	_, _, code, _ = clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-cfg2 --filename=%s --rw=randrw --bs=4k "+
			"--size=10M --direct=1 --ioengine=libaio --randrepeat=1", dev))
	if code != 0 {
		t.Fatalf("fio phase 2 failed")
	}
	t.Log("config restart passed via weed volume")
}

func testWeedVolMonkeyAttachDetach(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	if err := wt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	n := 5
	if testing.Short() {
		n = 3
	}
	for i := 0; i < n; i++ {
		t.Logf("attach/detach %d/%d", i+1, n)

		iscsiC.Discover(ctx, host, wt.config.Port)
		dev, err := iscsiC.Login(ctx, wt.IQN())
		if err != nil {
			t.Fatalf("iter %d login: %v", i, err)
		}

		_, _, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("fio --name=wv-ad%d --filename=%s --rw=randrw --verify=crc32 "+
				"--bs=4k --size=10M --direct=1 --ioengine=libaio --randrepeat=1", i, dev))
		if code != 0 {
			t.Fatalf("iter %d fio failed", i)
		}

		if err := iscsiC.Logout(ctx, wt.IQN()); err != nil {
			t.Fatalf("iter %d logout: %v", i, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("%dx attach/detach completed via weed volume", n)
}

func testWeedVolMonkeyWALFull(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Use small volume to pressure WAL
	wt, iscsiC, host := newWeedTestTarget(t, "50M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	_, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-walfull --filename=%s --rw=write --bs=64k "+
			"--size=40M --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	t.Log("WAL full pressure passed via weed volume")
}

// ============================================================
// 3B-4: Filesystem Stress
// ============================================================

func testWeedVolFsMkfsStress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "100M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)
	mnt := "/tmp/blockvol-mnt"

	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cc()
		clientNode.RunRoot(cctx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
	})

	// mkfs + mount + create many files + verify
	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	// Create 200 files
	for i := 0; i < 200; i++ {
		clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=/dev/urandom of=%s/file%d bs=1k count=5 2>/dev/null", mnt, i))
	}

	clientNode.RunRoot(ctx, "sync")

	// Count files
	stdout, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("ls %s | wc -l", mnt))
	count := strings.TrimSpace(stdout)
	t.Logf("created %s files on ext4 via weed volume", count)

	// Unmount + remount + verify count
	clientNode.RunRoot(ctx, fmt.Sprintf("umount %s", mnt))
	time.Sleep(1 * time.Second)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("remount failed")
	}

	stdout2, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("ls %s | wc -l", mnt))
	if strings.TrimSpace(stdout2) != count {
		t.Fatalf("file count mismatch after remount: %s vs %s", count, strings.TrimSpace(stdout2))
	}
	t.Log("ext4 stress: 200 files persist via weed volume")
}

func testWeedVolFsTarExtract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "200M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)
	mnt := "/tmp/blockvol-mnt"

	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cc()
		clientNode.RunRoot(cctx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
	})

	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	// Create source files, tar, extract, verify checksums
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s/src", mnt))
	for i := 0; i < 100; i++ {
		clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=/dev/urandom of=%s/src/file%d bs=1k count=10 2>/dev/null", mnt, i))
	}

	clientNode.RunRoot(ctx, fmt.Sprintf("cd %s && tar cf archive.tar src/", mnt))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s/dst && cd %s/dst && tar xf %s/archive.tar", mnt, mnt, mnt))

	sum1, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("cd %s/src && find . -type f -exec md5sum {} \\; | sort", mnt))
	sum2, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("cd %s/dst/src && find . -type f -exec md5sum {} \\; | sort", mnt))
	if sum1 != sum2 {
		t.Fatalf("tar extract checksums differ")
	}
	t.Log("tar extract + verify passed via weed volume")
}

func testWeedVolFsLongSoak(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}
	if testing.Short() {
		t.Skip("skipping long soak in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	wt, iscsiC, host := newWeedTestTarget(t, "200M")
	dev := startAndLoginWeed(t, ctx, wt, iscsiC, host)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=wv-soak --filename=%s --rw=randrw --verify=crc32 "+
			"--bs=4k --size=100M --randrepeat=1 --direct=1 --ioengine=libaio "+
			"--runtime=1800 --time_based --output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
		t.Fatal("fio verify errors during soak")
	}
	t.Log("30-minute soak passed via weed volume")
}

func testWeedVolFsPostgres(t *testing.T) {
	if !clientNode.HasCommand("pg_isready") {
		t.Skip("postgresql not available")
	}
	t.Skip("postgres integration requires dedicated setup")
}

func testWeedVolFsFsstress(t *testing.T) {
	if !clientNode.HasCommand("fsstress") {
		t.Skip("fsstress not available (xfstests)")
	}
	t.Skip("fsstress requires XFS support (P3-BUG-11)")
}

// ensureWeedBinaryDeployed verifies the weed binary was built in TestWeedVol.
// Individual subtests should not be run standalone since they depend on TestWeedVol
// building and deploying the binary first.
func ensureWeedBinaryDeployed(t *testing.T) {
	t.Helper()
	if weedBinary == "" {
		t.Skip("weed binary not built -- run TestWeedVol parent test")
	}
	// Verify it exists
	if _, err := os.Stat(weedBinary); err != nil {
		absPath, _ := filepath.Abs(weedBinary)
		t.Skipf("weed binary not found at %s", absPath)
	}
}
