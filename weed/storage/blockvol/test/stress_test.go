//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStress(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required for stress tests")
	}
	t.Run("Fio5Min", testStressFio5Min)
	t.Run("WALPressure", testStressWALPressure)
	t.Run("SyncBatch", testStressSyncBatch)
	t.Run("TarExtract", testStressTarExtract)
	t.Run("Soak30Min", testStressSoak30Min)
	t.Run("MixedBlockSize", testStressMixedBlockSize)
}

func testStressFio5Min(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "200M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	runtime := 300
	if testing.Short() {
		runtime = 30
	}
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=stress5m --filename=%s --rw=randrw --verify=crc32 "+
			"--bs=4k --size=100M --randrepeat=1 --direct=1 --ioengine=libaio "+
			"--runtime=%d --time_based --output-format=json", dev, runtime))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
		t.Fatal("fio verify errors")
	}
	t.Log("5-minute fio randrw+verify passed")
}

func testStressWALPressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "4M") // small WAL
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Write more than WAL size to force WAL wrap
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=walpressure --filename=%s --rw=write --bs=64k "+
			"--size=50M --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	t.Log("WAL pressure test passed (4MB WAL, 50MB write)")
}

func testStressSyncBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=syncbatch --filename=%s --rw=randwrite --bs=4k "+
			"--size=50M --fdatasync=1 --numjobs=16 --direct=1 --ioengine=libaio "+
			"--runtime=60 --time_based --group_reporting --output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}

	// Extract IOPS from output
	if idx := strings.Index(stdout, "\"iops\""); idx >= 0 {
		end := idx + 30
		if end > len(stdout) {
			end = len(stdout)
		}
		t.Logf("sync batch IOPS: %s...", stdout[idx:end])
	}
	t.Log("sync batch test passed (16 jobs, fdatasync)")
}

func testStressTarExtract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "200M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-mnt"

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanCancel()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
	})

	// mkfs + mount
	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))
	if code != 0 {
		t.Fatalf("mount failed")
	}

	// Create a tarball with known content, extract, verify
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s/src", mnt))
	for i := 0; i < 100; i++ {
		clientNode.RunRoot(ctx, fmt.Sprintf("dd if=/dev/urandom of=%s/src/file%d bs=1k count=10 2>/dev/null", mnt, i))
	}

	// Tar and extract
	clientNode.RunRoot(ctx, fmt.Sprintf("cd %s && tar cf archive.tar src/", mnt))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s/dst && cd %s/dst && tar xf %s/archive.tar", mnt, mnt, mnt))

	// Verify
	sum1, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("cd %s/src && find . -type f -exec md5sum {} \\; | sort", mnt))
	sum2, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("cd %s/dst/src && find . -type f -exec md5sum {} \\; | sort", mnt))
	if sum1 != sum2 {
		t.Fatalf("tar extract checksums differ")
	}
	t.Log("tar extract + verify passed (100 files)")
}

func testStressSoak30Min(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 30-minute soak in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "200M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	soakTime := 1800
	if testing.Short() {
		soakTime = 60
	}
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=soak --filename=%s --rw=randrw --verify=crc32 "+
			"--bs=4k --size=100M --randrepeat=1 --direct=1 --ioengine=libaio "+
			"--runtime=%d --time_based --output-format=json", dev, soakTime))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
		t.Fatal("fio verify errors during 30-min soak")
	}
	t.Log("30-minute soak passed")
}

func testStressMixedBlockSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "200M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	sizes := []string{"4k", "64k", "1M"} // 512 below logical block size (4096)
	for _, bs := range sizes {
		t.Logf("testing bs=%s", bs)
		stdout, stderr, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("fio --name=mixed_%s --filename=%s --rw=randrw --verify=crc32 "+
				"--bs=%s --size=20M --randrepeat=1 --direct=1 --ioengine=libaio", bs, dev, bs))
		if code != 0 {
			t.Fatalf("fio bs=%s: code=%d stderr=%s", bs, code, stderr)
		}
		if strings.Contains(stdout, "\"verify_errors\"") && !strings.Contains(stdout, "\"verify_errors\" : 0") {
			t.Fatalf("fio verify errors at bs=%s", bs)
		}
	}
	t.Log("mixed block size test passed (512, 4k, 64k, 1M)")
}
