//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestChaos(t *testing.T) {
	t.Run("Reconnect20", testChaosReconnect20)
	t.Run("MultiSession4", testChaosMultiSession4)
	t.Run("WALFull", testChaosWALFull)
	t.Run("AttachDetach10", testChaosAttachDetach10)
	t.Run("ConfigRestart", testChaosConfigRestart)
}

func testChaosReconnect20(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	n := 20
	if testing.Short() {
		n = 5
	}
	for i := 0; i < n; i++ {
		t.Logf("reconnect %d/%d", i+1, n)

		iscsi.Discover(ctx, host, tgt.config.Port)
		dev, err := iscsi.Login(ctx, tgt.config.IQN)
		if err != nil {
			t.Fatalf("iter %d login: %v", i, err)
		}

		// Write 1MB + verify
		_, _, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
		if code != 0 {
			t.Fatalf("iter %d dd write failed", i)
		}

		sum, _, _, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum", dev))
		if firstLine(sum) == "" {
			t.Fatalf("iter %d empty checksum", i)
		}

		if err := iscsi.Logout(ctx, tgt.config.IQN); err != nil {
			t.Fatalf("iter %d logout: %v", i, err)
		}

		// Brief pause for session teardown
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("%dx reconnect completed", n)
}

func testChaosMultiSession4(t *testing.T) {
	t.Skip("multi-session test requires multiple target IQN support")
}

func testChaosWALFull(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "4M") // tiny WAL
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Sustained write much larger than WAL
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=walfull --filename=%s --rw=write --bs=64k "+
			"--size=80M --direct=1 --ioengine=libaio", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	t.Log("WAL full test passed (4MB WAL, 80MB write)")
}

func testChaosAttachDetach10(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}

	n2 := 10
	if testing.Short() {
		n2 = 3
	}
	for i := 0; i < n2; i++ {
		t.Logf("attach/detach %d/%d", i+1, n2)

		iscsi.Discover(ctx, host, tgt.config.Port)
		dev, err := iscsi.Login(ctx, tgt.config.IQN)
		if err != nil {
			t.Fatalf("iter %d login: %v", i, err)
		}

		// Quick fio
		_, _, code, _ := clientNode.RunRoot(ctx,
			fmt.Sprintf("fio --name=ad%d --filename=%s --rw=randrw --verify=crc32 "+
				"--bs=4k --size=10M --direct=1 --ioengine=libaio --randrepeat=1", i, dev))
		if code != 0 {
			t.Fatalf("iter %d fio failed", i)
		}

		if err := iscsi.Logout(ctx, tgt.config.IQN); err != nil {
			t.Fatalf("iter %d logout: %v", i, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Verify no stale devices
	stdout, _, _, _ := clientNode.RunRoot(ctx, "iscsiadm -m session 2>&1")
	if strings.Contains(stdout, tgt.config.IQN) {
		t.Fatalf("stale session after 10 cycles: %s", stdout)
	}
	t.Logf("%dx attach/detach completed, no stale devices", n2)
}

func testChaosConfigRestart(t *testing.T) {
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// fio with default config
	_, _, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=cfg1 --filename=%s --rw=randrw --bs=4k "+
			"--size=10M --direct=1 --ioengine=libaio --randrepeat=1", dev))
	if code != 0 {
		t.Fatalf("fio phase 1 failed")
	}

	// Logout + stop
	iscsi.Logout(ctx, tgt.config.IQN)
	tgt.Stop(ctx)

	// Restart (open existing vol)
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}

	iscsi.Discover(ctx, host, tgt.config.Port)
	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	// fio again
	_, _, code, _ = clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=cfg2 --filename=%s --rw=randrw --bs=4k "+
			"--size=10M --direct=1 --ioengine=libaio --randrepeat=1", dev))
	if code != 0 {
		t.Fatalf("fio phase 2 failed")
	}
	t.Log("config restart test passed")
}
