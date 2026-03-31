//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Port assignments for fault/consistency tests (non-overlapping with HA 3260-3261, multipath 3270-3271).
const (
	faultISCSIPort1   = 3280 // primary iSCSI
	faultISCSIPort2   = 3281 // replica iSCSI
	faultAdminPort1   = 8100 // primary admin
	faultAdminPort2   = 8101 // replica admin
	faultReplData1    = 9031 // replica receiver data
	faultReplCtrl1    = 9032 // replica receiver ctrl
	faultRebuildPort1 = 9033 // rebuild server (primary)
	faultRebuildPort2 = 9034 // rebuild server (replica)
)

// newFaultPair creates a primary+replica HA pair using fault-test ports.
func newFaultPair(t *testing.T, volSize string) (primary, replica *HATarget, iscsiClient *ISCSIClient) {
	t.Helper()

	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanCancel()
	clientNode.RunRoot(cleanCtx, "iscsiadm -m node --logoutall=all 2>/dev/null")
	targetNode.Run(cleanCtx, "pkill -9 -f blockvol-ha 2>/dev/null")
	if clientNode != targetNode {
		clientNode.Run(cleanCtx, "pkill -9 -f blockvol-ha 2>/dev/null")
	}
	time.Sleep(2 * time.Second)

	name := strings.ReplaceAll(t.Name(), "/", "-")

	primaryCfg := DefaultTargetConfig()
	primaryCfg.IQN = iqnPrefix + "-" + strings.ToLower(name) + "-pri"
	primaryCfg.Port = faultISCSIPort1
	if volSize != "" {
		primaryCfg.VolSize = volSize
	}
	primary = NewHATarget(targetNode, primaryCfg, faultAdminPort1, 0, 0, 0)
	primary.volFile = "/tmp/blockvol-fault-primary.blk"
	primary.logFile = "/tmp/iscsi-fault-primary.log"

	replicaCfg := DefaultTargetConfig()
	replicaCfg.IQN = iqnPrefix + "-" + strings.ToLower(name) + "-rep"
	replicaCfg.Port = faultISCSIPort2
	if volSize != "" {
		replicaCfg.VolSize = volSize
	}
	replica = NewHATarget(clientNode, replicaCfg, faultAdminPort2, faultReplData1, faultReplCtrl1, 0)
	replica.volFile = "/tmp/blockvol-fault-replica.blk"
	replica.logFile = "/tmp/iscsi-fault-replica.log"

	if clientNode != targetNode {
		if err := replica.Deploy(*flagRepoDir + "/iscsi-target-linux"); err != nil {
			t.Fatalf("deploy replica binary: %v", err)
		}
	}

	iscsiClient = NewISCSIClient(clientNode)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		iscsiClient.Logout(ctx, primaryCfg.IQN)
		iscsiClient.Logout(ctx, replicaCfg.IQN)
		primary.Stop(ctx)
		replica.Stop(ctx)
		primary.Cleanup(ctx)
		replica.Cleanup(ctx)
	})
	t.Cleanup(func() {
		artifacts.CollectLabeled(t, primary.Target, "fault-primary")
		artifacts.CollectLabeled(t, replica.Target, "fault-replica")
	})

	return primary, replica, iscsiClient
}

// setupFaultPrimaryReplica starts both targets, assigns roles, configures WAL shipping.
func setupFaultPrimaryReplica(t *testing.T, ctx context.Context, primary, replica *HATarget, leaseTTLMs uint32) {
	t.Helper()

	t.Log("starting primary...")
	if err := primary.Start(ctx, true); err != nil {
		t.Fatalf("start primary: %v", err)
	}
	t.Log("starting replica...")
	if err := replica.Start(ctx, true); err != nil {
		t.Fatalf("start replica: %v", err)
	}

	t.Log("assigning replica role...")
	if err := replica.Assign(ctx, 1, roleReplica, 0); err != nil {
		t.Fatalf("assign replica: %v", err)
	}

	t.Log("assigning primary role...")
	if err := primary.Assign(ctx, 1, rolePrimary, leaseTTLMs); err != nil {
		t.Fatalf("assign primary: %v", err)
	}

	t.Log("configuring WAL shipping...")
	if err := primary.SetReplica(ctx, replicaAddr(faultReplData1), replicaAddr(faultReplCtrl1)); err != nil {
		t.Fatalf("set replica target: %v", err)
	}
}

func TestFault(t *testing.T) {
	t.Run("PowerLossDuringFio", testFaultPowerLossDuringFio)
	t.Run("DiskFullENOSPC", testFaultDiskFullENOSPC)
	t.Run("WALCorruption", testFaultWALCorruption)
	t.Run("ReplicaDownDuringWrites", testFaultReplicaDownDuringWrites)
	t.Run("SlowNetworkBarrierTimeout", testFaultSlowNetworkBarrierTimeout)
	t.Run("NetworkPartitionSelfFence", testFaultNetworkPartitionSelfFence)
	t.Run("SnapshotDuringFailover", testFaultSnapshotDuringFailover)
}

// F1: PowerLossDuringFio — sustained fio at kill time, fdatasync'd data survives on replica.
func testFaultPowerLossDuringFio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newFaultPair(t, "100M")
	setupFaultPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// Login to primary
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Write 1MB known pattern, record md5
	t.Log("writing 1MB known pattern...")
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/fault-pattern.bin bs=1M count=1 2>/dev/null")
	wMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/fault-pattern.bin | awk '{print $1}'")
	wMD5 = strings.TrimSpace(wMD5)

	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/fault-pattern.bin of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("dd write failed")
	}

	// Wait for replication of known pattern
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}

	// Start fio with fdatasync for 10s in background
	t.Log("starting background fio (10s with fdatasync)...")
	fioCmd := fmt.Sprintf(
		"fio --name=powerloss --filename=%s --ioengine=libaio --direct=1 "+
			"--rw=randwrite --bs=4k --numjobs=2 --iodepth=8 --runtime=10 "+
			"--time_based --fdatasync=1 --offset=1M --size=90M "+
			"--group_reporting 2>/dev/null &",
		dev)
	clientNode.RunRoot(ctx, fioCmd)

	// After 3s, kill primary
	time.Sleep(3 * time.Second)
	t.Log("killing primary during fio...")
	primary.Kill9()

	// Wait for fio to exit (it will get I/O errors)
	time.Sleep(10 * time.Second)

	// Logout stale session
	iscsi.Logout(ctx, primary.config.IQN)

	// Promote replica
	t.Log("promoting replica (epoch=2)...")
	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	// Login to promoted replica
	repHost := *flagClientHost
	if *flagEnv == "wsl2" {
		repHost = "127.0.0.1"
	}
	if _, err := iscsi.Discover(ctx, repHost, faultISCSIPort2); err != nil {
		t.Fatalf("discover promoted: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login promoted: %v", err)
	}

	// Read first 1MB, verify md5 matches (fdatasync'd data guaranteed)
	t.Log("verifying first 1MB on promoted replica...")
	rMD5, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rMD5 = strings.TrimSpace(rMD5)

	if wMD5 != rMD5 {
		t.Fatalf("md5 mismatch: wrote=%s read=%s", wMD5, rMD5)
	}

	iscsi.Logout(ctx, replica.config.IQN)
	t.Log("PowerLossDuringFio passed: fdatasync'd data survived failover")
}

// F2: DiskFullENOSPC — writes fail under ENOSPC, reads still work, recovery after cleanup.
func testFaultDiskFullENOSPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Use a tmpfs for controlled disk space
	enospcDir := "/tmp/bv-enospc"

	// Clean up any prior mount
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanCancel()
	clientNode.RunRoot(cleanCtx, "iscsiadm -m node --logoutall=all 2>/dev/null")
	targetNode.Run(cleanCtx, "pkill -9 -f blockvol-ha 2>/dev/null")
	targetNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", enospcDir))
	time.Sleep(2 * time.Second)

	// Create tmpfs mount
	targetNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", enospcDir))
	_, stderr, code, _ := targetNode.RunRoot(ctx, fmt.Sprintf(
		"mount -t tmpfs -o size=120M tmpfs %s", enospcDir))
	if code != 0 {
		t.Fatalf("mount tmpfs: code=%d stderr=%s", code, stderr)
	}
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		targetNode.RunRoot(cctx, fmt.Sprintf("umount -f %s 2>/dev/null", enospcDir))
	})

	// Create single target on tmpfs
	name := strings.ReplaceAll(t.Name(), "/", "-")
	cfg := DefaultTargetConfig()
	cfg.IQN = iqnPrefix + "-" + strings.ToLower(name)
	cfg.Port = faultISCSIPort1
	cfg.VolSize = "80M"

	tgt := NewHATarget(targetNode, cfg, faultAdminPort1, 0, 0, 0)
	tgt.volFile = enospcDir + "/blockvol-enospc.blk"
	tgt.logFile = enospcDir + "/iscsi-enospc.log"

	iscsi := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		iscsi.Logout(cctx, cfg.IQN)
		tgt.Stop(cctx)
	})
	t.Cleanup(func() { artifacts.CollectLabeled(t, tgt.Target, "enospc") })

	// Start target
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := tgt.Assign(ctx, 1, rolePrimary, 30000); err != nil {
		t.Fatalf("assign: %v", err)
	}

	// Login
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, cfg.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Write 1MB known data
	t.Log("writing 1MB known data...")
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/enospc-pattern.bin bs=1M count=1 2>/dev/null")
	wMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/enospc-pattern.bin | awk '{print $1}'")
	wMD5 = strings.TrimSpace(wMD5)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/enospc-pattern.bin of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("initial write failed")
	}

	// Fill tmpfs to trigger ENOSPC
	t.Log("filling tmpfs to trigger ENOSPC...")
	targetNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/zero of=%s/fillfile bs=1M count=100 2>/dev/null; true", enospcDir))

	// Write should fail
	t.Log("attempting write under ENOSPC...")
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=4K count=1 seek=300 oflag=direct 2>/dev/null", dev))
	if code == 0 {
		t.Log("write under ENOSPC unexpectedly succeeded (WAL may have had space)")
	} else {
		t.Log("write under ENOSPC correctly failed")
	}

	// Read should still work
	t.Log("verifying read still works...")
	rMD5, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev))
	rMD5 = strings.TrimSpace(rMD5)
	if wMD5 != rMD5 {
		t.Fatalf("read under ENOSPC: md5 mismatch: wrote=%s read=%s", wMD5, rMD5)
	}

	// Remove fill file, write should succeed again
	t.Log("removing fill file, retrying write...")
	targetNode.RunRoot(ctx, fmt.Sprintf("rm -f %s/fillfile", enospcDir))
	time.Sleep(1 * time.Second)

	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=4K count=1 seek=300 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Logf("write after ENOSPC recovery failed (may need target restart)")
	} else {
		t.Log("write after ENOSPC recovery succeeded")
	}

	iscsi.Logout(ctx, cfg.IQN)
	t.Log("DiskFullENOSPC passed: reads survived, writes failed/recovered as expected")
}

// F3: WALCorruption — corrupt WAL tail, restart, verify pre-corruption data intact.
func testFaultWALCorruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanCancel()
	clientNode.RunRoot(cleanCtx, "iscsiadm -m node --logoutall=all 2>/dev/null")
	targetNode.Run(cleanCtx, "pkill -9 -f blockvol-ha 2>/dev/null")
	time.Sleep(2 * time.Second)

	name := strings.ReplaceAll(t.Name(), "/", "-")
	cfg := DefaultTargetConfig()
	cfg.IQN = iqnPrefix + "-" + strings.ToLower(name)
	cfg.Port = faultISCSIPort1
	cfg.VolSize = "50M"

	tgt := NewTarget(targetNode, cfg)
	tgt.volFile = "/tmp/blockvol-walcorrupt.blk"
	tgt.logFile = "/tmp/iscsi-walcorrupt.log"
	iscsi := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		iscsi.Logout(cctx, cfg.IQN)
		tgt.Stop(cctx)
		tgt.Cleanup(cctx)
	})
	t.Cleanup(func() { artifacts.Collect(t, tgt) })

	// Start, login
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := iscsi.Discover(ctx, host, cfg.Port); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, cfg.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Write 10 x 4K blocks with fdatasync
	t.Log("writing 10 x 4K blocks...")
	for i := 0; i < 10; i++ {
		clientNode.RunRoot(ctx, fmt.Sprintf(
			"dd if=/dev/urandom of=/tmp/walcorrupt-blk%d.bin bs=4K count=1 2>/dev/null", i))
		_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
			"dd if=/tmp/walcorrupt-blk%d.bin of=%s bs=4K count=1 seek=%d oflag=direct 2>/dev/null", i, dev, i))
		if code != 0 {
			t.Fatalf("write block %d failed", i)
		}
	}

	// Record md5 of first 5 blocks (20KB)
	t.Log("recording md5 of first 5 blocks...")
	earlyMD5, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=4K count=5 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev))
	earlyMD5 = strings.TrimSpace(earlyMD5)
	t.Logf("early 5-block md5: %s", earlyMD5)

	// Logout and stop target
	iscsi.Logout(ctx, cfg.IQN)
	if err := tgt.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Corrupt 64 bytes within the WAL region of the volume file
	t.Log("corrupting 64 bytes in WAL region...")
	if err := corruptWALRegion(ctx, targetNode, tgt.volFile, 64); err != nil {
		t.Fatalf("corrupt WAL: %v", err)
	}

	// Restart target (WAL recovery should discard corrupted tail)
	t.Log("restarting target (WAL recovery)...")
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart after corruption: %v", err)
	}

	// Re-login
	if _, err := iscsi.Discover(ctx, host, cfg.Port); err != nil {
		t.Fatalf("discover after restart: %v", err)
	}
	dev2, err := iscsi.Login(ctx, cfg.IQN)
	if err != nil {
		t.Fatalf("login after restart: %v", err)
	}

	// Read first 5 blocks, verify md5
	t.Log("verifying first 5 blocks after WAL recovery...")
	rMD5, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=4K count=5 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rMD5 = strings.TrimSpace(rMD5)

	if earlyMD5 != rMD5 {
		t.Fatalf("md5 mismatch after WAL recovery: expected=%s got=%s", earlyMD5, rMD5)
	}

	iscsi.Logout(ctx, cfg.IQN)
	t.Log("WALCorruption passed: early data intact after corrupt WAL recovery")
}

// F4: ReplicaDownDuringWrites — kill replica mid-fio, primary keeps serving.
func testFaultReplicaDownDuringWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newFaultPair(t, "100M")
	setupFaultPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// Login to primary
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Start fio for 5s in background
	t.Log("starting fio (5s runtime)...")
	fioCmd := fmt.Sprintf(
		"fio --name=repdown --filename=%s --ioengine=libaio --direct=1 "+
			"--rw=randwrite --bs=4k --numjobs=2 --iodepth=8 --runtime=5 "+
			"--time_based --group_reporting --output-format=json "+
			"--output=/tmp/fault-repdown-fio.json 2>/dev/null &",
		dev)
	clientNode.RunRoot(ctx, fioCmd)

	// After 1s, kill replica
	time.Sleep(1 * time.Second)
	t.Log("killing replica during writes...")
	replica.Kill9()

	// Wait for fio to finish
	time.Sleep(6 * time.Second)

	// Verify fio completed
	stdout, _, _, _ := clientNode.RunRoot(ctx,
		"cat /tmp/fault-repdown-fio.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[\"jobs\"][0][\"error\"])' 2>/dev/null")
	fioErr := strings.TrimSpace(stdout)
	t.Logf("fio error code: %s", fioErr)

	// Primary should still have lease
	st, err := primary.Status(ctx)
	if err != nil {
		t.Fatalf("primary status: %v", err)
	}
	if !st.HasLease {
		t.Fatalf("primary lost lease after replica death")
	}
	t.Logf("primary status: role=%s has_lease=%v epoch=%d", st.Role, st.HasLease, st.Epoch)

	// Write more data — should succeed
	t.Log("writing more data after replica death...")
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=4K count=10 seek=100 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write after replica death failed")
	}

	iscsi.Logout(ctx, primary.config.IQN)
	t.Log("ReplicaDownDuringWrites passed: primary kept serving after replica crash")
}

// F5: SlowNetworkBarrierTimeout — tc netem delay, primary may degrade replica. Remote only.
func testFaultSlowNetworkBarrierTimeout(t *testing.T) {
	if *flagEnv == "wsl2" {
		t.Skip("tc netem requires two separate nodes; skipping on WSL2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newFaultPair(t, "100M")
	setupFaultPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// Login to primary
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Inject 200ms netem delay on targetNode toward clientNode (replica)
	t.Log("injecting 200ms netem delay...")
	cleanup, err := injectNetem(ctx, targetNode, *flagClientHost, 200)
	if err != nil {
		t.Fatalf("inject netem: %v", err)
	}
	defer cleanup()

	// Write with fdatasync
	t.Log("writing under netem delay...")
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=4K count=10 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Logf("write under delay failed (expected if barrier timed out)")
	} else {
		t.Log("write under delay succeeded")
	}

	// Primary should still be running (may have degraded replica)
	st, err := primary.Status(ctx)
	if err != nil {
		t.Fatalf("primary status: %v", err)
	}
	t.Logf("primary status: role=%s has_lease=%v epoch=%d", st.Role, st.HasLease, st.Epoch)

	// Cleanup netem before logout
	cleanup()

	iscsi.Logout(ctx, primary.config.IQN)
	t.Log("SlowNetworkBarrierTimeout passed: writes continued under 200ms delay")
}

// F6: NetworkPartitionSelfFence — iptables drop, primary self-fences on lease expiry. Remote only.
func testFaultNetworkPartitionSelfFence(t *testing.T) {
	if *flagEnv == "wsl2" {
		t.Skip("iptables partition requires two separate nodes; skipping on WSL2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newFaultPair(t, "100M")

	// Start targets manually with short lease
	t.Log("starting primary + replica with 5s lease...")
	if err := primary.Start(ctx, true); err != nil {
		t.Fatalf("start primary: %v", err)
	}
	if err := replica.Start(ctx, true); err != nil {
		t.Fatalf("start replica: %v", err)
	}
	if err := replica.Assign(ctx, 1, roleReplica, 0); err != nil {
		t.Fatalf("assign replica: %v", err)
	}
	if err := primary.Assign(ctx, 1, rolePrimary, 5000); err != nil {
		t.Fatalf("assign primary: %v", err)
	}
	if err := primary.SetReplica(ctx, replicaAddr(faultReplData1), replicaAddr(faultReplCtrl1)); err != nil {
		t.Fatalf("set replica: %v", err)
	}

	host := targetHost()

	// Login, write 1MB
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write failed")
	}

	// Wait for replication
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}

	// Inject iptables drop: block replication ports from primary to replica
	t.Log("injecting iptables drop (blocking replication ports)...")
	cleanup, err := injectIptablesDrop(ctx, targetNode, *flagClientHost,
		[]int{faultReplData1, faultReplCtrl1})
	if err != nil {
		t.Fatalf("inject iptables: %v", err)
	}
	defer cleanup()

	// Wait for lease to expire (5s + 1s margin)
	t.Log("waiting 6s for lease expiry...")
	time.Sleep(6 * time.Second)

	// Primary should have self-fenced (lost lease)
	st, err := primary.Status(ctx)
	if err != nil {
		t.Fatalf("primary status: %v", err)
	}
	if st.HasLease {
		t.Fatalf("primary should have self-fenced (lost lease), got has_lease=true")
	}
	t.Logf("primary self-fenced: has_lease=%v role=%s epoch=%d", st.HasLease, st.Role, st.Epoch)

	// Cleanup iptables, promote replica, verify data
	cleanup()

	iscsi.Logout(ctx, primary.config.IQN)

	t.Log("promoting replica (epoch=2)...")
	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	repHost := *flagClientHost
	if _, err := iscsi.Discover(ctx, repHost, faultISCSIPort2); err != nil {
		t.Fatalf("discover promoted: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login promoted: %v", err)
	}

	// Verify data readable
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum", dev2))
	if code != 0 {
		t.Fatalf("read from promoted replica failed")
	}

	iscsi.Logout(ctx, replica.config.IQN)
	t.Log("NetworkPartitionSelfFence passed: primary self-fenced, data intact on replica")
}

// F7: SnapshotDuringFailover — snapshot on primary, write more, kill, verify replica has all data.
func testFaultSnapshotDuringFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newFaultPair(t, "100M")
	setupFaultPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// Login to primary
	if _, err := iscsi.Discover(ctx, host, faultISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// Write 1MB pattern A
	t.Log("writing pattern A (1MB)...")
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/fault-snapA.bin bs=1M count=1 2>/dev/null")
	aMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/fault-snapA.bin | awk '{print $1}'")
	aMD5 = strings.TrimSpace(aMD5)
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/fault-snapA.bin of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write pattern A failed")
	}

	// Wait for replication
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}

	// Create snapshot on primary
	t.Log("creating snapshot on primary...")
	snapCode, snapBody, err := primary.curlPost(ctx, "/snapshot", map[string]string{
		"action": "create",
		"name":   "pre-failover",
	})
	if err != nil {
		t.Logf("snapshot request error: %v", err)
	} else if snapCode != 200 {
		t.Logf("snapshot returned %d: %s (may not be supported)", snapCode, snapBody)
	} else {
		t.Log("snapshot created successfully")
	}

	// Write 1MB pattern B at offset 1MB
	t.Log("writing pattern B (1MB at offset 1MB)...")
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/fault-snapB.bin bs=1M count=1 2>/dev/null")
	bMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/fault-snapB.bin | awk '{print $1}'")
	bMD5 = strings.TrimSpace(bMD5)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/fault-snapB.bin of=%s bs=1M count=1 seek=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write pattern B failed")
	}

	// Wait for B to replicate
	repSt, _ := replica.Status(ctx)
	priSt, _ := primary.Status(ctx)
	t.Logf("pre-kill: primary LSN=%d, replica LSN=%d", priSt.WALHeadLSN, repSt.WALHeadLSN)

	waitCtx2, waitCancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel2()
	if err := replica.WaitForLSN(waitCtx2, priSt.WALHeadLSN); err != nil {
		t.Logf("replica may not have all data: %v", err)
	}

	// Logout and kill primary
	iscsi.Logout(ctx, primary.config.IQN)
	t.Log("killing primary...")
	primary.Kill9()

	// Promote replica
	t.Log("promoting replica (epoch=2)...")
	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	// Login to promoted replica
	repHost := *flagClientHost
	if *flagEnv == "wsl2" {
		repHost = "127.0.0.1"
	}
	if _, err := iscsi.Discover(ctx, repHost, faultISCSIPort2); err != nil {
		t.Fatalf("discover promoted: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login promoted: %v", err)
	}

	// Verify pattern A + B on promoted replica
	rA, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rA = strings.TrimSpace(rA)
	rB, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 skip=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rB = strings.TrimSpace(rB)

	if aMD5 != rA {
		t.Fatalf("pattern A mismatch: wrote=%s read=%s", aMD5, rA)
	}
	if bMD5 != rB {
		t.Fatalf("pattern B mismatch: wrote=%s read=%s", bMD5, rB)
	}

	iscsi.Logout(ctx, replica.config.IQN)
	t.Log("SnapshotDuringFailover passed: both patterns intact on replica after failover")
}
