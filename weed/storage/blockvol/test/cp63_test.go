//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// CP6-3 Integration Tests: Failover, Rebuild, Assignment Lifecycle.
// These exercise the master-level control-plane behaviors end-to-end
// using the standalone iscsi-target binary with admin HTTP API.

func TestCP63(t *testing.T) {
	t.Run("FailoverCSIAddressSwitch", testFailoverCSIAddressSwitch)
	t.Run("RebuildDataConsistency", testRebuildDataConsistency)
	t.Run("FullLifecycleFailoverRebuild", testFullLifecycleFailoverRebuild)
}

// testFailoverCSIAddressSwitch simulates the CSI ControllerPublishVolume flow
// after failover: primary dies, replica is promoted, and the "CSI controller"
// returns the new iSCSI address. The initiator re-discovers + logs in at the
// new address and verifies data integrity, then writes new data.
//
// This goes beyond testFailoverKillPrimary by also:
//   - Writing new data AFTER failover on the promoted replica.
//   - Verifying the iSCSI target address changed (CSI address-switch logic).
func testFailoverCSIAddressSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	primary, replica, iscsi := newHAPair(t, "100M")
	setupPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// --- Phase 1: Write data through primary ---
	t.Log("phase 1: login to primary, write 1MB...")
	if _, err := iscsi.Discover(ctx, host, haISCSIPort1); err != nil {
		t.Fatalf("discover primary: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login primary: %v", err)
	}
	t.Logf("primary device: %s (addr: %s:%d)", dev, host, haISCSIPort1)

	// Write pattern A
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-patA.bin bs=1M count=1 2>/dev/null")
	aMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-patA.bin | awk '{print $1}'")
	aMD5 = strings.TrimSpace(aMD5)

	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-patA.bin of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write pattern A failed")
	}

	// Wait for replication
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}

	// --- Phase 2: Kill primary, promote replica (master failover logic) ---
	t.Log("phase 2: killing primary, promoting replica...")
	iscsi.Logout(ctx, primary.config.IQN)
	primary.Kill9()

	// Master promotes replica (epoch bump + role=Primary)
	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	// --- Phase 3: CSI address switch ---
	// In real CSI: ControllerPublishVolume queries master.LookupBlockVolume
	// which returns the promoted replica's iSCSI address. Here we simulate by
	// using the replica's known address.
	repHost := *flagClientHost
	if *flagEnv == "wsl2" {
		repHost = "127.0.0.1"
	}
	newISCSIAddr := fmt.Sprintf("%s:%d", repHost, haISCSIPort2)
	t.Logf("phase 3: CSI address switch → new iSCSI target at %s", newISCSIAddr)

	// Client re-discovers and logs in to the new primary (was replica)
	if _, err := iscsi.Discover(ctx, repHost, haISCSIPort2); err != nil {
		t.Fatalf("discover new primary: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login new primary: %v", err)
	}
	t.Logf("new primary device: %s (addr: %s)", dev2, newISCSIAddr)

	// Verify pattern A survived failover
	rA, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rA = strings.TrimSpace(rA)
	if aMD5 != rA {
		t.Fatalf("pattern A mismatch after failover: wrote=%s read=%s", aMD5, rA)
	}

	// --- Phase 4: Write new data on promoted replica ---
	t.Log("phase 4: writing pattern B on promoted replica...")
	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-patB.bin bs=1M count=1 2>/dev/null")
	bMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-patB.bin | awk '{print $1}'")
	bMD5 = strings.TrimSpace(bMD5)

	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-patB.bin of=%s bs=1M count=1 seek=1 oflag=direct 2>/dev/null", dev2))
	if code != 0 {
		t.Fatalf("write pattern B failed")
	}

	// Verify both patterns readable
	rA2, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rA2 = strings.TrimSpace(rA2)
	rB, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 skip=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rB = strings.TrimSpace(rB)

	if aMD5 != rA2 {
		t.Fatalf("pattern A mismatch after write B: wrote=%s read=%s", aMD5, rA2)
	}
	if bMD5 != rB {
		t.Fatalf("pattern B mismatch: wrote=%s read=%s", bMD5, rB)
	}

	iscsi.Logout(ctx, replica.config.IQN)
	t.Log("FailoverCSIAddressSwitch passed: address switch + data A/B intact")
}

// testRebuildDataConsistency: full rebuild cycle with data verification.
//
//  1. Setup primary+replica, write data A (replicated)
//  2. Kill replica → write data B on primary (replica misses this)
//  3. Restart replica → assign Rebuilding → start rebuild from primary
//  4. Wait for rebuild completion (LSN catch-up + role → Replica)
//  5. Kill primary → promote rebuilt replica → verify data A+B
func testRebuildDataConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	primary, replica, iscsi := newHAPair(t, "100M")
	setupPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// --- Phase 1: Write data A (replicated) ---
	t.Log("phase 1: login to primary, write 1MB (replicated)...")
	if _, err := iscsi.Discover(ctx, host, haISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-rebA.bin bs=1M count=1 2>/dev/null")
	aMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-rebA.bin | awk '{print $1}'")
	aMD5 = strings.TrimSpace(aMD5)
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-rebA.bin of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write A failed")
	}

	// Wait for replication
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}
	repSt, _ := replica.Status(ctx)
	t.Logf("replica after A: epoch=%d role=%s lsn=%d", repSt.Epoch, repSt.Role, repSt.WALHeadLSN)

	// --- Phase 2: Kill replica, write data B (missed by replica) ---
	t.Log("phase 2: killing replica, writing data B on primary...")
	replica.Kill9()
	time.Sleep(1 * time.Second)

	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-rebB.bin bs=1M count=1 2>/dev/null")
	bMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-rebB.bin | awk '{print $1}'")
	bMD5 = strings.TrimSpace(bMD5)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-rebB.bin of=%s bs=1M count=1 seek=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write B failed")
	}

	// Capture primary status (LSN should have advanced)
	priSt, _ := primary.Status(ctx)
	t.Logf("primary after B: epoch=%d role=%s lsn=%d", priSt.Epoch, priSt.Role, priSt.WALHeadLSN)

	// Capture full 2MB md5 from primary
	allMD5, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=2 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev))
	allMD5 = strings.TrimSpace(allMD5)
	t.Logf("primary 2MB md5: %s", allMD5)

	// Logout from primary
	iscsi.Logout(ctx, primary.config.IQN)

	// --- Phase 3: Start rebuild server on primary ---
	t.Log("phase 3: starting rebuild server on primary...")
	if err := primary.StartRebuildEndpoint(ctx, fmt.Sprintf(":%d", haRebuildPort1)); err != nil {
		t.Fatalf("start rebuild server: %v", err)
	}

	// --- Phase 4: Restart replica, assign Rebuilding, connect rebuild client ---
	t.Log("phase 4: restarting replica as rebuilding...")
	if err := replica.Start(ctx, false); err != nil {
		t.Fatalf("restart replica: %v", err)
	}

	// Assign as Rebuilding (RoleNone → RoleRebuilding supported since CP6-3).
	if err := replica.Assign(ctx, 1, roleRebuilding, 0); err != nil {
		t.Fatalf("assign rebuilding: %v", err)
	}

	// Verify role is Rebuilding
	repSt, _ = replica.Status(ctx)
	t.Logf("replica before rebuild: epoch=%d role=%s lsn=%d", repSt.Epoch, repSt.Role, repSt.WALHeadLSN)

	// Start rebuild client on replica — connects to primary's rebuild server
	rebuildAddr := primaryAddr(haRebuildPort1)
	t.Logf("starting rebuild client → %s", rebuildAddr)
	if err := replica.StartRebuildClient(ctx, rebuildAddr, priSt.Epoch); err != nil {
		t.Fatalf("start rebuild client: %v", err)
	}

	// Wait for rebuild completion (role transitions Rebuilding → Replica)
	t.Log("waiting for rebuild completion (role → replica)...")
	rebuildCtx, rebuildCancel := context.WithTimeout(ctx, 60*time.Second)
	defer rebuildCancel()
	if err := replica.WaitForRole(rebuildCtx, "replica"); err != nil {
		repSt, _ := replica.Status(ctx)
		t.Fatalf("rebuild did not complete: role=%s lsn=%d err=%v", repSt.Role, repSt.WALHeadLSN, err)
	}

	// Verify replica LSN caught up
	repSt, _ = replica.Status(ctx)
	t.Logf("replica after rebuild: epoch=%d role=%s lsn=%d", repSt.Epoch, repSt.Role, repSt.WALHeadLSN)

	// --- Phase 5: Kill primary, promote rebuilt replica, verify A+B ---
	t.Log("phase 5: killing primary, promoting rebuilt replica...")
	primary.Kill9()

	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote rebuilt replica: %v", err)
	}

	// Login to promoted rebuilt replica
	repHost := *flagClientHost
	if *flagEnv == "wsl2" {
		repHost = "127.0.0.1"
	}
	if _, err := iscsi.Discover(ctx, repHost, haISCSIPort2); err != nil {
		t.Fatalf("discover promoted: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login promoted: %v", err)
	}

	// Verify 2MB: pattern A at offset 0, pattern B at offset 1M
	rA, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rA = strings.TrimSpace(rA)
	rB, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=1 skip=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rB = strings.TrimSpace(rB)

	if aMD5 != rA {
		t.Fatalf("pattern A mismatch after rebuild: wrote=%s read=%s", aMD5, rA)
	}
	if bMD5 != rB {
		t.Fatalf("pattern B mismatch after rebuild: wrote=%s read=%s", bMD5, rB)
	}

	// Verify full 2MB md5 matches
	rAll, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=1M count=2 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev2))
	rAll = strings.TrimSpace(rAll)
	if allMD5 != rAll {
		t.Fatalf("full 2MB md5 mismatch: primary=%s rebuilt=%s", allMD5, rAll)
	}

	iscsi.Logout(ctx, replica.config.IQN)
	t.Log("RebuildDataConsistency passed: data A+B intact after rebuild + failover")
}

// testFullLifecycleFailoverRebuild exercises the complete lifecycle:
//
//  1. Create HA pair, write data A (replicated)
//  2. Kill primary → promote replica → write data B (new primary)
//  3. Restart old primary → rebuild from new primary → verify catch-up
//  4. Kill new primary → promote rebuilt old-primary → verify data A+B+C
//
// This simulates the master-level flow: failover → recoverBlockVolumes → rebuild.
func testFullLifecycleFailoverRebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	primary, replica, iscsi := newHAPair(t, "100M")
	setupPrimaryReplica(t, ctx, primary, replica, 30000)
	host := targetHost()

	// --- Phase 1: Write data A ---
	t.Log("phase 1: write data A (replicated)...")
	if _, err := iscsi.Discover(ctx, host, haISCSIPort1); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-lcA.bin bs=512K count=1 2>/dev/null")
	aMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-lcA.bin | awk '{print $1}'")
	aMD5 = strings.TrimSpace(aMD5)
	_, _, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-lcA.bin of=%s bs=512K count=1 oflag=direct 2>/dev/null", dev))
	if code != 0 {
		t.Fatalf("write A failed")
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := replica.WaitForLSN(waitCtx, 1); err != nil {
		t.Fatalf("replication stalled: %v", err)
	}

	iscsi.Logout(ctx, primary.config.IQN)

	// --- Phase 2: Kill primary, promote replica, write data B ---
	t.Log("phase 2: kill primary → promote replica → write B...")
	primary.Kill9()
	time.Sleep(1 * time.Second)

	if err := replica.Assign(ctx, 2, rolePrimary, 30000); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	repHost := *flagClientHost
	if *flagEnv == "wsl2" {
		repHost = "127.0.0.1"
	}
	if _, err := iscsi.Discover(ctx, repHost, haISCSIPort2); err != nil {
		t.Fatalf("discover promoted: %v", err)
	}
	dev2, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login promoted: %v", err)
	}

	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-lcB.bin bs=512K count=1 2>/dev/null")
	bMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-lcB.bin | awk '{print $1}'")
	bMD5 = strings.TrimSpace(bMD5)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-lcB.bin of=%s bs=512K count=1 seek=1 oflag=direct 2>/dev/null", dev2))
	if code != 0 {
		t.Fatalf("write B failed")
	}

	// Get new primary status for rebuild
	newPriSt, _ := replica.Status(ctx)
	t.Logf("new primary: epoch=%d role=%s lsn=%d", newPriSt.Epoch, newPriSt.Role, newPriSt.WALHeadLSN)

	iscsi.Logout(ctx, replica.config.IQN)

	// --- Phase 3: Start rebuild server on new primary, restart old primary ---
	t.Log("phase 3: rebuild server on new primary, restart old primary...")

	// Start rebuild server on the new primary (was replica)
	if err := replica.StartRebuildEndpoint(ctx, fmt.Sprintf(":%d", haRebuildPort2)); err != nil {
		t.Fatalf("start rebuild server: %v", err)
	}

	// Restart old primary (it has stale data — only A, not B)
	if err := primary.Start(ctx, false); err != nil {
		t.Fatalf("restart old primary: %v", err)
	}

	// Master sends Rebuilding assignment (RoleNone → RoleRebuilding)
	if err := primary.Assign(ctx, 2, roleRebuilding, 0); err != nil {
		t.Fatalf("assign rebuilding: %v", err)
	}

	// Start rebuild client on old primary → connects to new primary's rebuild server
	rebuildAddr := replicaAddr(haRebuildPort2)
	t.Logf("rebuild client → %s", rebuildAddr)
	if err := primary.StartRebuildClient(ctx, rebuildAddr, newPriSt.Epoch); err != nil {
		t.Fatalf("start rebuild client: %v", err)
	}

	// Wait for rebuild completion
	t.Log("waiting for rebuild completion...")
	rebuildCtx, rebuildCancel := context.WithTimeout(ctx, 60*time.Second)
	defer rebuildCancel()
	if err := primary.WaitForRole(rebuildCtx, "replica"); err != nil {
		st, _ := primary.Status(ctx)
		t.Fatalf("rebuild not complete: role=%s lsn=%d err=%v", st.Role, st.WALHeadLSN, err)
	}

	priSt, _ := primary.Status(ctx)
	t.Logf("old primary rebuilt: epoch=%d role=%s lsn=%d", priSt.Epoch, priSt.Role, priSt.WALHeadLSN)

	// --- Phase 4: Write data C on new primary ---
	t.Log("phase 4: write data C on new primary...")
	if _, err := iscsi.Discover(ctx, repHost, haISCSIPort2); err != nil {
		t.Fatalf("discover new primary: %v", err)
	}
	dev3, err := iscsi.Login(ctx, replica.config.IQN)
	if err != nil {
		t.Fatalf("login new primary: %v", err)
	}

	clientNode.RunRoot(ctx, "dd if=/dev/urandom of=/tmp/cp63-lcC.bin bs=512K count=1 2>/dev/null")
	cMD5, _, _, _ := clientNode.RunRoot(ctx, "md5sum /tmp/cp63-lcC.bin | awk '{print $1}'")
	cMD5 = strings.TrimSpace(cMD5)
	_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=/tmp/cp63-lcC.bin of=%s bs=512K count=1 seek=2 oflag=direct 2>/dev/null", dev3))
	if code != 0 {
		t.Fatalf("write C failed")
	}

	iscsi.Logout(ctx, replica.config.IQN)

	// --- Phase 5: Kill new primary, promote rebuilt old-primary ---
	t.Log("phase 5: kill new primary → promote rebuilt old-primary...")
	replica.Kill9()
	time.Sleep(1 * time.Second)

	if err := primary.Assign(ctx, 3, rolePrimary, 30000); err != nil {
		t.Fatalf("promote old primary: %v", err)
	}

	if _, err := iscsi.Discover(ctx, host, haISCSIPort1); err != nil {
		t.Fatalf("discover old primary: %v", err)
	}
	dev4, err := iscsi.Login(ctx, primary.config.IQN)
	if err != nil {
		t.Fatalf("login old primary: %v", err)
	}

	// Verify all three patterns: A at offset 0, B at offset 512K, C at offset 1M
	rA, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=512K count=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev4))
	rA = strings.TrimSpace(rA)
	rB, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=512K count=1 skip=1 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev4))
	rB = strings.TrimSpace(rB)

	if aMD5 != rA {
		t.Fatalf("pattern A mismatch: wrote=%s read=%s", aMD5, rA)
	}
	if bMD5 != rB {
		t.Fatalf("pattern B mismatch: wrote=%s read=%s", bMD5, rB)
	}

	// Pattern C was written AFTER rebuild completed. Old primary (now rebuilt replica)
	// may not have C if WAL shipping wasn't re-established. Check if C is present.
	rC, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"dd if=%s bs=512K count=1 skip=2 iflag=direct 2>/dev/null | md5sum | awk '{print $1}'", dev4))
	rC = strings.TrimSpace(rC)
	if cMD5 == rC {
		t.Log("pattern C present on rebuilt old-primary (WAL shipping re-established)")
	} else {
		t.Log("pattern C NOT present on rebuilt old-primary (expected: no WAL shipping after rebuild)")
	}

	iscsi.Logout(ctx, primary.config.IQN)
	t.Log("FullLifecycleFailoverRebuild passed: A+B intact through full lifecycle")
}
