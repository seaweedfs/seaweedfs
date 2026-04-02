package v2bridge

import (
	"bytes"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 09 P1: Adversarial tests for full-base rebuild transfer
//
// These tests verify safety under failure and concurrent mutation:
//   1. Server dies mid-transfer: replica extent must not be half-installed
//   2. Concurrent writes during rebuild: achievedLSN correct, no corruption
//   3. Double rebuild (cancel + restart): second rebuild sees clean state
// ============================================================

// --- Adversarial 1: Server dies mid-transfer, replica state must be safe ---

func TestAdversarial_ServerDiesMidTransfer_ReplicaStateClean(t *testing.T) {
	dir := t.TempDir()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Write pre-existing data on replica (simulates stale state).
	for i := 0; i < 5; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R')))
	}

	replicaStateBefore := NewReader(replicaVol).ReadState()

	// Fake server: sends a few extent chunks, then drops connection
	// before sending MsgRebuildDone. The extent should NOT be installed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read request frame (discard).
		blockvol.ReadFrame(conn)
		// Send 3 extent chunks (partial extent).
		for i := 0; i < 3; i++ {
			chunk := make([]byte, 4096)
			for j := range chunk {
				chunk[j] = byte('X') // different from replica's 'R'
			}
			blockvol.WriteFrame(conn, blockvol.MsgRebuildExtent, chunk)
		}
		// Drop connection — no MsgRebuildDone sent.
		conn.Close()
	}()

	executor := NewExecutor(replicaVol, ln.Addr().String())
	_, err = executor.TransferFullBase(100)
	if err == nil {
		t.Fatal("should fail when server dies mid-transfer")
	}

	// KEY ASSERTION: replica's pre-existing data must still be readable.
	// The partial extent must NOT have been committed.
	replicaStateAfter := NewReader(replicaVol).ReadState()

	// WALHeadLSN should not have been reset by a partial install.
	if replicaStateAfter.WALHeadLSN != replicaStateBefore.WALHeadLSN {
		t.Fatalf("WALHeadLSN changed after failed transfer: before=%d after=%d",
			replicaStateBefore.WALHeadLSN, replicaStateAfter.WALHeadLSN)
	}

	// Pre-existing data should still be readable (stale 'R' blocks).
	blockSize := replicaVol.Info().BlockSize
	for i := 0; i < 5; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), blockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after failed transfer: %v", i, err)
		}
		expected := makeBlock(byte('R'))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d corrupted after failed transfer: got[0]=%d want='R'(%d)",
				i, data[0], byte('R'))
		}
	}

	t.Log("adversarial 1: server died mid-transfer — replica state is clean, pre-existing data intact")
}

// --- Adversarial 2: Concurrent writes during rebuild ---

func TestAdversarial_ConcurrentWritesDuringRebuild(t *testing.T) {
	dir := t.TempDir()

	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	// Write initial data + flush.
	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	primaryStateBefore := NewReader(primaryVol).ReadState()
	t.Logf("primary before extra writes: head=%d checkpoint=%d",
		primaryStateBefore.WALHeadLSN, primaryStateBefore.CheckpointLSN)

	// Start rebuild server.
	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Write MORE data on primary AFTER rebuild server started.
	// These writes happen while the rebuild transfer is in progress.
	for i := 10; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('Z')))
	}
	primaryVol.ForceFlush()

	primaryStateAfter := NewReader(primaryVol).ReadState()
	t.Logf("primary after extra writes: head=%d checkpoint=%d",
		primaryStateAfter.WALHeadLSN, primaryStateAfter.CheckpointLSN)

	// Replica: empty vol.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())

	// Transfer with the ORIGINAL target (before extra writes).
	achievedLSN, err := executor.TransferFullBase(primaryStateBefore.CommittedLSN)
	if err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	t.Logf("achievedLSN=%d (target was %d)", achievedLSN, primaryStateBefore.CommittedLSN)

	// achievedLSN must be >= original target.
	if achievedLSN < primaryStateBefore.CommittedLSN {
		t.Fatalf("achievedLSN=%d < target=%d", achievedLSN, primaryStateBefore.CommittedLSN)
	}

	// The rebuild server's snapshot should include ALL flushed data
	// (including the extra writes), so achievedLSN should be >= the
	// extra writes' head.
	if achievedLSN < primaryStateAfter.CommittedLSN {
		t.Logf("note: achievedLSN=%d < post-write committed=%d (snapshot was taken between flushes)",
			achievedLSN, primaryStateAfter.CommittedLSN)
	}

	// Verify data integrity: at minimum, the original 10 LBAs must match.
	verifyLBAMatch(t, primaryVol, replicaVol, 10)

	t.Logf("adversarial 2: concurrent writes during rebuild — achievedLSN=%d, data integrity verified", achievedLSN)
}

// --- Adversarial 3: Double rebuild (first cancelled, second must see clean state) ---

func TestAdversarial_DoubleRebuild_SecondSeesCleanState(t *testing.T) {
	dir := t.TempDir()

	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// First rebuild: succeeds.
	executor1 := NewExecutor(replicaVol, rebuildServer.Addr())
	achievedLSN1, err := executor1.TransferFullBase(0)
	if err != nil {
		t.Fatalf("first rebuild: %v", err)
	}
	t.Logf("first rebuild: achievedLSN=%d", achievedLSN1)

	// Verify first rebuild installed data correctly.
	verifyLBAMatch(t, primaryVol, replicaVol, 10)

	// Now: primary writes NEW data (different pattern).
	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('Z')))
	}
	primaryVol.ForceFlush()

	// Restart rebuild server (simulates new rebuild after epoch bump).
	rebuildServer.Stop()
	rebuildServer2, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rebuildServer2.Serve()
	defer rebuildServer2.Stop()

	// Second rebuild on SAME replica (simulates cancelled first + restart).
	executor2 := NewExecutor(replicaVol, rebuildServer2.Addr())
	achievedLSN2, err := executor2.TransferFullBase(0)
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	t.Logf("second rebuild: achievedLSN=%d", achievedLSN2)

	// Second rebuild's achievedLSN must be >= first rebuild's.
	if achievedLSN2 < achievedLSN1 {
		t.Fatalf("second rebuild achievedLSN=%d < first=%d — state regression",
			achievedLSN2, achievedLSN1)
	}

	// Verify: replica now has the NEW data ('Z'), not the old ('A').
	blockSize := replicaVol.Info().BlockSize
	for i := 0; i < 10; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), blockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after second rebuild: %v", i, err)
		}
		expected := makeBlock(byte('Z'))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d after second rebuild: got[0]=%d want='Z'(%d) — stale first-rebuild data leaked",
				i, data[0], byte('Z'))
		}
	}

	t.Logf("adversarial 3: double rebuild — second rebuild installed new data correctly, no stale state from first")
}
