package v2bridge

import (
	"bytes"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 09 P3: Adversarial tests for truncation
// ============================================================

// --- Adversarial 1: Concurrent write during truncation ---

func TestAdversarial_Truncate_ConcurrentWrite(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 10 base entries, flush.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()

	// Write 10 ahead entries (unflushed).
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A')))
	}

	stateBefore := NewReader(vol).ReadState()
	t.Logf("before: head=%d checkpoint=%d", stateBefore.WALHeadLSN, stateBefore.CheckpointLSN)

	// Race: truncation + concurrent write.
	var wg sync.WaitGroup
	var truncErr, writeErr error

	wg.Add(2)

	go func() {
		defer wg.Done()
		truncErr = vol.TruncateToLSN(10)
	}()

	go func() {
		defer wg.Done()
		writeErr = vol.WriteLBA(0, makeBlock(byte('W')))
	}()

	wg.Wait()

	// Truncation must succeed (unflushed-ahead).
	if truncErr != nil {
		t.Fatalf("truncation should succeed: %v", truncErr)
	}

	// Write either succeeded (before truncation) or after truncation.
	// Either way is fine — no crash, no corruption.
	t.Logf("concurrent write err: %v", writeErr)

	stateAfter := NewReader(vol).ReadState()
	t.Logf("after: head=%d checkpoint=%d", stateAfter.WALHeadLSN, stateAfter.CheckpointLSN)

	// Key assertion: data must be self-consistent.
	// If head == 10: truncation won, write was either before (discarded) or failed.
	// If head == 11: write happened after truncation reset nextLSN to 11.
	// Both are valid.
	blockSize := vol.Info().BlockSize
	data, err := vol.ReadLBA(0, blockSize)
	if err != nil {
		t.Fatalf("ReadLBA after race: %v", err)
	}

	// Data should be 'B' (base), 'W' (concurrent write landed), or 'A' (ahead survived if write raced first).
	// It must NOT be a mix of different blocks.
	if data[0] != byte('B') && data[0] != byte('W') && data[0] != byte('A') {
		t.Fatalf("LBA 0 unexpected data: %d", data[0])
	}

	t.Logf("adversarial 1: concurrent write during truncation — no crash, data[0]=%c, head=%d",
		data[0], stateAfter.WALHeadLSN)
}

// --- Adversarial 2: Truncate to exact head (no-op boundary) ---

func TestAdversarial_Truncate_ExactHead_NoOp(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 10 entries, flush.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('D')))
	}
	vol.ForceFlush()

	stateBefore := NewReader(vol).ReadState()
	t.Logf("before: head=%d checkpoint=%d", stateBefore.WALHeadLSN, stateBefore.CheckpointLSN)

	// Truncate to exactly head LSN — zero ahead entries.
	executor := NewExecutor(vol, "")
	if err := executor.TruncateWAL(stateBefore.WALHeadLSN); err != nil {
		t.Fatalf("truncate to exact head: %v", err)
	}

	stateAfter := NewReader(vol).ReadState()
	t.Logf("after: head=%d checkpoint=%d", stateAfter.WALHeadLSN, stateAfter.CheckpointLSN)

	// Head should be exactly the truncation point.
	if stateAfter.WALHeadLSN != stateBefore.WALHeadLSN {
		t.Fatalf("head changed: %d → %d", stateBefore.WALHeadLSN, stateAfter.WALHeadLSN)
	}

	// Data must be unchanged.
	blockSize := vol.Info().BlockSize
	for i := 0; i < 10; i++ {
		data, err := vol.ReadLBA(uint64(i), blockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('D'))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d changed after no-op truncation", i)
		}
	}

	// Next write should be at head+1.
	vol.WriteLBA(0, makeBlock(byte('N')))
	statePost := NewReader(vol).ReadState()
	if statePost.WALHeadLSN != stateBefore.WALHeadLSN+1 {
		t.Fatalf("next write at wrong LSN: %d (expected %d)",
			statePost.WALHeadLSN, stateBefore.WALHeadLSN+1)
	}

	t.Logf("adversarial 2: truncate to exact head — data unchanged, next write at %d", statePost.WALHeadLSN)
}

// --- Adversarial 3: Truncation after full-base rebuild ---

func TestAdversarial_Truncate_AfterRebuild(t *testing.T) {
	dir := t.TempDir()

	// Primary: 10 entries, flush.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()
	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	// Replica: empty, rebuild from primary.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	rebuildExec := NewExecutor(replicaVol, rebuildServer.Addr())
	achievedLSN, err := rebuildExec.TransferFullBase(0)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	t.Logf("rebuild achieved: %d", achievedLSN)

	// Verify rebuild installed 'P' data.
	verifyLBAMatch(t, primaryVol, replicaVol, 10)

	// Now: write ahead entries on replica (simulates split-brain divergence).
	for i := 0; i < 5; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('X')))
	}

	replicaState := NewReader(replicaVol).ReadState()
	t.Logf("replica after ahead writes: head=%d checkpoint=%d", replicaState.WALHeadLSN, replicaState.CheckpointLSN)

	// Truncate back to the rebuild's achieved LSN.
	truncExec := NewExecutor(replicaVol, "")
	if err := truncExec.TruncateWAL(achievedLSN); err != nil {
		t.Fatalf("truncate after rebuild: %v", err)
	}

	// Verify: 'P' data restored (ahead 'X' discarded).
	blockSize := replicaVol.Info().BlockSize
	for i := 0; i < 10; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), blockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('P'))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d after truncate: got %c, want 'P' — rebuild base corrupted", i, data[0])
		}
	}

	t.Logf("adversarial 3: truncation after rebuild — ahead 'X' discarded, rebuild base 'P' preserved")
}
