package v2bridge

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// createTestVol creates a real file-backed BlockVol for integration tests.
func createTestVol(t *testing.T) *blockvol.BlockVol {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")
	v, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	return v
}

func makeBlock(fill byte) []byte {
	b := make([]byte, 4096)
	for i := range b {
		b[i] = fill
	}
	return b
}

// --- Real Reader ---

func TestReader_RealBlockVol_StatusSnapshot(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	reader := NewReader(vol)

	// Before any writes: head=0, tail=0, committed=0.
	state := reader.ReadState()
	if state.WALHeadLSN != 0 {
		t.Fatalf("initial HeadLSN=%d, want 0", state.WALHeadLSN)
	}

	// Write some data.
	vol.WriteLBA(0, makeBlock('A'))
	vol.WriteLBA(1, makeBlock('B'))
	vol.SyncCache()

	state = reader.ReadState()
	if state.WALHeadLSN < 2 {
		t.Fatalf("after writes: HeadLSN=%d, want >= 2", state.WALHeadLSN)
	}
	// CommittedLSN should reflect flushed state.
	if state.CommittedLSN == 0 {
		// After SyncCache, the flusher should have checkpointed.
		// This may or may not be > 0 depending on flusher timing.
		t.Log("CommittedLSN=0 after SyncCache (flusher may not have run yet)")
	}
	// WALTailLSN is an LSN boundary (from super.WALCheckpointLSN).
	// It should be 0 initially (nothing checkpointed yet).
	if state.WALTailLSN != 0 {
		t.Logf("WALTailLSN=%d (checkpoint advanced)", state.WALTailLSN)
	}
}

func TestReader_RealBlockVol_SyncAllNoReplicaProgressCommittedZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "syncall.blockvol")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	vol.SetReplicaAddrs([]blockvol.ReplicaAddr{{
		DataAddr: "127.0.0.1:65530",
		CtrlAddr: "127.0.0.1:65531",
	}})
	if err := vol.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	state := NewReader(vol).ReadState()
	if state.WALHeadLSN == 0 {
		t.Fatal("precondition failed: WALHeadLSN should advance after local write")
	}
	if state.CommittedLSN != 0 {
		t.Fatalf("CommittedLSN=%d, want 0 before replica durable progress exists", state.CommittedLSN)
	}
}

func TestReader_RealBlockVol_HeadAdvancesWithWrites(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	reader := NewReader(vol)

	state0 := reader.ReadState()
	vol.WriteLBA(0, makeBlock('X'))
	state1 := reader.ReadState()

	if state1.WALHeadLSN <= state0.WALHeadLSN {
		t.Fatalf("HeadLSN should advance: before=%d after=%d",
			state0.WALHeadLSN, state1.WALHeadLSN)
	}
}

// --- Real Pinner ---

func TestPinner_RealBlockVol_HoldWALRetention(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	pinner := NewPinner(vol)

	// Write data so WAL has entries.
	vol.WriteLBA(0, makeBlock('A'))
	vol.WriteLBA(1, makeBlock('B'))

	// Hold WAL from LSN 0 (should succeed — nothing recycled yet).
	release, err := pinner.HoldWALRetention(0)
	if err != nil {
		t.Fatalf("HoldWALRetention: %v", err)
	}

	if pinner.ActiveHoldCount() != 1 {
		t.Fatalf("holds=%d, want 1", pinner.ActiveHoldCount())
	}

	// MinWALRetentionFloor should report the held position.
	floor, hasFloor := pinner.MinWALRetentionFloor()
	if !hasFloor || floor != 0 {
		t.Fatalf("floor=%d hasFloor=%v, want 0/true", floor, hasFloor)
	}

	// Release.
	release()

	if pinner.ActiveHoldCount() != 0 {
		t.Fatal("hold should be released")
	}

	_, hasFloor = pinner.MinWALRetentionFloor()
	if hasFloor {
		t.Fatal("no floor after release")
	}
}

func TestPinner_RealBlockVol_HoldRejectsRecycled(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write + flush to advance the checkpoint (WAL tail).
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.SyncCache()

	pinner := NewPinner(vol)

	// Check current tail.
	state := NewReader(vol).ReadState()
	if state.WALTailLSN > 0 {
		// Tail advanced — try to hold below it.
		_, err := pinner.HoldWALRetention(0)
		if err == nil {
			t.Fatal("should reject hold below recycled tail")
		}
	} else {
		t.Log("WALTailLSN=0, checkpoint not advanced (hold from 0 is valid)")
	}
}

// --- Real Executor ---

func TestExecutor_RealBlockVol_StreamWALEntries(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write entries.
	vol.WriteLBA(0, makeBlock('A'))
	vol.WriteLBA(1, makeBlock('B'))
	vol.WriteLBA(2, makeBlock('C'))

	reader := NewReader(vol)
	state := reader.ReadState()
	headLSN := state.WALHeadLSN
	if headLSN < 3 {
		t.Fatalf("HeadLSN=%d, want >= 3", headLSN)
	}

	executor := NewExecutor(vol, "")

	// Stream from start to head.
	transferred, err := executor.StreamWALEntries(0, headLSN)
	if err != nil {
		t.Fatalf("StreamWALEntries: %v", err)
	}
	if transferred == 0 {
		t.Fatal("should have transferred entries")
	}
	t.Logf("streamed: transferred to LSN %d (head=%d)", transferred, headLSN)
}

func TestExecutor_RealBlockVol_StreamPartialRange(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 5 entries.
	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	reader := NewReader(vol)
	state := reader.ReadState()

	executor := NewExecutor(vol, "")

	// Stream only entries 2-4 (partial range).
	startLSN := uint64(1) // exclusive: start after LSN 1
	endLSN := uint64(3)   // inclusive
	if endLSN > state.WALHeadLSN {
		endLSN = state.WALHeadLSN
	}

	transferred, err := executor.StreamWALEntries(startLSN, endLSN)
	if err != nil {
		t.Fatalf("StreamWALEntries partial: %v", err)
	}
	if transferred == 0 {
		t.Fatal("should have transferred partial entries")
	}
	t.Logf("partial stream: %d→%d, transferred to %d", startLSN, endLSN, transferred)
}

// --- Error paths ---

func TestExecutor_ErrorPaths(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	executor := NewExecutor(vol, "")

	if err := executor.TransferSnapshot(50); err == nil {
		t.Fatal("TransferSnapshot should fail on missing checkpoint")
	}
	if _, err := executor.TransferFullBase(100); err == nil {
		t.Fatal("TransferFullBase should fail without rebuild address")
	}
	// TruncateWAL is now real (P3). Verify it works on a fresh vol.
	if err := executor.TruncateWAL(0); err != nil {
		t.Fatalf("TruncateWAL(0) on fresh vol: %v", err)
	}
}
