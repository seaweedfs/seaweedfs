package v2bridge

import (
	"testing"
)

// ============================================================
// Phase 09 P3: Safety tests for the mixed-case truncation bug
//
// Bug: checkpointLSN < truncateLSN is allowed but entries
// (checkpointLSN, truncateLSN] may live only in WAL/dirty map.
// Truncation clears both, losing committed data.
//
// Correct safety predicate:
//   checkpointLSN == truncateLSN  →  safe
//   checkpointLSN > truncateLSN   →  unsafe (flushed-ahead)
//   checkpointLSN < truncateLSN   →  unsafe (kept data in WAL only)
// ============================================================

func TestSafety_MixedCase_CheckpointBelowTruncateLSN(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Phase 1: Write 10 entries + flush → checkpoint = 10.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()

	state1 := NewReader(vol).ReadState()
	checkpointLSN := state1.CheckpointLSN
	t.Logf("after flush: checkpoint=%d", checkpointLSN)

	// Phase 2: Write 5 MORE entries WITHOUT flushing (in WAL only).
	for i := 10; i < 15; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('K')))
	}

	// Phase 3: Write 5 AHEAD entries (divergent).
	for i := 15; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A')))
	}

	state2 := NewReader(vol).ReadState()
	t.Logf("before truncation: head=%d checkpoint=%d committed=%d",
		state2.WALHeadLSN, state2.CheckpointLSN, state2.CommittedLSN)

	// Truncate to 15: checkpoint(10) < truncateLSN(15).
	// Entries 11..15 are in WAL only — truncation would lose them.
	executor := NewExecutor(vol, "")
	err := executor.TruncateWAL(15)

	if err == nil {
		// BUG CONFIRMED: Show the data loss.
		blockSize := vol.Info().BlockSize
		for i := 10; i < 15; i++ {
			data, err := vol.ReadLBA(uint64(i), blockSize)
			if err != nil {
				t.Logf("  LBA %d: read error: %v", i, err)
				continue
			}
			t.Logf("  LBA %d: %c (want 'K')", i, data[0])
		}
		t.Fatal("BUG: truncation succeeded with checkpoint < truncateLSN — entries 11..15 lost")
	}

	// Correctly rejected.
	t.Logf("correctly rejected: %v", err)
	if !containsSubstring(err.Error(), "unsafe") {
		t.Logf("warning: error should contain 'unsafe' for engine escalation, got: %v", err)
	}

	t.Logf("PASS: checkpoint=%d < truncateLSN=15 correctly rejected", checkpointLSN)
}

func TestSafety_MixedCase_EngineEscalates(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Same mixed state as above.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()
	for i := 10; i < 15; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('K')))
	}
	for i := 15; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A')))
	}

	state := NewReader(vol).ReadState()
	t.Logf("mixed: head=%d checkpoint=%d", state.WALHeadLSN, state.CheckpointLSN)

	executor := NewExecutor(vol, "")
	err := executor.TruncateWAL(15)

	if err == nil {
		t.Fatal("BUG: engine chain would complete InSync after data loss")
	}

	t.Logf("PASS: mixed case rejected — engine would escalate: %v", err)
}
