package blockvol

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// CP11A-2 QA Adversarial Tests — Coordinated Expand
// =============================================================================

// --- Engine-level adversarial tests ---

func createQAExpandVol(t *testing.T) (*BlockVol, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "qa-expand.blk")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: expandVolSize,
		BlockSize:  expandBlkSize,
		WALSize:    expandWALSize,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return vol, path
}

// T1: ConcurrentPrepare — two goroutines race to PrepareExpand;
// exactly one must win, the other gets ErrExpandAlreadyInFlight.
func TestQA_Expand_ConcurrentPrepare(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	const goroutines = 10
	var wins, rejects atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		epoch := uint64(i + 1)
		go func() {
			defer wg.Done()
			<-start
			err := vol.PrepareExpand(expandNewSize, epoch)
			if err == nil {
				wins.Add(1)
			} else if errors.Is(err, ErrExpandAlreadyInFlight) {
				rejects.Add(1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if wins.Load() != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", wins.Load())
	}
	if rejects.Load() != int32(goroutines-1) {
		t.Fatalf("expected %d rejects, got %d", goroutines-1, rejects.Load())
	}
}

// T2: CommitWithoutPrepare — CommitExpand with no prior PrepareExpand.
func TestQA_Expand_CommitWithoutPrepare(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	err := vol.CommitExpand(42)
	if !errors.Is(err, ErrNoExpandInFlight) {
		t.Fatalf("expected ErrNoExpandInFlight, got %v", err)
	}
	// VolumeSize must not change.
	if vol.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize corrupted: %d", vol.Info().VolumeSize)
	}
}

// T3: CancelWithoutPrepare — CancelExpand when nothing is in flight.
// With epoch=0 (force-cancel), should be a harmless no-op.
func TestQA_Expand_CancelWithoutPrepare_ForceEpoch(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	// Force-cancel (epoch=0) when nothing is in flight — should succeed.
	if err := vol.CancelExpand(0); err != nil {
		t.Fatalf("force-cancel with no inflight should succeed: %v", err)
	}
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState should be clean: (%d, %d)", ps, ee)
	}
}

// T4: CancelWithWrongEpoch — CancelExpand with non-zero wrong epoch.
func TestQA_Expand_CancelWithWrongEpoch(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 5); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	err := vol.CancelExpand(99)
	if !errors.Is(err, ErrExpandEpochMismatch) {
		t.Fatalf("expected ErrExpandEpochMismatch, got %v", err)
	}

	// PreparedSize must still be set (cancel failed).
	ps, ee := vol.ExpandState()
	if ps != expandNewSize || ee != 5 {
		t.Fatalf("ExpandState should be unchanged: (%d, %d)", ps, ee)
	}
}

// T5: ForceCancel — epoch=0 cancels regardless of actual epoch.
func TestQA_Expand_ForceCancel_IgnoresEpoch(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 777); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Force-cancel with epoch=0 should clear regardless.
	if err := vol.CancelExpand(0); err != nil {
		t.Fatalf("force-cancel: %v", err)
	}
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState should be cleared: (%d, %d)", ps, ee)
	}
}

// T6: DoubleCommit — commit, then commit again. Second must fail.
func TestQA_Expand_DoubleCommit(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Second commit: PreparedSize is now 0, so ErrNoExpandInFlight.
	err := vol.CommitExpand(1)
	if !errors.Is(err, ErrNoExpandInFlight) {
		t.Fatalf("expected ErrNoExpandInFlight on double commit, got %v", err)
	}
}

// T7: PrepareAfterCommit — after a successful prepare+commit cycle,
// a new prepare should work (the state machine resets).
func TestQA_Expand_PrepareAfterCommit(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	// First cycle: 1MB -> 2MB.
	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare1: %v", err)
	}
	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("commit1: %v", err)
	}
	if vol.Info().VolumeSize != expandNewSize {
		t.Fatalf("size after first commit: %d", vol.Info().VolumeSize)
	}

	// Second cycle: 2MB -> 4MB.
	newSize2 := uint64(4 * 1024 * 1024)
	if err := vol.PrepareExpand(newSize2, 2); err != nil {
		t.Fatalf("prepare2: %v", err)
	}
	if err := vol.CommitExpand(2); err != nil {
		t.Fatalf("commit2: %v", err)
	}
	if vol.Info().VolumeSize != newSize2 {
		t.Fatalf("size after second commit: %d", vol.Info().VolumeSize)
	}
}

// T8: PrepareAfterCancel — after cancel, a new prepare should succeed.
func TestQA_Expand_PrepareAfterCancel(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare1: %v", err)
	}
	if err := vol.CancelExpand(1); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Second prepare with different epoch should work.
	if err := vol.PrepareExpand(expandNewSize, 2); err != nil {
		t.Fatalf("prepare2 after cancel: %v", err)
	}
	ps, ee := vol.ExpandState()
	if ps != expandNewSize || ee != 2 {
		t.Fatalf("ExpandState: (%d, %d), want (%d, 2)", ps, ee, expandNewSize)
	}
}

// T9: PrepareShrink — PrepareExpand with size < current must be rejected.
func TestQA_Expand_PrepareShrink(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	err := vol.PrepareExpand(expandVolSize/2, 1)
	if !errors.Is(err, ErrShrinkNotSupported) {
		t.Fatalf("expected ErrShrinkNotSupported, got %v", err)
	}
}

// T10: PrepareUnaligned — unaligned size rejected.
func TestQA_Expand_PrepareUnaligned(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	err := vol.PrepareExpand(expandNewSize+1, 1)
	if !errors.Is(err, ErrAlignment) {
		t.Fatalf("expected ErrAlignment, got %v", err)
	}
	// Must not leave state dirty.
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState should be clean after alignment reject: (%d, %d)", ps, ee)
	}
}

// T11: DataIntegrity — write data before prepare, commit, then verify
// data in both old and new regions.
func TestQA_Expand_DataIntegrityAcrossCommit(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	// Write to LBA 0 before expand.
	data := make([]byte, expandBlkSize)
	for i := range data {
		data[i] = 0xAB
	}
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write pre-expand: %v", err)
	}

	// Prepare + commit.
	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Write to LBA 0 during prepared state (within old range — allowed).
	data2 := make([]byte, expandBlkSize)
	for i := range data2 {
		data2[i] = 0xCD
	}
	if err := vol.WriteLBA(0, data2); err != nil {
		t.Fatalf("write during prepared: %v", err)
	}

	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Read LBA 0 — should have data2 (0xCD).
	got, err := vol.ReadLBA(0, expandBlkSize)
	if err != nil {
		t.Fatalf("read LBA 0: %v", err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatalf("data mismatch at LBA 0: got %x, want %x", got[0], data2[0])
	}

	// Write to new region (LBA beyond old size).
	newLBA := uint64(expandVolSize / expandBlkSize)
	data3 := make([]byte, expandBlkSize)
	for i := range data3 {
		data3[i] = 0xEF
	}
	if err := vol.WriteLBA(newLBA, data3); err != nil {
		t.Fatalf("write new region: %v", err)
	}
	got3, err := vol.ReadLBA(newLBA, expandBlkSize)
	if err != nil {
		t.Fatalf("read new region: %v", err)
	}
	if !bytes.Equal(got3, data3) {
		t.Fatalf("data mismatch in new region")
	}
}

// T12: RecoveryClearsAndDataSurvives — crash with PreparedSize set,
// reopen clears it, old data is intact.
func TestQA_Expand_RecoveryClearsAndDataSurvives(t *testing.T) {
	vol, path := createQAExpandVol(t)

	// Write data.
	data := make([]byte, expandBlkSize)
	data[0] = 0x77
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Flush so data reaches extent.
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let flusher flush

	// Prepare expand (not committed).
	if err := vol.PrepareExpand(expandNewSize, 99); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	vol.Close()

	// Reopen — recovery should clear PreparedSize.
	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer vol2.Close()

	ps, ee := vol2.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState after recovery: (%d, %d)", ps, ee)
	}
	if vol2.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize should be original: %d", vol2.Info().VolumeSize)
	}

	// Data written before prepare should survive.
	got, err := vol2.ReadLBA(0, expandBlkSize)
	if err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	if got[0] != 0x77 {
		t.Fatalf("data[0]: got %x, want 0x77", got[0])
	}
}

// T13: CommittedExpandSurvivesReopen — committed expand persists.
func TestQA_Expand_CommittedSurvivesReopen(t *testing.T) {
	vol, path := createQAExpandVol(t)

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Write in new region.
	newLBA := uint64(expandVolSize / expandBlkSize)
	data := make([]byte, expandBlkSize)
	data[0] = 0xAA
	if err := vol.WriteLBA(newLBA, data); err != nil {
		t.Fatalf("write new region: %v", err)
	}
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	vol.Close()

	// Reopen.
	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer vol2.Close()

	if vol2.Info().VolumeSize != expandNewSize {
		t.Fatalf("VolumeSize: got %d, want %d", vol2.Info().VolumeSize, expandNewSize)
	}
	got, err := vol2.ReadLBA(newLBA, expandBlkSize)
	if err != nil {
		t.Fatalf("read new region: %v", err)
	}
	if got[0] != 0xAA {
		t.Fatalf("data[0]: got %x, want 0xAA", got[0])
	}
}

// T14: ExpandOnClosedVolume — all expand ops must return ErrVolumeClosed.
func TestQA_Expand_ClosedVolume(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	vol.Close()

	if err := vol.Expand(expandNewSize); !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("Expand on closed: expected ErrVolumeClosed, got %v", err)
	}
	if err := vol.PrepareExpand(expandNewSize, 1); !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("PrepareExpand on closed: expected ErrVolumeClosed, got %v", err)
	}
	if err := vol.CommitExpand(1); !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("CommitExpand on closed: expected ErrVolumeClosed, got %v", err)
	}
	if err := vol.CancelExpand(1); !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("CancelExpand on closed: expected ErrVolumeClosed, got %v", err)
	}
}

// T15: PrepareExpandSameSize — PrepareExpand with newSize == VolumeSize must fail.
// BUG-CP11A2-1 fix: PrepareExpand rejects same-size with ErrSameSize.
func TestQA_Expand_PrepareSameSize(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	err := vol.PrepareExpand(expandVolSize, 1)
	if !errors.Is(err, ErrSameSize) {
		t.Fatalf("PrepareExpand(sameSize): expected ErrSameSize, got %v", err)
	}
	// Verify no state was left behind.
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("state leaked: PreparedSize=%d ExpandEpoch=%d", ps, ee)
	}
}

// T16: ConcurrentPrepareAndWrite — write I/O during PrepareExpand.
// Writes within old range must succeed, writes beyond must fail.
func TestQA_Expand_ConcurrentWriteDuringPrepare(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	// Start background writes to LBA 0 (within old range).
	var writeCount atomic.Int32
	var writeErr atomic.Value
	stopCh := make(chan struct{})
	go func() {
		data := make([]byte, expandBlkSize)
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			err := vol.WriteLBA(0, data)
			if err != nil {
				writeErr.Store(err)
				return
			}
			writeCount.Add(1)
		}
	}()

	// Let writes run briefly.
	time.Sleep(10 * time.Millisecond)

	// PrepareExpand while writes are happening.
	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		close(stopCh)
		t.Fatalf("prepare: %v", err)
	}

	// Let a few more writes happen.
	time.Sleep(10 * time.Millisecond)
	close(stopCh)

	if e := writeErr.Load(); e != nil {
		t.Fatalf("write error during prepare: %v", e)
	}
	if writeCount.Load() == 0 {
		t.Fatal("no writes completed during test")
	}
}

// T17: ExpandStateRaceWithCommit — concurrent ExpandState reads during commit.
func TestQA_Expand_ExpandStateRaceWithCommit(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	var wg sync.WaitGroup
	// Concurrent ExpandState readers.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ps, ee := vol.ExpandState()
				// Valid states: (expandNewSize, 1) before commit, or (0, 0) after.
				if ps != 0 && ps != expandNewSize {
					t.Errorf("invalid PreparedSize: %d", ps)
				}
				if ee != 0 && ee != 1 {
					t.Errorf("invalid ExpandEpoch: %d", ee)
				}
				// PreparedSize and ExpandEpoch must be consistent (both set or both cleared).
				if (ps == 0) != (ee == 0) {
					t.Errorf("inconsistent ExpandState: (%d, %d)", ps, ee)
				}
			}
		}()
	}

	// Commit while readers are running.
	time.Sleep(1 * time.Millisecond)
	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("commit: %v", err)
	}

	wg.Wait()
}

// T18: TrimDuringPreparedExpand — trim within old range must work.
func TestQA_Expand_TrimDuringPrepared(t *testing.T) {
	vol, _ := createQAExpandVol(t)
	defer vol.Close()

	// Write data.
	data := make([]byte, expandBlkSize)
	data[0] = 0xFF
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Prepare expand.
	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Trim LBA 0 (within old range).
	if err := vol.Trim(0, expandBlkSize); err != nil {
		t.Fatalf("trim during prepared: %v", err)
	}

	// Read should return zeros.
	got, err := vol.ReadLBA(0, expandBlkSize)
	if err != nil {
		t.Fatalf("read after trim: %v", err)
	}
	zeros := make([]byte, expandBlkSize)
	if !bytes.Equal(got, zeros) {
		t.Fatalf("expected zeros after trim, got %x at [0]", got[0])
	}
}

// T19: SuperblockValidate — manually construct superblock with
// PreparedSize == VolumeSize and verify Validate() rejects it.
func TestQA_Expand_SuperblockValidatePreparedSize(t *testing.T) {
	sb := Superblock{
		Version:      CurrentVersion,
		VolumeSize:   1024 * 1024,
		BlockSize:    4096,
		ExtentSize:   65536,
		WALSize:      65536,
		WALOffset:    SuperblockSize,
		PreparedSize: 1024 * 1024, // == VolumeSize, should fail
		ExpandEpoch:  1,
	}
	copy(sb.Magic[:], MagicSWBK)

	if err := sb.Validate(); err == nil {
		t.Fatal("Validate should reject PreparedSize == VolumeSize")
	}
}

// T20: SuperblockValidate — ExpandEpoch != 0 with PreparedSize == 0.
func TestQA_Expand_SuperblockValidateOrphanEpoch(t *testing.T) {
	sb := Superblock{
		Version:      CurrentVersion,
		VolumeSize:   1024 * 1024,
		BlockSize:    4096,
		ExtentSize:   65536,
		WALSize:      65536,
		WALOffset:    SuperblockSize,
		PreparedSize: 0,
		ExpandEpoch:  5, // orphan epoch
	}
	copy(sb.Magic[:], MagicSWBK)

	if err := sb.Validate(); err == nil {
		t.Fatal("Validate should reject ExpandEpoch!=0 when PreparedSize==0")
	}
}
