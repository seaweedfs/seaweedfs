package blockvol

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestQAResize runs adversarial tests for CP5-3 online volume expand.
func TestQAResize(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// Rejection
		{name: "shrink_rejected", run: testQAResize_ShrinkRejected},
		{name: "same_size_noop", run: testQAResize_SameSize},
		{name: "unaligned_size_rejected", run: testQAResize_UnalignedRejected},
		{name: "expand_with_snapshots_rejected", run: testQAResize_WithSnapshotsRejected},
		{name: "expand_on_closed_volume", run: testQAResize_ClosedVolume},
		{name: "expand_on_replica_rejected", run: testQAResize_ReplicaRejected},

		// Correctness
		{name: "expand_doubles_size", run: testQAResize_DoublesSize},
		{name: "expand_preserves_existing_data", run: testQAResize_PreservesData},
		{name: "expand_new_region_writable", run: testQAResize_NewRegionWritable},
		{name: "expand_survives_reopen", run: testQAResize_SurvivesReopen},
		{name: "expand_file_size_correct", run: testQAResize_FileSizeCorrect},

		// Concurrency
		{name: "concurrent_expand_and_writes", run: testQAResize_ConcurrentWrites},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// --- Rejection ---

func testQAResize_ShrinkRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	err := v.Expand(512 * 1024) // 512KB < 1MB
	if err != ErrShrinkNotSupported {
		t.Fatalf("expected ErrShrinkNotSupported, got %v", err)
	}
}

func testQAResize_SameSize(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	err := v.Expand(1 * 1024 * 1024) // same as creation size
	if err != nil {
		t.Fatalf("same size should be no-op, got %v", err)
	}
	if v.Info().VolumeSize != 1*1024*1024 {
		t.Fatalf("size changed unexpectedly")
	}
}

func testQAResize_UnalignedRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	err := v.Expand(1*1024*1024 + 1000) // not aligned to 4096
	if err != ErrAlignment {
		t.Fatalf("expected ErrAlignment, got %v", err)
	}
}

func testQAResize_WithSnapshotsRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	err := v.Expand(2 * 1024 * 1024)
	if err != ErrSnapshotsPreventResize {
		t.Fatalf("expected ErrSnapshotsPreventResize, got %v", err)
	}
}

func testQAResize_ClosedVolume(t *testing.T) {
	v := createTestVol(t)
	v.Close()

	err := v.Expand(2 * 1024 * 1024)
	if err == nil {
		t.Fatal("expand on closed volume should fail")
	}
}

func testQAResize_ReplicaRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Replicas have writeGate rejecting writes; Expand also calls writeGate.
	v.SetRole(RoleReplica)

	err := v.Expand(2 * 1024 * 1024)
	if err == nil {
		t.Fatal("expand on replica should be rejected by writeGate")
	}
}

// --- Correctness ---

func testQAResize_DoublesSize(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	if err := v.Expand(2 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	if v.Info().VolumeSize != 2*1024*1024 {
		t.Fatalf("VolumeSize = %d, want %d", v.Info().VolumeSize, 2*1024*1024)
	}
}

func testQAResize_PreservesData(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write to last LBA of original size.
	lastLBA := uint64(255) // 1MB / 4KB - 1
	v.WriteLBA(lastLBA, makeBlock('Z'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Expand.
	if err := v.Expand(2 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}

	// Old data should still be there.
	data, err := v.ReadLBA(lastLBA, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'Z' {
		t.Fatalf("data after expand: got %c, want Z", data[0])
	}
}

func testQAResize_NewRegionWritable(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	if err := v.Expand(2 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}

	// Write to a block in the new region (LBA 300, beyond original 256).
	newLBA := uint64(300)
	if err := v.WriteLBA(newLBA, makeBlock('N')); err != nil {
		t.Fatalf("write to new region: %v", err)
	}
	v.SyncCache()

	data, err := v.ReadLBA(newLBA, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'N' {
		t.Fatalf("read new region: got %c, want N", data[0])
	}
}

func testQAResize_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.Expand(2 * 1024 * 1024)
	v.WriteLBA(300, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()
	v.Close()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	if v2.Info().VolumeSize != 2*1024*1024 {
		t.Fatalf("VolumeSize after reopen = %d, want %d", v2.Info().VolumeSize, 2*1024*1024)
	}

	d0, _ := v2.ReadLBA(0, 4096)
	if d0[0] != 'A' {
		t.Fatalf("LBA 0: got %c, want A", d0[0])
	}
	d300, _ := v2.ReadLBA(300, 4096)
	if d300[0] != 'B' {
		t.Fatalf("LBA 300: got %c, want B", d300[0])
	}
}

func testQAResize_FileSizeCorrect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	// Original file size = superblock(4096) + WAL(256KB) + extent(1MB)
	expectedBefore := int64(4096 + 256*1024 + 1*1024*1024)
	fi, _ := os.Stat(path)
	if fi.Size() != expectedBefore {
		t.Fatalf("original file size = %d, want %d", fi.Size(), expectedBefore)
	}

	v.Expand(2 * 1024 * 1024)

	// New file size = superblock(4096) + WAL(256KB) + extent(2MB)
	expectedAfter := int64(4096 + 256*1024 + 2*1024*1024)
	fi2, _ := os.Stat(path)
	if fi2.Size() != expectedAfter {
		t.Fatalf("expanded file size = %d, want %d", fi2.Size(), expectedAfter)
	}
}

// --- Concurrency ---

func testQAResize_ConcurrentWrites(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Start writers.
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := v.WriteLBA(uint64(i%10), makeBlock(byte('0'+i%10))); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Expand while writes are in progress.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := v.Expand(2 * 1024 * 1024); err != nil {
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}

	// Volume should be expanded and functional.
	if v.Info().VolumeSize != 2*1024*1024 {
		t.Fatalf("VolumeSize = %d after concurrent expand", v.Info().VolumeSize)
	}
}
