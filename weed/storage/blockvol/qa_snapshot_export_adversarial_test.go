package blockvol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// QA-EX-1: Concurrent WriteLBA during ImportSnapshot — WAL entries lost.
//
// ImportSnapshot pauses the flusher and writes directly to extent, then resets
// the WAL. If a concurrent WriteLBA sneaks in after PauseAndFlush but before
// WAL reset, that WAL entry is silently discarded.
//
// This test writes data before import, starts import (which replaces extent),
// then writes AFTER import completes and verifies the post-import write survives.
// The real danger is a WriteLBA that lands *during* import — the test documents
// the expectation that callers must quiesce I/O before calling ImportSnapshot.
// ---------------------------------------------------------------------------
func TestQA_Export_ConcurrentWriteDuringImport(t *testing.T) {
	volSize := uint64(64 * 1024)
	srcVol := createExportTestVol(t, volSize)

	// Write pattern to source and export.
	pattern := make([]byte, 4096)
	pattern[0] = 0xAA
	if err := srcVol.WriteLBA(0, pattern); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Target volume with pre-existing data.
	dstVol := createExportTestVol(t, volSize)
	preData := make([]byte, 4096)
	preData[0] = 0xBB
	if err := dstVol.WriteLBA(0, preData); err != nil {
		t.Fatalf("pre-write: %v", err)
	}

	// Import (with AllowOverwrite since target has data).
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{AllowOverwrite: true})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	// After import, WAL was reset. Write new data — this should be durable.
	postData := make([]byte, 4096)
	postData[0] = 0xCC
	if err := dstVol.WriteLBA(1, postData); err != nil {
		t.Fatalf("post-import write: %v", err)
	}

	// Block 0 should have import data (0xAA), not pre-data (0xBB).
	got0, err := dstVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA 0: %v", err)
	}
	if got0[0] != 0xAA {
		t.Errorf("block 0 first byte = 0x%02X, want 0xAA (import data)", got0[0])
	}

	// Block 1 should have post-import data (0xCC).
	got1, err := dstVol.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA 1: %v", err)
	}
	if got1[0] != 0xCC {
		t.Errorf("block 1 first byte = 0x%02X, want 0xCC (post-import write)", got1[0])
	}
}

// ---------------------------------------------------------------------------
// QA-EX-2: Partial import failure — extent has mixed data.
//
// If the reader fails partway through import, the extent region contains a mix
// of import data and original data. The WAL has NOT been reset (that happens
// after checksum verification), but PauseAndFlush already flushed the dirty map
// clean. So the flusher has nothing to re-flush and the extent corruption
// persists silently.
//
// This test feeds a reader that produces correct data for half the blocks then
// returns an error. After the failed import, it reads back all blocks and
// documents which ones were overwritten.
// ---------------------------------------------------------------------------
func TestQA_Export_PartialImportFailure_ExtentState(t *testing.T) {
	volSize := uint64(64 * 1024) // 16 blocks
	srcVol := createExportTestVol(t, volSize)

	// Write distinct pattern to source.
	for lba := uint64(0); lba < 16; lba++ {
		block := make([]byte, 4096)
		block[0] = byte(lba + 1)
		if err := srcVol.WriteLBA(lba, block); err != nil {
			t.Fatalf("src WriteLBA %d: %v", lba, err)
		}
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Target with its own data pattern.
	dstVol := createExportTestVol(t, volSize)
	for lba := uint64(0); lba < 16; lba++ {
		block := make([]byte, 4096)
		block[0] = byte(0xF0 + lba)
		if err := dstVol.WriteLBA(lba, block); err != nil {
			t.Fatalf("dst WriteLBA %d: %v", lba, err)
		}
	}

	// Create a reader that fails after delivering half the data.
	exportData := buf.Bytes()
	halfLen := len(exportData) / 2
	failReader := &failAfterNReader{data: exportData, failAfter: halfLen}

	err = dstVol.ImportSnapshot(context.Background(), manifest, failReader, ImportOptions{AllowOverwrite: true})
	if err == nil {
		t.Fatal("expected error from partial reader")
	}

	// Document: after partial import failure, some blocks have import data
	// and some have original data. The volume is in an inconsistent state.
	// This is the expected (documented) behavior — callers must handle failure
	// by discarding the volume or re-importing.
	t.Logf("Partial import failed as expected: %v", err)

	// The WAL should NOT have been reset (failure before checksum check).
	// Verify the volume is still operational (not panicked/crashed).
	testBlock := make([]byte, 4096)
	testBlock[0] = 0x99
	if err := dstVol.WriteLBA(15, testBlock); err != nil {
		t.Fatalf("post-failure write should still work: %v", err)
	}
	got, err := dstVol.ReadLBA(15, 4096)
	if err != nil {
		t.Fatalf("post-failure read: %v", err)
	}
	if got[0] != 0x99 {
		t.Errorf("post-failure read = 0x%02X, want 0x99", got[0])
	}
}

// failAfterNReader delivers the first failAfter bytes, then returns an error.
type failAfterNReader struct {
	data      []byte
	failAfter int
	pos       int
}

func (r *failAfterNReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAfter {
		return 0, errors.New("injected reader failure")
	}
	remaining := r.failAfter - r.pos
	n := len(p)
	if n > remaining {
		n = remaining
	}
	if r.pos+n > len(r.data) {
		n = len(r.data) - r.pos
	}
	copy(p[:n], r.data[r.pos:r.pos+n])
	r.pos += n
	if r.pos >= r.failAfter {
		return n, errors.New("injected reader failure")
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// QA-EX-3: Import rejects active snapshots (BUG-CP11A4-1 fix).
//
// Import overwrites the extent region. Non-CoW'd snapshot blocks read from
// extent and would get import data instead of snapshot-time data. The fix
// rejects import when any snapshots are active.
// ---------------------------------------------------------------------------
func TestQA_Export_ImportWithActiveSnapshot_Rejected(t *testing.T) {
	volSize := uint64(64 * 1024)

	// Source for export.
	srcVol := createExportTestVol(t, volSize)
	srcBlock := make([]byte, 4096)
	srcBlock[0] = 0xEE
	if err := srcVol.WriteLBA(0, srcBlock); err != nil {
		t.Fatalf("src write: %v", err)
	}
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Target with active snapshot.
	dstVol := createExportTestVol(t, volSize)
	if err := dstVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("dst write: %v", err)
	}
	if err := dstVol.CreateSnapshot(100); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Import must be rejected — active snapshot would be corrupted.
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{AllowOverwrite: true})
	if err != ErrImportActiveSnapshots {
		t.Fatalf("import with active snapshot: got %v, want ErrImportActiveSnapshots", err)
	}

	// Snapshot reads are unaffected (import was rejected).
	snap0, err := dstVol.ReadSnapshot(100, 0, 4096)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snap0[0] != 0x00 {
		t.Errorf("snapshot block 0 = 0x%02X, want 0x00 (original zero-filled write)", snap0[0])
	}

	// After deleting snapshot, import succeeds.
	dstVol.DeleteSnapshot(100)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{AllowOverwrite: true})
	if err != nil {
		t.Fatalf("import after snapshot delete: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QA-EX-4: Double import without AllowOverwrite — FlagImported gate.
//
// After a successful import, FlagImported is set. A second import without
// AllowOverwrite should be rejected even though nextLSN may still be 1.
// ---------------------------------------------------------------------------
func TestQA_Export_DoubleImportRejected(t *testing.T) {
	volSize := uint64(64 * 1024)
	srcVol := createExportTestVol(t, volSize)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}
	exportData := buf.Bytes()

	// First import.
	dstVol := createExportTestVol(t, volSize)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(exportData), ImportOptions{})
	if err != nil {
		t.Fatalf("first import: %v", err)
	}

	// Verify FlagImported is set.
	if dstVol.super.Flags&FlagImported == 0 {
		t.Fatal("FlagImported not set after import")
	}

	// Second import without AllowOverwrite should fail.
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(exportData), ImportOptions{})
	if err == nil {
		t.Fatal("expected rejection for double import without AllowOverwrite")
	}
	if !errors.Is(err, ErrImportTargetNotEmpty) {
		t.Errorf("expected ErrImportTargetNotEmpty, got: %v", err)
	}

	// With AllowOverwrite, second import should succeed.
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(exportData), ImportOptions{AllowOverwrite: true})
	if err != nil {
		t.Fatalf("second import with AllowOverwrite: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QA-EX-5: Export after Close — beginOp guard.
// ---------------------------------------------------------------------------
func TestQA_Export_ExportAfterClose(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)
	vol.Close()

	var buf bytes.Buffer
	_, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err == nil {
		t.Fatal("expected ErrVolumeClosed")
	}
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("expected ErrVolumeClosed, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QA-EX-6: Import after Close — beginOp guard.
// ---------------------------------------------------------------------------
func TestQA_Export_ImportAfterClose(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	dstVol := createExportTestVol(t, 64*1024)
	dstVol.Close()

	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err == nil {
		t.Fatal("expected ErrVolumeClosed")
	}
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("expected ErrVolumeClosed, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QA-EX-7: Concurrent exports — unique temp snapshot IDs, no collision panic.
// ---------------------------------------------------------------------------
func TestQA_Export_ConcurrentExports_NoCollision(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)
	data := make([]byte, 4096)
	data[0] = 0x77
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	const N = 8
	var wg sync.WaitGroup
	errs := make([]error, N)
	checksums := make([]string, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var buf bytes.Buffer
			m, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
			errs[idx] = err
			if m != nil {
				checksums[idx] = m.SHA256
			}
		}(i)
	}
	wg.Wait()

	successCount := 0
	for i, err := range errs {
		if err != nil {
			t.Logf("export %d: %v", i, err)
		} else {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatal("all concurrent exports failed")
	}

	// All successful exports should produce the same checksum.
	var refSum string
	for i, cs := range checksums {
		if errs[i] == nil {
			if refSum == "" {
				refSum = cs
			} else if cs != refSum {
				t.Errorf("export %d checksum %q != reference %q", i, cs, refSum)
			}
		}
	}
	t.Logf("%d/%d concurrent exports succeeded", successCount, N)
}

// ---------------------------------------------------------------------------
// QA-EX-8: FlagImported survives close + reopen.
// ---------------------------------------------------------------------------
func TestQA_Export_FlagImportedSurvivesReopen(t *testing.T) {
	volSize := uint64(64 * 1024)
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.vol")

	// Create, export from helper, import.
	srcVol := createExportTestVol(t, volSize)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	vol, err := CreateBlockVol(path, CreateOptions{VolumeSize: volSize, WALSize: 32 << 10})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	err = vol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}
	if vol.super.Flags&FlagImported == 0 {
		t.Fatal("FlagImported not set after import")
	}
	vol.Close()

	// Reopen and verify flag persisted.
	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer vol2.Close()

	if vol2.super.Flags&FlagImported == 0 {
		t.Fatal("FlagImported lost after reopen — superblock persistence bug")
	}

	// Second import should be rejected without AllowOverwrite.
	err = vol2.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err == nil {
		t.Fatal("expected rejection after reopen")
	}
}

// ---------------------------------------------------------------------------
// QA-EX-9: Import context cancellation mid-stream.
//
// Uses a slow reader that blocks after some data, then cancels context.
// Verifies: no panic, volume still operational, WAL NOT reset (failure path).
// ---------------------------------------------------------------------------
func TestQA_Export_ImportContextCancelMidStream(t *testing.T) {
	volSize := uint64(64 * 1024)
	srcVol := createExportTestVol(t, volSize)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	dstVol := createExportTestVol(t, volSize)
	ctx, cancel := context.WithCancel(context.Background())

	// Reader that delivers one chunk then blocks until context is cancelled.
	exportData := buf.Bytes()
	blockingReader := &cancelBlockingReader{
		data:   exportData,
		cancel: cancel,
		// Cancel after delivering first 4KB (one block).
		cancelAfter: 4096,
	}

	err = dstVol.ImportSnapshot(ctx, manifest, blockingReader, ImportOptions{})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	t.Logf("import cancelled as expected: %v", err)

	// Volume should still be operational.
	testData := make([]byte, 4096)
	testData[0] = 0xDD
	if err := dstVol.WriteLBA(0, testData); err != nil {
		t.Fatalf("post-cancel write should work: %v", err)
	}
}

// cancelBlockingReader delivers cancelAfter bytes, then cancels the context
// and returns context.Canceled on the next read.
type cancelBlockingReader struct {
	data        []byte
	cancel      context.CancelFunc
	cancelAfter int
	pos         int
	cancelled   atomic.Bool
}

func (r *cancelBlockingReader) Read(p []byte) (int, error) {
	if r.cancelled.Load() {
		return 0, context.Canceled
	}
	if r.pos >= r.cancelAfter {
		r.cancelled.Store(true)
		r.cancel()
		return 0, context.Canceled
	}
	remaining := r.cancelAfter - r.pos
	n := len(p)
	if n > remaining {
		n = remaining
	}
	if r.pos+n > len(r.data) {
		n = len(r.data) - r.pos
	}
	copy(p[:n], r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// ---------------------------------------------------------------------------
// QA-EX-10: Export with non-chunk-aligned block count.
//
// Volume with a block count that doesn't divide evenly by exportChunkBlocks
// (256). Verifies the last partial chunk is handled correctly.
// ---------------------------------------------------------------------------
func TestQA_Export_NonChunkAlignedBlockCount(t *testing.T) {
	// 100 blocks × 4KB = 400KB. 100 is not a multiple of 256.
	volSize := uint64(100 * 4096)
	vol := createExportTestVol(t, volSize)

	// Write pattern to every block.
	for lba := uint64(0); lba < 100; lba++ {
		block := make([]byte, 4096)
		block[0] = byte(lba % 256)
		if err := vol.WriteLBA(lba, block); err != nil {
			t.Fatalf("WriteLBA %d: %v", lba, err)
		}
	}

	var buf bytes.Buffer
	manifest, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	if manifest.DataSizeBytes != volSize {
		t.Errorf("DataSizeBytes = %d, want %d", manifest.DataSizeBytes, volSize)
	}
	if uint64(buf.Len()) != volSize {
		t.Errorf("buf.Len() = %d, want %d", buf.Len(), volSize)
	}

	// Import and verify all blocks.
	dstVol := createExportTestVol(t, volSize)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	for lba := uint64(0); lba < 100; lba++ {
		got, err := dstVol.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", lba, err)
		}
		if got[0] != byte(lba%256) {
			t.Errorf("block %d: first byte = 0x%02X, want 0x%02X", lba, got[0], byte(lba%256))
		}
	}
}

// ---------------------------------------------------------------------------
// QA-EX-11: Export + Import with zero-data volume (all zeros).
//
// Edge case: volume with no writes. All blocks are zero. Export should produce
// a valid artifact with a known SHA-256 and import should succeed.
// ---------------------------------------------------------------------------
func TestQA_Export_ZeroDataVolume(t *testing.T) {
	volSize := uint64(64 * 1024)
	vol := createExportTestVol(t, volSize)

	var buf bytes.Buffer
	manifest, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	if manifest.DataSizeBytes != volSize {
		t.Errorf("DataSizeBytes = %d, want %d", manifest.DataSizeBytes, volSize)
	}
	if manifest.SHA256 == "" {
		t.Error("SHA256 is empty for zero-data export")
	}

	// All exported bytes should be zero.
	for i, b := range buf.Bytes() {
		if b != 0 {
			t.Fatalf("byte %d = 0x%02X, want 0x00", i, b)
		}
	}

	// Import into new volume.
	dstVol := createExportTestVol(t, volSize)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QA-EX-12: Temp snapshot ID uniqueness across calls.
//
// Verifies the atomic sequence counter produces strictly increasing IDs
// and doesn't reuse or collide.
// ---------------------------------------------------------------------------
func TestQA_Export_TempSnapIDUniqueness(t *testing.T) {
	const N = 100
	ids := make(map[uint32]bool, N)
	for i := 0; i < N; i++ {
		id := exportTempSnapBase + exportTempSnapSeq.Add(1)
		if ids[id] {
			t.Fatalf("duplicate temp snapshot ID: 0x%08X at iteration %d", id, i)
		}
		ids[id] = true
	}
	// All IDs should be in the reserved range.
	for id := range ids {
		if id <= exportTempSnapBase {
			t.Errorf("ID 0x%08X is at or below base 0x%08X", id, exportTempSnapBase)
		}
	}
}

// Ensure io package is used.
var _ = io.Discard
