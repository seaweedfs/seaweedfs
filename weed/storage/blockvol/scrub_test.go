package blockvol

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createScrubVol(t *testing.T) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	v, err := CreateBlockVol(filepath.Join(dir, "scrub.blk"), CreateOptions{
		VolumeSize: 64 * 1024, // 64 KiB = 16 blocks
		BlockSize:  4096,
		WALSize:    64 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	return v
}

func TestScrub_CleanVolume(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0

	// First pass: establishes baseline CRCs.
	scrubber.runPass()
	stats := scrubber.Stats()
	if stats.PassCount != 1 {
		t.Fatalf("PassCount: got %d, want 1", stats.PassCount)
	}
	if stats.ErrorCount != 0 {
		t.Fatalf("ErrorCount: got %d, want 0", stats.ErrorCount)
	}

	// Second pass: same data, no errors.
	scrubber.runPass()
	stats = scrubber.Stats()
	if stats.PassCount != 2 {
		t.Fatalf("PassCount: got %d, want 2", stats.PassCount)
	}
	if stats.ErrorCount != 0 {
		t.Fatalf("ErrorCount: got %d, want 0", stats.ErrorCount)
	}
}

func TestScrub_DetectCorruption(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	// Write some data and flush it to extent.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	// Wait for flusher to flush.
	time.Sleep(50 * time.Millisecond)

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0

	// First pass: establish baseline.
	scrubber.runPass()
	if scrubber.Stats().ErrorCount != 0 {
		t.Fatalf("first pass errors: %d", scrubber.Stats().ErrorCount)
	}

	// Corrupt the extent data directly.
	extentStart := v.super.WALOffset + v.super.WALSize
	corruptBuf := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	v.fd.WriteAt(corruptBuf, int64(extentStart))
	v.fd.Sync()

	// Second pass: should detect corruption.
	scrubber.runPass()
	stats := scrubber.Stats()
	if stats.ErrorCount == 0 {
		t.Fatal("expected corruption error but got none")
	}
}

func TestScrub_SkipDirtyBlocks(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0

	// First pass: baseline.
	scrubber.runPass()

	// Write data (stays in dirty map since flusher interval is slow relative to us).
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xBB
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Second pass: segment 0 has dirty blocks, should be skipped.
	scrubber.runPass()
	stats := scrubber.Stats()
	if stats.ErrorCount != 0 {
		t.Fatalf("dirty block should be skipped, not flagged as error: errors=%d", stats.ErrorCount)
	}
}

func TestScrub_SkipRecentlyWritten(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0

	// First pass: baseline.
	scrubber.runPass()

	// Write, sync, flush (data in extent, dirty map cleared).
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xCC
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	v.SyncCache()
	time.Sleep(50 * time.Millisecond) // let flusher clear dirty map

	// Manually notify scrubber of write (simulates hook in appendWithRetry).
	scrubber.NotifyWrite(0, 1)

	// Second pass: segment 0 was recently written, skip CRC comparison.
	scrubber.runPass()
	stats := scrubber.Stats()
	if stats.ErrorCount != 0 {
		t.Fatalf("recently written segment should be skipped: errors=%d", stats.ErrorCount)
	}
}

func TestScrub_StatsUpdated(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0
	scrubber.runPass()

	stats := scrubber.Stats()
	if stats.LastPassTime == 0 {
		t.Fatal("LastPassTime should be set")
	}
	if stats.SegmentsTotal == 0 {
		t.Fatal("SegmentsTotal should be > 0")
	}
}

func TestScrub_TriggerNow(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 1*time.Hour) // long interval
	scrubber.throttle = 0
	scrubber.Start()
	defer scrubber.Stop()

	scrubber.TriggerNow()
	// Wait for the triggered pass to complete.
	time.Sleep(100 * time.Millisecond)

	stats := scrubber.Stats()
	if stats.PassCount < 1 {
		t.Fatalf("TriggerNow did not cause a pass: PassCount=%d", stats.PassCount)
	}
}

func TestScrub_StopIdempotent(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.Start()
	scrubber.Stop()
	scrubber.Stop() // second stop should be no-op
}

func TestScrub_HealthScoreImpact(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	path := filepath.Join(dir, "health.blk")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	if v.HealthScore() != 1.0 {
		t.Fatalf("initial health: got %f, want 1.0", v.HealthScore())
	}

	// Write + flush to extent.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xDD
	}
	v.WriteLBA(0, data)
	v.SyncCache()
	time.Sleep(50 * time.Millisecond)

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0
	scrubber.runPass()

	// Corrupt extent.
	extentStart := v.super.WALOffset + v.super.WALSize
	corruptBuf := []byte{0xFF}
	v.fd.WriteAt(corruptBuf, int64(extentStart))
	v.fd.Sync()

	scrubber.runPass()
	if v.HealthScore() >= 1.0 {
		t.Fatalf("health should drop after corruption: got %f", v.HealthScore())
	}

	// Cleanup temp files on Windows: remove delta files if any.
	_ = os.RemoveAll(dir)
}

// Fix #5: Write arriving during a scrub pass must not cause a false corruption detection.
func TestScrub_InPassWrite_NoFalsePositive(t *testing.T) {
	v := createScrubVol(t)
	defer v.Close()

	// Write initial data and flush to extent.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	v.SyncCache()
	time.Sleep(50 * time.Millisecond) // let flusher clear dirty map

	scrubber := NewScrubber(v, 24*time.Hour)
	scrubber.throttle = 0

	// First pass: establish baseline CRC for segment 0.
	scrubber.runPass()
	if scrubber.Stats().ErrorCount != 0 {
		t.Fatalf("baseline pass should have no errors: %d", scrubber.Stats().ErrorCount)
	}

	// Now write different data and flush — this changes the extent.
	for i := range data {
		data[i] = 0xBB
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	v.SyncCache()
	time.Sleep(50 * time.Millisecond) // flushed to extent, dirty map cleared

	// Simulate in-pass write: NotifyWrite arrives AFTER pass would have
	// snapshotted writtenSegs (old bug), but BEFORE segment 0 is read.
	// With the fix, writtenSegs is checked live during the pass.
	scrubber.NotifyWrite(0, 1)

	// Second pass: segment 0 extent data changed, but NotifyWrite should
	// prevent false corruption — segment should be skipped.
	scrubber.runPass()
	stats := scrubber.Stats()
	if stats.ErrorCount != 0 {
		t.Fatalf("in-pass write caused false corruption: errors=%d", stats.ErrorCount)
	}
	if stats.SegmentsDirty == 0 {
		t.Fatal("segment 0 should be counted as dirty (recently written)")
	}
}
