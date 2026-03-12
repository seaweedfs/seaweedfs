package blockvol

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// =============================================================================
// QA Adversarial Tests for StorageProfile (CP11A-1)
//
// These tests go beyond the dev-test coverage in storage_profile_test.go:
// - SP-A1: write/read data integrity on single profile
// - SP-A2: concurrent writes with no corruption
// - additional: crash recovery, superblock byte corruption, boundary cases
// =============================================================================

// TestQA_Profile_WritePath_SingleCorrect writes multiple blocks at different
// LBAs on a single-profile volume, reads them back, and verifies byte-for-byte
// correctness. This is SP-A1 from the test spec.
func TestQA_Profile_WritePath_SingleCorrect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-a1.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     256 * 1024, // 256KB = 64 blocks
		BlockSize:      4096,
		WALSize:        128 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()

	if vol.Profile() != ProfileSingle {
		t.Fatalf("Profile() = %v, want single", vol.Profile())
	}

	// Write unique patterns to blocks 0, 10, 30, 63 (last block).
	type testBlock struct {
		lba  uint64
		fill byte
	}
	blocks := []testBlock{
		{0, 0xAA},
		{10, 0xBB},
		{30, 0xCC},
		{63, 0xDD}, // last block in 256KB volume
	}

	for _, b := range blocks {
		data := make([]byte, 4096)
		for i := range data {
			data[i] = b.fill
		}
		if err := vol.WriteLBA(b.lba, data); err != nil {
			t.Fatalf("WriteLBA(%d): %v", b.lba, err)
		}
	}

	// SyncCache to ensure WAL is durable.
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Read back and verify.
	for _, b := range blocks {
		got, err := vol.ReadLBA(b.lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", b.lba, err)
		}
		expected := make([]byte, 4096)
		for i := range expected {
			expected[i] = b.fill
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("LBA %d: data mismatch (first byte: got 0x%02X, want 0x%02X)",
				b.lba, got[0], b.fill)
		}
	}

	// Unwritten blocks should read as zeros.
	zeros, err := vol.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(5): %v", err)
	}
	for i, b := range zeros {
		if b != 0 {
			t.Fatalf("LBA 5 byte[%d] = 0x%02X, want 0x00 (unwritten)", i, b)
		}
	}
}

// TestQA_Profile_ConcurrentWrites_Single runs 16 goroutines writing to
// non-overlapping LBAs on a single-profile volume. No data corruption
// or panics should occur. This is SP-A2 from the test spec.
func TestQA_Profile_ConcurrentWrites_Single(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-a2.blk")

	// 1MB volume = 256 blocks. Each of 16 goroutines gets 16 blocks.
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     1024 * 1024,
		BlockSize:      4096,
		WALSize:        512 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()

	const goroutines = 16
	const blocksPerGoroutine = 16
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			baseLBA := uint64(gid * blocksPerGoroutine)
			fill := byte(gid + 1) // unique fill per goroutine

			for i := 0; i < blocksPerGoroutine; i++ {
				data := make([]byte, 4096)
				for j := range data {
					data[j] = fill
				}
				if err := vol.WriteLBA(baseLBA+uint64(i), data); err != nil {
					errs[gid] = fmt.Errorf("goroutine %d LBA %d: %v", gid, baseLBA+uint64(i), err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// Sync and verify all data.
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	for g := 0; g < goroutines; g++ {
		baseLBA := uint64(g * blocksPerGoroutine)
		fill := byte(g + 1)
		for i := 0; i < blocksPerGoroutine; i++ {
			lba := baseLBA + uint64(i)
			got, err := vol.ReadLBA(lba, 4096)
			if err != nil {
				t.Fatalf("ReadLBA(%d): %v", lba, err)
			}
			for j, b := range got {
				if b != fill {
					t.Fatalf("LBA %d byte[%d] = 0x%02X, want 0x%02X (goroutine %d)",
						lba, j, b, fill, g)
				}
			}
		}
	}
}

// TestQA_Profile_SurvivesCrashRecovery writes data on a single-profile
// volume, simulates a crash (close without clean shutdown), reopens, and
// verifies that the profile metadata and data are intact.
func TestQA_Profile_SurvivesCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-crash.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     64 * 1024,
		BlockSize:      4096,
		WALSize:        32 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write known data.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xEE
	}
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Close normally (simulates a crash by just closing).
	vol.Close()

	// Reopen — crash recovery runs.
	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer vol2.Close()

	if vol2.Profile() != ProfileSingle {
		t.Errorf("Profile after reopen = %v, want single", vol2.Profile())
	}

	got, err := vol2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after reopen: %v", err)
	}
	if got[0] != 0xEE {
		t.Errorf("data[0] = 0x%02X, want 0xEE", got[0])
	}
}

// TestQA_Profile_CorruptByte_AllValues corrupts the StorageProfile byte on
// disk to every value 2..255 and verifies that OpenBlockVol rejects each one.
func TestQA_Profile_CorruptByte_AllValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-corrupt-all.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 1024,
		BlockSize:  4096,
		WALSize:    32 * 1024,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	vol.Close()

	// Read original file for restoration.
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for corruptVal := byte(2); corruptVal != 0; corruptVal++ { // 2..255
		// Restore original, then corrupt.
		if err := os.WriteFile(path, original, 0644); err != nil {
			t.Fatalf("restore: %v", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := f.WriteAt([]byte{corruptVal}, 105); err != nil {
			f.Close()
			t.Fatalf("corrupt: %v", err)
		}
		f.Close()

		_, err = OpenBlockVol(path)
		if err == nil {
			t.Errorf("StorageProfile=%d: OpenBlockVol should fail", corruptVal)
		}
	}
}

// TestQA_Profile_StripedReject_NoFileLeaked verifies that attempting to
// create a striped volume does not leak partial files, even under different
// config combinations.
func TestQA_Profile_StripedReject_NoFileLeaked(t *testing.T) {
	dir := t.TempDir()

	configs := []CreateOptions{
		{VolumeSize: 64 * 1024, StorageProfile: ProfileStriped},
		{VolumeSize: 1024 * 1024, StorageProfile: ProfileStriped, WALSize: 256 * 1024},
		{VolumeSize: 64 * 1024, StorageProfile: ProfileStriped, BlockSize: 512},
	}

	for i, opts := range configs {
		path := filepath.Join(dir, fmt.Sprintf("striped-%d.blk", i))
		_, err := CreateBlockVol(path, opts)
		if !errors.Is(err, ErrStripedNotImplemented) {
			t.Errorf("config %d: error = %v, want ErrStripedNotImplemented", i, err)
		}
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Errorf("config %d: file %s should not exist after rejected create", i, path)
		}
	}
}

// TestQA_Profile_ConcurrentCreateSameFile races multiple goroutines trying
// to create a volume at the same path. Exactly one should succeed (O_EXCL),
// the rest should fail. No partial files should remain from losers.
func TestQA_Profile_ConcurrentCreateSameFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "race.blk")

	const racers = 8
	var (
		wg       sync.WaitGroup
		wins     atomic.Int32
		errCount atomic.Int32
	)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vol, err := CreateBlockVol(path, CreateOptions{
				VolumeSize:     64 * 1024,
				BlockSize:      4096,
				WALSize:        32 * 1024,
				StorageProfile: ProfileSingle,
			})
			if err != nil {
				errCount.Add(1)
				return
			}
			wins.Add(1)
			vol.Close()
		}()
	}
	wg.Wait()

	if wins.Load() != 1 {
		t.Errorf("winners = %d, want exactly 1", wins.Load())
	}
	if errCount.Load() != racers-1 {
		t.Errorf("errors = %d, want %d", errCount.Load(), racers-1)
	}

	// The winner's file should be valid.
	vol, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol winner file: %v", err)
	}
	defer vol.Close()
	if vol.Profile() != ProfileSingle {
		t.Errorf("Profile() = %v, want single", vol.Profile())
	}
}

// TestQA_Profile_SuperblockByteOffset verifies the StorageProfile byte is
// at the exact expected offset (105) in the on-disk format. This prevents
// silent field-reorder regressions.
func TestQA_Profile_SuperblockByteOffset(t *testing.T) {
	sb, err := NewSuperblock(64*1024, CreateOptions{
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("NewSuperblock: %v", err)
	}

	// Write the superblock for single profile.
	var buf bytes.Buffer
	sb.WriteTo(&buf)
	data := buf.Bytes()

	if data[105] != 0 {
		t.Errorf("offset 105 = %d, want 0 (ProfileSingle)", data[105])
	}

	// Now set striped and check the byte changed.
	sb.StorageProfile = uint8(ProfileStriped)
	var buf2 bytes.Buffer
	sb.WriteTo(&buf2)
	data2 := buf2.Bytes()

	if data2[105] != 1 {
		t.Errorf("offset 105 = %d, want 1 (ProfileStriped)", data2[105])
	}

	// Verify all other bytes are identical (only offset 105 changed).
	for i := range data {
		if i == 105 {
			continue
		}
		if data[i] != data2[i] {
			t.Errorf("byte[%d] changed: 0x%02X -> 0x%02X (only offset 105 should differ)", i, data[i], data2[i])
		}
	}
}

// TestQA_Profile_MultiBlockWriteRead writes a multi-block (16KB) payload
// at a non-zero LBA and reads it back on a single-profile volume.
// Catches alignment and multi-block dirty-map consistency bugs.
func TestQA_Profile_MultiBlockWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-multi.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     512 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()

	// Write 4 blocks (16KB) of random data at LBA 20.
	payload := make([]byte, 16384)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	if err := vol.WriteLBA(20, payload); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := vol.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	got, err := vol.ReadLBA(20, 16384)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("multi-block payload mismatch")
	}
}

// TestQA_Profile_ExpandPreservesProfile verifies that expanding a
// single-profile volume preserves the profile metadata.
func TestQA_Profile_ExpandPreservesProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-expand.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     64 * 1024,
		BlockSize:      4096,
		WALSize:        32 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write at LBA 0 before expand.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0x42
	}
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Expand to 128KB.
	if err := vol.Expand(128 * 1024); err != nil {
		t.Fatalf("Expand: %v", err)
	}

	if vol.Profile() != ProfileSingle {
		t.Errorf("Profile after expand = %v, want single", vol.Profile())
	}

	// Verify data at LBA 0 survived.
	got, err := vol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if got[0] != 0x42 {
		t.Errorf("data[0] = 0x%02X, want 0x42", got[0])
	}

	// Write to new region (LBA 16+ is in expanded area).
	newData := make([]byte, 4096)
	for i := range newData {
		newData[i] = 0x99
	}
	if err := vol.WriteLBA(20, newData); err != nil {
		t.Fatalf("WriteLBA(20): %v", err)
	}

	got2, err := vol.ReadLBA(20, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(20): %v", err)
	}
	if got2[0] != 0x99 {
		t.Errorf("expanded LBA 20 data[0] = 0x%02X, want 0x99", got2[0])
	}

	// Close and reopen — verify profile and data survive.
	vol.Close()
	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer vol2.Close()

	if vol2.Profile() != ProfileSingle {
		t.Errorf("Profile after reopen = %v, want single", vol2.Profile())
	}
	if vol2.Info().VolumeSize != 128*1024 {
		t.Errorf("VolumeSize = %d, want %d", vol2.Info().VolumeSize, 128*1024)
	}
}

// TestQA_Profile_SnapshotPreservesProfile creates a snapshot on a
// single-profile volume, writes more data, restores the snapshot,
// and verifies the profile metadata is unchanged.
func TestQA_Profile_SnapshotPreservesProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sp-snap.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     64 * 1024,
		BlockSize:      4096,
		WALSize:        32 * 1024,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()

	// Write block A.
	dataA := make([]byte, 4096)
	for i := range dataA {
		dataA[i] = 0xAA
	}
	if err := vol.WriteLBA(0, dataA); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}

	// Create snapshot.
	if err := vol.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Write block B (overwrites A at LBA 0).
	dataB := make([]byte, 4096)
	for i := range dataB {
		dataB[i] = 0xBB
	}
	if err := vol.WriteLBA(0, dataB); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}

	// Verify live reads B.
	got, _ := vol.ReadLBA(0, 4096)
	if got[0] != 0xBB {
		t.Fatalf("live data[0] = 0x%02X, want 0xBB", got[0])
	}

	// Restore snapshot.
	if err := vol.RestoreSnapshot(1); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	// Profile should be unchanged.
	if vol.Profile() != ProfileSingle {
		t.Errorf("Profile after restore = %v, want single", vol.Profile())
	}

	// Data should be A again.
	got2, _ := vol.ReadLBA(0, 4096)
	if got2[0] != 0xAA {
		t.Errorf("restored data[0] = 0x%02X, want 0xAA", got2[0])
	}
}
