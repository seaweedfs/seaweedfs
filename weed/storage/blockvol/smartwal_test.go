package blockvol

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"testing"
)

// ============================================================
// SmartWAL Single-Node Crash Tests (Prototype)
//
// These tests prove the core SmartWAL algorithm:
//   1. Extent-first write + metadata-only WAL
//   2. SyncCache barrier ordering (extent before WAL)
//   3. Crash recovery via CRC verification
// ============================================================

func createTestSmartWALVolume(t *testing.T, numBlocks uint64) *SmartWALVolume {
	t.Helper()
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  numBlocks,
		WALSlots:   1024,
		Epoch:      1,
	}
	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatalf("CreateSmartWALVolume: %v", err)
	}
	t.Cleanup(func() { v.Close() })
	return v
}

func makeTestBlock(pattern byte) []byte {
	b := make([]byte, 4096)
	for i := range b {
		b[i] = pattern
	}
	return b
}

// Test 1: Basic crash recovery
// Write blocks → SyncCache → "crash" (close + reopen) → verify all blocks
func TestSmartWAL_BasicCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  256,
		WALSlots:   1024,
		Epoch:      1,
	}

	// Write 100 blocks and sync
	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 100; i++ {
		if err := v.WriteLBA(i, makeTestBlock(byte(i))); err != nil {
			t.Fatalf("WriteLBA %d: %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	v.Close()

	// "Crash" recovery: reopen
	v2, err := OpenSmartWALVolume(cfg)
	if err != nil {
		t.Fatalf("OpenSmartWALVolume: %v", err)
	}
	defer v2.Close()

	// Verify all 100 blocks
	for i := uint32(0); i < 100; i++ {
		data, err := v2.ReadLBA(i)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", i, err)
		}
		expected := makeTestBlock(byte(i))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d: data mismatch after recovery", i)
		}
	}

	// NextLSN should be > 100
	if v2.NextLSN() <= 100 {
		t.Fatalf("NextLSN=%d, want >100", v2.NextLSN())
	}
}

// Test 2: Crash before SyncCache
// Write blocks → NO SyncCache → "crash" → recover
// All recovered records should have matching CRCs. No corruption.
func TestSmartWAL_CrashBeforeSyncCache(t *testing.T) {
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  256,
		WALSlots:   1024,
		Epoch:      1,
	}

	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 50; i++ {
		if err := v.WriteLBA(i, makeTestBlock(byte(i+0x80))); err != nil {
			t.Fatalf("WriteLBA %d: %v", i, err)
		}
	}
	// NO SyncCache — close directly (simulates crash)
	v.Close()

	// Recovery: whatever records survived are CRC-verified
	v2, err := OpenSmartWALVolume(cfg)
	if err != nil {
		t.Fatalf("OpenSmartWALVolume: %v", err)
	}
	defer v2.Close()

	// Read all blocks — no corruption allowed.
	// Some may have the written data, some may be zeros (not flushed).
	// But NO CRC mismatch in the recovery output (recovery logs mismatches).
	for i := uint32(0); i < 50; i++ {
		data, err := v2.ReadLBA(i)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", i, err)
		}
		// Data is either the written pattern or zeros — both valid.
		expected := makeTestBlock(byte(i + 0x80))
		zeros := make([]byte, 4096)
		if !bytes.Equal(data, expected) && !bytes.Equal(data, zeros) {
			t.Fatalf("LBA %d: unexpected data (neither written nor zeros)", i)
		}
	}
}

// Test 3: Overwrite crash
// Write dataA → SyncCache → Write dataB → crash (no sync) → recover
// LBA should contain dataA (the durable version) OR dataB (if lucky flush).
// Either is valid. Corruption is not.
func TestSmartWAL_OverwriteCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  256,
		WALSlots:   1024,
		Epoch:      1,
	}

	dataA := makeTestBlock(0xAA)
	dataB := makeTestBlock(0xBB)

	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.WriteLBA(100, dataA); err != nil {
		t.Fatal(err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}
	// Overwrite without sync
	if err := v.WriteLBA(100, dataB); err != nil {
		t.Fatal(err)
	}
	v.Close()

	// Recovery
	v2, err := OpenSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	data, err := v2.ReadLBA(100)
	if err != nil {
		t.Fatal(err)
	}
	// Either dataA (durable) or dataB (lucky flush) — both valid
	if !bytes.Equal(data, dataA) && !bytes.Equal(data, dataB) {
		t.Fatalf("LBA 100: data is neither dataA nor dataB after overwrite crash")
	}
}

// Test 4: WAL wrap-around
// Write enough blocks to wrap the ring buffer, syncing periodically.
func TestSmartWAL_WALWrapAround(t *testing.T) {
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  1024,
		WALSlots:   64, // small: wraps quickly
		Epoch:      1,
	}

	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Write 200 blocks (wraps 64-slot WAL ~3 times), sync every 50
	for i := uint32(0); i < 200; i++ {
		if err := v.WriteLBA(i%1024, makeTestBlock(byte(i))); err != nil {
			t.Fatalf("WriteLBA %d: %v", i, err)
		}
		if i%50 == 49 {
			if err := v.SyncCache(); err != nil {
				t.Fatalf("SyncCache at %d: %v", i, err)
			}
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}
	v.Close()

	// Recovery
	v2, err := OpenSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// Verify the last written data for LBAs 0-199 (mod 1024)
	// Only the LAST write to each LBA matters
	for i := uint32(0); i < 200; i++ {
		lba := i % 1024
		data, err := v2.ReadLBA(lba)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", lba, err)
		}
		// Last writer wins. For LBA 0: last write was i=0 (pattern 0).
		// For LBA 1: i=1 (pattern 1). Etc.
		// Since all LBAs < 200 and numBlocks=1024, no wrapping on LBAs.
		expected := makeTestBlock(byte(i))
		if !bytes.Equal(data, expected) {
			t.Fatalf("LBA %d: data mismatch after WAL wrap recovery (wrote at i=%d)", lba, i)
		}
	}
}

// Test 5: Record encode/decode round-trip
func TestSmartWAL_RecordRoundTrip(t *testing.T) {
	rec := SmartWALRecord{
		LSN:       42,
		Epoch:     7,
		LBA:       0x1234,
		Flags:     SmartFlagWrite,
		DataCRC32: 0xDEADBEEF,
	}
	encoded := EncodeSmartWALRecord(rec)
	decoded, ok := DecodeSmartWALRecord(encoded[:])
	if !ok {
		t.Fatal("decode failed for valid record")
	}
	if decoded.LSN != rec.LSN || decoded.Epoch != rec.Epoch ||
		decoded.LBA != rec.LBA || decoded.Flags != rec.Flags ||
		decoded.DataCRC32 != rec.DataCRC32 {
		t.Fatalf("round-trip mismatch: %+v → %+v", rec, decoded)
	}
}

// Test 6: Invalid record detection
func TestSmartWAL_InvalidRecordDetection(t *testing.T) {
	// All zeros → invalid (no magic)
	zeros := make([]byte, SmartWALRecordSize)
	if _, ok := DecodeSmartWALRecord(zeros); ok {
		t.Fatal("zeros should decode as invalid")
	}

	// Valid record with corrupted CRC
	rec := SmartWALRecord{LSN: 1, Epoch: 1, LBA: 0, Flags: SmartFlagWrite}
	encoded := EncodeSmartWALRecord(rec)
	encoded[30] ^= 0xFF // corrupt record CRC
	if _, ok := DecodeSmartWALRecord(encoded[:]); ok {
		t.Fatal("corrupted CRC should decode as invalid")
	}
}

// Test 7: Sustained random writes + crash + recovery (fuzz-like)
func TestSmartWAL_SustainedRandomWriteCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := SmartWALVolumeConfig{
		ExtentPath: filepath.Join(dir, "extent.dat"),
		WALPath:    filepath.Join(dir, "wal.dat"),
		BlockSize:  4096,
		NumBlocks:  512,
		WALSlots:   256,
		Epoch:      1,
	}

	v, err := CreateSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(42))
	written := make(map[uint32][]byte) // last synced data per LBA
	pending := make(map[uint32][]byte) // unsynced data

	// Phase 1: write + sync 500 blocks
	for i := 0; i < 500; i++ {
		lba := uint32(rng.Intn(512))
		data := make([]byte, 4096)
		rng.Read(data)
		if err := v.WriteLBA(lba, data); err != nil {
			t.Fatalf("WriteLBA: %v", err)
		}
		pending[lba] = data

		if i%100 == 99 {
			if err := v.SyncCache(); err != nil {
				t.Fatalf("SyncCache: %v", err)
			}
			for lba, data := range pending {
				written[lba] = data
			}
			pending = make(map[uint32][]byte)
		}
	}

	// Phase 2: write 200 more WITHOUT syncing
	for i := 0; i < 200; i++ {
		lba := uint32(rng.Intn(512))
		data := make([]byte, 4096)
		rng.Read(data)
		if err := v.WriteLBA(lba, data); err != nil {
			t.Fatalf("WriteLBA: %v", err)
		}
		pending[lba] = data
	}

	// "Crash" — close without final sync
	v.Close()

	// Recovery
	v2, err := OpenSmartWALVolume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// Verify synced data is intact
	for lba, expected := range written {
		data, err := v2.ReadLBA(lba)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", lba, err)
		}
		// Data should be either the synced version or a later unsynced write
		// (if it happened to flush). Either is valid.
		syncedCRC := crc32.ChecksumIEEE(expected)
		actualCRC := crc32.ChecksumIEEE(data)
		if syncedCRC != actualCRC {
			// Check if it's a valid pending write
			if pendingData, ok := pending[lba]; ok {
				pendingCRC := crc32.ChecksumIEEE(pendingData)
				if actualCRC == pendingCRC {
					continue // valid: pending write flushed
				}
			}
			// Data is neither synced nor pending — corruption
			t.Fatalf("LBA %d: data is neither synced nor pending version — corruption", lba)
		}
	}
}

// Test: Trim + recovery
func TestSmartWAL_TrimRecovery(t *testing.T) {
	v := createTestSmartWALVolume(t, 64)

	// Write, sync, trim, sync
	if err := v.WriteLBA(10, makeTestBlock(0xDD)); err != nil {
		t.Fatal(err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}
	if err := v.TrimLBA(10); err != nil {
		t.Fatal(err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Verify trimmed block is zeros
	data, err := v.ReadLBA(10)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, make([]byte, 4096)) {
		t.Fatal("trimmed block should be zeros")
	}
}

// Test: Concurrent writes (no race)
func TestSmartWAL_ConcurrentWrites(t *testing.T) {
	v := createTestSmartWALVolume(t, 256)
	done := make(chan error, 8)

	for g := 0; g < 8; g++ {
		g := g
		go func() {
			for i := 0; i < 50; i++ {
				lba := uint32(g*32 + i%32)
				data := makeTestBlock(byte(g*32 + i))
				if err := v.WriteLBA(lba, data); err != nil {
					done <- fmt.Errorf("goroutine %d write %d: %v", g, i, err)
					return
				}
			}
			done <- nil
		}()
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}
}
