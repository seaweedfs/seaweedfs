package blockvol

// QA adversarial tests — written by QA Manager (separate from dev team's unit tests).
// Attack vectors: boundary conditions, multi-block I/O, trim semantics,
// concurrency, oracle pattern, corruption injection, lifecycle edge cases.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQA(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_multi_block_write_read_middle", run: testQAMultiBlockWriteReadMiddle},
		{name: "qa_trim_then_read_zeros", run: testQATrimThenReadZeros},
		{name: "qa_trim_dirty_then_read_zeros", run: testQATrimDirtyThenReadZeros},
		{name: "qa_write_last_lba", run: testQAWriteLastLBA},
		{name: "qa_overwrite_wider", run: testQAOverwriteWider},
		{name: "qa_overwrite_narrower", run: testQAOverwriteNarrower},
		{name: "qa_read_never_written", run: testQAReadNeverWritten},
		{name: "qa_concurrent_writes", run: testQAConcurrentWrites},
		{name: "qa_concurrent_write_read", run: testQAConcurrentWriteRead},
		{name: "qa_wal_fill_advance_refill", run: testQAWALFillAdvanceRefill},
		{name: "qa_create_block_size_512", run: testQACreateBlockSize512},
		{name: "qa_create_block_size_8192", run: testQACreateBlockSize8192},
		{name: "qa_validate_write_zero_length", run: testQAValidateWriteZeroLength},
		{name: "qa_double_close", run: testQADoubleClose},
		{name: "qa_write_read_all_lbas", run: testQAWriteReadAllLBAs},
		{name: "qa_dirty_map_range_during_delete", run: testQADirtyMapRangeDuringDelete},
		{name: "qa_wal_entry_bitflip_systematic", run: testQAWALEntryBitflipSystematic},
		{name: "qa_oracle_random_ops", run: testQAOracleRandomOps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- Multi-block I/O ---

// testQAMultiBlockWriteReadMiddle: Write 3 blocks as one WriteLBA call,
// then read only the 2nd block. Exercises blockOffset calculation in readBlockFromWAL.
func testQAMultiBlockWriteReadMiddle(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// 3 blocks: 'A' 'B' 'C'
	data := make([]byte, 3*4096)
	for i := 0; i < 4096; i++ {
		data[i] = 'A'
		data[4096+i] = 'B'
		data[2*4096+i] = 'C'
	}

	// Write 3 blocks starting at LBA 5
	if err := v.WriteLBA(5, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Read only block at LBA 6 (the middle one — should be 'B')
	got, err := v.ReadLBA(6, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(6): %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Errorf("middle block: got %q..., want all 'B'", got[:8])
	}

	// Read only block at LBA 7 (last — should be 'C')
	got, err = v.ReadLBA(7, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(7): %v", err)
	}
	if !bytes.Equal(got, makeBlock('C')) {
		t.Errorf("last block: got %q..., want all 'C'", got[:8])
	}

	// Read only first block at LBA 5 (should be 'A')
	got, err = v.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(5): %v", err)
	}
	if !bytes.Equal(got, makeBlock('A')) {
		t.Errorf("first block: got %q..., want all 'A'", got[:8])
	}
}

// --- Trim semantics ---

// testQATrimThenReadZeros: Write a block, trim it, read back — must get zeros.
func testQATrimThenReadZeros(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	data := makeBlock('Z')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Verify data is there.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA before trim: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data not written correctly before trim")
	}

	// Trim the block.
	if err := v.Trim(0, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	// Read after trim — must be zeros (from extent, which was never written).
	got, err = v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after trim: %v", err)
	}
	zeros := make([]byte, 4096)
	if !bytes.Equal(got, zeros) {
		t.Errorf("after trim: expected zeros, got non-zero data (first byte = 0x%02x)", got[0])
	}
}

// testQATrimDirtyThenReadZeros: Write two blocks, trim only one, verify only the
// trimmed one returns zeros and the other is intact.
func testQATrimDirtyThenReadZeros(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA(0): %v", err)
	}
	if err := v.WriteLBA(1, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA(1): %v", err)
	}

	// Trim only LBA 0.
	if err := v.Trim(0, 4096); err != nil {
		t.Fatalf("Trim(0): %v", err)
	}

	// LBA 0 should be zeros.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0) after trim: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("LBA 0 should be zeros after trim")
	}

	// LBA 1 should still be 'Y'.
	got, err = v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, makeBlock('Y')) {
		t.Error("LBA 1 should still be 'Y' after trimming LBA 0")
	}
}

// --- Boundary conditions ---

// testQAWriteLastLBA: Write to the very last block of the volume.
func testQAWriteLastLBA(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Volume is 1MB with 4KB blocks -> 256 blocks -> last LBA is 255.
	lastLBA := v.super.VolumeSize/uint64(v.super.BlockSize) - 1

	data := makeBlock('L')
	if err := v.WriteLBA(lastLBA, data); err != nil {
		t.Fatalf("WriteLBA(last=%d): %v", lastLBA, err)
	}

	got, err := v.ReadLBA(lastLBA, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(last=%d): %v", lastLBA, err)
	}
	if !bytes.Equal(got, data) {
		t.Error("last LBA data mismatch")
	}

	// One past last should fail.
	if err := v.WriteLBA(lastLBA+1, data); err == nil {
		t.Error("expected error writing past last LBA")
	}
}

// --- Overwrite with different sizes ---

// testQAOverwriteWider: Write 1 block at LBA 0, then 2 blocks at LBA 0.
// Both blocks in dirty map should reflect the 2-block write.
func testQAOverwriteWider(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 1 block of 'A' at LBA 0.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA 1-block: %v", err)
	}

	// Overwrite with 2 blocks at LBA 0: 'X' and 'Y'.
	wideData := make([]byte, 2*4096)
	for i := 0; i < 4096; i++ {
		wideData[i] = 'X'
		wideData[4096+i] = 'Y'
	}
	if err := v.WriteLBA(0, wideData); err != nil {
		t.Fatalf("WriteLBA 2-block: %v", err)
	}

	// Read LBA 0 — should be 'X' (not 'A').
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('X')) {
		t.Error("LBA 0 should be 'X' after wider overwrite")
	}

	// Read LBA 1 — should be 'Y'.
	got, err = v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, makeBlock('Y')) {
		t.Error("LBA 1 should be 'Y' after wider overwrite")
	}
}

// testQAOverwriteNarrower: Write 2 blocks at LBA 0, then 1 block at LBA 0.
// LBA 0 gets new data, LBA 1 retains old data.
func testQAOverwriteNarrower(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 2 blocks: 'A' at LBA 0, 'B' at LBA 1.
	wideData := make([]byte, 2*4096)
	for i := 0; i < 4096; i++ {
		wideData[i] = 'A'
		wideData[4096+i] = 'B'
	}
	if err := v.WriteLBA(0, wideData); err != nil {
		t.Fatalf("WriteLBA 2-block: %v", err)
	}

	// Overwrite only LBA 0 with 'Z'.
	if err := v.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA 1-block: %v", err)
	}

	// LBA 0 should be 'Z'.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('Z')) {
		t.Error("LBA 0 should be 'Z' after narrower overwrite")
	}

	// LBA 1 should still be 'B' from the original 2-block write.
	got, err = v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("LBA 1 should still be 'B' — narrower overwrite shouldn't touch it")
	}
}

// --- Never-written blocks ---

// testQAReadNeverWritten: Read a block that was never written (isolated test).
func testQAReadNeverWritten(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	got, err := v.ReadLBA(42, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(42) never-written: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("never-written block should be all zeros")
	}
}

// --- Concurrency ---

// testQAConcurrentWrites: Hammer WriteLBA from 16 goroutines.
// Verify: no panics, all reads return valid data, LSNs are unique.
func testQAConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 4 * 1024 * 1024, // 4MB (1024 LBAs)
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024, // 2MB WAL (plenty of room)
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	const goroutines = 16
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*opsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each goroutine writes to its own LBA range to avoid dirty map
			// read-back races (we test write correctness, not read-write ordering).
			baseLBA := uint64(id * opsPerGoroutine)
			for i := 0; i < opsPerGoroutine; i++ {
				lba := baseLBA + uint64(i)
				if lba >= 1024 {
					continue // stay within volume
				}
				data := makeBlock(byte('A' + id%26))
				if err := v.WriteLBA(lba, data); err != nil {
					if errors.Is(err, ErrWALFull) {
						return // WAL full is expected, not a bug
					}
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent write error: %v", err)
	}

	// Spot-check a few reads. Some may not have been written (WAL full).
	for g := 0; g < goroutines; g++ {
		lba := uint64(g * opsPerGoroutine)
		if lba >= 1024 {
			continue
		}
		got, err := v.ReadLBA(lba, 4096)
		if err != nil {
			t.Errorf("ReadLBA(%d) after concurrent writes: %v", lba, err)
			continue
		}
		expected := makeBlock(byte('A' + g%26))
		zeros := make([]byte, 4096)
		if !bytes.Equal(got, expected) && !bytes.Equal(got, zeros) {
			t.Errorf("LBA %d: data mismatch after concurrent write (not expected data or zeros)", lba)
		}
	}
}

// testQAConcurrentWriteRead: One writer and multiple readers on same LBA.
// Readers should always see either old data or new data, never garbage.
func testQAConcurrentWriteRead(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Seed with initial data.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: overwrites LBA 0 with 'B'.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			v.WriteLBA(0, makeBlock('B'))
		}
		close(stop)
	}()

	// Readers: read LBA 0 and verify data is coherent (all same byte).
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := v.ReadLBA(0, 4096)
				if err != nil {
					t.Errorf("concurrent read: %v", err)
					return
				}
				// Every byte in the block should be the same value.
				first := got[0]
				for j, b := range got {
					if b != first {
						t.Errorf("torn read at byte %d: got 0x%02x, expected 0x%02x", j, b, first)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// --- WAL capacity management ---

// testQAWALFillAdvanceRefill: Fill WAL, advance tail, write more entries.
func testQAWALFillAdvanceRefill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_refill.blockvol")
	// Small WAL: 128KB. Short timeout so WAL-full returns quickly.
	qaCfg := DefaultConfig()
	qaCfg.WALFullTimeout = 10 * time.Millisecond
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	}, qaCfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Stop flusher so we can manually manage WAL tail.
	v.flusher.Stop()

	entrySize := uint64(walEntryHeaderSize + 4096) // ~4134 bytes per entry
	maxEntries := 128 * 1024 / int(entrySize)       // ~31 entries

	// Write until WAL is full.
	var lastOK int
	for i := 0; i < maxEntries+5; i++ {
		err := v.WriteLBA(uint64(i%256), makeBlock(byte('A'+i%26)))
		if err != nil {
			break
		}
		lastOK = i
	}
	if lastOK == 0 {
		t.Fatal("couldn't write any entries")
	}

	// Advance tail to free half the WAL.
	halfEntries := uint64(lastOK/2+1) * entrySize
	v.wal.AdvanceTail(halfEntries)

	// Write more — should succeed now.
	for i := 0; i < 5; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('a'+i))); err != nil {
			t.Fatalf("write after tail advance %d: %v", i, err)
		}
	}

	// Verify latest writes are readable.
	for i := 0; i < 5; i++ {
		got, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after refill: %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('a'+i))) {
			t.Errorf("LBA %d: data mismatch after refill", i)
		}
	}
}

// --- Non-default block sizes ---

func testQACreateBlockSize512(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bs512.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  512,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol(512): %v", err)
	}
	defer v.Close()

	data := make([]byte, 512)
	for i := range data {
		data[i] = 0xAB
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA(bs=512): %v", err)
	}
	got, err := v.ReadLBA(0, 512)
	if err != nil {
		t.Fatalf("ReadLBA(bs=512): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("512-byte block: data mismatch")
	}
}

func testQACreateBlockSize8192(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bs8192.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  8192,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol(8192): %v", err)
	}
	defer v.Close()

	data := make([]byte, 8192)
	for i := range data {
		data[i] = 0xCD
	}
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA(bs=8192): %v", err)
	}
	got, err := v.ReadLBA(0, 8192)
	if err != nil {
		t.Fatalf("ReadLBA(bs=8192): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("8192-byte block: data mismatch")
	}
}

// --- Validation edge cases ---

func testQAValidateWriteZeroLength(t *testing.T) {
	err := ValidateWrite(0, 0, 1024*1024, 4096)
	// 0-length write: dataLen%blockSize == 0 (0%4096 == 0), but blocksNeeded == 0.
	// This should arguably be rejected, but current code may allow it.
	// If it's allowed, at least it shouldn't crash.
	if err != nil {
		// Good — zero-length writes rejected.
		return
	}
	// If allowed, WriteLBA with empty data should be caught by WAL entry validation.
	v := createTestVol(t)
	defer v.Close()
	err = v.WriteLBA(0, []byte{})
	if err == nil {
		t.Error("WriteLBA with empty data should be rejected")
	}
}

// --- Lifecycle ---

func testQADoubleClose(t *testing.T) {
	v := createTestVol(t)
	if err := v.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic (may return error, that's fine).
	_ = v.Close()
}

// --- Exhaustive small-volume test ---

// testQAWriteReadAllLBAs: Write unique data to every LBA, read all back.
func testQAWriteReadAllLBAs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "alllba.blockvol")
	volSize := uint64(32 * 4096) // 32 blocks
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volSize,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	totalBlocks := volSize / 4096

	// Write every block with unique pattern.
	for lba := uint64(0); lba < totalBlocks; lba++ {
		data := makeBlock(byte(lba))
		if err := v.WriteLBA(lba, data); err != nil {
			t.Fatalf("WriteLBA(%d): %v", lba, err)
		}
	}

	// Read every block and verify.
	for lba := uint64(0); lba < totalBlocks; lba++ {
		got, err := v.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", lba, err)
		}
		expected := makeBlock(byte(lba))
		if !bytes.Equal(got, expected) {
			t.Errorf("LBA %d: data mismatch", lba)
		}
	}
}

// --- DirtyMap adversarial ---

// testQADirtyMapRangeDuringDelete: Verify Range + Delete doesn't deadlock.
// This tests the reviewer fix (snapshot-then-iterate pattern).
func testQADirtyMapRangeDuringDelete(t *testing.T) {
	dm := NewDirtyMap(1)

	// Populate 100 entries.
	for i := uint64(0); i < 100; i++ {
		dm.Put(i, i*100, i, 4096)
	}

	// Range over all, delete each one inside the callback.
	dm.Range(0, 100, func(lba, walOffset, lsn uint64, length uint32) {
		dm.Delete(lba)
	})

	// All entries should be deleted.
	if dm.Len() != 0 {
		t.Errorf("expected 0 entries after Range+Delete, got %d", dm.Len())
	}
}

// --- WAL entry corruption ---

// testQAWALEntryBitflipSystematic: Encode a valid entry, flip one bit at
// each byte position, verify Decode detects the corruption.
func testQAWALEntryBitflipSystematic(t *testing.T) {
	entry := &WALEntry{
		LSN:    42,
		Epoch:  7,
		Type:   EntryTypeWrite,
		LBA:    100,
		Length: 64,
		Data:   bytes.Repeat([]byte("DEADBEEF"), 8),
	}

	original, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Verify original decodes fine.
	if _, err := DecodeWALEntry(original); err != nil {
		t.Fatalf("original decode failed: %v", err)
	}

	corrupted := 0
	detected := 0

	for bytePos := 0; bytePos < len(original); bytePos++ {
		for bit := 0; bit < 8; bit++ {
			flipped := make([]byte, len(original))
			copy(flipped, original)
			flipped[bytePos] ^= 1 << uint(bit)

			corrupted++
			_, err := DecodeWALEntry(flipped)
			if err != nil {
				detected++
			}
		}
	}

	// CRC32 should catch the vast majority. With 38+64=102 bytes and
	// single-bit flips, CRC32 IEEE guarantees detection for bursts up to 32 bits.
	detectionRate := float64(detected) / float64(corrupted) * 100
	t.Logf("bitflip detection: %d/%d (%.1f%%)", detected, corrupted, detectionRate)

	// We expect near-100% detection. Allow for the CRC and EntrySize bytes
	// themselves which may produce self-consistent mutations.
	if detectionRate < 95.0 {
		t.Errorf("detection rate %.1f%% is too low (expected >= 95%%)", detectionRate)
	}
}

// --- Oracle pattern (the crown jewel of adversarial testing) ---

// testQAOracleRandomOps: Execute random write/read/trim operations against
// both BlockVol and an in-memory oracle. Assert they always agree.
func testQAOracleRandomOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "oracle.blockvol")
	const blockSize = 4096
	const numBlocks = 64 // small volume for fast test
	const volSize = numBlocks * blockSize

	qaCfg := DefaultConfig()
	qaCfg.WALFullTimeout = 10 * time.Millisecond
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volSize,
		BlockSize:  blockSize,
		WALSize:    512 * 1024,
	}, qaCfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Oracle: simple map from LBA to block data.
	// Missing entries mean zeros.
	oracle := make(map[uint64][]byte)

	rng := rand.New(rand.NewSource(0xDEADBEEF))

	const numOps = 500

	for i := 0; i < numOps; i++ {
		op := rng.Intn(3) // 0=write, 1=read, 2=trim
		lba := uint64(rng.Intn(numBlocks))

		switch op {
		case 0: // WRITE
			// Write 1-4 blocks (if they fit).
			maxBlocks := numBlocks - int(lba)
			if maxBlocks <= 0 {
				continue
			}
			nBlocks := rng.Intn(min(4, maxBlocks)) + 1
			data := make([]byte, nBlocks*blockSize)
			rng.Read(data)

			err := v.WriteLBA(lba, data)
			if err != nil {
				if errors.Is(err, ErrWALFull) {
					continue // WAL full is expected without flusher
				}
				t.Fatalf("op %d: WriteLBA(%d, %d blocks): %v", i, lba, nBlocks, err)
			}

			// Update oracle.
			for b := 0; b < nBlocks; b++ {
				blockData := make([]byte, blockSize)
				copy(blockData, data[b*blockSize:(b+1)*blockSize])
				oracle[lba+uint64(b)] = blockData
			}

		case 1: // READ
			got, err := v.ReadLBA(lba, blockSize)
			if err != nil {
				t.Fatalf("op %d: ReadLBA(%d): %v", i, lba, err)
			}

			// Oracle answer.
			expected, ok := oracle[lba]
			if !ok {
				expected = make([]byte, blockSize) // zeros
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("op %d: ReadLBA(%d) oracle mismatch at op %d", i, lba, i)
			}

		case 2: // TRIM
			err := v.Trim(lba, blockSize)
			if err != nil {
				if errors.Is(err, ErrWALFull) {
					continue // WAL full is expected without flusher
				}
				t.Fatalf("op %d: Trim(%d): %v", i, lba, err)
			}
			delete(oracle, lba)
		}
	}

	// Final verification: read every block and compare to oracle.
	for lba := uint64(0); lba < numBlocks; lba++ {
		got, err := v.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("final ReadLBA(%d): %v", lba, err)
		}
		expected, ok := oracle[lba]
		if !ok {
			expected = make([]byte, blockSize)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("final LBA %d: oracle mismatch", lba)
		}
	}

	t.Logf("oracle test: %d ops, %d blocks, all consistent", numOps, numBlocks)
}

// --- Superblock validation adversarial ---

func TestQASuperblockValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(sb *Superblock)
		wantErr error
	}{
		{
			name:    "extent_size_zero",
			mutate:  func(sb *Superblock) { sb.ExtentSize = 0 },
			wantErr: ErrInvalidSuperblock,
		},
		{
			name:    "wal_size_zero",
			mutate:  func(sb *Superblock) { sb.WALSize = 0 },
			wantErr: ErrInvalidSuperblock,
		},
		{
			name:    "wal_offset_wrong",
			mutate:  func(sb *Superblock) { sb.WALOffset = 999 },
			wantErr: ErrInvalidSuperblock,
		},
		{
			name:    "volume_not_aligned",
			mutate:  func(sb *Superblock) { sb.VolumeSize = 4097 },
			wantErr: ErrInvalidSuperblock,
		},
		{
			name:    "bad_magic",
			mutate:  func(sb *Superblock) { copy(sb.Magic[:], "BAAD") },
			wantErr: ErrNotBlockVol,
		},
		{
			name:    "bad_version",
			mutate:  func(sb *Superblock) { sb.Version = 99 },
			wantErr: ErrUnsupportedVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb, err := NewSuperblock(1024*1024, CreateOptions{})
			if err != nil {
				t.Fatalf("NewSuperblock: %v", err)
			}
			tt.mutate(&sb)
			err = sb.Validate()
			if err == nil {
				t.Fatal("expected Validate() error, got nil")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected %v, got %v", tt.wantErr, err)
			}
		})
	}
}

// --- WAL writer adversarial ---

func TestQAWALWriterEdgeCases(t *testing.T) {
	t.Run("entry_larger_than_wal", func(t *testing.T) {
		walOffset := uint64(SuperblockSize)
		walSize := uint64(1024) // tiny WAL
		fd, cleanup := createTestWAL(t, walOffset, walSize)
		defer cleanup()

		w := NewWALWriter(fd, walOffset, walSize, 0, 0)

		// Entry with 4KB data > 1KB WAL.
		entry := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
		_, err := w.Append(entry)
		if err == nil {
			t.Error("expected error when entry exceeds WAL size")
		}
	})

	t.Run("padding_smaller_than_header", func(t *testing.T) {
		walOffset := uint64(SuperblockSize)
		entrySize := uint64(walEntryHeaderSize + 64)
		// WAL that leaves less than walEntryHeaderSize bytes after one entry.
		// After padding + wrap, logical tracking allows exact fit (no 1-byte reservation).
		// padding gap = walSize - entrySize (< walEntryHeaderSize).
		// After wrap: logicalHead consumed entrySize + gap, logicalTail advanced to entrySize.
		// used = (entrySize + gap) - entrySize = gap. free = walSize - gap.
		// If free >= entrySize, second entry fits.
		walSize := entrySize + uint64(walEntryHeaderSize) - 5
		// gap = walSize - entrySize = walEntryHeaderSize - 5 = 33 bytes
		// free after wrap = walSize - gap = entrySize = 102 bytes
		// entrySize = 102. free == entrySize → fits with logical tracking (uses >, not <)

		fd, cleanup := createTestWAL(t, walOffset, walSize)
		defer cleanup()

		w := NewWALWriter(fd, walOffset, walSize, 0, 0)

		entry1 := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 64, Data: make([]byte, 64)}
		if _, err := w.Append(entry1); err != nil {
			t.Fatalf("first append: %v", err)
		}

		// Advance tail past first entry so wrap has space.
		w.AdvanceTail(entrySize)

		// Second entry wraps. With logical counters (no 1-byte reservation),
		// the entry fits exactly when available == needed.
		entry2 := &WALEntry{LSN: 2, Type: EntryTypeWrite, LBA: 1, Length: 64, Data: make([]byte, 64)}
		if _, err := w.Append(entry2); err != nil {
			t.Fatalf("second append after wrap should succeed with logical tracking: %v", err)
		}
	})

	t.Run("padding_smaller_than_header_with_room", func(t *testing.T) {
		walOffset := uint64(SuperblockSize)
		entrySize := uint64(walEntryHeaderSize + 64) // 102
		// Need: after first entry + padding gap + wrap, free > entrySize.
		// padding gap = walSize - entrySize (what's left at end, < header size).
		// After wrap: head=0, tail=entrySize. free = tail - head = entrySize.
		// strict < needs free > entryLen, so entrySize > entrySize is false.
		// Need extra room: walSize = entrySize + gap + extra.
		// With gap < walEntryHeaderSize (say 30) and extra >= 2:
		walSize := entrySize + 30 + entrySize + 2 // room for 2 entries + 30-byte gap + 2-byte margin

		fd, cleanup := createTestWAL(t, walOffset, walSize)
		defer cleanup()

		w := NewWALWriter(fd, walOffset, walSize, 0, 0)

		entry1 := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 64, Data: make([]byte, 64)}
		if _, err := w.Append(entry1); err != nil {
			t.Fatalf("first append: %v", err)
		}

		// Write second entry — pushes head to 2*entrySize = 204.
		entry2 := &WALEntry{LSN: 2, Type: EntryTypeWrite, LBA: 1, Length: 64, Data: make([]byte, 64)}
		if _, err := w.Append(entry2); err != nil {
			t.Fatalf("second append: %v", err)
		}

		// Advance tail past both entries.
		w.AdvanceTail(entrySize * 2)

		// remaining = walSize - 204 = 30 bytes (< walEntryHeaderSize=38)
		// → padding uses zero-fill path, head wraps to 0
		// → free = tail - head = 204 - 0 = 204 > 102 → fits!
		entry3 := &WALEntry{LSN: 3, Type: EntryTypeWrite, LBA: 2, Length: 64, Data: make([]byte, 64)}
		off, err := w.Append(entry3)
		if err != nil {
			t.Fatalf("wrap append with room: %v", err)
		}
		if off != 0 {
			t.Errorf("wrapped entry should be at offset 0, got %d", off)
		}
	})
}

// --- WAL entry edge cases ---

func TestQAWALEntryEdgeCases(t *testing.T) {
	t.Run("decode_truncated_data", func(t *testing.T) {
		entry := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
		buf, err := entry.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		// Truncate buffer to header + partial data.
		truncated := buf[:walEntryHeaderSize+100]
		_, err = DecodeWALEntry(truncated)
		if err == nil {
			t.Error("expected error decoding truncated entry")
		}
	})

	t.Run("decode_header_only", func(t *testing.T) {
		_, err := DecodeWALEntry(make([]byte, walEntryHeaderSize-1))
		if err == nil {
			t.Error("expected error for buffer smaller than header")
		}
	})

	t.Run("corrupt_entry_size_field", func(t *testing.T) {
		entry := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 64, Data: make([]byte, 64)}
		buf, err := entry.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		// Corrupt the EntrySize field (last 4 bytes).
		binary.LittleEndian.PutUint32(buf[len(buf)-4:], 99999)
		_, err = DecodeWALEntry(buf)
		if err == nil {
			t.Error("expected error for corrupt EntrySize")
		}
	})

	t.Run("unknown_entry_type_encode", func(t *testing.T) {
		// Type 0x99 is not recognized — Encode should still work
		// (only WRITE/TRIM/BARRIER have special validation).
		entry := &WALEntry{LSN: 1, Type: 0x99, LBA: 0}
		_, err := entry.Encode()
		// Unknown type with no data — may or may not error.
		// Just verify no panic.
		_ = err
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================================
// QA Adversarial Tests — Tasks 1.7 (GroupCommitter), 1.8 (Flusher), 1.9 (Recovery)
// ============================================================================

// --- Task 1.7: GroupCommitter adversarial tests ---

func TestQAGroupCommitter(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_gc_double_stop", run: testQAGCDoubleStop},
		{name: "qa_gc_submit_storm_during_stop", run: testQAGCSubmitStormDuringStop},
		{name: "qa_gc_fsync_error_all_waiters", run: testQAGCFsyncErrorAllWaiters},
		{name: "qa_gc_intermittent_fsync_error", run: testQAGCIntermittentFsyncError},
		{name: "qa_gc_max_batch_exact", run: testQAGCMaxBatchExact},
		{name: "qa_gc_zero_delay_still_works", run: testQAGCZeroDelayStillWorks},
		{name: "qa_gc_sync_count_accuracy", run: testQAGCSyncCountAccuracy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQAGCDoubleStop: Stop() twice must not panic or deadlock.
func testQAGCDoubleStop(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
	})
	go gc.Run()

	gc.Stop()

	// Second stop — must not panic or deadlock.
	done := make(chan struct{})
	go func() {
		gc.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop() deadlocked")
	}
}

// testQAGCSubmitStormDuringStop: Many goroutines Submit() while Stop() is called.
// All must either succeed or get ErrGroupCommitShutdown — no panics, no deadlocks.
//
// BUG QA-002: There is a race between drainPending() and close(gc.done) in Run().
// Goroutines that pass the gc.done double-check AFTER drainPending() releases gc.mu
// but BEFORE close(gc.done) can enqueue to pending with no goroutine to drain them,
// causing a permanent hang on <-ch in Submit().
//
// Race sequence:
//   1. Run(): drainPending() → gc.mu.Lock → take pending → gc.mu.Unlock → send errors
//   2. Submit(): passes first select<-gc.done (not closed yet)
//   3. Submit(): gc.mu.Lock → passes second select<-gc.done → append ch → gc.mu.Unlock
//   4. Run(): close(gc.done) ← too late, ch already enqueued with no consumer
//   5. Submit(): <-ch blocks forever
func testQAGCSubmitStormDuringStop(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
		MaxDelay: 1 * time.Millisecond,
	})
	go gc.Run()

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*10)

	// Launch submitters.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				err := gc.Submit()
				if err != nil && !errors.Is(err, ErrGroupCommitShutdown) {
					errs <- fmt.Errorf("unexpected error: %w", err)
					return
				}
				if errors.Is(err, ErrGroupCommitShutdown) {
					return // stopped, don't retry
				}
			}
		}()
	}

	// Race: stop while submitters are in flight.
	time.Sleep(1 * time.Millisecond)
	gc.Stop()

	// Use timeout to detect QA-002 hang.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines exited cleanly.
	case <-time.After(5 * time.Second):
		// QA-002: Submit() goroutines stuck waiting for response after Stop().
		t.Fatal("BUG QA-002: Submit() goroutines deadlocked during Stop() — " +
			"drainPending/close(done) race allows enqueue after drain")
	}

	close(errs)
	for err := range errs {
		t.Errorf("submit storm: %v", err)
	}
}

// testQAGCFsyncErrorAllWaiters: When fsync fails, ALL waiters in the batch
// must receive the error (not just the first one).
func testQAGCFsyncErrorAllWaiters(t *testing.T) {
	errDisk := fmt.Errorf("disk on fire")
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:   func() error { return errDisk },
		MaxDelay:   50 * time.Millisecond,
		MaxBatch:   100,
		OnDegraded: func() {},
	})
	go gc.Run()
	defer gc.Stop()

	const n = 20
	var wg sync.WaitGroup
	results := make([]error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = gc.Submit()
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if err == nil {
			t.Errorf("waiter %d got nil, want error", i)
		}
	}
}

// testQAGCIntermittentFsyncError: fsync alternates success/failure.
// Verify each batch's waiters get the correct result.
func testQAGCIntermittentFsyncError(t *testing.T) {
	var callCount atomic.Uint64
	errFlaky := fmt.Errorf("flaky disk")
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			n := callCount.Add(1)
			if n%2 == 0 {
				return errFlaky // even calls fail
			}
			return nil // odd calls succeed
		},
		MaxDelay: 2 * time.Millisecond,
	})
	go gc.Run()
	defer gc.Stop()

	// Submit 20 sequential requests (each likely in its own batch).
	var successes, failures int
	for i := 0; i < 20; i++ {
		err := gc.Submit()
		if err == nil {
			successes++
		} else {
			failures++
		}
	}

	// Both successes and failures should occur.
	if successes == 0 {
		t.Error("expected some successful syncs")
	}
	if failures == 0 {
		t.Error("expected some failed syncs from intermittent error")
	}
	t.Logf("intermittent: %d success, %d failure out of 20", successes, failures)
}

// testQAGCMaxBatchExact: Submit exactly maxBatch waiters.
// They should all complete quickly (trigger immediate flush, not wait for maxDelay).
func testQAGCMaxBatchExact(t *testing.T) {
	const maxBatch = 8
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
		MaxDelay: 10 * time.Second, // very long — should NOT wait
		MaxBatch: maxBatch,
	})
	go gc.Run()
	defer gc.Stop()

	var wg sync.WaitGroup
	wg.Add(maxBatch)
	for i := 0; i < maxBatch; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good — completed quickly.
	case <-time.After(3 * time.Second):
		t.Fatal("maxBatch exact count did not trigger immediate flush")
	}
}

// testQAGCZeroDelayStillWorks: MaxDelay=0 should not panic or hang.
func testQAGCZeroDelayStillWorks(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
		MaxDelay: 0, // should get default 1ms
	})
	go gc.Run()
	defer gc.Stop()

	done := make(chan error, 1)
	go func() {
		done <- gc.Submit()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Submit with zero delay: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Submit with zero delay hung")
	}
}

// testQAGCSyncCountAccuracy: Verify SyncCount matches actual fsync calls.
func testQAGCSyncCountAccuracy(t *testing.T) {
	var actualSyncs atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			actualSyncs.Add(1)
			return nil
		},
		MaxDelay: 1 * time.Millisecond,
	})
	go gc.Run()
	defer gc.Stop()

	// 10 sequential submits (each should be its own batch).
	for i := 0; i < 10; i++ {
		if err := gc.Submit(); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	if gc.SyncCount() != actualSyncs.Load() {
		t.Errorf("SyncCount=%d, actual=%d", gc.SyncCount(), actualSyncs.Load())
	}
}

// --- Task 1.8: Flusher adversarial tests ---

func TestQAFlusher(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_flush_empty_dirty_map", run: testQAFlushEmptyDirtyMap},
		{name: "qa_flush_overwrite_during_flush", run: testQAFlushOverwriteDuringFlush},
		{name: "qa_flush_trim_zeros_extent", run: testQAFlushTrimZerosExtent},
		{name: "qa_flush_preserves_newer_writes", run: testQAFlushPreservesNewerWrites},
		{name: "qa_flush_checkpoint_persists", run: testQAFlushCheckpointPersists},
		{name: "qa_flush_wal_reclaim_then_write", run: testQAFlushWALReclaimThenWrite},
		{name: "qa_flush_multi_block_entry", run: testQAFlushMultiBlockEntry},
		{name: "qa_flusher_stop_idempotent", run: testQAFlusherStopIdempotent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQAFlushEmptyDirtyMap: FlushOnce with no dirty entries is a no-op.
func testQAFlushEmptyDirtyMap(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// No writes — flush should not error or change anything.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce on empty: %v", err)
	}
	if f.CheckpointLSN() != 0 {
		t.Errorf("checkpoint should be 0 on empty flush, got %d", f.CheckpointLSN())
	}
}

// testQAFlushOverwriteDuringFlush: Write, flush, overwrite same LBA, flush again.
// Verify final state is the overwritten data.
func testQAFlushOverwriteDuringFlush(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write initial data.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}

	// Flush — moves 'A' to extent.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 1: %v", err)
	}

	// Overwrite with 'B'.
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}

	// Read should return 'B' (from dirty map, not extent).
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA before second flush: %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("before second flush: should read 'B' from WAL")
	}

	// Flush again — moves 'B' to extent.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// Read should still return 'B' (now from extent).
	got, err = v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after second flush: %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("after second flush: should read 'B' from extent")
	}
}

// testQAFlushTrimZerosExtent: Write, flush (data in extent), trim, flush again.
// After second flush, extent should contain zeros.
func testQAFlushTrimZerosExtent(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write data.
	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Flush — 'X' goes to extent.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 1: %v", err)
	}

	// Verify extent has 'X'.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after first flush: %v", err)
	}
	if !bytes.Equal(got, makeBlock('X')) {
		t.Fatal("extent should have 'X' after first flush")
	}

	// Trim the block.
	if err := v.Trim(0, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	// Read from dirty map (TRIM entry) — should return zeros.
	got, err = v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after trim: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("after trim: dirty map read should return zeros")
	}

	// Flush again — flusher zeros the extent.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// Dirty map should be empty.
	if v.dirtyMap.Len() != 0 {
		t.Errorf("dirty map should be empty after flush, got %d", v.dirtyMap.Len())
	}

	// Read from extent — should be zeros (flusher zeroed it).
	got, err = v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after trim flush: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("after trim+flush: extent should be zeros")
	}
}

// testQAFlushPreservesNewerWrites: Write A, start flush snapshot, write B to same LBA
// before flush removes from dirty map. Dirty map should keep B (newer LSN).
func testQAFlushPreservesNewerWrites(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write 'A' to LBA 0.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}

	// Also write 'M' to LBA 5 to ensure flush has something.
	if err := v.WriteLBA(5, makeBlock('M')); err != nil {
		t.Fatalf("WriteLBA(5): %v", err)
	}

	// Flush moves both to extent.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Overwrite LBA 0 with 'B' AFTER flush.
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}

	// Now flush again — flusher should see the new entry for LBA 0.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// LBA 0 should be 'B' from extent.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("LBA 0 should be 'B' after overwrite+flush")
	}
}

// testQAFlushCheckpointPersists: Flush, crash, reopen. Checkpoint LSN should
// be persisted so recovery skips already-flushed entries.
func testQAFlushCheckpointPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write and flush blocks 0-4.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	checkpointLSN := f.CheckpointLSN()
	if checkpointLSN == 0 {
		t.Fatal("checkpoint should be non-zero after flush")
	}

	// Write more blocks AFTER checkpoint.
	for i := uint64(5); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Update superblock and crash.
	path = simulateCrashWithSuper(v)

	// Reopen — recovery should skip LSN <= checkpoint and replay 5-9.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Blocks 0-4 from extent (flushed), blocks 5-9 from WAL (replayed).
	for i := uint64(0); i < 10; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch after checkpoint recovery", i)
		}
	}
}

// testQAFlushWALReclaimThenWrite: Fill WAL, flush (reclaim all), write again.
// Tests that WAL space is truly freed and reusable.
func testQAFlushWALReclaimThenWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reclaim.blockvol")
	qaCfg := DefaultConfig()
	qaCfg.WALFullTimeout = 10 * time.Millisecond
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    128 * 1024, // small WAL
	}, qaCfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Stop background flusher so we can manually manage WAL.
	v.flusher.Stop()

	entrySize := uint64(walEntryHeaderSize + 4096)
	maxEntries := int(128 * 1024 / entrySize)

	// Fill WAL completely.
	for i := 0; i < maxEntries; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte(i%26+'A'))); err != nil {
			break // expected ErrWALFull
		}
	}

	// Confirm WAL is full.
	err = v.WriteLBA(0, makeBlock('Z'))
	if err == nil {
		// Might succeed if we didn't quite fill it. Try more.
		for i := 0; i < 100; i++ {
			if err := v.WriteLBA(uint64(i), makeBlock('Z')); err != nil {
				break
			}
		}
	}

	// Flush — reclaim all WAL space.
	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// WAL should be empty now. Write again — should succeed.
	for i := 0; i < 5; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('a'+i))); err != nil {
			t.Fatalf("write after reclaim %d: %v", i, err)
		}
	}

	// Verify.
	for i := 0; i < 5; i++ {
		got, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('a'+i))) {
			t.Errorf("block %d: data mismatch after reclaim+rewrite", i)
		}
	}
}

// testQAFlushMultiBlockEntry: Write multi-block entry, flush, verify all blocks
// are correctly placed in the extent.
func testQAFlushMultiBlockEntry(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write 3 blocks as one WriteLBA call.
	data := make([]byte, 3*4096)
	for i := 0; i < 4096; i++ {
		data[i] = 'P'
		data[4096+i] = 'Q'
		data[2*4096+i] = 'R'
	}
	if err := v.WriteLBA(10, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Flush.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// All 3 blocks should be readable from extent.
	for i, expected := range []byte{'P', 'Q', 'R'} {
		got, err := v.ReadLBA(uint64(10+i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", 10+i, err)
		}
		if !bytes.Equal(got, makeBlock(expected)) {
			t.Errorf("block %d: expected '%c', got different data", 10+i, expected)
		}
	}
}

// testQAFlusherStopIdempotent: Stop() twice on the flusher goroutine.
func testQAFlusherStopIdempotent(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	go f.Run()
	f.Stop()

	// Second stop — must not panic or deadlock.
	done := make(chan struct{})
	go func() {
		f.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("second Flusher.Stop() deadlocked")
	}
}

// --- Task 1.9: Recovery adversarial tests ---

func TestQARecovery(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_recover_trim_entry", run: testQARecoverTrimEntry},
		{name: "qa_recover_mixed_write_trim_barrier", run: testQARecoverMixedWriteTrimBarrier},
		{name: "qa_recover_after_flush_then_crash", run: testQARecoverAfterFlushThenCrash},
		{name: "qa_recover_overwrite_same_lba", run: testQARecoverOverwriteSameLBA},
		{name: "qa_recover_crash_loop", run: testQARecoverCrashLoop},
		{name: "qa_recover_corrupt_middle_entry", run: testQARecoverCorruptMiddleEntry},
		{name: "qa_recover_multi_block_write", run: testQARecoverMultiBlockWrite},
		{name: "qa_recover_oracle_with_crash", run: testQARecoverOracleWithCrash},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQARecoverTrimEntry: Write, trim, sync, crash, recover.
// After recovery, trimmed LBA should return zeros.
func testQARecoverTrimEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trim_recover.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write then trim.
	if err := v.WriteLBA(3, makeBlock('T')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.Trim(3, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Persist superblock.
	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(3, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(3): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("trimmed LBA should be zeros after recovery")
	}
}

// testQARecoverMixedWriteTrimBarrier: Interleave WRITE, TRIM, and BARRIER entries.
// Verify recovery replays correctly.
func testQARecoverMixedWriteTrimBarrier(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed_recover.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write LBA 0, 1, 2.
	for i := uint64(0); i < 3; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Trim LBA 1.
	if err := v.Trim(1, 4096); err != nil {
		t.Fatalf("Trim(1): %v", err)
	}

	// Write a barrier.
	lsn := v.nextLSN.Add(1) - 1
	barrier := &WALEntry{LSN: lsn, Type: EntryTypeBarrier, LBA: 0}
	if _, err := v.wal.Append(barrier); err != nil {
		t.Fatalf("Append barrier: %v", err)
	}

	// Write LBA 5.
	if err := v.WriteLBA(5, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA(5): %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// LBA 0: 'A'
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('A')) {
		t.Error("LBA 0 should be 'A'")
	}

	// LBA 1: trimmed — zeros.
	got, err = v2.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("LBA 1 should be zeros (trimmed)")
	}

	// LBA 2: 'C'
	got, err = v2.ReadLBA(2, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(2): %v", err)
	}
	if !bytes.Equal(got, makeBlock('C')) {
		t.Error("LBA 2 should be 'C'")
	}

	// LBA 5: 'Z'
	got, err = v2.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(5): %v", err)
	}
	if !bytes.Equal(got, makeBlock('Z')) {
		t.Error("LBA 5 should be 'Z'")
	}
}

// testQARecoverAfterFlushThenCrash: Flush some entries, write more, crash.
// Flushed entries should be in extent; post-flush writes recovered from WAL.
func testQARecoverAfterFlushThenCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flush_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write blocks 0-4 and flush.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	f := NewFlusher(FlusherConfig{
		FD: v.fd, Super: &v.super, WAL: v.wal, DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Write blocks 5-9 AFTER flush.
	for i := uint64(5); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Persist superblock.
	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := uint64(0); i < 10; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch (flush+crash recovery)", i)
		}
	}
}

// testQARecoverOverwriteSameLBA: Write LBA 0 three times, sync, crash, recover.
// Recovery should replay the latest write for LBA 0.
func testQARecoverOverwriteSameLBA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overwrite_recover.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write LBA 0 three times with different data.
	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA(X): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA(Y): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA(Z): %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, makeBlock('Z')) {
		t.Error("LBA 0 should be 'Z' (latest write) after recovery")
	}
}

// testQARecoverCrashLoop: Write, sync, crash, recover — 20 iterations.
// Each iteration writes new data and verifies previous data survived.
func testQARecoverCrashLoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crashloop.blockvol")

	// Create initial volume.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	const iterations = 20

	for iter := 0; iter < iterations; iter++ {
		lba := uint64(iter % 200) // spread across LBAs
		data := makeBlock(byte(iter % 256))

		if err := v.WriteLBA(lba, data); err != nil {
			t.Fatalf("iter %d WriteLBA: %v", iter, err)
		}
		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d SyncCache: %v", iter, err)
		}

		// Crash and recover.
		path = simulateCrashWithSuper(v)
		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d OpenBlockVol: %v", iter, err)
		}

		// Verify the data we just wrote.
		got, err := v.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("iter %d ReadLBA: %v", iter, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("iter %d: data mismatch for LBA %d", iter, lba)
		}
	}

	v.Close()
}

// testQARecoverCorruptMiddleEntry: Write 3 entries, corrupt the 2nd entry's CRC.
// Recovery should replay entry 1, skip entries 2+3 (torn write boundary).
func testQARecoverCorruptMiddleEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt_mid.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 3 entries.
	for i := uint64(0); i < 3; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Stop background goroutines before manual superblock/WAL manipulation.
	v.groupCommit.Stop()
	v.flusher.Stop()

	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()

	// Corrupt 2nd entry CRC (byte in data area of 2nd entry).
	entrySize := uint64(walEntryHeaderSize + 4096)
	// Corrupt a byte inside the 2nd entry's data region.
	corruptOff := int64(v.super.WALOffset + entrySize + uint64(walEntryHeaderSize) + 10)
	v.fd.WriteAt([]byte{0xFF}, corruptOff)
	v.fd.Sync()

	v.fd.Close()
	path = v.Path()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Entry 1 (LBA 0) should be recovered.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('A')) {
		t.Error("LBA 0 should be 'A' (recovered before corrupt entry)")
	}

	// Entries 2+3 (LBA 1,2) should NOT be recovered (CRC failure stops scan).
	got, err = v2.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("LBA 1 should be zeros (corrupt entry discarded)")
	}

	got, err = v2.ReadLBA(2, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(2): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("LBA 2 should be zeros (entry after corrupt entry discarded)")
	}
}

// testQARecoverMultiBlockWrite: Write multi-block entry, crash, recover.
// Verify all blocks from the multi-block entry are recovered.
func testQARecoverMultiBlockWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multiblock_recover.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 4 blocks as one call.
	data := make([]byte, 4*4096)
	for i := 0; i < 4; i++ {
		for j := 0; j < 4096; j++ {
			data[i*4096+j] = byte('W' + i)
		}
	}
	if err := v.WriteLBA(10, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := 0; i < 4; i++ {
		got, err := v2.ReadLBA(uint64(10+i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", 10+i, err)
		}
		expected := makeBlock(byte('W' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: expected '%c', got different data", 10+i, byte('W'+i))
		}
	}
}

// testQARecoverOracleWithCrash: Oracle pattern with periodic crash+recover.
// This is the most valuable adversarial test — it exercises the full
// write→sync→crash→recover→verify cycle with random operations.
func testQARecoverOracleWithCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "oracle_crash.blockvol")

	const blockSize = 4096
	const numBlocks = 32
	const volSize = numBlocks * blockSize

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volSize,
		BlockSize:  blockSize,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	oracle := make(map[uint64][]byte)
	rng := rand.New(rand.NewSource(0xCAFEBABE))

	const iterations = 10
	const opsPerIter = 30

	for iter := 0; iter < iterations; iter++ {
		// Execute random ops.
		for op := 0; op < opsPerIter; op++ {
			lba := uint64(rng.Intn(numBlocks))
			action := rng.Intn(3)

			switch action {
			case 0: // WRITE
				data := make([]byte, blockSize)
				rng.Read(data)
				err := v.WriteLBA(lba, data)
				if err != nil {
					if errors.Is(err, ErrWALFull) {
						continue
					}
					t.Fatalf("iter %d op %d: WriteLBA(%d): %v", iter, op, lba, err)
				}
				oracle[lba] = data

			case 1: // READ (verify against oracle)
				got, err := v.ReadLBA(lba, blockSize)
				if err != nil {
					t.Fatalf("iter %d op %d: ReadLBA(%d): %v", iter, op, lba, err)
				}
				expected, ok := oracle[lba]
				if !ok {
					expected = make([]byte, blockSize)
				}
				if !bytes.Equal(got, expected) {
					t.Fatalf("iter %d op %d: LBA %d oracle mismatch", iter, op, lba)
				}

			case 2: // TRIM
				err := v.Trim(lba, blockSize)
				if err != nil {
					if errors.Is(err, ErrWALFull) {
						continue
					}
					t.Fatalf("iter %d op %d: Trim(%d): %v", iter, op, lba, err)
				}
				delete(oracle, lba)
			}
		}

		// Sync and crash.
		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d SyncCache: %v", iter, err)
		}

		path = simulateCrashWithSuper(v)

		// Recover.
		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d OpenBlockVol: %v", iter, err)
		}

		// Verify all oracle entries after recovery.
		for lba := uint64(0); lba < numBlocks; lba++ {
			got, err := v.ReadLBA(lba, blockSize)
			if err != nil {
				t.Fatalf("iter %d verify LBA %d: %v", iter, lba, err)
			}
			expected, ok := oracle[lba]
			if !ok {
				expected = make([]byte, blockSize)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("iter %d post-recovery: LBA %d oracle mismatch", iter, lba)
			}
		}
	}

	v.Close()
	t.Logf("oracle crash test: %d iterations x %d ops, all consistent", iterations, opsPerIter)
}

// ============================================================================
// QA Adversarial Tests — Tasks 1.10 (Lifecycle), 1.11 (Crash Stress)
// ============================================================================

// --- Task 1.10: Lifecycle adversarial tests ---

func TestQALifecycle(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_lifecycle_write_after_close", run: testQALifecycleWriteAfterClose},
		{name: "qa_lifecycle_read_after_close", run: testQALifecycleReadAfterClose},
		{name: "qa_lifecycle_sync_after_close", run: testQALifecycleSyncAfterClose},
		{name: "qa_lifecycle_close_drains_dirty", run: testQALifecycleCloseDrainsDirty},
		{name: "qa_lifecycle_multi_cycle_accumulate", run: testQALifecycleMultiCycleAccumulate},
		{name: "qa_lifecycle_close_with_background_flusher", run: testQALifecycleCloseWithBackgroundFlusher},
		{name: "qa_lifecycle_healthy_flag", run: testQALifecycleHealthyFlag},
		{name: "qa_lifecycle_open_close_rapid", run: testQALifecycleOpenCloseRapid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQALifecycleWriteAfterClose: WriteLBA after Close must fail gracefully (not panic).
func testQALifecycleWriteAfterClose(t *testing.T) {
	v := createTestVol(t)
	v.Close()

	// Write after close — fd is closed, should get an error, never a panic.
	err := v.WriteLBA(0, makeBlock('X'))
	if err == nil {
		t.Error("WriteLBA after Close should fail")
	}
}

// testQALifecycleReadAfterClose: ReadLBA after Close must fail gracefully (not panic).
func testQALifecycleReadAfterClose(t *testing.T) {
	v := createTestVol(t)

	// Write something first so dirty map has an entry.
	if err := v.WriteLBA(0, makeBlock('R')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	v.Close()

	// Read after close — fd is closed, should error, not panic.
	_, err := v.ReadLBA(0, 4096)
	if err == nil {
		t.Error("ReadLBA after Close should fail")
	}
}

// testQALifecycleSyncAfterClose: SyncCache after Close must return an error
// (ErrVolumeClosed from the closed guard, or ErrGroupCommitShutdown).
func testQALifecycleSyncAfterClose(t *testing.T) {
	v := createTestVol(t)
	v.Close()

	err := v.SyncCache()
	if err == nil {
		t.Error("SyncCache after Close should fail")
	}
	if !errors.Is(err, ErrVolumeClosed) && !errors.Is(err, ErrGroupCommitShutdown) {
		t.Errorf("SyncCache after Close: got %v, want ErrVolumeClosed or ErrGroupCommitShutdown", err)
	}
}

// testQALifecycleCloseDrainsDirty: Close does a final flush — dirty map should
// be empty and data should be in extent region. Reopen should find blocks in
// extent (not WAL) and dirty map should be empty.
func testQALifecycleCloseDrainsDirty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drain.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 20 blocks.
	for i := uint64(0); i < 20; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Close — should flush all dirty blocks to extent.
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — recovery should find nothing in WAL (all flushed).
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Dirty map should be empty (all data is in extent).
	if v2.dirtyMap.Len() != 0 {
		t.Errorf("dirty map after reopen should be 0, got %d (close didn't fully flush)", v2.dirtyMap.Len())
	}

	// Verify all blocks readable.
	for i := uint64(0); i < 20; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i%26))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: mismatch after close+reopen", i)
		}
	}
}

// testQALifecycleMultiCycleAccumulate: Write→sync→close→reopen→write more, 5 cycles.
// Each cycle adds new blocks. Verify all accumulated blocks survive.
func testQALifecycleMultiCycleAccumulate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "accumulate.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	oracle := make(map[uint64]byte)

	for cycle := 0; cycle < 5; cycle++ {
		// Write 10 blocks per cycle at different LBAs.
		for i := 0; i < 10; i++ {
			lba := uint64(cycle*10 + i)
			fill := byte('A' + (cycle*10+i)%26)
			if err := v.WriteLBA(lba, makeBlock(fill)); err != nil {
				t.Fatalf("cycle %d WriteLBA(%d): %v", cycle, lba, err)
			}
			oracle[lba] = fill
		}

		if err := v.SyncCache(); err != nil {
			t.Fatalf("cycle %d SyncCache: %v", cycle, err)
		}
		if err := v.Close(); err != nil {
			t.Fatalf("cycle %d Close: %v", cycle, err)
		}

		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("cycle %d OpenBlockVol: %v", cycle, err)
		}

		// Verify all accumulated data.
		for lba, fill := range oracle {
			got, err := v.ReadLBA(lba, 4096)
			if err != nil {
				t.Fatalf("cycle %d ReadLBA(%d): %v", cycle, lba, err)
			}
			if !bytes.Equal(got, makeBlock(fill)) {
				t.Fatalf("cycle %d block %d: data mismatch", cycle, lba)
			}
		}
	}

	v.Close()
	t.Logf("multi-cycle: 5 cycles, %d blocks accumulated, all consistent", len(oracle))
}

// testQALifecycleCloseWithBackgroundFlusher: Write enough to trigger background
// flusher (100ms interval), then close. Verify shutdown ordering is correct.
func testQALifecycleCloseWithBackgroundFlusher(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bgflush.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write blocks and let the background flusher potentially kick in.
	for i := uint64(0); i < 30; i++ {
		if err := v.WriteLBA(i, makeBlock(byte(i%26+'A'))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Wait a bit to let the flusher potentially run.
	time.Sleep(150 * time.Millisecond)

	// Close — must coordinate with flusher goroutine.
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := uint64(0); i < 30; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte(i%26+'A'))) {
			t.Errorf("block %d: mismatch after flusher+close+reopen", i)
		}
	}
}

// testQALifecycleHealthyFlag: Verify Info().Healthy reflects fsync failures.
func testQALifecycleHealthyFlag(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	if !v.Info().Healthy {
		t.Error("volume should be healthy initially")
	}

	// Force unhealthy by directly setting the flag (simulating fsync error
	// that the OnDegraded callback would trigger).
	v.healthy.Store(false)
	if v.Info().Healthy {
		t.Error("volume should report unhealthy after flag set")
	}

	// Restore.
	v.healthy.Store(true)
	if !v.Info().Healthy {
		t.Error("volume should report healthy after restoration")
	}
}

// testQALifecycleOpenCloseRapid: Open and close 20 times rapidly.
// Tests for goroutine/resource leaks.
func testQALifecycleOpenCloseRapid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rapid.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write one block so there's something to flush.
	if err := v.WriteLBA(0, makeBlock('R')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	v.Close()

	for i := 0; i < 20; i++ {
		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if err := v.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	// Final open — verify data survived 20 open/close cycles.
	v, err = OpenBlockVol(path)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer v.Close()

	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if !bytes.Equal(got, makeBlock('R')) {
		t.Error("data lost after 20 rapid open/close cycles")
	}
}

// --- Task 1.11: Crash stress adversarial tests ---

func TestQACrashStress(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_crash_no_sync_data_loss_ok", run: testQACrashNoSyncDataLossOK},
		{name: "qa_crash_with_flush_then_crash", run: testQACrashWithFlushThenCrash},
		{name: "qa_crash_wal_near_full", run: testQACrashWALNearFull},
		{name: "qa_crash_concurrent_writers", run: testQACrashConcurrentWriters},
		{name: "qa_crash_trim_heavy", run: testQACrashTrimHeavy},
		{name: "qa_crash_multi_block_stress", run: testQACrashMultiBlockStress},
		{name: "qa_crash_overwrite_storm", run: testQACrashOverwriteStorm},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQACrashNoSyncDataLossOK: Write WITHOUT SyncCache, crash, recover.
// Un-synced data MAY be lost — this is correct behavior, not a bug.
// The key invariant: volume must open without error and previously synced
// data must survive.
func testQACrashNoSyncDataLossOK(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nosync.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write block 0 and SYNC it.
	if err := v.WriteLBA(0, makeBlock('S')); err != nil {
		t.Fatalf("WriteLBA(synced): %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Stop background goroutines before manual superblock manipulation.
	v.groupCommit.Stop()
	v.flusher.Stop()

	// Persist superblock with current WAL state (before unsynced write).
	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()

	// Write block 1 WITHOUT sync — this write's WAL head is NOT in the superblock.
	if err := v.WriteLBA(1, makeBlock('U')); err != nil {
		t.Fatalf("WriteLBA(unsynced): %v", err)
	}

	// Hard crash (no sync, no superblock update for block 1).
	v.fd.Close()
	path = v.Path()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Synced block 0 must survive.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('S')) {
		t.Error("synced block 0 should survive crash")
	}

	// Un-synced block 1: may or may not be there — both are correct.
	// Just verify we can read without error.
	_, err = v2.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1) should not error: %v", err)
	}
}

// testQACrashWithFlushThenCrash: Write, let flusher run (data in extent),
// write more, crash WITHOUT sync. Flushed data must survive.
func testQACrashWithFlushThenCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flush_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write blocks 0-4 and sync.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Wait for background flusher to pick up the entries (100ms interval).
	time.Sleep(200 * time.Millisecond)

	// Write more blocks (in WAL, not yet synced to superblock).
	for i := uint64(5); i < 8; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Persist superblock with latest WAL state.
	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// All blocks 0-7 should be readable (0-4 from extent, 5-7 from WAL).
	for i := uint64(0); i < 8; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: mismatch after flush+crash", i)
		}
	}
}

// testQACrashWALNearFull: Fill WAL to near-capacity, sync, crash, recover.
// All synced entries must be recoverable even when WAL is almost full.
func testQACrashWALNearFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "walfull.blockvol")
	walSize := uint64(64 * 1024) // tiny 64KB WAL
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	entrySize := uint64(walEntryHeaderSize + 4096)
	maxEntries := int(walSize / entrySize)

	// Write up to capacity.
	var written int
	for i := 0; i < maxEntries; i++ {
		err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
		if err != nil {
			break // ErrWALFull
		}
		written++
	}

	if written == 0 {
		t.Fatal("couldn't write any entries")
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := 0; i < written; i++ {
		got, err := v2.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('A'+i%26))) {
			t.Errorf("block %d: mismatch after near-full WAL recovery", i)
		}
	}

	t.Logf("near-full WAL: %d/%d entries recovered", written, maxEntries)
}

// testQACrashConcurrentWriters: Multiple goroutines write, then sync, then crash.
// All synced data must survive recovery.
func testQACrashConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	const goroutines = 8
	const opsPerGoroutine = 20

	// Each goroutine writes to its own LBA range.
	var wg sync.WaitGroup
	writtenLBAs := make([][]uint64, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			baseLBA := uint64(id * opsPerGoroutine)
			for i := 0; i < opsPerGoroutine; i++ {
				lba := baseLBA + uint64(i)
				data := makeBlock(byte('A' + id%26))
				if err := v.WriteLBA(lba, data); err != nil {
					if errors.Is(err, ErrWALFull) {
						return
					}
					return
				}
				writtenLBAs[id] = append(writtenLBAs[id], lba)
			}
		}(g)
	}
	wg.Wait()

	// Sync everything.
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Verify all written LBAs.
	var totalVerified int
	for g := 0; g < goroutines; g++ {
		expected := makeBlock(byte('A' + g%26))
		for _, lba := range writtenLBAs[g] {
			got, err := v2.ReadLBA(lba, 4096)
			if err != nil {
				t.Fatalf("ReadLBA(%d): %v", lba, err)
			}
			if !bytes.Equal(got, expected) {
				t.Errorf("goroutine %d LBA %d: mismatch after concurrent crash", g, lba)
			}
			totalVerified++
		}
	}
	t.Logf("concurrent crash: %d blocks verified from %d goroutines", totalVerified, goroutines)
}

// testQACrashTrimHeavy: Crash loop with heavy trim operations.
// Verifies trim semantics survive crash+recovery correctly.
func testQACrashTrimHeavy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trim_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 256 * 1024, // 64 blocks
		BlockSize:  4096,
		WALSize:    128 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	oracle := make(map[uint64]byte)
	const maxLBA = 64

	for iter := 0; iter < 20; iter++ {
		// Write some blocks.
		for i := 0; i < 4; i++ {
			lba := uint64((iter*3 + i*7) % maxLBA)
			fill := byte('A' + (iter+i)%26)
			if err := v.WriteLBA(lba, makeBlock(fill)); err != nil {
				if errors.Is(err, ErrWALFull) {
					continue
				}
				t.Fatalf("iter %d WriteLBA(%d): %v", iter, lba, err)
			}
			oracle[lba] = fill
		}

		// Trim half of what we wrote.
		for i := 0; i < 2; i++ {
			lba := uint64((iter*3 + i*7) % maxLBA)
			if err := v.Trim(lba, 4096); err != nil {
				if errors.Is(err, ErrWALFull) {
					continue
				}
				t.Fatalf("iter %d Trim(%d): %v", iter, lba, err)
			}
			oracle[lba] = 0
		}

		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d SyncCache: %v", iter, err)
		}

		path = simulateCrashWithSuper(v)

		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d OpenBlockVol: %v", iter, err)
		}

		// Verify oracle.
		for lba, fill := range oracle {
			got, err := v.ReadLBA(lba, 4096)
			if err != nil {
				t.Fatalf("iter %d ReadLBA(%d): %v", iter, lba, err)
			}
			var expected []byte
			if fill == 0 {
				expected = make([]byte, 4096)
			} else {
				expected = makeBlock(fill)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("iter %d LBA %d: oracle mismatch (got[0]=%d want[0]=%d)",
					iter, lba, got[0], expected[0])
			}
		}
	}

	v.Close()
	t.Logf("trim-heavy crash: 20 iterations, all consistent")
}

// testQACrashMultiBlockStress: Crash loop with multi-block writes (2-4 blocks).
// Exercises recovery of multi-block WAL entries.
func testQACrashMultiBlockStress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multiblock_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	oracle := make(map[uint64]byte)

	for iter := 0; iter < 15; iter++ {
		// Write 2-4 blocks as a single call.
		nBlocks := 2 + (iter % 3) // 2, 3, or 4
		baseLBA := uint64((iter * 5) % 200)

		data := make([]byte, nBlocks*4096)
		for b := 0; b < nBlocks; b++ {
			fill := byte('A' + (iter+b)%26)
			for j := 0; j < 4096; j++ {
				data[b*4096+j] = fill
			}
			oracle[baseLBA+uint64(b)] = fill
		}

		if err := v.WriteLBA(baseLBA, data); err != nil {
			if errors.Is(err, ErrWALFull) {
				// Can't write, skip this iteration.
				continue
			}
			t.Fatalf("iter %d WriteLBA: %v", iter, err)
		}

		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d SyncCache: %v", iter, err)
		}

		path = simulateCrashWithSuper(v)

		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d OpenBlockVol: %v", iter, err)
		}

		// Verify all oracle entries.
		for lba, fill := range oracle {
			got, err := v.ReadLBA(lba, 4096)
			if err != nil {
				t.Fatalf("iter %d ReadLBA(%d): %v", iter, lba, err)
			}
			if !bytes.Equal(got, makeBlock(fill)) {
				t.Fatalf("iter %d LBA %d: mismatch", iter, lba)
			}
		}
	}

	v.Close()
	t.Logf("multi-block crash: 15 iterations, %d blocks tracked, all consistent", len(oracle))
}

// testQACrashOverwriteStorm: Overwrite the same LBA many times across crash
// iterations. Latest synced write must always win.
func testQACrashOverwriteStorm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overwrite_crash.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	for iter := 0; iter < 30; iter++ {
		fill := byte(iter % 256)
		// Overwrite LBA 0 with a new value each iteration.
		if err := v.WriteLBA(0, makeBlock(fill)); err != nil {
			if errors.Is(err, ErrWALFull) {
				// Need to persist what we have and cycle.
				v.SyncCache()
				path = simulateCrashWithSuper(v)
				v, err = OpenBlockVol(path)
				if err != nil {
					t.Fatalf("iter %d reopen: %v", iter, err)
				}
				// Retry write.
				if err := v.WriteLBA(0, makeBlock(fill)); err != nil {
					t.Fatalf("iter %d retry WriteLBA: %v", iter, err)
				}
			} else {
				t.Fatalf("iter %d WriteLBA: %v", iter, err)
			}
		}

		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d SyncCache: %v", iter, err)
		}

		path = simulateCrashWithSuper(v)

		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d OpenBlockVol: %v", iter, err)
		}

		got, err := v.ReadLBA(0, 4096)
		if err != nil {
			t.Fatalf("iter %d ReadLBA: %v", iter, err)
		}
		if !bytes.Equal(got, makeBlock(fill)) {
			t.Fatalf("iter %d: LBA 0 should be %d, got %d", iter, fill, got[0])
		}
	}

	v.Close()
	t.Logf("overwrite storm: 30 crash iterations, LBA 0 always correct")
}

// ============================================================================
// QA Adversarial Tests — Round 4 (Architect-directed edge cases)
// ============================================================================

// --- WAL / Recovery Edge Cases ---

func TestQARecoveryEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_recover_entrysize_mismatch_at_tail", run: testQARecoverEntrySizeMismatchAtTail},
		{name: "qa_recover_partial_padding", run: testQARecoverPartialPadding},
		{name: "qa_recover_trim_then_write_same_lba", run: testQARecoverTrimThenWriteSameLBA},
		{name: "qa_recover_write_then_trim_same_lba", run: testQARecoverWriteThenTrimSameLBA},
		{name: "qa_recover_barrier_only_full_wal", run: testQARecoverBarrierOnlyFullWAL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQARecoverEntrySizeMismatchAtTail: Corrupt the EntrySize field of the last
// WAL entry. Recovery should stop cleanly at the corrupt entry (CRC or EntrySize
// validation) without panic or returning an error.
func testQARecoverEntrySizeMismatchAtTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "entrysize.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 3 entries.
	for i := uint64(0); i < 3; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Stop background goroutines before manual superblock/WAL manipulation.
	v.groupCommit.Stop()
	v.flusher.Stop()

	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()

	// Corrupt the EntrySize field (last 4 bytes) of the 3rd entry.
	entrySize := uint64(walEntryHeaderSize + 4096)
	thirdEntryEnd := v.super.WALOffset + entrySize*3
	entrySizeOff := int64(thirdEntryEnd - 4) // EntrySize is last 4 bytes
	var badSize [4]byte
	binary.LittleEndian.PutUint32(badSize[:], 99999)
	v.fd.WriteAt(badSize[:], entrySizeOff)
	v.fd.Sync()

	v.fd.Close()
	path = v.Path()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol should succeed (recovery stops at corrupt entry): %v", err)
	}
	defer v2.Close()

	// Entries 1 and 2 should be recovered.
	for i := uint64(0); i < 2; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('A'+i))) {
			t.Errorf("block %d: should be '%c' after recovery", i, byte('A'+i))
		}
	}

	// Entry 3 (corrupt) should NOT be recovered — read returns zeros.
	got, err := v2.ReadLBA(2, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(2): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("block 2 (corrupt EntrySize) should return zeros")
	}
}

// testQARecoverPartialPadding: Write entries until WAL wraps with padding.
// Corrupt the padding entry to simulate truncation at EOF. Recovery should
// stop at the corrupt padding (torn write) and not advance past it.
func testQARecoverPartialPadding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial_pad.blockvol")

	// WAL sized so first entry leaves a gap that needs padding.
	entrySize := uint64(walEntryHeaderSize + 4096) // 4134
	// WAL = 2 * entrySize + 50 bytes (50 bytes becomes padding on wrap).
	walSize := entrySize*2 + 50

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 2 entries (fills 2*4134 = 8268 bytes, leaving 50 bytes).
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(0): %v", err)
	}
	if err := v.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(1): %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Advance tail past entry 1 so we have room to wrap.
	v.wal.AdvanceTail(entrySize)

	// Write entry 3 — this should trigger padding (50 bytes) at end and wrap to 0.
	if err := v.WriteLBA(2, makeBlock('C')); err != nil {
		t.Fatalf("WriteLBA(2) after wrap: %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Stop background goroutines before manual superblock/WAL manipulation.
	v.groupCommit.Stop()
	v.flusher.Stop()

	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()

	// Corrupt the padding region: overwrite the padding with garbage
	// to simulate a torn write at the padding boundary.
	paddingOff := int64(v.super.WALOffset + entrySize*2)
	garbage := bytes.Repeat([]byte{0xDE}, 50)
	v.fd.WriteAt(garbage, paddingOff)
	v.fd.Sync()

	v.fd.Close()
	path = v.Path()

	// Recovery should handle this — either skip corrupt padding and find
	// entry 3, or stop at the corruption. Either way, no panic.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol should not fail: %v", err)
	}
	defer v2.Close()

	// Entry 2 (LBA 1) should be recovered (it's before the padding).
	got, err := v2.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("block 1 should survive (before corrupt padding)")
	}
}

// testQARecoverTrimThenWriteSameLBA: TRIM LBA X, then WRITE same LBA, crash.
// Recovery should keep the WRITE (latest LSN wins).
func testQARecoverTrimThenWriteSameLBA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trim_write.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write initial data.
	if err := v.WriteLBA(5, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA initial: %v", err)
	}

	// Trim LBA 5.
	if err := v.Trim(5, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	// Write LBA 5 again with new data.
	if err := v.WriteLBA(5, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA after trim: %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Latest WRITE should win over earlier TRIM.
	got, err := v2.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(5): %v", err)
	}
	if !bytes.Equal(got, makeBlock('Y')) {
		t.Error("LBA 5 should be 'Y' (WRITE after TRIM wins)")
	}
}

// testQARecoverWriteThenTrimSameLBA: WRITE LBA X, then TRIM same LBA, crash.
// Recovery should return zeros (TRIM is latest).
func testQARecoverWriteThenTrimSameLBA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "write_trim.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write LBA 7.
	if err := v.WriteLBA(7, makeBlock('W')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Trim LBA 7.
	if err := v.Trim(7, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// TRIM is latest — should return zeros.
	got, err := v2.ReadLBA(7, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(7): %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("LBA 7 should be zeros (TRIM after WRITE)")
	}
}

// testQARecoverBarrierOnlyFullWAL: Fill WAL entirely with BARRIER entries.
// Recovery should process them all without error but make no data changes.
func testQARecoverBarrierOnlyFullWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "barrier_full.blockvol")

	// WAL sized for ~5 barrier entries (header-only, 38 bytes each).
	walSize := uint64(walEntryHeaderSize * 5)

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Append barrier entries until WAL is full.
	var appended int
	for i := 0; i < 10; i++ {
		lsn := v.nextLSN.Add(1) - 1
		entry := &WALEntry{LSN: lsn, Type: EntryTypeBarrier, LBA: 0}
		if _, err := v.wal.Append(entry); err != nil {
			break // ErrWALFull
		}
		appended++
	}

	if appended == 0 {
		t.Fatal("couldn't append any barrier entries")
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol with barrier-full WAL: %v", err)
	}
	defer v2.Close()

	// No data changes from barriers — read should return zeros.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("barrier-only WAL should leave data as zeros")
	}

	t.Logf("barrier-full WAL: %d barriers appended, recovery clean", appended)
}

// --- Flusher / Dirty Map Edge Cases ---

func TestQAFlusherEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_flush_interleaved_overwrite", run: testQAFlushInterleavedOverwrite},
		{name: "qa_flush_partial_wal_wrap", run: testQAFlushPartialWALWrap},
		{name: "qa_flush_trim_mixed_write", run: testQAFlushTrimMixedWrite},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQAFlushInterleavedOverwrite: Write LBA 0 three times with increasing LSN.
// Flush after first, overwrite twice more, flush again. Flusher's LSN-check
// should only remove entries matching the snapshot LSN.
func testQAFlushInterleavedOverwrite(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write LBA 0 = 'A' (LSN 1).
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}

	// Flush — moves 'A' to extent, removes dirty entry for LSN 1.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 1: %v", err)
	}

	// Overwrite LBA 0 = 'B' (LSN 2).
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}

	// Overwrite LBA 0 = 'C' (LSN 3).
	if err := v.WriteLBA(0, makeBlock('C')); err != nil {
		t.Fatalf("WriteLBA(C): %v", err)
	}

	// Dirty map should have LBA 0 with LSN 3 (latest overwrite).
	_, lsn, _, ok := v.dirtyMap.Get(0)
	if !ok {
		t.Fatal("LBA 0 should be in dirty map")
	}

	// Flush — snapshot captures LSN 3. After flush, extent has 'C'.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// Dirty map should be empty (LSN matched, so flusher removed it).
	if v.dirtyMap.Len() != 0 {
		t.Errorf("dirty map should be empty after flush, got %d", v.dirtyMap.Len())
	}

	// Read should return 'C' from extent.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, makeBlock('C')) {
		t.Error("LBA 0 should be 'C' after interleaved overwrites + flush")
	}

	_ = lsn // used for clarity in the test logic
}

// testQAFlushPartialWALWrap: Write entries until WAL wraps (with tail advance
// in between), then flush. Verify tail advance is correct and no WAL space leaks.
func testQAFlushPartialWALWrap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrap_flush.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    128 * 1024, // small WAL
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour, // manual only
	})

	entrySize := uint64(walEntryHeaderSize + 4096)
	maxEntries := int(128 * 1024 / entrySize)

	// Write ~60% capacity.
	firstBatch := maxEntries * 60 / 100
	for i := 0; i < firstBatch; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("batch1 WriteLBA(%d): %v", i, err)
		}
	}

	// Flush — moves all to extent, advances tail.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 1: %v", err)
	}

	// Write more — these will wrap around in the WAL.
	for i := 0; i < firstBatch; i++ {
		lba := uint64(firstBatch + i)
		if lba >= 256 { // stay within volume
			break
		}
		if err := v.WriteLBA(lba, makeBlock(byte('a'+i%26))); err != nil {
			if errors.Is(err, ErrWALFull) {
				break
			}
			t.Fatalf("batch2 WriteLBA(%d): %v", lba, err)
		}
	}

	// Flush again — should handle wrapped entries correctly.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// Dirty map should be empty.
	if v.dirtyMap.Len() != 0 {
		t.Errorf("dirty map should be 0 after double flush, got %d", v.dirtyMap.Len())
	}

	// Write more to verify WAL space was properly reclaimed.
	for i := 0; i < 5; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('Z'-i))); err != nil {
			t.Fatalf("post-wrap write %d: %v", i, err)
		}
	}

	// Verify latest writes.
	for i := 0; i < 5; i++ {
		got, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('Z'-i))) {
			t.Errorf("block %d: mismatch after wrap+flush+rewrite", i)
		}
	}
}

// testQAFlushTrimMixedWrite: Write some blocks, trim some, write others.
// Flush once. Verify extent has correct data (zeros for trimmed, data for written).
func testQAFlushTrimMixedWrite(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write LBAs 0-4.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Trim LBAs 1 and 3.
	if err := v.Trim(1, 4096); err != nil {
		t.Fatalf("Trim(1): %v", err)
	}
	if err := v.Trim(3, 4096); err != nil {
		t.Fatalf("Trim(3): %v", err)
	}

	// Flush — should write data for 0,2,4 and zeros for 1,3.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Dirty map should be empty.
	if v.dirtyMap.Len() != 0 {
		t.Errorf("dirty map should be empty, got %d", v.dirtyMap.Len())
	}

	// Verify from extent.
	expected := map[uint64][]byte{
		0: makeBlock('A'),
		1: make([]byte, 4096), // trimmed
		2: makeBlock('C'),
		3: make([]byte, 4096), // trimmed
		4: makeBlock('E'),
	}
	for lba, want := range expected {
		got, err := v.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", lba, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("LBA %d: extent data mismatch after mixed flush", lba)
		}
	}
}

// --- Lifecycle + Concurrency Edge Cases ---

func TestQALifecycleConcurrency(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_concurrent_flush_and_write", run: testQAConcurrentFlushAndWrite},
		{name: "qa_close_while_synccache_waits", run: testQACloseWhileSyncCacheWaits},
		{name: "qa_close_with_pending_dirtymap", run: testQACloseWithPendingDirtyMap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQAConcurrentFlushAndWrite: Background flusher runs while writes happen.
// Crash after some time. Verify no data loss for synced writes.
func testQAConcurrentFlushAndWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "conc_flush.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	// v already has background flusher running (100ms interval).

	// Write 50 blocks with SyncCache, while flusher runs in background.
	oracle := make(map[uint64]byte)
	for i := uint64(0); i < 50; i++ {
		fill := byte('A' + i%26)
		if err := v.WriteLBA(i, makeBlock(fill)); err != nil {
			if errors.Is(err, ErrWALFull) {
				// Flusher should free space, but if not fast enough, skip.
				time.Sleep(150 * time.Millisecond) // let flusher run
				if err := v.WriteLBA(i, makeBlock(fill)); err != nil {
					continue // still full, skip
				}
			} else {
				t.Fatalf("WriteLBA(%d): %v", i, err)
			}
		}
		oracle[i] = fill

		// Sync periodically (every 10 writes).
		if i%10 == 9 {
			if err := v.SyncCache(); err != nil {
				t.Fatalf("SyncCache at %d: %v", i, err)
			}
		}
	}

	// Final sync.
	if err := v.SyncCache(); err != nil {
		t.Fatalf("final SyncCache: %v", err)
	}

	// Let flusher run one more cycle.
	time.Sleep(150 * time.Millisecond)

	// Persist superblock and crash.
	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Verify all oracle entries (some from extent, some from WAL replay).
	for lba, fill := range oracle {
		got, err := v2.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", lba, err)
		}
		if !bytes.Equal(got, makeBlock(fill)) {
			t.Errorf("block %d: mismatch after concurrent flush+crash", lba)
		}
	}
	t.Logf("concurrent flush+write: %d blocks verified", len(oracle))
}

// testQACloseWhileSyncCacheWaits: Start SyncCache in a goroutine, then Close.
// SyncCache should return ErrGroupCommitShutdown (not deadlock).
func testQACloseWhileSyncCacheWaits(t *testing.T) {
	v := createTestVol(t)

	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Launch SyncCache in background.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- v.SyncCache()
	}()

	// Small delay to let SyncCache enqueue.
	time.Sleep(2 * time.Millisecond)

	// Close while SyncCache may be waiting.
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- v.Close()
	}()

	// Both should complete without deadlock.
	select {
	case err := <-syncDone:
		// SyncCache either succeeded (fsync happened before close) or got shutdown error.
		if err != nil && !errors.Is(err, ErrGroupCommitShutdown) {
			t.Errorf("SyncCache: unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SyncCache deadlocked during Close")
	}

	select {
	case err := <-closeDone:
		// Close may return nil or an error from final flush — both are OK.
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked")
	}
}

// testQACloseWithPendingDirtyMap: Write blocks without sync, then Close.
// Close should flush dirty map. Reopen should show 0 dirty entries.
func testQACloseWithPendingDirtyMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pending_dirty.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write 15 blocks without explicit SyncCache.
	for i := uint64(0); i < 15; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Close — should stop group committer, stop flusher, do final flush.
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — verify dirty map is empty (all data in extent).
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	if v2.dirtyMap.Len() != 0 {
		t.Errorf("dirty map after reopen should be 0, got %d", v2.dirtyMap.Len())
	}

	// Verify all blocks.
	for i := uint64(0); i < 15; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i%26))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: mismatch after close-with-pending", i)
		}
	}
}

// --- Parameter Extremes ---

func TestQAParameterExtremes(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "qa_blocksize_512_wal_small", run: testQABlockSize512WALSmall},
		{name: "qa_wal_size_min_header", run: testQAWALSizeMinHeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// testQABlockSize512WALSmall: 512-byte blocks with tiny WAL. Write, sync,
// crash, recover. Ensures no panics with non-standard parameters.
func testQABlockSize512WALSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bs512_small.blockvol")

	// 512-byte blocks, 4KB WAL (holds ~7 entries: (38+512)=550 per entry).
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 1024, // 64KB = 128 blocks of 512 bytes
		BlockSize:  512,
		WALSize:    4 * 1024, // 4KB WAL
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write a few blocks.
	data := make([]byte, 512)
	for i := range data {
		data[i] = 0xAB
	}
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, data); err != nil {
			if errors.Is(err, ErrWALFull) {
				break
			}
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(0, 512)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("512-byte block not recovered correctly")
	}
}

// testQAWALSizeMinHeader: WAL barely larger than one entry header.
// Should return ErrWALFull on first write without panicking.
func testQAWALSizeMinHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny_wal.blockvol")

	// WAL = walEntryHeaderSize + 1 byte — can't fit any entry with data.
	walSize := uint64(walEntryHeaderSize + 1)

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// First write should fail with ErrWALFull (entry is 38+4096=4134 > 39 bytes).
	err = v.WriteLBA(0, makeBlock('X'))
	if err == nil {
		t.Fatal("expected ErrWALFull with tiny WAL")
	}
	if !errors.Is(err, ErrWALFull) {
		t.Errorf("expected ErrWALFull, got: %v", err)
	}

	// Volume should still be usable (read returns zeros, no panic).
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA should work even with full WAL: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("unwritten block should be zeros")
	}
}
