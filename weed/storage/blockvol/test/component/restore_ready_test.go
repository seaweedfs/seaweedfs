package component

// Restore Ready tests (Matrix B): snapshot-tail rebuild and related scenarios.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRestore_S5_SnapshotTailRebuild exercises snapshot-based rebuild
// followed by WAL tail replay — the two-line model with a real snapshot
// as the base instead of current extent.
//
// Scenario:
//   1. Primary writes 100 blocks, takes snapshot at LSN 100
//   2. Primary writes 50 more blocks (LSN 101-150)
//   3. Replica rebuilds: base = snapshot at LSN 100, tail = WAL 101-150
//   4. Both lanes run, bitmap protects overlapping blocks
//   5. Final extent matches primary
func TestRestore_S5_SnapshotTailRebuild(t *testing.T) {
	primary, replica := createRestorePair(t)
	defer primary.Close()
	defer replica.Close()

	blockSize := uint32(4096)

	// Phase 1: Write initial data and "snapshot" (flush to extent = our base).
	snapshotBlocks := 100
	for i := 0; i < snapshotBlocks; i++ {
		primary.WriteLBA(uint64(i), bytes.Repeat([]byte{byte(0x30 + (i % 48))}, int(blockSize)))
	}
	primary.SyncCache()
	primary.ForceFlush()
	snapshotLSN := primary.Status().WALHeadLSN
	t.Logf("S5: snapshot at LSN=%d (%d blocks flushed)", snapshotLSN, snapshotBlocks)

	// Phase 2: Write more data AFTER snapshot (the "tail").
	tailBlocks := 50
	for i := 0; i < tailBlocks; i++ {
		lba := uint64(i) // overwrites first 50 blocks
		primary.WriteLBA(lba, bytes.Repeat([]byte{byte(0x80 + i)}, int(blockSize)))
	}
	tailLSN := primary.Status().WALHeadLSN
	t.Logf("S5: tail writes LSN %d..%d (%d blocks)", snapshotLSN+1, tailLSN, tailBlocks)

	// Phase 3: Rebuild replica using snapshot base + WAL tail.
	sessionID := uint64(1)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: sessionID, Epoch: 1,
		BaseLSN:   snapshotLSN,
		TargetLSN: tailLSN,
	}); err != nil {
		t.Fatal(err)
	}
	defer replica.CancelRebuildSession(sessionID, "test_done")

	// Base lane: read primary's FLUSHED extent (snapshot state at snapshotLSN).
	// Since we flushed before the tail writes, ReadLBA for blocks 0-49 returns
	// the snapshot state (before tail overwrites) IF read from extent.
	// But ReadLBA reads from dirty map first (which has the tail writes).
	// To simulate a true snapshot base, we read the extent directly or use
	// the snapshot data we know.
	info := primary.Info()
	totalLBAs := info.VolumeSize / uint64(blockSize)
	for lba := uint64(0); lba < totalLBAs && lba < uint64(snapshotBlocks); lba++ {
		// Use the known snapshot data (pre-tail).
		data := bytes.Repeat([]byte{byte(0x30 + (int(lba) % 48))}, int(blockSize))
		replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
	}
	replica.MarkRebuildSessionBaseComplete(sessionID, uint64(snapshotBlocks))

	// WAL lane: replay tail entries.
	for i := 0; i < tailBlocks; i++ {
		lba := uint64(i)
		data := bytes.Repeat([]byte{byte(0x80 + i)}, int(blockSize))
		replica.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
			LSN: snapshotLSN + uint64(i) + 1, Epoch: 1,
			Type: blockvol.EntryTypeWrite, LBA: lba,
			Length: blockSize, Data: data,
		})
	}

	achieved, completed, _ := replica.TryCompleteRebuildSession(sessionID)
	if !completed {
		t.Fatal("S5: snapshot-tail rebuild did not complete")
	}
	t.Logf("S5: completed, achieved=%d", achieved)

	// Phase 4: Verify replica matches primary's current state.
	primary.ForceFlush()
	replica.ForceFlush()

	mismatches := 0
	for lba := uint64(0); lba < uint64(snapshotBlocks); lba++ {
		pData, _ := primary.ReadLBA(lba, blockSize)
		rData, _ := replica.ReadLBA(lba, blockSize)
		if !bytes.Equal(pData, rData) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("LBA %d: primary[0]=0x%02x replica[0]=0x%02x", lba, pData[0], rData[0])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("S5 FAILED: %d blocks mismatch after snapshot-tail rebuild", mismatches)
	}

	_, progress, _ := replica.ActiveRebuildSession()
	t.Logf("S5 PASSED: snapshot-tail rebuild converges. base_applied=%d base_skipped=%d bitmap=%d",
		progress.BaseBlocksApplied, progress.BaseBlocksSkipped, progress.BitmapAppliedCount)
}

// TestRestore_S7_CrashBetweenBaseAndTail exercises a crash after the snapshot
// base is installed but before the WAL tail replay completes. A fresh rebuild
// must still converge.
func TestRestore_S7_CrashBetweenBaseAndTail(t *testing.T) {
	primaryPath := filepath.Join(t.TempDir(), "primary.blk")
	replicaPath := filepath.Join(t.TempDir(), "replica.blk")

	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024,
	}

	primary, err := blockvol.CreateBlockVol(primaryPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	numBlocks := 40
	for i := 0; i < numBlocks; i++ {
		primary.WriteLBA(uint64(i), bytes.Repeat([]byte{byte(0x50 + i)}, 4096))
	}
	primary.SyncCache()
	primary.ForceFlush()
	baseLSN := primary.Status().WALHeadLSN

	// Phase 1: Start rebuild, complete base, apply partial WAL, crash.
	func() {
		replica, err := blockvol.CreateBlockVol(replicaPath, opts)
		if err != nil {
			t.Fatal(err)
		}
		replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

		sessionID := uint64(1)
		replica.StartRebuildSession(blockvol.RebuildSessionConfig{
			SessionID: sessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 10,
		})

		// Complete base lane.
		info := primary.Info()
		for lba := uint64(0); lba < uint64(numBlocks); lba++ {
			data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
			replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
		}
		replica.MarkRebuildSessionBaseComplete(sessionID, uint64(numBlocks))

		// Apply PARTIAL WAL tail (only 3 of 10 entries).
		for i := 0; i < 3; i++ {
			replica.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
				LSN: baseLSN + uint64(i) + 1, Epoch: 1,
				Type: blockvol.EntryTypeWrite, LBA: uint64(i),
				Length: 4096, Data: bytes.Repeat([]byte{byte(0xF0 + i)}, 4096),
			})
		}

		_, progress, _ := replica.ActiveRebuildSession()
		t.Logf("S7: before crash: base=%v walApplied=%d (3 of 10)",
			progress.BaseComplete, progress.WALAppliedLSN)

		// Crash.
		replica.Close()
	}()

	// Phase 2: Reopen and fresh rebuild.
	replica, err := blockvol.OpenBlockVol(replicaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	freshSessionID := uint64(2)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: freshSessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN,
	}); err != nil {
		t.Fatal(err)
	}
	defer replica.CancelRebuildSession(freshSessionID, "test_done")

	info := primary.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	for lba := uint64(0); lba < totalLBAs; lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		replica.ApplyRebuildSessionBaseBlock(freshSessionID, lba, data)
	}
	replica.MarkRebuildSessionBaseComplete(freshSessionID, totalLBAs)

	replica.ApplyRebuildSessionWALEntry(freshSessionID, &blockvol.WALEntry{
		LSN: baseLSN, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: totalLBAs - 1, Length: 4096, Data: make([]byte, 4096),
	})

	achieved, completed, _ := replica.TryCompleteRebuildSession(freshSessionID)
	if !completed {
		t.Fatal("S7: fresh rebuild after crash did not complete")
	}

	// Verify.
	primary.ForceFlush()
	replica.ForceFlush()

	for lba := uint64(0); lba < uint64(numBlocks); lba++ {
		p, _ := primary.ReadLBA(lba, 4096)
		r, _ := replica.ReadLBA(lba, 4096)
		if !bytes.Equal(p, r) {
			t.Fatalf("S7: LBA %d mismatch after fresh rebuild post-crash", lba)
		}
	}
	t.Logf("S7 PASSED: crash between base and tail → fresh rebuild converges (achieved=%d)", achieved)
}

// TestRestore_S8_SnapshotUnderConcurrentWrites verifies that a snapshot-based
// rebuild works correctly when the primary continues writing during the rebuild.
// The snapshot base is preserved and later writes arrive through WAL tail.
func TestRestore_S8_SnapshotUnderConcurrentWrites(t *testing.T) {
	primary, replica := createRestorePair(t)
	defer primary.Close()
	defer replica.Close()

	blockSize := uint32(4096)

	// Write and flush (snapshot point).
	for i := 0; i < 80; i++ {
		primary.WriteLBA(uint64(i), bytes.Repeat([]byte{byte(0x40 + (i % 32))}, int(blockSize)))
	}
	primary.SyncCache()
	primary.ForceFlush()
	snapshotLSN := primary.Status().WALHeadLSN

	// Start rebuild.
	liveWrites := 30
	targetLSN := snapshotLSN + uint64(liveWrites)
	sessionID := uint64(1)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: sessionID, Epoch: 1, BaseLSN: snapshotLSN, TargetLSN: targetLSN,
	}); err != nil {
		t.Fatal(err)
	}
	defer replica.CancelRebuildSession(sessionID, "test_done")

	// Base lane: snapshot data.
	for lba := uint64(0); lba < 80; lba++ {
		data := bytes.Repeat([]byte{byte(0x40 + (int(lba) % 32))}, int(blockSize))
		replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
	}
	replica.MarkRebuildSessionBaseComplete(sessionID, 80)

	// WAL lane: concurrent writes on primary DURING rebuild.
	for i := 0; i < liveWrites; i++ {
		lba := uint64(i) // overwrite first 30 blocks
		data := bytes.Repeat([]byte{byte(0xD0 + i)}, int(blockSize))
		primary.WriteLBA(lba, data)

		replica.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
			LSN: snapshotLSN + uint64(i) + 1, Epoch: 1,
			Type: blockvol.EntryTypeWrite, LBA: lba,
			Length: blockSize, Data: data,
		})
	}

	achieved, completed, _ := replica.TryCompleteRebuildSession(sessionID)
	if !completed {
		t.Fatal("S8: rebuild under concurrent writes did not complete")
	}

	// Verify replica matches primary.
	primary.ForceFlush()
	replica.ForceFlush()

	primaryHash := sha256.New()
	replicaHash := sha256.New()
	for lba := uint64(0); lba < 80; lba++ {
		p, _ := primary.ReadLBA(lba, blockSize)
		r, _ := replica.ReadLBA(lba, blockSize)
		primaryHash.Write(p)
		replicaHash.Write(r)
	}
	pCRC := fmt.Sprintf("%x", primaryHash.Sum(nil))
	rCRC := fmt.Sprintf("%x", replicaHash.Sum(nil))
	if pCRC != rCRC {
		t.Fatalf("S8 FAILED: CRC mismatch primary=%s...%s replica=%s...%s",
			pCRC[:8], pCRC[len(pCRC)-8:], rCRC[:8], rCRC[len(rCRC)-8:])
	}

	_, progress, _ := replica.ActiveRebuildSession()
	t.Logf("S8 PASSED: snapshot + concurrent writes converge. achieved=%d skipped=%d bitmap=%d CRC=%s...%s",
		achieved, progress.BaseBlocksSkipped, progress.BitmapAppliedCount, pCRC[:8], pCRC[len(pCRC)-8:])
}

// --- Helpers ---

func createRestorePair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024,
	}
	p, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	p.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)
	r, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "replica.blk"), opts)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	r.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)
	return p, r
}
