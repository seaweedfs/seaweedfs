package component

// R11: Non-empty stale replica with divergent data — full overwrite rebuild.
// R12: Crash mid-rebuild — restart with fresh session converges.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRebuild_R11_DivergentReplicaFullOverwrite exercises a replica that has
// DIFFERENT data from the primary at the same LBAs. The rebuild must fully
// overwrite all divergent blocks, not just fill gaps.
//
// Scenario:
//   1. Primary writes pattern A to LBAs 0-99
//   2. Replica independently writes pattern B to LBAs 0-99 (divergent!)
//   3. Rebuild from primary to replica
//   4. Verify replica has pattern A everywhere, not pattern B
func TestRebuild_R11_DivergentReplicaFullOverwrite(t *testing.T) {
	primaryPath := filepath.Join(t.TempDir(), "primary.blk")
	replicaPath := filepath.Join(t.TempDir(), "replica.blk")

	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024,
	}

	// Primary: pattern A (0xAA-based).
	primary, err := blockvol.CreateBlockVol(primaryPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	numBlocks := 100
	for i := 0; i < numBlocks; i++ {
		data := bytes.Repeat([]byte{byte(0xA0 + (i % 32))}, 4096)
		primary.WriteLBA(uint64(i), data)
	}
	primary.SyncCache()
	primary.ForceFlush()
	baseLSN := primary.Status().WALHeadLSN

	// Replica: pattern B (0xBB-based) — completely DIFFERENT data.
	replica, err := blockvol.CreateBlockVol(replicaPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	for i := 0; i < numBlocks; i++ {
		data := bytes.Repeat([]byte{byte(0xB0 + (i % 32))}, 4096)
		replica.WriteLBA(uint64(i), data)
	}
	replica.SyncCache()
	replica.ForceFlush()

	// Verify divergence: LBA 0 should be different.
	pData, _ := primary.ReadLBA(0, 4096)
	rData, _ := replica.ReadLBA(0, 4096)
	if bytes.Equal(pData, rData) {
		t.Fatal("setup error: primary and replica should have different data")
	}
	t.Logf("R11: confirmed divergence — primary[0]=0x%02x replica[0]=0x%02x", pData[0], rData[0])

	// Rebuild: overwrite replica with primary's data.
	sessionID := uint64(1)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: sessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN,
	}); err != nil {
		t.Fatal(err)
	}
	defer replica.CancelRebuildSession(sessionID, "test_done")

	info := primary.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	for lba := uint64(0); lba < totalLBAs; lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
	}
	replica.MarkRebuildSessionBaseComplete(sessionID, totalLBAs)

	// WAL entry to satisfy target.
	replica.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
		LSN: baseLSN, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: totalLBAs - 1, Length: 4096, Data: make([]byte, 4096),
	})

	achieved, completed, _ := replica.TryCompleteRebuildSession(sessionID)
	if !completed {
		t.Fatal("R11: rebuild did not complete")
	}
	t.Logf("R11: rebuild completed, achieved=%d", achieved)

	// Flush both and compare CRC.
	primary.ForceFlush()
	replica.ForceFlush()

	primaryHash := sha256.New()
	replicaHash := sha256.New()
	mismatches := 0

	for lba := uint64(0); lba < totalLBAs; lba++ {
		p, _ := primary.ReadLBA(lba, 4096)
		r, _ := replica.ReadLBA(lba, 4096)
		primaryHash.Write(p)
		replicaHash.Write(r)
		if !bytes.Equal(p, r) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("LBA %d: primary[0]=0x%02x replica[0]=0x%02x", lba, p[0], r[0])
			}
		}
	}

	pCRC := fmt.Sprintf("%x", primaryHash.Sum(nil))
	rCRC := fmt.Sprintf("%x", replicaHash.Sum(nil))

	if mismatches > 0 {
		t.Fatalf("R11 FAILED: %d/%d blocks still divergent after rebuild. CRC primary=%s...%s replica=%s...%s",
			mismatches, totalLBAs, pCRC[:8], pCRC[len(pCRC)-8:], rCRC[:8], rCRC[len(rCRC)-8:])
	}
	t.Logf("R11 PASSED: all %d blocks overwritten. Divergent replica now matches primary. CRC=%s...%s",
		totalLBAs, pCRC[:8], pCRC[len(pCRC)-8:])
}

// TestRebuild_R12_CrashMidRebuild_FreshSessionConverges exercises crash
// during an active rebuild session, then a completely fresh rebuild from
// scratch that must converge correctly.
//
// Scenario:
//   1. Primary has 50 blocks of data
//   2. Start rebuild on replica, apply ~50% of base blocks
//   3. Apply some WAL entries to set bitmap
//   4. "Crash" replica (close without completing session)
//   5. Reopen replica
//   6. Start FRESH rebuild session (not resume)
//   7. Complete rebuild from scratch
//   8. Verify all data matches primary
func TestRebuild_R12_CrashMidRebuild_FreshSessionConverges(t *testing.T) {
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

	numBlocks := 50
	for i := 0; i < numBlocks; i++ {
		primary.WriteLBA(uint64(i), bytes.Repeat([]byte{byte(0xC0 + i)}, 4096))
	}
	primary.SyncCache()
	primary.ForceFlush()
	baseLSN := primary.Status().WALHeadLSN

	// Phase 1: Start rebuild, apply partially, then crash.
	func() {
		replica, err := blockvol.CreateBlockVol(replicaPath, opts)
		if err != nil {
			t.Fatal(err)
		}
		replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

		sessionID := uint64(1)
		replica.StartRebuildSession(blockvol.RebuildSessionConfig{
			SessionID: sessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 5,
		})

		// Apply ~50% of base blocks.
		info := primary.Info()
		for lba := uint64(0); lba < 25; lba++ {
			data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
			replica.ApplyRebuildSessionBaseBlock(sessionID, lba, data)
		}

		// Apply a few WAL entries.
		for i := uint64(0); i < 3; i++ {
			replica.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
				LSN: baseLSN + i + 1, Epoch: 1, Type: blockvol.EntryTypeWrite,
				LBA: i, Length: 4096, Data: bytes.Repeat([]byte{byte(0xF0 + i)}, 4096),
			})
		}

		_, progress, _ := replica.ActiveRebuildSession()
		t.Logf("R12: mid-rebuild state before crash: walApplied=%d baseApplied=%d bitmap=%d",
			progress.WALAppliedLSN, progress.BaseBlocksApplied, progress.BitmapAppliedCount)

		// "Crash" — close without completing.
		replica.Close()
		t.Log("R12: replica crashed mid-rebuild")
	}()

	// Phase 2: Reopen and start FRESH rebuild (not resume).
	replica, err := blockvol.OpenBlockVol(replicaPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer replica.Close()
	replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	// No active session after restart.
	_, _, ok := replica.ActiveRebuildSession()
	if ok {
		t.Fatal("R12: stale rebuild session should not survive restart")
	}

	// Fresh session from scratch.
	freshSessionID := uint64(2)
	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: freshSessionID, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN,
	}); err != nil {
		t.Fatalf("start fresh rebuild: %v", err)
	}
	defer replica.CancelRebuildSession(freshSessionID, "test_done")

	// Apply ALL base blocks from scratch.
	info := primary.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	for lba := uint64(0); lba < totalLBAs; lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		replica.ApplyRebuildSessionBaseBlock(freshSessionID, lba, data)
	}
	replica.MarkRebuildSessionBaseComplete(freshSessionID, totalLBAs)

	// WAL to satisfy target.
	replica.ApplyRebuildSessionWALEntry(freshSessionID, &blockvol.WALEntry{
		LSN: baseLSN, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: totalLBAs - 1, Length: 4096, Data: make([]byte, 4096),
	})

	achieved, completed, _ := replica.TryCompleteRebuildSession(freshSessionID)
	if !completed {
		t.Fatal("R12: fresh rebuild did not complete")
	}
	t.Logf("R12: fresh rebuild completed, achieved=%d", achieved)

	// Phase 3: Verify data correctness.
	primary.ForceFlush()
	replica.ForceFlush()

	mismatches := 0
	for lba := uint64(0); lba < totalLBAs; lba++ {
		p, _ := primary.ReadLBA(lba, 4096)
		r, _ := replica.ReadLBA(lba, 4096)
		if !bytes.Equal(p, r) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("LBA %d: primary[0]=0x%02x replica[0]=0x%02x", lba, p[0], r[0])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("R12 FAILED: %d blocks mismatch after fresh rebuild post-crash", mismatches)
	}
	t.Logf("R12 PASSED: crash mid-rebuild → fresh session from scratch → all %d blocks match primary", totalLBAs)
}
