package component

// Crash and failure tests for the rebuild MVP.
//
// From v2-rebuild-mvp-session-protocol.md test matrix:
//   1. Crash after WAL receive but before apply: base may cover LBA safely
//   2. Crash after WAL apply: recovered WAL preserves correctness
//   3. Rebuild completion does not happen with only base or only WAL
//   4. Multi-block WAL entry bitmap coverage
//   5. WAL full during rebuild: WAL entry rejected, session can fail gracefully
//   6. Concurrent base + WAL on overlapping LBAs at scale

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRebuild_CrashAfterWALApply_RecoveredCorrectly simulates a crash after
// WAL entries are applied. On restart, the volume's WAL replay should recover
// the applied data, and ReadLBA should return the correct values.
func TestRebuild_CrashAfterWALApply_RecoveredCorrectly(t *testing.T) {
	replicaPath := filepath.Join(t.TempDir(), "replica.blk")

	// Phase 1: Create replica, start rebuild, apply WAL entries.
	func() {
		replica, err := blockvol.CreateBlockVol(replicaPath, blockvol.CreateOptions{
			VolumeSize: 4 * 1024 * 1024,
			BlockSize:  4096,
			WALSize:    1 * 1024 * 1024,
		})
		if err != nil {
			t.Fatal(err)
		}
		replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

		session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
			SessionID: 1, Epoch: 1, BaseLSN: 10, TargetLSN: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		session.Start()

		// Apply WAL entries for LBA 0, 1, 2.
		for lba := uint64(0); lba < 3; lba++ {
			entry := &blockvol.WALEntry{
				LSN: lba + 1, Epoch: 1, Type: blockvol.EntryTypeWrite,
				LBA: lba, Length: 4096,
				Data: bytes.Repeat([]byte{byte(0xD0 + lba)}, 4096),
			}
			if err := session.ApplyWALEntry(entry); err != nil {
				t.Fatalf("WAL apply LBA %d: %v", lba, err)
			}
		}

		// "Crash" — close without completing the session.
		replica.Close()
	}()

	// Phase 2: Reopen (simulates restart + WAL recovery).
	recovered, err := blockvol.OpenBlockVol(replicaPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()

	// Verify WAL-applied data survived the crash.
	for lba := uint64(0); lba < 3; lba++ {
		data, err := recovered.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("read LBA %d after recovery: %v", lba, err)
		}
		expected := byte(0xD0 + lba)
		if data[0] != expected {
			t.Fatalf("LBA %d after recovery: got 0x%02x, want 0x%02x", lba, data[0], expected)
		}
	}
	t.Log("crash after WAL apply: all 3 LBAs recovered correctly from WAL replay")
}

// TestRebuild_CleanRestart_SeedsSessionAtCheckpointBoundary verifies that after
// a clean restart, a rebuild session starts with the trusted base boundary as
// its initial applied position even when there is no residual WAL to hydrate.
func TestRebuild_CleanRestart_SeedsSessionAtCheckpointBoundary(t *testing.T) {
	replicaPath := filepath.Join(t.TempDir(), "replica.blk")

	func() {
		replica, err := blockvol.CreateBlockVol(replicaPath, blockvol.CreateOptions{
			VolumeSize: 4 * 1024 * 1024,
			BlockSize:  4096,
			WALSize:    1 * 1024 * 1024,
		})
		if err != nil {
			t.Fatal(err)
		}
		replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

		session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
			SessionID: 1, Epoch: 1, BaseLSN: 0, TargetLSN: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Start(); err != nil {
			t.Fatal(err)
		}
		for lba := uint64(0); lba < 3; lba++ {
			if err := session.ApplyWALEntry(&blockvol.WALEntry{
				LSN:    lba + 1,
				Epoch:  1,
				Type:   blockvol.EntryTypeWrite,
				LBA:    lba,
				Length: 4096,
				Data:   bytes.Repeat([]byte{byte(0xC0 + lba)}, 4096),
			}); err != nil {
				t.Fatalf("seed WAL apply LBA %d: %v", lba, err)
			}
		}
		replica.Close()
	}()

	recovered, err := blockvol.OpenBlockVol(replicaPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()

	baseLSN := recovered.StatusSnapshot().CheckpointLSN
	if err := recovered.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 2, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN,
	}); err != nil {
		t.Fatalf("start clean-restart rebuild session: %v", err)
	}
	_, progress, ok := recovered.ActiveRebuildSession()
	if !ok {
		t.Fatal("expected active rebuild session after restart")
	}
	if progress.WALAppliedLSN != baseLSN {
		t.Fatalf("initial WALAppliedLSN=%d, want checkpoint/base LSN %d", progress.WALAppliedLSN, baseLSN)
	}
	t.Logf("clean restart seeded session boundary from trusted checkpoint LSN=%d", baseLSN)
}

// TestRebuild_StartFailsClosedWhenLocalCheckpointPastBaseLSN verifies the
// silent-truncation guard: if the local durable extent is already newer than
// the incoming base boundary, rebuild startup must fail closed.
func TestRebuild_StartFailsClosedWhenLocalCheckpointPastBaseLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.blk")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()
	vol.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	if err := vol.WriteLBA(0, bytes.Repeat([]byte{0xAB}, 4096)); err != nil {
		t.Fatalf("write before flush: %v", err)
	}
	if err := vol.ForceFlush(); err != nil {
		t.Fatalf("force flush: %v", err)
	}

	err = vol.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 1,
		Epoch:     1,
		BaseLSN:   0,
		TargetLSN: 1,
	})
	if err == nil {
		t.Fatal("expected fail-closed rebuild start when checkpoint is newer than base")
	}
	if !strings.Contains(err.Error(), "local checkpoint") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, ok := vol.ActiveRebuildSession(); ok {
		t.Fatal("rebuild session should not become active on hydration failure")
	}
	t.Logf("fail-closed rebuild start rejected stale base as expected: %v", err)
}

// TestRebuild_CompletionRequiresBothLanes verifies the dual completion gate:
// neither base-only nor WAL-only is sufficient for completion.
func TestRebuild_CompletionRequiresBothLanes(t *testing.T) {
	primary, replica := createRebuildCrashPair(t)
	defer primary.Close()
	defer replica.Close()

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1, Epoch: 1, BaseLSN: 5, TargetLSN: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Start()

	// WAL only — no base complete.
	for i := uint64(1); i <= 5; i++ {
		session.ApplyWALEntry(&blockvol.WALEntry{
			LSN: i, Epoch: 1, Type: blockvol.EntryTypeWrite,
			LBA: i, Length: 4096, Data: bytes.Repeat([]byte{0xAA}, 4096),
		})
	}
	_, completed := session.TryComplete()
	if completed {
		t.Fatal("should not complete with WAL only (base not complete)")
	}

	// Base only — reset and try without WAL reaching target.
	session2, _ := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 2, Epoch: 1, BaseLSN: 100, TargetLSN: 101,
	})
	session2.Start()
	session2.MarkBaseComplete(10)
	_, completed2 := session2.TryComplete()
	if completed2 {
		t.Fatal("should not complete with base only (WAL not at target)")
	}

	// Both conditions met.
	session2.ApplyWALEntry(&blockvol.WALEntry{
		LSN: 101, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: 0, Length: 4096, Data: bytes.Repeat([]byte{0xBB}, 4096),
	})
	achieved, completed3 := session2.TryComplete()
	if !completed3 {
		t.Fatal("should complete when both base and WAL conditions met")
	}
	if achieved != 101 {
		t.Fatalf("achieved=%d, want 101", achieved)
	}
	t.Log("dual completion gate verified: needs both base_complete AND wal >= target")
}

// TestRebuild_MultiBlockWALEntry_BitmapCoversAllLBAs verifies that a WAL
// entry spanning multiple blocks marks ALL covered LBAs in the bitmap.
func TestRebuild_MultiBlockWALEntry_BitmapCoversAllLBAs(t *testing.T) {
	primary, replica := createRebuildCrashPair(t)
	defer primary.Close()
	defer replica.Close()

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1, Epoch: 1, BaseLSN: 10, TargetLSN: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Start()

	// One WAL entry covering 4 blocks (LBA 10-13, 16KB total).
	multiBlockData := bytes.Repeat([]byte{0xCC}, 4*4096)
	entry := &blockvol.WALEntry{
		LSN: 1, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: 10, Length: 4 * 4096, Data: multiBlockData,
	}
	if err := session.ApplyWALEntry(entry); err != nil {
		t.Fatal(err)
	}

	// All 4 LBAs should be bitmap-protected.
	for lba := uint64(10); lba < 14; lba++ {
		applied, err := session.ApplyBaseBlock(lba, bytes.Repeat([]byte{0x11}, 4096))
		if err != nil {
			t.Fatalf("base apply LBA %d: %v", lba, err)
		}
		if applied {
			t.Fatalf("LBA %d: base should be skipped (multi-block WAL covered it)", lba)
		}
	}

	// LBA 14 (not covered) should accept base.
	applied, err := session.ApplyBaseBlock(14, bytes.Repeat([]byte{0x22}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("LBA 14 should accept base (not covered by WAL)")
	}

	progress := session.Progress()
	if progress.BitmapAppliedCount != 4 {
		t.Fatalf("bitmap count=%d, want 4 (one entry covering 4 blocks)", progress.BitmapAppliedCount)
	}
	t.Log("multi-block WAL entry correctly marks all 4 LBAs in bitmap")
}

// TestRebuild_EpochMismatch_WALEntryRejected verifies that WAL entries with
// wrong epoch are rejected by the session.
func TestRebuild_EpochMismatch_WALEntryRejected(t *testing.T) {
	primary, replica := createRebuildCrashPair(t)
	defer primary.Close()
	defer replica.Close()

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1, Epoch: 5, BaseLSN: 10, TargetLSN: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Start()

	// Stale epoch entry.
	err = session.ApplyWALEntry(&blockvol.WALEntry{
		LSN: 1, Epoch: 3, Type: blockvol.EntryTypeWrite,
		LBA: 0, Length: 4096, Data: bytes.Repeat([]byte{0xAA}, 4096),
	})
	if err == nil {
		t.Fatal("expected epoch mismatch rejection for epoch=3 vs session epoch=5")
	}

	// Correct epoch entry.
	err = session.ApplyWALEntry(&blockvol.WALEntry{
		LSN: 1, Epoch: 5, Type: blockvol.EntryTypeWrite,
		LBA: 0, Length: 4096, Data: bytes.Repeat([]byte{0xBB}, 4096),
	})
	if err != nil {
		t.Fatalf("correct epoch should be accepted: %v", err)
	}
	t.Log("epoch mismatch correctly rejected; matching epoch accepted")
}

// TestRebuild_SessionFailDoesNotAutoEscalate verifies that calling Fail()
// on a session leaves it in failed state but doesn't affect the volume's
// ability to start a new session.
func TestRebuild_SessionFailDoesNotAutoEscalate(t *testing.T) {
	primary, replica := createRebuildCrashPair(t)
	defer primary.Close()
	defer replica.Close()

	// Start and fail session 1.
	replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 1, Epoch: 1, BaseLSN: 10, TargetLSN: 10,
	})
	replica.CancelRebuildSession(1, "transport_lost")

	// Verify no active session.
	_, _, ok := replica.ActiveRebuildSession()
	if ok {
		t.Fatal("expected no active session after cancel")
	}

	// Start fresh session 2 — should work fine.
	err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 2, Epoch: 1, BaseLSN: 20, TargetLSN: 20,
	})
	if err != nil {
		t.Fatalf("start session 2 after failure: %v", err)
	}
	cfg, _, ok := replica.ActiveRebuildSession()
	if !ok || cfg.SessionID != 2 {
		t.Fatalf("expected active session 2, got ok=%v id=%d", ok, cfg.SessionID)
	}
	t.Log("session failure does not prevent starting a new session")
}

// TestRebuild_LargeScale_100Blocks verifies rebuild correctness at moderate
// scale with interleaved base and WAL operations.
func TestRebuild_LargeScale_100Blocks(t *testing.T) {
	primary, replica := createRebuildCrashPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write 100 blocks on primary.
	numBlocks := 100
	expected := make(map[uint64][]byte)
	for i := 0; i < numBlocks; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 4096)
		expected[uint64(i)] = data
		primary.WriteLBA(uint64(i), data)
	}
	primary.SyncCache()
	primary.ForceFlush()
	baseLSN := primary.Status().WALHeadLSN

	session, _ := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 10,
	})
	session.Start()

	// Interleave: base 0-49, WAL for 25-74, base 50-99.
	// LBAs 25-49 will be WAL-protected when base arrives.
	// LBAs 50-74 will be WAL-protected, base skipped.

	// WAL lane: overwrite LBAs 25-74 with different data.
	for lba := uint64(25); lba < 75; lba++ {
		newData := bytes.Repeat([]byte{byte(lba + 128)}, 4096)
		expected[lba] = newData // WAL data is what we expect
		session.ApplyWALEntry(&blockvol.WALEntry{
			LSN: baseLSN + (lba - 24), Epoch: 1, Type: blockvol.EntryTypeWrite,
			LBA: lba, Length: 4096, Data: newData,
		})
	}

	// Base lane: all 100 blocks from primary extent.
	info := primary.Info()
	for lba := uint64(0); lba < uint64(numBlocks); lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		session.ApplyBaseBlock(lba, data)
	}
	session.MarkBaseComplete(uint64(numBlocks))

	// Remaining WAL to reach target — write to high LBAs that don't conflict
	// with the test data at LBAs 0-99.
	for i := uint64(51); i <= 60; i++ {
		lba := uint64(200) + i // use LBAs 251-260, outside test range
		session.ApplyWALEntry(&blockvol.WALEntry{
			LSN: baseLSN + i, Epoch: 1, Type: blockvol.EntryTypeWrite,
			LBA: lba, Length: 4096, Data: bytes.Repeat([]byte{0xFF}, 4096),
		})
	}

	achieved, completed := session.TryComplete()
	if !completed {
		t.Fatalf("session did not complete: achieved=%d", achieved)
	}

	// Verify all 100 blocks.
	progress := session.Progress()
	t.Logf("100-block rebuild: applied=%d skipped=%d bitmap=%d",
		progress.BaseBlocksApplied, progress.BaseBlocksSkipped, progress.BitmapAppliedCount)

	for lba := uint64(0); lba < uint64(numBlocks); lba++ {
		got, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, expected[lba]) {
			t.Fatalf("LBA %d mismatch: got[0]=0x%02x want[0]=0x%02x", lba, got[0], expected[lba][0])
		}
	}
	t.Logf("all %d blocks verified correct", numBlocks)
}

// --- Helpers ---

func createRebuildCrashPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
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
