package component

// Component tests for the rebuild MVP session protocol.
//
// These prove the three core correctness invariants from
// v2-rebuild-mvp-session-protocol.md:
//
//   1. Base lane + WAL lane converge to target
//   2. WAL-applied LBA is never overwritten by later base-copy data
//   3. Bitmap bit is set on "applied" (local WAL write), not "received"
//
// Each test uses real BlockVol (WAL, extent, dirty map) but no network.
// The rebuild session is exercised directly in-process.

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Test 1: Base lane + WAL lane converge to target.
//
// Scenario: primary has 10 blocks of data. Rebuild session receives
// base blocks for all 10 LBAs AND WAL entries for some of them.
// After both lanes complete, the replica has all 10 blocks readable
// and the session reaches Completed phase.
func TestRebuild_BasePlusWAL_ConvergesToTarget(t *testing.T) {
	primary, replica := createRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write 10 blocks on primary at LBA 0-9.
	blocks := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		blocks[i] = bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		if err := primary.WriteLBA(uint64(i), blocks[i]); err != nil {
			t.Fatalf("primary write LBA %d: %v", i, err)
		}
	}
	primaryHead := primary.Status().WALHeadLSN
	t.Logf("primary WALHeadLSN=%d after 10 writes", primaryHead)

	// Create rebuild session on replica targeting primary's head.
	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1,
		Epoch:     1,
		BaseLSN:   primaryHead,
		TargetLSN: primaryHead,
	})
	if err != nil {
		t.Fatalf("new rebuild session: %v", err)
	}
	if err := session.Start(); err != nil {
		t.Fatalf("start session: %v", err)
	}

	// WAL lane: apply WAL entries for LBAs 0-4 (first half).
	for lba := uint64(0); lba < 5; lba++ {
		entry := &blockvol.WALEntry{
			LSN:    lba + 1, // LSN 1-5
			Epoch:  1,
			Type:   blockvol.EntryTypeWrite,
			LBA:    lba,
			Length: 4096,
			Data:   blocks[lba],
		}
		if err := session.ApplyWALEntry(entry); err != nil {
			t.Fatalf("WAL apply LBA %d: %v", lba, err)
		}
	}

	// Base lane: apply base blocks for LBAs 0-9 (all).
	// LBAs 0-4 should be SKIPPED (bitmap set by WAL lane).
	// LBAs 5-9 should be APPLIED (bitmap clear).
	for lba := uint64(0); lba < 10; lba++ {
		applied, err := session.ApplyBaseBlock(lba, blocks[lba])
		if err != nil {
			t.Fatalf("base apply LBA %d: %v", lba, err)
		}
		if lba < 5 && applied {
			t.Fatalf("LBA %d: base should be skipped (WAL-applied), got applied=true", lba)
		}
		if lba >= 5 && !applied {
			t.Fatalf("LBA %d: base should be applied (bitmap clear), got applied=false", lba)
		}
	}

	// Mark base complete + apply remaining WAL entries to reach target.
	session.MarkBaseComplete(10)
	for lba := uint64(5); lba < 10; lba++ {
		entry := &blockvol.WALEntry{
			LSN:    lba + 1, // LSN 6-10
			Epoch:  1,
			Type:   blockvol.EntryTypeWrite,
			LBA:    lba,
			Length: 4096,
			Data:   blocks[lba],
		}
		if err := session.ApplyWALEntry(entry); err != nil {
			t.Fatalf("WAL apply LBA %d: %v", lba, err)
		}
	}

	// Try completion: both conditions should be met.
	achievedLSN, completed := session.TryComplete()
	if !completed {
		progress := session.Progress()
		t.Fatalf("session did not complete: walApplied=%d target=%d baseComplete=%v",
			progress.WALAppliedLSN, primaryHead, progress.BaseComplete)
	}
	t.Logf("session completed: achievedLSN=%d", achievedLSN)

	// Verify all 10 blocks are readable on replica.
	for lba := uint64(0); lba < 10; lba++ {
		data, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(data, blocks[lba]) {
			t.Fatalf("replica LBA %d mismatch: got[0]=0x%02x want[0]=0x%02x",
				lba, data[0], blocks[lba][0])
		}
	}
	t.Log("all 10 blocks converged correctly on replica")
}

// Test 2: WAL-applied LBA is never overwritten by later base-copy data.
//
// Scenario: WAL entry writes 0xBB to LBA 5, then base lane tries to
// write 0xAA to the same LBA. The base write must be skipped, and the
// replica must read 0xBB (WAL wins).
func TestRebuild_WALApplied_NeverOverwrittenByBase(t *testing.T) {
	primary, replica := createRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 2,
		Epoch:     1,
		BaseLSN:   100,
		TargetLSN: 100,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if err := session.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// WAL lane: apply 0xBB to LBA 5.
	walData := bytes.Repeat([]byte{0xBB}, 4096)
	walEntry := &blockvol.WALEntry{
		LSN:    1,
		Epoch:  1,
		Type:   blockvol.EntryTypeWrite,
		LBA:    5,
		Length: 4096,
		Data:   walData,
	}
	if err := session.ApplyWALEntry(walEntry); err != nil {
		t.Fatalf("WAL apply: %v", err)
	}

	// Base lane: try to apply 0xAA to same LBA 5.
	baseData := bytes.Repeat([]byte{0xAA}, 4096)
	applied, err := session.ApplyBaseBlock(5, baseData)
	if err != nil {
		t.Fatalf("base apply: %v", err)
	}
	if applied {
		t.Fatal("BUG: base block applied to WAL-covered LBA — bitmap conflict not enforced")
	}

	// Read from replica: must be 0xBB (WAL wins), not 0xAA.
	readBack, err := replica.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("replica read: %v", err)
	}
	if readBack[0] != 0xBB {
		t.Fatalf("BUG: replica LBA 5 = 0x%02x, want 0xBB (WAL must win over base)", readBack[0])
	}
	t.Log("WAL-applied LBA correctly protected: base data skipped, WAL data preserved")
}

// Test 3: Bitmap bit is set on "applied" (local WAL write), not "received".
//
// Scenario: We verify the bitmap state at precise points:
//   - Before ApplyWALEntry: bitmap must be clear
//   - After ApplyWALEntry succeeds: bitmap must be set
//
// This proves the bit is set AFTER successful local WAL append, which is
// the key correctness invariant for crash safety.
func TestRebuild_BitmapSetOnApplied_NotReceived(t *testing.T) {
	primary, replica := createRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 3,
		Epoch:     1,
		BaseLSN:   50,
		TargetLSN: 50,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if err := session.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Before WAL apply: base lane should be allowed for LBA 7.
	applied, err := session.ApplyBaseBlock(7, bytes.Repeat([]byte{0x11}, 4096))
	if err != nil {
		t.Fatalf("pre-WAL base apply: %v", err)
	}
	if !applied {
		t.Fatal("base block at LBA 7 should be applied before any WAL entry")
	}

	// Apply WAL entry to LBA 7.
	walEntry := &blockvol.WALEntry{
		LSN:    1,
		Epoch:  1,
		Type:   blockvol.EntryTypeWrite,
		LBA:    7,
		Length: 4096,
		Data:   bytes.Repeat([]byte{0x22}, 4096),
	}
	if err := session.ApplyWALEntry(walEntry); err != nil {
		t.Fatalf("WAL apply LBA 7: %v", err)
	}

	// After WAL apply: base lane must be BLOCKED for LBA 7.
	applied2, err := session.ApplyBaseBlock(7, bytes.Repeat([]byte{0x33}, 4096))
	if err != nil {
		t.Fatalf("post-WAL base apply: %v", err)
	}
	if applied2 {
		t.Fatal("BUG: base block applied after WAL entry — bitmap was not set on apply")
	}

	// Verify replica reads WAL data (0x22), not base (0x11) or second base (0x33).
	readBack, err := replica.ReadLBA(7, 4096)
	if err != nil {
		t.Fatalf("replica read: %v", err)
	}
	if readBack[0] != 0x22 {
		t.Fatalf("replica LBA 7 = 0x%02x, want 0x22 (WAL-applied data)", readBack[0])
	}

	// Verify bitmap count: exactly 1 LBA should be marked.
	progress := session.Progress()
	if progress.BitmapAppliedCount != 1 {
		t.Fatalf("bitmap applied count=%d, want 1", progress.BitmapAppliedCount)
	}
	t.Log("bitmap set on applied (after WAL append), not on received — correctness invariant holds")
}

func TestRebuild_ControlSurface_StartSupersedeAndComplete(t *testing.T) {
	primary, replica := createRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 10,
		Epoch:     1,
		BaseLSN:   1,
		TargetLSN: 1,
	}); err != nil {
		t.Fatalf("start session 10: %v", err)
	}

	cfg, progress, ok := replica.ActiveRebuildSession()
	if !ok {
		t.Fatal("expected active rebuild session")
	}
	if cfg.SessionID != 10 {
		t.Fatalf("active session ID=%d, want 10", cfg.SessionID)
	}
	if progress.Phase != blockvol.RebuildPhaseRunning {
		t.Fatalf("active phase=%s, want running", progress.Phase)
	}

	if err := replica.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: 11,
		Epoch:     1,
		BaseLSN:   1,
		TargetLSN: 1,
	}); err != nil {
		t.Fatalf("start session 11: %v", err)
	}

	cfg, _, ok = replica.ActiveRebuildSession()
	if !ok || cfg.SessionID != 11 {
		t.Fatalf("expected superseded active session 11, got ok=%v id=%d", ok, cfg.SessionID)
	}

	err := replica.ApplyRebuildSessionWALEntry(10, &blockvol.WALEntry{
		LSN:    1,
		Epoch:  1,
		Type:   blockvol.EntryTypeWrite,
		LBA:    0,
		Length: 4096,
		Data:   bytes.Repeat([]byte{0xAA}, 4096),
	})
	if err == nil {
		t.Fatal("expected stale session ID to be rejected")
	}

	if err := replica.ApplyRebuildSessionWALEntry(11, &blockvol.WALEntry{
		LSN:    1,
		Epoch:  1,
		Type:   blockvol.EntryTypeWrite,
		LBA:    0,
		Length: 4096,
		Data:   bytes.Repeat([]byte{0xBB}, 4096),
	}); err != nil {
		t.Fatalf("apply WAL through control surface: %v", err)
	}
	if err := replica.MarkRebuildSessionBaseComplete(11, 0); err != nil {
		t.Fatalf("mark base complete: %v", err)
	}
	achieved, completed, err := replica.TryCompleteRebuildSession(11)
	if err != nil {
		t.Fatalf("try complete: %v", err)
	}
	if !completed || achieved != 1 {
		t.Fatalf("completion result achieved=%d completed=%v, want achieved=1 completed=true", achieved, completed)
	}

	_, progress, ok = replica.ActiveRebuildSession()
	if !ok {
		t.Fatal("expected completed session to remain queryable")
	}
	if !progress.Completed() {
		t.Fatalf("progress phase=%s, want completed", progress.Phase)
	}

	if err := replica.CancelRebuildSession(11, "test_done"); err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	if _, _, ok := replica.ActiveRebuildSession(); ok {
		t.Fatal("expected no active session after cancel")
	}
}

// --- Helpers ---

func createRebuildPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024, // 4MB = 1024 LBAs at 4K
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
	}
	p, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second); err != nil {
		p.Close()
		t.Fatal(err)
	}
	r, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "replica.blk"), opts)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	if err := r.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second); err != nil {
		p.Close()
		r.Close()
		t.Fatal(err)
	}
	return p, r
}
