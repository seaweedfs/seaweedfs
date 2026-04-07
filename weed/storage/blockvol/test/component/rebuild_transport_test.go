package component

// Component tests for rebuild with real WAL transport between two BlockVol
// instances. Unlike rebuild_mvp_test.go (direct function calls), these tests
// use real TCP shipping for the WAL lane and real extent read for the base lane.
//
// Architecture:
//   Primary BlockVol (real WAL + extent + snapshot)
//     ├─ WAL lane: ShipAll → TCP → ReplicaReceiver → replica WAL
//     └─ Base lane: read primary extent → ApplyBaseBlock on session
//
//   Replica BlockVol (real WAL + extent + RebuildSession + bitmap)

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestRebuild_Transport_TwoLineWithRealShipping exercises the full two-line
// rebuild with real TCP WAL shipping between primary and replica.
//
// Flow:
//   1. Primary writes 20 blocks (LBA 0-19), creating WAL entries
//   2. Replica starts receiver, primary wires shipper (real TCP)
//   3. Rebuild session on replica: base lane + WAL lane in parallel
//   4. Base lane reads primary extent directly, sends to replica session
//   5. WAL lane: primary continues writing, entries ship via TCP
//   6. Verify replica has all data correct after rebuild completes
func TestRebuild_Transport_TwoLineWithRealShipping(t *testing.T) {
	primary, replica := createTransportRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	// Step 1: Write initial data on primary (pre-rebuild baseline).
	initialBlocks := 20
	blockData := make(map[uint64][]byte)
	for i := 0; i < initialBlocks; i++ {
		data := bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		blockData[uint64(i)] = data
		if err := primary.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("primary write LBA %d: %v", i, err)
		}
	}
	// Flush primary so extent has the data (needed for base lane read).
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("primary SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("primary ForceFlush: %v", err)
	}

	baseLSN := primary.Status().WALHeadLSN
	t.Logf("primary baseline: %d blocks, WALHeadLSN=%d", initialBlocks, baseLSN)

	// Step 2: Wire real TCP shipping (WAL lane transport).
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)
	t.Logf("WAL lane wired: primary → %s/%s", recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Step 3: Create rebuild session on replica.
	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 1,
		Epoch:     1,
		BaseLSN:   baseLSN,
		TargetLSN: baseLSN + 5, // expect 5 more WAL entries during rebuild
	})
	if err != nil {
		t.Fatalf("new rebuild session: %v", err)
	}
	if err := session.Start(); err != nil {
		t.Fatalf("start session: %v", err)
	}

	// Step 4: Base lane — read primary extent, send to replica session.
	// This simulates the snapshot/base block transfer.
	info := primary.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	baseApplied := 0
	baseSkipped := 0
	for lba := uint64(0); lba < totalLBAs && lba < uint64(initialBlocks); lba++ {
		extentData, err := primary.ReadLBA(lba, uint32(info.BlockSize))
		if err != nil {
			t.Fatalf("primary read LBA %d: %v", lba, err)
		}
		applied, err := session.ApplyBaseBlock(lba, extentData)
		if err != nil {
			t.Fatalf("base apply LBA %d: %v", lba, err)
		}
		if applied {
			baseApplied++
		} else {
			baseSkipped++
		}
	}
	session.MarkBaseComplete(uint64(initialBlocks))
	t.Logf("base lane: %d applied, %d skipped (WAL conflict)", baseApplied, baseSkipped)

	// Step 5: WAL lane — primary writes more blocks, shipping via real TCP.
	// These writes go through ShipAll → TCP → ReplicaReceiver on replica.
	liveBlocks := 5
	for i := 0; i < liveBlocks; i++ {
		lba := uint64(initialBlocks + i)
		data := bytes.Repeat([]byte{byte(0xF0 + i)}, 4096)
		blockData[lba] = data
		if err := primary.WriteLBA(lba, data); err != nil {
			t.Fatalf("primary live write LBA %d: %v", lba, err)
		}
	}

	// Wait for WAL entries to arrive at replica via TCP.
	time.Sleep(1 * time.Second)

	// Also apply the live WAL entries to the rebuild session.
	// In production, the replica receiver would route these to the session.
	// Here we manually apply them since the receiver doesn't know about
	// the rebuild session yet (that wiring is a server-layer concern).
	for i := 0; i < liveBlocks; i++ {
		lba := uint64(initialBlocks + i)
		entry := &blockvol.WALEntry{
			LSN:    baseLSN + uint64(i) + 1,
			Epoch:  1,
			Type:   blockvol.EntryTypeWrite,
			LBA:    lba,
			Length: 4096,
			Data:   blockData[lba],
		}
		if err := session.ApplyWALEntry(entry); err != nil {
			t.Fatalf("session WAL apply LBA %d: %v", lba, err)
		}
	}

	// Step 6: Try completion.
	achievedLSN, completed := session.TryComplete()
	if !completed {
		progress := session.Progress()
		t.Fatalf("session did not complete: walApplied=%d target=%d baseComplete=%v phase=%s",
			progress.WALAppliedLSN, baseLSN+5, progress.BaseComplete, progress.Phase)
	}
	t.Logf("rebuild completed: achievedLSN=%d", achievedLSN)

	// Step 7: Verify ALL blocks on replica.
	for lba, expected := range blockData {
		got, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("replica LBA %d mismatch: got[0]=0x%02x want[0]=0x%02x", lba, got[0], expected[0])
		}
	}
	t.Logf("all %d blocks verified on replica", len(blockData))
}

// TestRebuild_Transport_LiveWritesDuringBaseCopy verifies that writes
// happening on the primary DURING base copy are correctly handled.
// The WAL lane ships them via TCP, and bitmap protects them from being
// overwritten by the base copy.
func TestRebuild_Transport_LiveWritesDuringBaseCopy(t *testing.T) {
	primary, replica := createTransportRebuildPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write initial data.
	for i := 0; i < 10; i++ {
		data := bytes.Repeat([]byte{byte(0x10 + i)}, 4096)
		if err := primary.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("primary write LBA %d: %v", i, err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if err := primary.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	baseLSN := primary.Status().WALHeadLSN

	// Wire TCP shipping.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	session, err := blockvol.NewRebuildSession(replica, blockvol.RebuildSessionConfig{
		SessionID: 2, Epoch: 1, BaseLSN: baseLSN, TargetLSN: baseLSN + 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Start()

	// Simulate interleaved base copy + live writes:
	// 1. Copy base blocks 0-4
	// 2. Primary writes NEW data to LBA 3 (live write during rebuild)
	// 3. Apply that live write via WAL lane to session
	// 4. Copy base blocks 5-9 (LBA 3 already covered by WAL)

	// Base blocks 0-4.
	info := primary.Info()
	for lba := uint64(0); lba < 5; lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		session.ApplyBaseBlock(lba, data)
	}

	// Live write to LBA 3 (overrides what base just wrote).
	liveData := bytes.Repeat([]byte{0xFF}, 4096)
	if err := primary.WriteLBA(3, liveData); err != nil {
		t.Fatalf("primary live write LBA 3: %v", err)
	}

	// Apply live WAL entry to session (WAL lane).
	session.ApplyWALEntry(&blockvol.WALEntry{
		LSN: baseLSN + 1, Epoch: 1, Type: blockvol.EntryTypeWrite,
		LBA: 3, Length: 4096, Data: liveData,
	})

	// Base blocks 5-9.
	for lba := uint64(5); lba < 10; lba++ {
		data, _ := primary.ReadLBA(lba, uint32(info.BlockSize))
		session.ApplyBaseBlock(lba, data)
	}

	// Try to re-send base block for LBA 3 — should be SKIPPED (bitmap set).
	oldData := bytes.Repeat([]byte{0x13}, 4096) // original data at LBA 3
	applied, _ := session.ApplyBaseBlock(3, oldData)
	if applied {
		t.Fatal("BUG: base block for LBA 3 applied AFTER live WAL write")
	}

	// Apply remaining WAL entries to reach target.
	for i := uint64(2); i <= 3; i++ {
		session.ApplyWALEntry(&blockvol.WALEntry{
			LSN: baseLSN + i, Epoch: 1, Type: blockvol.EntryTypeWrite,
			LBA: uint64(i + 5), Length: 4096,
			Data: bytes.Repeat([]byte{byte(0xE0 + i)}, 4096),
		})
	}
	session.MarkBaseComplete(10)

	achievedLSN, completed := session.TryComplete()
	if !completed {
		t.Fatalf("session did not complete")
	}
	t.Logf("completed: achievedLSN=%d", achievedLSN)

	// Verify LBA 3 has the LIVE data (0xFF), not old base (0x13).
	got, err := replica.ReadLBA(3, 4096)
	if err != nil {
		t.Fatalf("read LBA 3: %v", err)
	}
	if got[0] != 0xFF {
		t.Fatalf("LBA 3 = 0x%02x, want 0xFF (live write during rebuild must win)", got[0])
	}
	t.Log("live write during base copy correctly preserved via bitmap")
}

// --- Helpers ---

func createTransportRebuildPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize:     4 * 1024 * 1024, // 4MB
		BlockSize:      4096,
		WALSize:        1 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
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
