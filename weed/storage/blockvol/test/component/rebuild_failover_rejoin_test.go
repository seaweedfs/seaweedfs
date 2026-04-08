package component

// R10: Failover-rejoin rebuild — the highest-value P1 scenario.
//
// Exercises all three authority layers in sequence:
//   Assignment: master swaps primary/replica roles
//   Session:    new primary decides old primary needs rebuild
//   Projection: rebuild completes, volume returns to healthy
//
// Production scenario:
//   1. Primary (node A) serves writes
//   2. Primary dies (network partition, crash, etc.)
//   3. Replica (node B) is promoted to primary by master
//   4. New primary (node B) continues serving writes
//   5. Old primary (node A) restarts as replica
//   6. New primary evaluates: old primary's data is stale → rebuild
//   7. Rebuild runs, old primary converges with new primary
//   8. Both nodes have identical extent data

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/sw-block/protocol"
)

// TestRebuild_R10_FailoverRejoinRebuild exercises the full failover-rejoin
// cycle with rebuild and extent CRC validation.
func TestRebuild_R10_FailoverRejoinRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode")
	}

	nodeAPath := filepath.Join(t.TempDir(), "nodeA.blk")
	nodeBPath := filepath.Join(t.TempDir(), "nodeB.blk")

	blockSize := uint32(4096)
	opts := blockvol.CreateOptions{
		VolumeSize: 8 * 1024 * 1024, // 8MB
		BlockSize:  blockSize,
		WALSize:    128 * 1024, // 128KB — small WAL to force recycling
	}
	totalLBAs := opts.VolumeSize / uint64(blockSize)

	// ---------------------------------------------------------------
	// Phase 1: Node A = primary, Node B = replica, both in sync
	// ---------------------------------------------------------------
	nodeA, err := blockvol.CreateBlockVol(nodeAPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	nodeA.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	nodeB, err := blockvol.CreateBlockVol(nodeBPath, opts)
	if err != nil {
		nodeA.Close()
		t.Fatal(err)
	}
	defer nodeB.Close()
	nodeB.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	// Write initial data on primary (node A).
	initialBlocks := 200
	for i := 0; i < initialBlocks; i++ {
		data := deterministicBlockR10(uint64(i), 1, blockSize)
		if err := nodeA.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("nodeA initial write LBA %d: %v", i, err)
		}
	}
	nodeA.SyncCache()
	nodeA.ForceFlush()

	// Simulate sync: copy initial data to node B.
	for i := 0; i < initialBlocks; i++ {
		data, _ := nodeA.ReadLBA(uint64(i), blockSize)
		nodeB.WriteLBA(uint64(i), data)
	}
	nodeB.SyncCache()
	nodeB.ForceFlush()

	nodeALSN_before := nodeA.Status().WALHeadLSN
	nodeBLSN_before := nodeB.Status().WALHeadLSN
	t.Logf("Phase 1: both synced. nodeA LSN=%d, nodeB LSN=%d, %d blocks",
		nodeALSN_before, nodeBLSN_before, initialBlocks)

	// ---------------------------------------------------------------
	// Phase 2: Node A "dies" — close it
	// ---------------------------------------------------------------
	nodeA.Close()
	t.Log("Phase 2: nodeA (old primary) died")

	// ---------------------------------------------------------------
	// Phase 3: Node B promoted to primary (epoch bump)
	// ---------------------------------------------------------------
	nodeB.HandleAssignment(2, blockvol.RolePrimary, 30*time.Second)
	t.Log("Phase 3: nodeB promoted to primary (epoch=2)")

	// New primary (node B) writes enough to force WAL recycling past nodeA's
	// applied_lsn. NodeA had applied_lsn=200 from its time as primary. NodeB
	// must advance its wal_tail past 200 so the engine sees the gap.
	// With 128KB WAL (~32 blocks fit), we need many flush cycles.
	postFailoverWrites := 0
	for round := 0; round < 10; round++ {
		for i := 0; i < 50; i++ {
			lba := uint64(i % initialBlocks)
			data := deterministicBlockR10(lba, uint64(round*50+i+2), blockSize)
			if err := nodeB.WriteLBA(lba, data); err != nil {
				t.Fatalf("nodeB post-failover write round=%d i=%d: %v", round, i, err)
			}
			postFailoverWrites++
		}
		nodeB.SyncCache()
		nodeB.ForceFlush()
	}
	nodeBLSN_after := nodeB.Status().WALHeadLSN
	t.Logf("Phase 3: nodeB wrote %d blocks across 4 flush cycles, LSN=%d", postFailoverWrites, nodeBLSN_after)

	// ---------------------------------------------------------------
	// Phase 4: Node A restarts as replica
	// ---------------------------------------------------------------
	nodeA, err = blockvol.OpenBlockVol(nodeAPath)
	if err != nil {
		t.Fatalf("reopen nodeA: %v", err)
	}
	defer nodeA.Close()
	nodeA.HandleAssignment(2, blockvol.RoleReplica, 30*time.Second)
	nodeALSN_restart := nodeA.Status().WALHeadLSN
	t.Logf("Phase 4: nodeA restarted as replica, WALHeadLSN=%d (stale)", nodeALSN_restart)

	// ---------------------------------------------------------------
	// Phase 5: New primary (nodeB) decides: old primary needs rebuild
	// ---------------------------------------------------------------
	eng := protocol.NewEngine()
	eng.ApplyEvent(protocol.AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 2, Role: protocol.RolePrimary,
		Replicas: []protocol.ReplicaAssignment{
			{ReplicaID: "nodeA", Endpoint: protocol.Endpoint{DataAddr: "a", CtrlAddr: "b"}},
		},
	})
	boolTrue := true
	eng.ApplyEvent(protocol.ReadinessObserved{
		VolumeID: "vol-1", RoleApplied: &boolTrue,
		ShipperConfigured: &boolTrue, ShipperConnected: &boolTrue,
	})

	nodeBWALTail := nodeB.Status().CheckpointLSN
	result := eng.ApplyEvent(protocol.SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "nodeA",
		Ack:            protocol.SyncAck{AppliedLSN: nodeALSN_restart, DurableLSN: nodeALSN_restart},
		PrimaryWALTail: nodeBWALTail,
		PrimaryWALHead: nodeBLSN_after,
	})

	var rebuildCmd *protocol.IssueRebuildCommand
	var catchUpCmd *protocol.IssueCatchUpCommand
	for _, cmd := range result.Commands {
		if c, ok := cmd.(protocol.IssueRebuildCommand); ok {
			rebuildCmd = &c
		}
		if c, ok := cmd.(protocol.IssueCatchUpCommand); ok {
			catchUpCmd = &c
		}
	}

	// Engine MUST decide rebuild — nodeA's applied_lsn is beyond nodeB's retained WAL.
	if rebuildCmd == nil {
		if catchUpCmd != nil {
			t.Fatalf("R10: engine chose catch-up but WAL should be recycled past nodeA's position "+
				"(nodeA applied=%d, nodeB wal_tail=%d)", nodeALSN_restart, nodeBWALTail)
		}
		t.Fatalf("R10: engine chose keepup — WAL recycling insufficient "+
			"(nodeA applied=%d, nodeB wal_tail=%d, nodeB head=%d)",
			nodeALSN_restart, nodeBWALTail, nodeBLSN_after)
	}
	t.Logf("Phase 5: engine decided REBUILD (nodeA applied=%d < nodeB wal_tail=%d)",
		nodeALSN_restart, nodeBWALTail)

	// ---------------------------------------------------------------
	// Phase 6: Rebuild node A from node B
	// ---------------------------------------------------------------
	sessionID := uint64(50)
	baseLSN := nodeBLSN_after

	if err := nodeA.StartRebuildSession(blockvol.RebuildSessionConfig{
		SessionID: sessionID,
		Epoch:     2,
		BaseLSN:   baseLSN,
		TargetLSN: baseLSN,
	}); err != nil {
		t.Fatalf("start rebuild on nodeA: %v", err)
	}
	defer nodeA.CancelRebuildSession(sessionID, "test_done")

	// Base lane: copy all blocks from nodeB (new primary) to nodeA (old primary).
	info := nodeB.Info()
	for lba := uint64(0); lba < totalLBAs; lba++ {
		data, err := nodeB.ReadLBA(lba, uint32(info.BlockSize))
		if err != nil {
			t.Fatalf("nodeB read LBA %d: %v", lba, err)
		}
		if _, err := nodeA.ApplyRebuildSessionBaseBlock(sessionID, lba, data); err != nil {
			t.Fatalf("nodeA base apply LBA %d: %v", lba, err)
		}
	}
	nodeA.MarkRebuildSessionBaseComplete(sessionID, totalLBAs)

	// WAL entry to satisfy target.
	nodeA.ApplyRebuildSessionWALEntry(sessionID, &blockvol.WALEntry{
		LSN: baseLSN, Epoch: 2, Type: blockvol.EntryTypeWrite,
		LBA: totalLBAs - 1, Length: uint32(blockSize),
		Data: make([]byte, blockSize), // dummy, last LBA
	})

	achieved, completed, err := nodeA.TryCompleteRebuildSession(sessionID)
	if err != nil {
		t.Fatalf("try complete: %v", err)
	}
	if !completed {
		_, progress, _ := nodeA.ActiveRebuildSession()
		t.Fatalf("rebuild not completed: walApplied=%d target=%d base=%v",
			progress.WALAppliedLSN, baseLSN, progress.BaseComplete)
	}
	t.Logf("Phase 6: rebuild completed, achieved=%d", achieved)

	// ---------------------------------------------------------------
	// Phase 7: Flush both and compare extent CRC
	// ---------------------------------------------------------------
	nodeB.SyncCache()
	nodeB.ForceFlush()
	nodeA.ForceFlush()

	t.Log("Phase 7: comparing extents...")
	nodeBHash := sha256.New()
	nodeAHash := sha256.New()
	mismatches := 0

	for lba := uint64(0); lba < totalLBAs; lba++ {
		bData, err := nodeB.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("nodeB read LBA %d: %v", lba, err)
		}
		aData, err := nodeA.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("nodeA read LBA %d: %v", lba, err)
		}
		nodeBHash.Write(bData)
		nodeAHash.Write(aData)

		if !bytes.Equal(bData, aData) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("LBA %d MISMATCH: nodeB[0]=0x%02x nodeA[0]=0x%02x", lba, bData[0], aData[0])
			}
		}
	}

	bCRC := fmt.Sprintf("%x", nodeBHash.Sum(nil))
	aCRC := fmt.Sprintf("%x", nodeAHash.Sum(nil))

	if mismatches > 0 {
		t.Fatalf("R10 FAILED: %d/%d blocks mismatch. nodeB CRC=%s...%s nodeA CRC=%s...%s",
			mismatches, totalLBAs, bCRC[:8], bCRC[len(bCRC)-8:], aCRC[:8], aCRC[len(aCRC)-8:])
	}
	if bCRC != aCRC {
		t.Fatalf("R10 FAILED: CRC mismatch. nodeB=%s nodeA=%s", bCRC, aCRC)
	}

	t.Logf("R10 PASSED: failover-rejoin rebuild complete")
	t.Logf("  nodeA: old primary → died → restarted as replica → rebuilt from nodeB")
	t.Logf("  nodeB: replica → promoted to primary → served %d post-failover writes → rebuilt nodeA", postFailoverWrites)
	t.Logf("  %d blocks, CRC=%s...%s", totalLBAs, bCRC[:8], bCRC[len(bCRC)-8:])
}

// deterministicBlockR10 creates a deterministic block for the failover test.
func deterministicBlockR10(lba uint64, gen uint64, blockSize uint32) []byte {
	data := make([]byte, blockSize)
	seed := byte((lba*11 + gen*17) & 0xFF)
	for i := range data {
		data[i] = seed ^ byte(i&0xFF)
	}
	data[0] = byte(lba & 0xFF)
	data[1] = byte((lba >> 8) & 0xFF)
	data[8] = byte(gen & 0xFF)
	data[9] = byte((gen >> 8) & 0xFF)
	return data
}
