package weed_server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// ============================================================
// Phase 10 P4: Adversarial — stale epoch on PROMOTED volume
//
// Corrected: targets the promoted BS + promoted volume path,
// matching the real failover test shape.
// ============================================================

func TestAdversarial_P10P4_StaleEpochOnPromotedVolume(t *testing.T) {
	s := newP4Setup(t)

	// Step 1: create volume at epoch 1 (primary=vs1, replica=vs2).
	ctx := context.Background()
	createResp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "pvc-stale-2",
		SizeBytes:      1 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatal(err)
	}
	primaryVS := createResp.VolumeServer
	t.Logf("created: primary=%s", primaryVS)

	// Deliver epoch 1 to primary BS.
	s.deliverAssignments(primaryVS)
	time.Sleep(200 * time.Millisecond)

	// Step 2: failover → epoch 2, new primary is the old replica.
	s.ms.blockRegistry.UpdateEntry("pvc-stale-2", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	s.ms.failoverBlockVolumes(primaryVS)
	time.Sleep(100 * time.Millisecond)

	entryAfter, ok := s.ms.blockRegistry.Lookup("pvc-stale-2")
	if !ok {
		t.Fatal("volume not found after failover")
	}
	if entryAfter.Epoch != 2 {
		t.Fatalf("registry epoch: %d, want 2", entryAfter.Epoch)
	}
	newPrimary := entryAfter.VolumeServer
	t.Logf("after failover: new primary=%s epoch=2", newPrimary)

	// Step 3: create a SEPARATE promoted BlockService (same as P4 failover test).
	// This is the promoted VS — it has its own store with the promoted volume.
	sanitized := strings.ReplaceAll(newPrimary, ":", "_")
	promotedDir := filepath.Join(s.dir, sanitized+"_promoted")
	promotedStore := storage.NewBlockVolumeStore()
	volPath := filepath.Join(s.dir, sanitized, "pvc-stale-2.blk")
	if _, err := promotedStore.AddBlockVolume(volPath, ""); err != nil {
		t.Fatalf("add promoted vol: %v", err)
	}
	promotedBS := &BlockService{
		blockStore:    promotedStore,
		blockDir:      promotedDir,
		listenAddr:    "127.0.0.1:3260",
		localServerID: newPrimary,
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		replStates:     make(map[string]*volReplState),
	}
	promotedBS.v2Recovery = NewRecoveryManager(promotedBS)
	t.Cleanup(func() {
		promotedBS.v2Recovery.Shutdown()
		promotedStore.Close()
	})

	// Deliver epoch 2 assignment to promoted BS (not s.bs).
	pending := s.ms.blockAssignmentQueue.Peek(newPrimary)
	if len(pending) > 0 {
		protoAssignments := blockvol.AssignmentsToProto(pending)
		goAssignments := blockvol.AssignmentsFromProto(protoAssignments)
		promotedBS.ProcessAssignments(goAssignments)
	}
	time.Sleep(200 * time.Millisecond)

	// Verify: promoted vol has epoch 2.
	var promotedEpoch uint64
	promotedStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		promotedEpoch = vol.Epoch()
		return nil
	})
	if promotedEpoch != 2 {
		t.Fatalf("promoted vol epoch: %d, want 2", promotedEpoch)
	}

	// Step 4: deliver STALE epoch 1 to the PROMOTED BS.
	staleAssignment := blockvol.BlockVolumeAssignment{
		Path:       volPath,
		Epoch:      1, // STALE — lower than current epoch 2
		Role:       uint32(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	}
	promotedBS.ProcessAssignments([]blockvol.BlockVolumeAssignment{staleAssignment})
	time.Sleep(100 * time.Millisecond)

	// Step 5: verify NOT reverted on the PROMOTED volume.
	var afterStale uint64
	promotedStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		afterStale = vol.Epoch()
		return nil
	})
	if afterStale < 2 {
		t.Fatalf("BUG: promoted vol epoch reverted from 2 to %d after stale delivery", afterStale)
	}

	t.Logf("adversarial: stale epoch 1 on promoted vol → epoch stays at %d (HandleAssignment rejects regression)", afterStale)
}
