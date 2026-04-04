package weed_server

import (
	"path/filepath"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// ============================================================
// Phase 10 P2: Reassignment / result convergence
//
// Proofs:
//   1. Reassignment convergence: epoch bump updates control truth
//   2. Stale owner removal: old sender/session gone after reassignment
//   3. Reported truth: heartbeat reflects new replica assignment
//   4. No split truth: ingress + runtime + reported all agree
// ============================================================

func createP2BlockService(t *testing.T) (*BlockService, string) {
	t.Helper()
	dir := t.TempDir()

	volPath := filepath.Join(dir, "vol1.blk")
	vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	vol.Close()

	store := storage.NewBlockVolumeStore()
	if _, err := store.AddBlockVolume(volPath, ""); err != nil {
		t.Fatalf("AddBlockVolume: %v", err)
	}

	bs := &BlockService{
		blockStore:     store,
		blockDir:       dir,
		listenAddr:     "127.0.0.1:3260",
		localServerID:  "vs1-node:18080",
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		v2Core:         engine.NewCoreEngine(),
		replStates:     make(map[string]*volReplState),
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	t.Cleanup(func() {
		bs.v2Recovery.Shutdown()
		store.Close()
	})

	return bs, volPath
}

// --- 1. Reassignment convergence: epoch bump updates control truth ---

func TestP10P2_ReassignmentConvergence_EpochBump(t *testing.T) {
	bs, volPath := createP2BlockService(t)

	// Epoch 1: primary with replica vs2.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	time.Sleep(200 * time.Millisecond)

	replicaID1 := volPath + "/vs2-node:18080"
	s1 := bs.v2Orchestrator.Registry.Sender(replicaID1)
	if s1 == nil {
		t.Fatal("epoch 1: sender for vs2 not created")
	}
	epoch1Session := s1.SessionID()
	t.Logf("epoch 1: sender=%s sessionID=%d", replicaID1, epoch1Session)

	// Epoch 2: primary with DIFFERENT replica vs3.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs3-node:18080",
			ReplicaDataAddr: "10.0.0.3:14260",
			ReplicaCtrlAddr: "10.0.0.3:14261",
		},
	})

	time.Sleep(200 * time.Millisecond)

	// New replica sender should exist.
	replicaID2 := volPath + "/vs3-node:18080"
	s2 := bs.v2Orchestrator.Registry.Sender(replicaID2)
	if s2 == nil {
		t.Fatal("epoch 2: sender for vs3 not created")
	}
	t.Logf("epoch 2: sender=%s sessionID=%d", replicaID2, s2.SessionID())

	// Old replica sender should be gone (removed by Reconcile).
	s1After := bs.v2Orchestrator.Registry.Sender(replicaID1)
	if s1After != nil {
		t.Fatalf("epoch 2: old sender %s should be removed", replicaID1)
	}

	t.Log("P10P2 convergence: epoch bump → old replica removed, new replica active")
}

// --- 2. Stale owner removal: live old recovery cancelled during reassignment ---

func TestP10P2_StaleOwnerRemoval(t *testing.T) {
	bs, volPath := createP2BlockService(t)
	rm := bs.v2Recovery

	// Write data so recovery has real work (not zero-gap auto-complete).
	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Hook: block the first recovery goroutine so it's alive during reassignment.
	holdFirst := make(chan struct{})
	firstReached := make(chan struct{}, 1)
	callCount := 0
	rm.OnBeforeExecute = func(replicaID string) {
		callCount++
		if callCount == 1 {
			firstReached <- struct{}{}
			<-holdFirst // block until released
		}
	}

	// Epoch 1: start recovery for vs2 (will block at hook).
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	// Wait for old goroutine to be alive.
	select {
	case <-firstReached:
		t.Log("old recovery goroutine for vs2 is alive and blocked")
	case <-time.After(5 * time.Second):
		t.Fatal("old goroutine did not reach hook")
	}

	// Capture old task's done channel.
	oldReplicaID := volPath + "/vs2-node:18080"
	rm.mu.Lock()
	oldTask := rm.tasks[oldReplicaID]
	rm.mu.Unlock()
	if oldTask == nil {
		t.Fatal("old task must exist while goroutine is blocked")
	}
	oldDone := oldTask.done

	// Release old goroutine from another goroutine.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(holdFirst)
	}()

	// Epoch 2: reassignment to vs3. cancelAndDrain waits for old to exit.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs3-node:18080",
			ReplicaDataAddr: "10.0.0.3:14260",
			ReplicaCtrlAddr: "10.0.0.3:14261",
		},
	})

	// Old goroutine must be drained.
	select {
	case <-oldDone:
		t.Log("confirmed: old recovery goroutine drained during reassignment")
	default:
		t.Fatal("old done channel still open after reassignment")
	}

	// Old task must be gone.
	rm.mu.Lock()
	_, hasOld := rm.tasks[oldReplicaID]
	rm.mu.Unlock()
	if hasOld {
		t.Fatal("stale recovery task should be gone")
	}

	t.Log("P10P2 stale removal: live old owner cancelled + drained during reassignment")
}

// --- 3. Reported truth: actual heartbeat output reflects new replica ---

func TestP10P2_ReportedTruth_HeartbeatConverges(t *testing.T) {
	bs, volPath := createP2BlockService(t)

	// Epoch 1: primary with vs2.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	time.Sleep(100 * time.Millisecond)

	// Verify actual heartbeat output reports vs2.
	msgs1 := bs.CollectBlockVolumeHeartbeat()
	found1 := findHeartbeatMsg(msgs1, volPath)
	if found1 == nil {
		t.Fatal("epoch 1: volume not in heartbeat output")
	}
	if found1.ReplicaDataAddr != "10.0.0.2:14260" {
		t.Fatalf("epoch 1 heartbeat: ReplicaDataAddr=%q, want 10.0.0.2:14260", found1.ReplicaDataAddr)
	}
	t.Logf("epoch 1 heartbeat: ReplicaDataAddr=%s ✓", found1.ReplicaDataAddr)

	// Epoch 2: primary with vs3.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs3-node:18080",
			ReplicaDataAddr: "10.0.0.3:14260",
			ReplicaCtrlAddr: "10.0.0.3:14261",
		},
	})

	time.Sleep(100 * time.Millisecond)

	// Actual heartbeat output should report vs3, NOT stale vs2.
	msgs2 := bs.CollectBlockVolumeHeartbeat()
	found2 := findHeartbeatMsg(msgs2, volPath)
	if found2 == nil {
		t.Fatal("epoch 2: volume not in heartbeat output")
	}
	if found2.ReplicaDataAddr != "10.0.0.3:14260" {
		t.Fatalf("epoch 2 heartbeat: ReplicaDataAddr=%q, want 10.0.0.3:14260 (stale vs2)", found2.ReplicaDataAddr)
	}
	if found2.ReplicaCtrlAddr != "10.0.0.3:14261" {
		t.Fatalf("epoch 2 heartbeat: ReplicaCtrlAddr=%q, want 10.0.0.3:14261", found2.ReplicaCtrlAddr)
	}

	t.Logf("P10P2 reported truth: CollectBlockVolumeHeartbeat converged from vs2 → vs3")
}

func findHeartbeatMsg(msgs []blockvol.BlockVolumeInfoMessage, path string) *blockvol.BlockVolumeInfoMessage {
	for i := range msgs {
		if msgs[i].Path == path {
			return &msgs[i]
		}
	}
	return nil
}

// --- 4. No split truth: ingress + runtime + reported all agree ---

func TestP10P2_NoSplitTruth(t *testing.T) {
	bs, volPath := createP2BlockService(t)

	// Epoch 1: primary with vs2.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	time.Sleep(200 * time.Millisecond)

	// Epoch 2: primary with vs3.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs3-node:18080",
			ReplicaDataAddr: "10.0.0.3:14260",
			ReplicaCtrlAddr: "10.0.0.3:14261",
		},
	})

	time.Sleep(200 * time.Millisecond)

	// Check all three truth surfaces agree on vs3:

	// 1. Engine (assignment ingress): sender for vs3 exists, vs2 gone.
	vs3ID := volPath + "/vs3-node:18080"
	vs2ID := volPath + "/vs2-node:18080"

	if bs.v2Orchestrator.Registry.Sender(vs3ID) == nil {
		t.Fatal("engine: vs3 sender missing")
	}
	if bs.v2Orchestrator.Registry.Sender(vs2ID) != nil {
		t.Fatal("engine: vs2 sender should be removed")
	}

	// 2. Runtime (recovery manager): no task for vs2.
	bs.v2Recovery.mu.Lock()
	_, hasVs2Task := bs.v2Recovery.tasks[vs2ID]
	bs.v2Recovery.mu.Unlock()
	if hasVs2Task {
		t.Fatal("runtime: stale vs2 recovery task exists")
	}

	// 3. Reported (heartbeat output): addresses match vs3.
	msgs := bs.CollectBlockVolumeHeartbeat()
	hb := findHeartbeatMsg(msgs, volPath)
	if hb == nil {
		t.Fatal("reported: volume not in heartbeat")
	}
	if hb.ReplicaDataAddr != "10.0.0.3:14260" {
		t.Fatalf("reported: ReplicaDataAddr=%q, want 10.0.0.3:14260", hb.ReplicaDataAddr)
	}

	t.Log("P10P2 no-split-truth: engine(vs3) + runtime(no vs2 task) + heartbeat(vs3 addr) — all converged")
}
