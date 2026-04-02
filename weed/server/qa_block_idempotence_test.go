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
// Phase 10 P3: Bounded repeated-assignment idempotence
//
// Proofs:
//   1. Unchanged repeated assignment is idempotent (no rebuild-server relisten)
//   2. Changed assignment still triggers replacement (P2 guard)
//   3. No duplicate runtime side effects after repeated assignment
// ============================================================

func createP3BlockService(t *testing.T) (*BlockService, string) {
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
		replStates:     make(map[string]*volReplState),
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	t.Cleanup(func() {
		bs.v2Recovery.Shutdown()
		store.Close()
	})

	return bs, volPath
}

// --- 1. Unchanged repeated assignment is idempotent ---

func TestP10P3_RepeatedAssignment_Idempotent(t *testing.T) {
	bs, volPath := createP3BlockService(t)

	assignment := blockvol.BlockVolumeAssignment{
		Path:            volPath,
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs2-node:18080",
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
	}

	// First assignment: sets up replication + engine session + recovery.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{assignment})

	time.Sleep(200 * time.Millisecond)

	// Capture V2 engine event count after first assignment.
	replicaID := volPath + "/vs2-node:18080"
	eventsAfterFirst := len(bs.v2Orchestrator.Log.EventsFor(replicaID))
	t.Logf("first assignment: %d engine events", eventsAfterFirst)

	// Second assignment: identical. Must be fully idempotent — no V2 engine
	// processing, no new sessions, no recovery starts.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{assignment})

	time.Sleep(100 * time.Millisecond)

	eventsAfterSecond := len(bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterSecond != eventsAfterFirst {
		t.Fatalf("V2 engine events grew: %d → %d (repeated assignment not idempotent)",
			eventsAfterFirst, eventsAfterSecond)
	}

	// Third assignment: same again. Still no new events.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{assignment})

	time.Sleep(100 * time.Millisecond)

	eventsAfterThird := len(bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterThird != eventsAfterFirst {
		t.Fatalf("V2 engine events grew on third: %d → %d", eventsAfterFirst, eventsAfterThird)
	}

	// V1 state also unchanged.
	dataAddr, _ := bs.GetReplState(volPath)
	if dataAddr != "10.0.0.2:14260" {
		t.Fatalf("replState: %q", dataAddr)
	}

	t.Logf("P10P3 idempotent: 3x same → V2 events stable at %d, V1 state unchanged", eventsAfterFirst)
}

// --- 2. Changed assignment triggers replacement (P2 guard) ---

func TestP10P3_ChangedAssignment_StillReplaces(t *testing.T) {
	bs, volPath := createP3BlockService(t)

	// First assignment: vs2.
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

	dataAddr1, _ := bs.GetReplState(volPath)
	if dataAddr1 != "10.0.0.2:14260" {
		t.Fatalf("epoch 1: dataAddr=%q", dataAddr1)
	}

	// Second assignment: CHANGED replica (vs3). Must NOT be idempotent.
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

	// Verify: state updated to vs3.
	dataAddr2, _ := bs.GetReplState(volPath)
	if dataAddr2 != "10.0.0.3:14260" {
		t.Fatalf("epoch 2: dataAddr=%q, want 10.0.0.3:14260 (should have replaced)", dataAddr2)
	}

	// Engine should have vs3, not vs2.
	vs3ID := volPath + "/vs3-node:18080"
	vs2ID := volPath + "/vs2-node:18080"
	if bs.v2Orchestrator.Registry.Sender(vs3ID) == nil {
		t.Fatal("vs3 sender should exist")
	}
	if bs.v2Orchestrator.Registry.Sender(vs2ID) != nil {
		t.Fatal("vs2 sender should be removed")
	}

	t.Log("P10P3 guard: changed assignment still triggers full replacement")
}

// --- 3. Heartbeat coherent after repeated assignment ---

func TestP10P3_HeartbeatCoherent_AfterRepeated(t *testing.T) {
	bs, volPath := createP3BlockService(t)

	assignment := blockvol.BlockVolumeAssignment{
		Path:            volPath,
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs2-node:18080",
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
	}

	// Apply 3 times.
	for i := 0; i < 3; i++ {
		bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{assignment})
		time.Sleep(50 * time.Millisecond)
	}

	// Heartbeat should report vs2 exactly once (no duplicates in output).
	msgs := bs.CollectBlockVolumeHeartbeat()
	count := 0
	for _, m := range msgs {
		if m.Path == volPath {
			count++
			if m.ReplicaDataAddr != "10.0.0.2:14260" {
				t.Fatalf("heartbeat: ReplicaDataAddr=%q", m.ReplicaDataAddr)
			}
		}
	}
	if count != 1 {
		t.Fatalf("heartbeat: volume appeared %d times, want 1", count)
	}

	t.Log("P10P3 heartbeat: coherent after 3x repeated assignment")
}
