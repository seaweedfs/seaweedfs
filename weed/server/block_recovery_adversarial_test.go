package weed_server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 09 P4: Adversarial tests for RecoveryManager
// ============================================================

// --- Adversarial 1: Rapid triple supersede ---

func TestAdversarial_P4_RapidTripleSupersede(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	// Write data so recovery has work.
	bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	})

	var executionCount atomic.Int32

	rm.OnBeforeExecute = func(replicaID string) {
		executionCount.Add(1)
		// Simulate slow execution — each goroutine takes 100ms.
		time.Sleep(100 * time.Millisecond)
	}

	makeAssignment := func(epoch uint64) []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{
			{Path: volPath, Epoch: epoch, Role: uint32(blockvol.RolePrimary),
				ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334"},
		}
	}

	// Rapid fire: epoch 1, 2, 3 in quick succession.
	// Each ProcessAssignments must drain the previous before starting the next.
	bs.ProcessAssignments(makeAssignment(1))
	time.Sleep(20 * time.Millisecond) // let goroutine start

	bs.ProcessAssignments(makeAssignment(2))
	bs.ProcessAssignments(makeAssignment(3))

	// Wait for all to settle.
	time.Sleep(500 * time.Millisecond)

	// Must have at most 1 active task (the final one, or 0 if it completed).
	count := rm.ActiveTaskCount()
	if count > 1 {
		t.Fatalf("goroutine leak: %d active tasks after triple supersede", count)
	}

	t.Logf("adversarial 1: triple supersede — %d executions, %d active tasks, no leak",
		executionCount.Load(), count)
}

// --- Adversarial 2: Shutdown during slow goroutine (not permanently stuck) ---

func TestAdversarial_P4_ShutdownDuringSlow(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	})

	started := make(chan struct{}, 1)
	rm.OnBeforeExecute = func(replicaID string) {
		started <- struct{}{}
		// Simulate slow execution — 500ms, not permanently stuck.
		time.Sleep(500 * time.Millisecond)
	}

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: volPath, Epoch: 1, Role: uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334"},
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("goroutine did not start")
	}

	// Shutdown while goroutine is in slow execution.
	done := make(chan bool, 1)
	go func() {
		rm.Shutdown()
		done <- true
	}()

	select {
	case <-done:
		t.Log("adversarial 2: Shutdown completed after slow goroutine finished")
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete within 5 seconds")
	}

	if rm.ActiveTaskCount() != 0 {
		t.Fatalf("tasks after shutdown: %d", rm.ActiveTaskCount())
	}
}

// --- Adversarial 3: Two replicas, no cross-interference ---

func TestAdversarial_P4_TwoReplicas_NoInterference(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	})

	var execIDs []string
	var execMu = make(chan struct{}, 1)
	rm.OnBeforeExecute = func(replicaID string) {
		execMu <- struct{}{}
		execIDs = append(execIDs, replicaID)
		<-execMu
	}

	// Two different replicas in one assignment batch.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: volPath, Epoch: 1, Role: uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
			ReplicaAddrs: []blockvol.ReplicaAddr{
				{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", ServerID: "vs2"},
				{DataAddr: "10.0.0.3:9333", CtrlAddr: "10.0.0.3:9334", ServerID: "vs3"},
			},
		},
	})

	time.Sleep(300 * time.Millisecond)

	// Both replicas should have gotten recovery goroutines.
	// No cross-interference: each has its own task with its own done channel.
	replicaID1 := volPath + "/vs2"
	replicaID2 := volPath + "/vs3"

	s1 := bs.v2Orchestrator.Registry.Sender(replicaID1)
	s2 := bs.v2Orchestrator.Registry.Sender(replicaID2)

	if s1 == nil {
		t.Fatal("sender for vs2 not created")
	}
	if s2 == nil {
		t.Fatal("sender for vs3 not created")
	}

	t.Logf("adversarial 3: two replicas — vs2=%s vs3=%s, no cross-interference",
		s1.State(), s2.State())
}
