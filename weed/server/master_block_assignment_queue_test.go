package weed_server

import (
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func mkAssign(path string, epoch uint64, role uint32) blockvol.BlockVolumeAssignment {
	return blockvol.BlockVolumeAssignment{Path: path, Epoch: epoch, Role: role, LeaseTtlMs: 30000}
}

func TestQueue_EnqueuePeek(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	got := q.Peek("s1")
	if len(got) != 1 || got[0].Path != "/a.blk" {
		t.Fatalf("expected 1 assignment, got %v", got)
	}
}

func TestQueue_PeekEmpty(t *testing.T) {
	q := NewBlockAssignmentQueue()
	got := q.Peek("s1")
	if got != nil {
		t.Fatalf("expected nil for empty server, got %v", got)
	}
}

func TestQueue_EnqueueBatch(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.EnqueueBatch("s1", []blockvol.BlockVolumeAssignment{
		mkAssign("/a.blk", 1, 1),
		mkAssign("/b.blk", 1, 2),
	})
	if q.Pending("s1") != 2 {
		t.Fatalf("expected 2 pending, got %d", q.Pending("s1"))
	}
}

func TestQueue_PeekDoesNotRemove(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	q.Peek("s1")
	q.Peek("s1")
	if q.Pending("s1") != 1 {
		t.Fatalf("Peek should not remove: pending=%d", q.Pending("s1"))
	}
}

func TestQueue_PeekDoesNotAffectOtherServers(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	q.Enqueue("s2", mkAssign("/b.blk", 1, 1))
	got := q.Peek("s1")
	if len(got) != 1 {
		t.Fatalf("s1: expected 1, got %d", len(got))
	}
	if q.Pending("s2") != 1 {
		t.Fatalf("s2 should be unaffected: pending=%d", q.Pending("s2"))
	}
}

func TestQueue_ConcurrentEnqueuePeek(t *testing.T) {
	q := NewBlockAssignmentQueue()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			q.Enqueue("s1", mkAssign("/a.blk", uint64(i), 1))
		}(i)
		go func() {
			defer wg.Done()
			q.Peek("s1")
		}()
	}
	wg.Wait()
	// Just verifying no panics or data races.
}

func TestQueue_Pending(t *testing.T) {
	q := NewBlockAssignmentQueue()
	if q.Pending("s1") != 0 {
		t.Fatalf("expected 0 for unknown server, got %d", q.Pending("s1"))
	}
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	q.Enqueue("s1", mkAssign("/b.blk", 1, 1))
	if q.Pending("s1") != 2 {
		t.Fatalf("expected 2, got %d", q.Pending("s1"))
	}
}

func TestQueue_MultipleEnqueue(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	q.Enqueue("s1", mkAssign("/a.blk", 2, 1))
	q.Enqueue("s1", mkAssign("/b.blk", 1, 2))
	if q.Pending("s1") != 3 {
		t.Fatalf("expected 3 pending, got %d", q.Pending("s1"))
	}
}

func TestQueue_ConfirmRemovesMatching(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))
	q.Enqueue("s1", mkAssign("/b.blk", 1, 2))
	q.Confirm("s1", "/a.blk", 1)
	if q.Pending("s1") != 1 {
		t.Fatalf("expected 1 after confirm, got %d", q.Pending("s1"))
	}
	got := q.Peek("s1")
	if got[0].Path != "/b.blk" {
		t.Fatalf("wrong remaining: %v", got)
	}

	// Confirm non-existent: no-op.
	q.Confirm("s1", "/c.blk", 1)
	if q.Pending("s1") != 1 {
		t.Fatalf("confirm nonexistent should be no-op")
	}
}

func TestQueue_ConfirmFromHeartbeat_PrunesConfirmed(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 5, 1))
	q.Enqueue("s1", mkAssign("/b.blk", 3, 2))
	q.Enqueue("s1", mkAssign("/c.blk", 1, 1))

	// Heartbeat confirms /a.blk@5 and /c.blk@1.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{
		{Path: "/a.blk", Epoch: 5},
		{Path: "/c.blk", Epoch: 1},
	})

	if q.Pending("s1") != 1 {
		t.Fatalf("expected 1 after heartbeat confirm, got %d", q.Pending("s1"))
	}
	got := q.Peek("s1")
	if got[0].Path != "/b.blk" {
		t.Fatalf("wrong remaining: %v", got)
	}
}

func TestQueue_ConfirmFromHeartbeat_SameEpochRefreshWaitsForReplicaTransport(t *testing.T) {
	q := NewBlockAssignmentQueue()
	initial := mkAssign("/a.blk", 5, 1)
	refresh := mkAssign("/a.blk", 5, 1)
	refresh.ReplicaAddrs = []blockvol.ReplicaAddr{{
		DataAddr: "10.0.0.2:14260",
		CtrlAddr: "10.0.0.2:14261",
		ServerID: "vs2",
	}}
	q.Enqueue("s1", initial)
	q.Enqueue("s1", refresh)

	// Old heartbeat confirms the epoch/role but does not yet report the refreshed
	// replica transport, so only the original promote assignment is confirmed.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{{
		Path:  "/a.blk",
		Epoch: 5,
	}})

	got := q.Peek("s1")
	if len(got) != 1 {
		t.Fatalf("expected refresh assignment to remain pending, got %d: %+v", len(got), got)
	}
	if got[0].Path != "/a.blk" || got[0].Epoch != 5 {
		t.Fatalf("wrong remaining assignment: %+v", got[0])
	}
	if len(got[0].ReplicaAddrs) != 1 {
		t.Fatalf("remaining refresh assignment lost replica addrs: %+v", got[0])
	}

	// Once the promoted VS reports the refreshed replica transport in heartbeat,
	// the same-epoch refresh is confirmed and removed.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{{
		Path:            "/a.blk",
		Epoch:           5,
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
	}})

	if q.Pending("s1") != 0 {
		t.Fatalf("expected refresh assignment to be confirmed after transport appears, got %d pending", q.Pending("s1"))
	}
}

func TestQueue_ConfirmFromHeartbeat_PrimaryRefreshWaitsForCoreProjectionTransition(t *testing.T) {
	q := NewBlockAssignmentQueue()
	refresh := mkAssign("/a.blk", 5, 1)
	refresh.ReplicaAddrs = []blockvol.ReplicaAddr{{
		DataAddr: "10.0.0.2:14260",
		CtrlAddr: "10.0.0.2:14261",
		ServerID: "vs2",
	}}
	refresh.ReplicaDataAddr = "10.0.0.2:14260"
	refresh.ReplicaCtrlAddr = "10.0.0.2:14261"
	refresh.ReplicaServerID = "vs2"
	q.Enqueue("s1", refresh)

	// Legacy transport fields alone are not enough for a primary refresh if the
	// local core still projects allocated_only. Otherwise the refresh can be
	// dropped before the VS actually re-applies replica membership to the core.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{{
		Path:                 "/a.blk",
		Epoch:                5,
		ReplicaDataAddr:      "10.0.0.2:14260",
		ReplicaCtrlAddr:      "10.0.0.2:14261",
		EngineProjectionMode: "allocated_only",
	}})
	if q.Pending("s1") != 1 {
		t.Fatalf("allocated_only primary should not confirm refresh early, pending=%d", q.Pending("s1"))
	}

	// Once the local core leaves allocated_only, the same heartbeat transport
	// now proves the refresh reached the V2 projection layer.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{{
		Path:                 "/a.blk",
		Epoch:                5,
		ReplicaDataAddr:      "10.0.0.2:14260",
		ReplicaCtrlAddr:      "10.0.0.2:14261",
		EngineProjectionMode: "bootstrap_pending",
	}})
	if q.Pending("s1") != 0 {
		t.Fatalf("expected refresh assignment to confirm after projection transition, pending=%d", q.Pending("s1"))
	}
}

func TestQueue_PeekPrunesStaleEpochs(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1)) // stale
	q.Enqueue("s1", mkAssign("/a.blk", 5, 1)) // current
	q.Enqueue("s1", mkAssign("/b.blk", 3, 2)) // only one

	got := q.Peek("s1")
	// Should have 2: /a.blk@5 (epoch 1 pruned) + /b.blk@3.
	if len(got) != 2 {
		t.Fatalf("expected 2 after pruning, got %d: %v", len(got), got)
	}
	for _, a := range got {
		if a.Path == "/a.blk" && a.Epoch != 5 {
			t.Fatalf("/a.blk should have epoch 5, got %d", a.Epoch)
		}
	}
	// After pruning, pending should also be 2.
	if q.Pending("s1") != 2 {
		t.Fatalf("pending should be 2 after prune, got %d", q.Pending("s1"))
	}
}
