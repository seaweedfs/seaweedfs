package weed_server

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BlockAssignmentQueue holds pending assignments per volume server.
// Assignments are retained until confirmed by a matching heartbeat (F1).
type BlockAssignmentQueue struct {
	mu     sync.Mutex
	queues map[string][]blockvol.BlockVolumeAssignment // server -> pending
}

// NewBlockAssignmentQueue creates an empty queue.
func NewBlockAssignmentQueue() *BlockAssignmentQueue {
	return &BlockAssignmentQueue{
		queues: make(map[string][]blockvol.BlockVolumeAssignment),
	}
}

// Enqueue adds a single assignment to the server's queue.
func (q *BlockAssignmentQueue) Enqueue(server string, a blockvol.BlockVolumeAssignment) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues[server] = append(q.queues[server], a)
}

// EnqueueBatch adds multiple assignments to the server's queue.
func (q *BlockAssignmentQueue) EnqueueBatch(server string, as []blockvol.BlockVolumeAssignment) {
	if len(as) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues[server] = append(q.queues[server], as...)
}

// Peek returns a copy of pending assignments for the server without removing them.
// Stale assignments (superseded by a newer epoch for the same path) are pruned.
func (q *BlockAssignmentQueue) Peek(server string) []blockvol.BlockVolumeAssignment {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.queues[server]
	if len(pending) == 0 {
		return nil
	}

	// Prune stale: keep only the latest epoch per path.
	latest := make(map[string]uint64, len(pending))
	for _, a := range pending {
		if a.Epoch > latest[a.Path] {
			latest[a.Path] = a.Epoch
		}
	}
	pruned := pending[:0]
	for _, a := range pending {
		if a.Epoch >= latest[a.Path] {
			pruned = append(pruned, a)
		}
	}
	q.queues[server] = pruned

	// Return a copy.
	out := make([]blockvol.BlockVolumeAssignment, len(pruned))
	copy(out, pruned)
	return out
}

// Confirm removes a matching assignment (same path and epoch) from the server's queue.
func (q *BlockAssignmentQueue) Confirm(server string, path string, epoch uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.queues[server]
	for i, a := range pending {
		if a.Path == path && a.Epoch == epoch {
			q.queues[server] = append(pending[:i], pending[i+1:]...)
			return
		}
	}
}

// ConfirmFromHeartbeat batch-confirms assignments that match reported heartbeat info.
// Same-epoch refresh assignments that carry replica transport are only confirmed
// once the heartbeat reflects that transport, so they are not dropped before
// the promoted VS actually applies them.
func (q *BlockAssignmentQueue) ConfirmFromHeartbeat(server string, infos []blockvol.BlockVolumeInfoMessage) {
	if len(infos) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.queues[server]
	if len(pending) == 0 {
		return
	}

	// Keep only assignments not confirmed.
	kept := pending[:0]
	for _, a := range pending {
		if !assignmentConfirmedByHeartbeat(a, infos) {
			kept = append(kept, a)
		}
	}
	q.queues[server] = kept
}

func assignmentConfirmedByHeartbeat(a blockvol.BlockVolumeAssignment, infos []blockvol.BlockVolumeInfoMessage) bool {
	for _, info := range infos {
		if info.Path != a.Path || info.Epoch != a.Epoch {
			continue
		}
		expectedData, expectedCtrl, requiresReplicaTransport := assignmentReplicaTransport(a)
		if !requiresReplicaTransport {
			return true
		}
		if info.ReplicaDataAddr == expectedData && info.ReplicaCtrlAddr == expectedCtrl {
			return true
		}
	}
	return false
}

func assignmentReplicaTransport(a blockvol.BlockVolumeAssignment) (dataAddr, ctrlAddr string, ok bool) {
	if a.ReplicaDataAddr != "" || a.ReplicaCtrlAddr != "" {
		return a.ReplicaDataAddr, a.ReplicaCtrlAddr, true
	}
	if len(a.ReplicaAddrs) == 1 {
		return a.ReplicaAddrs[0].DataAddr, a.ReplicaAddrs[0].CtrlAddr, true
	}
	return "", "", false
}

// Pending returns the number of pending assignments for the server.
func (q *BlockAssignmentQueue) Pending(server string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queues[server])
}

// TotalPending returns the total number of pending assignments across all servers.
func (q *BlockAssignmentQueue) TotalPending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	total := 0
	for _, queue := range q.queues {
		total += len(queue)
	}
	return total
}
