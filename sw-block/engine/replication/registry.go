package replication

import (
	"sort"
	"sync"
)

// ReplicaAssignment describes one replica's identity + endpoint in an assignment.
type ReplicaAssignment struct {
	ReplicaID string   // stable identity (e.g., volume-scoped replica name)
	Endpoint  Endpoint // current network address (may change)
}

// AssignmentIntent represents a coordinator-driven assignment update.
// Replicas are identified by stable ReplicaID, not by address.
type AssignmentIntent struct {
	Replicas        []ReplicaAssignment        // desired replica set with stable IDs
	Epoch           uint64
	RecoveryTargets map[string]SessionKind     // keyed by ReplicaID
}

// AssignmentResult records the outcome of applying an assignment.
type AssignmentResult struct {
	Added              []string
	Removed            []string
	SessionsCreated    []string
	SessionsSuperseded []string
	SessionsFailed     []string
}

// Registry manages per-replica Senders with identity-preserving reconciliation.
type Registry struct {
	mu      sync.RWMutex
	senders map[string]*Sender
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{senders: map[string]*Sender{}}
}

// Reconcile diffs current senders against new replicas (by stable ReplicaID).
// Matching senders are preserved with endpoint/epoch update.
// Removed senders are stopped. New senders are created.
func (r *Registry) Reconcile(replicas []ReplicaAssignment, epoch uint64) (added, removed []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Build target set keyed by stable ReplicaID.
	target := make(map[string]Endpoint, len(replicas))
	for _, ra := range replicas {
		target[ra.ReplicaID] = ra.Endpoint
	}

	// Stop and remove senders not in the new set.
	for id, s := range r.senders {
		if _, keep := target[id]; !keep {
			s.Stop()
			delete(r.senders, id)
			removed = append(removed, id)
		}
	}

	// Add new senders; update endpoint+epoch for existing.
	for id, ep := range target {
		if existing, ok := r.senders[id]; ok {
			existing.UpdateEndpoint(ep)
			existing.UpdateEpoch(epoch)
		} else {
			r.senders[id] = NewSender(id, ep, epoch)
			added = append(added, id)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return
}

// ApplyAssignment reconciles topology and creates recovery sessions.
func (r *Registry) ApplyAssignment(intent AssignmentIntent) AssignmentResult {
	var result AssignmentResult
	result.Added, result.Removed = r.Reconcile(intent.Replicas, intent.Epoch)

	if intent.RecoveryTargets == nil {
		return result
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	for replicaID, kind := range intent.RecoveryTargets {
		sender, ok := r.senders[replicaID]
		if !ok {
			result.SessionsFailed = append(result.SessionsFailed, replicaID)
			continue
		}
		if intent.Epoch < sender.Epoch() {
			result.SessionsFailed = append(result.SessionsFailed, replicaID)
			continue
		}
		_, err := sender.AttachSession(intent.Epoch, kind)
		if err != nil {
			id := sender.SupersedeSession(kind, "assignment_intent")
			if id != 0 {
				result.SessionsSuperseded = append(result.SessionsSuperseded, replicaID)
			} else {
				result.SessionsFailed = append(result.SessionsFailed, replicaID)
			}
			continue
		}
		result.SessionsCreated = append(result.SessionsCreated, replicaID)
	}
	return result
}

// Sender returns the sender for a ReplicaID.
func (r *Registry) Sender(replicaID string) *Sender {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.senders[replicaID]
}

// All returns all senders in deterministic order.
func (r *Registry) All() []*Sender {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Sender, 0, len(r.senders))
	for _, s := range r.senders {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReplicaID() < out[j].ReplicaID()
	})
	return out
}

// Len returns the sender count.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.senders)
}

// StopAll stops all senders.
func (r *Registry) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.senders {
		s.Stop()
	}
}

// InSyncCount returns the number of InSync senders.
func (r *Registry) InSyncCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, s := range r.senders {
		if s.State() == StateInSync {
			count++
		}
	}
	return count
}

// InvalidateEpoch invalidates all stale-epoch sessions.
func (r *Registry) InvalidateEpoch(currentEpoch uint64) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, s := range r.senders {
		snap := s.SessionSnapshot()
		if snap != nil && snap.Epoch < currentEpoch && snap.Active {
			s.InvalidateSession("epoch_bump", StateDisconnected)
			count++
		}
	}
	return count
}
