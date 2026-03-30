package replication

import (
	"sort"
	"sync"
)

// AssignmentIntent represents a coordinator-driven assignment update.
type AssignmentIntent struct {
	Endpoints       map[string]Endpoint
	Epoch           uint64
	RecoveryTargets map[string]SessionKind
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

// Reconcile diffs current senders against new endpoints.
func (r *Registry) Reconcile(endpoints map[string]Endpoint, epoch uint64) (added, removed []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, s := range r.senders {
		if _, keep := endpoints[id]; !keep {
			s.Stop()
			delete(r.senders, id)
			removed = append(removed, id)
		}
	}
	for id, ep := range endpoints {
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
	result.Added, result.Removed = r.Reconcile(intent.Endpoints, intent.Epoch)

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
		if intent.Epoch < sender.Epoch {
			result.SessionsFailed = append(result.SessionsFailed, replicaID)
			continue
		}
		_, err := sender.AttachSession(intent.Epoch, kind)
		if err != nil {
			sess := sender.SupersedeSession(kind, "assignment_intent")
			if sess != nil {
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
		return out[i].ReplicaID < out[j].ReplicaID
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
		if s.State == StateInSync {
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
		sess := s.Session()
		if sess != nil && sess.Epoch < currentEpoch && sess.Active() {
			s.InvalidateSession("epoch_bump", StateDisconnected)
			count++
		}
	}
	return count
}
