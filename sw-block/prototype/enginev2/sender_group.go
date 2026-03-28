package enginev2

import (
	"sort"
	"sync"
)

// SenderGroup manages per-replica Senders with identity-preserving reconciliation.
// It is the V2 equivalent of ShipperGroup.
type SenderGroup struct {
	mu      sync.RWMutex
	senders map[string]*Sender // keyed by ReplicaID
}

// NewSenderGroup creates an empty SenderGroup.
func NewSenderGroup() *SenderGroup {
	return &SenderGroup{
		senders: map[string]*Sender{},
	}
}

// Reconcile diffs the current sender set against newEndpoints.
// Matching senders (same ReplicaID) are preserved with all state.
// Removed senders are stopped. New senders are created at the given epoch.
// Returns lists of added and removed ReplicaIDs.
func (sg *SenderGroup) Reconcile(newEndpoints map[string]Endpoint, epoch uint64) (added, removed []string) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	// Stop and remove senders not in the new set.
	for id, s := range sg.senders {
		if _, keep := newEndpoints[id]; !keep {
			s.Stop()
			delete(sg.senders, id)
			removed = append(removed, id)
		}
	}

	// Add new senders; update endpoints and epoch for existing.
	for id, ep := range newEndpoints {
		if existing, ok := sg.senders[id]; ok {
			existing.UpdateEndpoint(ep)
			existing.UpdateEpoch(epoch)
		} else {
			sg.senders[id] = NewSender(id, ep, epoch)
			added = append(added, id)
		}
	}

	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

// Sender returns the sender for a ReplicaID, or nil.
func (sg *SenderGroup) Sender(replicaID string) *Sender {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	return sg.senders[replicaID]
}

// All returns all senders in deterministic order (sorted by ReplicaID).
func (sg *SenderGroup) All() []*Sender {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	out := make([]*Sender, 0, len(sg.senders))
	for _, s := range sg.senders {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReplicaID < out[j].ReplicaID
	})
	return out
}

// Len returns the number of senders.
func (sg *SenderGroup) Len() int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	return len(sg.senders)
}

// StopAll stops all senders.
func (sg *SenderGroup) StopAll() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for _, s := range sg.senders {
		s.Stop()
	}
}

// InSyncCount returns the number of senders in StateInSync.
func (sg *SenderGroup) InSyncCount() int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	count := 0
	for _, s := range sg.senders {
		if s.State == StateInSync {
			count++
		}
	}
	return count
}

// InvalidateEpoch invalidates all active sessions that are bound to
// a stale epoch. Called after promotion/epoch bump.
func (sg *SenderGroup) InvalidateEpoch(currentEpoch uint64) int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	count := 0
	for _, s := range sg.senders {
		sess := s.Session()
		if sess != nil && sess.Epoch < currentEpoch && sess.Active() {
			s.InvalidateSession("epoch_bump", StateDisconnected)
			count++
		}
	}
	return count
}
