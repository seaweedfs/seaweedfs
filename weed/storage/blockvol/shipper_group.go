package blockvol

import (
	"log"
	"sync"
	"time"
)

// ReplicaShipperStatus reports per-replica state from the primary's shipper group.
// Used in heartbeat so master can identify which replica needs rebuild.
type ReplicaShipperStatus struct {
	DataAddr   string // replica identity
	State      string // "disconnected", "in_sync", "degraded", "needs_rebuild", etc.
	FlushedLSN uint64 // last known durable progress
}

// ShipperGroup wraps multiple WALShippers for fan-out replication to N replicas.
// Len()==0 means standalone (no replicas), Len()==1 is RF=2, Len()==2 is RF=3.
type ShipperGroup struct {
	mu       sync.RWMutex
	shippers []*WALShipper
}

// NewShipperGroup creates a ShipperGroup from the given shippers.
func NewShipperGroup(shippers []*WALShipper) *ShipperGroup {
	return &ShipperGroup{shippers: shippers}
}

// ShipAll sends a WAL entry to every non-degraded shipper (fire-and-forget).
func (sg *ShipperGroup) ShipAll(entry *WALEntry) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		s.Ship(entry)
	}
}

// BarrierAll sends barriers to all shippers in parallel.
// Returns a per-shipper error slice (nil entry = success).
func (sg *ShipperGroup) BarrierAll(lsnMax uint64) []error {
	sg.mu.RLock()
	n := len(sg.shippers)
	shippers := sg.shippers
	sg.mu.RUnlock()

	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = shippers[idx].Barrier(lsnMax)
		}(i)
	}
	wg.Wait()
	return errs
}

// AllDegraded returns true only if every shipper is degraded.
// Returns false when there are no shippers (standalone).
func (sg *ShipperGroup) AllDegraded() bool {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	if len(sg.shippers) == 0 {
		return false
	}
	for _, s := range sg.shippers {
		if !s.IsDegraded() {
			return false
		}
	}
	return true
}

// AnyDegraded returns true if at least one shipper is degraded.
func (sg *ShipperGroup) AnyDegraded() bool {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		if s.IsDegraded() {
			return true
		}
	}
	return false
}

// DegradedCount returns the number of degraded shippers.
func (sg *ShipperGroup) DegradedCount() int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	count := 0
	for _, s := range sg.shippers {
		if s.IsDegraded() {
			count++
		}
	}
	return count
}

// StopAll stops all shippers and closes their connections.
func (sg *ShipperGroup) StopAll() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for _, s := range sg.shippers {
		s.Stop()
	}
}

// Len returns the number of shippers. 0=standalone, 1=RF2, 2=RF3.
func (sg *ShipperGroup) Len() int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	return len(sg.shippers)
}

// MinReplicaFlushedLSN returns the minimum replicaFlushedLSN across all
// shippers that have reported valid progress (HasFlushedProgress == true).
// The bool return indicates whether any shipper has known progress.
// Returns (0, false) for empty groups or groups where no shipper has
// received a valid FlushedLSN response yet.
// Used by WAL retention (CP13-6) to gate WAL reclaim.
func (sg *ShipperGroup) MinReplicaFlushedLSN() (uint64, bool) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	var min uint64
	found := false
	for _, s := range sg.shippers {
		if !s.HasFlushedProgress() {
			continue
		}
		lsn := s.ReplicaFlushedLSN()
		if !found || lsn < min {
			min = lsn
			found = true
		}
	}
	return min, found
}

// MinRecoverableFlushedLSN returns the minimum replicaFlushedLSN across
// shippers that are catch-up candidates (not NeedsRebuild, have flushed progress).
// Pure read — does not mutate state. Returns (0, false) if no recoverable
// replica has known progress.
func (sg *ShipperGroup) MinRecoverableFlushedLSN() (uint64, bool) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	var min uint64
	found := false
	for _, s := range sg.shippers {
		if !s.HasFlushedProgress() {
			continue
		}
		if s.State() == ReplicaNeedsRebuild {
			continue
		}
		lsn := s.ReplicaFlushedLSN()
		if !found || lsn < min {
			min = lsn
			found = true
		}
	}
	return min, found
}

// EvaluateRetentionBudgets checks each recoverable replica's contact time
// against the timeout. Replicas that exceed the timeout are transitioned to
// NeedsRebuild, releasing their WAL hold. Must be called before
// MinRecoverableFlushedLSN to ensure stale replicas are escalated first.
func (sg *ShipperGroup) EvaluateRetentionBudgets(timeout time.Duration) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		if !s.HasFlushedProgress() {
			continue // bootstrap shippers — not retention candidates
		}
		if s.State() == ReplicaNeedsRebuild {
			continue
		}
		ct := s.LastContactTime()
		if ct.IsZero() {
			continue // no contact yet — skip
		}
		if time.Since(ct) > timeout {
			s.state.Store(uint32(ReplicaNeedsRebuild))
			log.Printf("shipper_group: retention timeout for %s (last contact %v ago), transitioning to NeedsRebuild",
				s.dataAddr, time.Since(ct).Round(time.Second))
		}
	}
}

// ShipperStates returns per-replica status for heartbeat reporting.
// Master uses this to identify which replicas need rebuild.
func (sg *ShipperGroup) ShipperStates() []ReplicaShipperStatus {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	states := make([]ReplicaShipperStatus, len(sg.shippers))
	for i, s := range sg.shippers {
		states[i] = ReplicaShipperStatus{
			DataAddr:   s.dataAddr,
			State:      s.State().String(),
			FlushedLSN: s.ReplicaFlushedLSN(),
		}
	}
	return states
}

// InSyncCount returns the number of shippers in ReplicaInSync state.
func (sg *ShipperGroup) InSyncCount() int {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	count := 0
	for _, s := range sg.shippers {
		if s.State() == ReplicaInSync {
			count++
		}
	}
	return count
}

// Shipper returns the shipper at index i. For internal/test use.
func (sg *ShipperGroup) Shipper(i int) *WALShipper {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	if i < 0 || i >= len(sg.shippers) {
		return nil
	}
	return sg.shippers[i]
}
