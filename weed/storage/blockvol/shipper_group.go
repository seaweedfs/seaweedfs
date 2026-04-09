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

// CatchUpReplica replays retained WAL to one configured replica up to targetLSN.
// It uses the shipper's stable ReplicaID to select the bounded recovery target.
func (sg *ShipperGroup) CatchUpReplica(replicaID string, targetLSN uint64) (uint64, error) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		if s.replicaID == replicaID {
			return s.CatchUpTo(targetLSN)
		}
	}
	return 0, ErrReplicaDegraded
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

// AnyHasFlushedProgress returns true if any shipper has ever had durable
// progress (CP13-5: used to seed replacement shippers on SetReplicaAddrs).
func (sg *ShipperGroup) AnyHasFlushedProgress() bool {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		if s.HasFlushedProgress() {
			return true
		}
	}
	return false
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

// ProbeReconnectAll probes all shippers that are not yet connected and
// returns per-replica results. Used by primary onboarding after assignment.
func (sg *ShipperGroup) ProbeReconnectAll() []ReplicaProbeResult {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	var results []ReplicaProbeResult
	for _, s := range sg.shippers {
		if !s.HasTransportContact() {
			results = append(results, s.ProbeReconnect())
		} else {
			results = append(results, ReplicaProbeResult{
				ReplicaID:         s.ReplicaID(),
				DataAddr:          s.DataAddr(),
				CtrlAddr:          s.CtrlAddr(),
				Outcome:           ProbeKeepUp,
				ReplicaFlushedLSN: s.replicaFlushedLSN.Load(),
			})
		}
	}
	return results
}

// AllHaveTransportContact returns true only when every configured shipper has
// established transport contact strong enough for bootstrap observability.
// This is intentionally weaker than InSync: it allows the V2 core to observe
// "shipper connected" before barrier durability has completed.
func (sg *ShipperGroup) AllHaveTransportContact() bool {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	if len(sg.shippers) == 0 {
		return false
	}
	for _, s := range sg.shippers {
		if !s.HasTransportContact() {
			return false
		}
	}
	return true
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

// MinReplicaFlushedLSNAll returns the minimum durable progress across the
// full configured replica set, but only when EVERY shipper has reported valid
// flushed progress. This is the safe committed boundary for sync_all
// observability: if any replica has not established durable progress yet, the
// cluster does not have a lineage-safe committed point for the full set.
func (sg *ShipperGroup) MinReplicaFlushedLSNAll() (uint64, bool) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	if len(sg.shippers) == 0 {
		return 0, false
	}
	var min uint64
	for i, s := range sg.shippers {
		if !s.HasFlushedProgress() {
			return 0, false
		}
		lsn := s.ReplicaFlushedLSN()
		if i == 0 || lsn < min {
			min = lsn
		}
	}
	return min, true
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

// MinShippedLSN returns the minimum shippedLSN across all active shippers
// (not NeedsRebuild). This is the Ceph-model retention watermark: the flusher
// must not recycle WAL entries past the slowest active shipper's shipped
// position, because those entries are needed for catch-up if the shipper
// degrades during sustained async writes.
//
// Returns (0, false) if no shipper has shipped anything yet.
func (sg *ShipperGroup) MinShippedLSN() (uint64, bool) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	var min uint64
	found := false
	for _, s := range sg.shippers {
		if s.State() == ReplicaNeedsRebuild {
			continue
		}
		lsn := s.ShippedLSN()
		if lsn == 0 {
			continue // hasn't shipped yet — don't pin at 0
		}
		if !found || lsn < min {
			min = lsn
			found = true
		}
	}
	return min, found
}

// RetentionBudgetParams holds the inputs for retention budget evaluation.
type RetentionBudgetParams struct {
	Timeout        time.Duration
	MaxBytes       uint64
	PrimaryHeadLSN uint64
	BlockSize      uint32 // from volume config, for lag byte estimation
}

// EvaluateRetentionBudgets checks each recoverable replica against timeout
// and max-bytes budgets. Replicas that exceed either budget are transitioned
// to NeedsRebuild, releasing their WAL hold. Must be called before
// MinRecoverableFlushedLSN to ensure stale replicas are escalated first.
//
// CP13-6: both timeout and max-bytes budgets have real state effects.
func (sg *ShipperGroup) EvaluateRetentionBudgets(params RetentionBudgetParams) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	blockSize := uint64(params.BlockSize)
	if blockSize == 0 {
		blockSize = 4096 // safe default
	}

	for _, s := range sg.shippers {
		if !s.HasFlushedProgress() {
			continue // bootstrap shippers — not retention candidates
		}
		if s.State() == ReplicaNeedsRebuild {
			continue
		}

		// Timeout budget: replica hasn't been heard from in too long.
		ct := s.LastContactTime()
		if !ct.IsZero() && time.Since(ct) > params.Timeout {
			s.state.Store(uint32(ReplicaNeedsRebuild))
			log.Printf("shipper_group: retention timeout for %s (last contact %v ago), transitioning to NeedsRebuild",
				s.dataAddr, time.Since(ct).Round(time.Second))
			continue
		}

		// Max-bytes budget: replica lag exceeds configured maximum.
		if params.MaxBytes > 0 && params.PrimaryHeadLSN > 0 {
			replicaLSN := s.ReplicaFlushedLSN()
			if params.PrimaryHeadLSN > replicaLSN {
				lag := params.PrimaryHeadLSN - replicaLSN
				lagBytes := lag * blockSize
				if lagBytes > params.MaxBytes {
					s.state.Store(uint32(ReplicaNeedsRebuild))
					log.Printf("shipper_group: retention max-bytes exceeded for %s (lag=%d entries, ~%dKB > %dKB), transitioning to NeedsRebuild",
						s.dataAddr, lag, lagBytes/1024, params.MaxBytes/1024)
				}
			}
		}
	}
}

// SetOnStateChange registers a callback on all current shippers for state transitions.
// Used by the volume server to trigger an immediate block heartbeat when a shipper
// transitions to/from degraded.
func (sg *ShipperGroup) SetOnStateChange(fn func(from, to ReplicaState)) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		s.SetOnStateChange(fn)
	}
}

// SetOnBarrierFailure registers a callback on all current shippers for failed
// barrier attempts.
func (sg *ShipperGroup) SetOnBarrierFailure(fn func(reason string)) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		s.SetOnBarrierFailure(fn)
	}
}

// SetLiveShippingPolicy installs a host-provided gate on all current shippers.
// The policy is evaluated before a live-tail WAL entry is dialed or sent.
func (sg *ShipperGroup) SetLiveShippingPolicy(fn func(replicaID string, entryLSN uint64) (allow bool, reason string)) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	for _, s := range sg.shippers {
		s.SetLiveShippingPolicy(fn)
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
