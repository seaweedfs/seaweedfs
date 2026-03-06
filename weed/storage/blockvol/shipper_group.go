package blockvol

import (
	"sync"
)

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

// Shipper returns the shipper at index i. For internal/test use.
func (sg *ShipperGroup) Shipper(i int) *WALShipper {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	if i < 0 || i >= len(sg.shippers) {
		return nil
	}
	return sg.shippers[i]
}
