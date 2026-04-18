package filer

import (
	"sync"
	"time"
)

// PeerRegistry is the in-memory mount-server registry (tier 1 of the peer
// chunk sharing design). The filer holds a map of mount-server address ->
// metadata with TTL-bounded entries refreshed by MountRegister heartbeats.
//
// The registry is small (O(fleet_size)) and slow-changing; fid-level state
// is NOT stored here — that lives on the mount fleet itself (tier 2).
//
// See design-weed-mount-peer-chunk-sharing.md §4.2.1.
type PeerRegistry struct {
	mu      sync.RWMutex
	entries map[string]*peerRegistryEntry
	clock   func() time.Time // injectable for tests
}

type peerRegistryEntry struct {
	peerAddr string
	rack     string
	expiry   time.Time
	lastSeen time.Time
}

// PeerInfo is the public view of a registered mount.
type PeerInfo struct {
	PeerAddr   string
	Rack       string
	LastSeenNs int64
}

// NewPeerRegistry constructs an empty registry using the real wall clock.
func NewPeerRegistry() *PeerRegistry {
	return newPeerRegistryWithClock(time.Now)
}

func newPeerRegistryWithClock(clock func() time.Time) *PeerRegistry {
	return &PeerRegistry{
		entries: make(map[string]*peerRegistryEntry),
		clock:   clock,
	}
}

// Register inserts or renews an entry. A zero or negative ttl is treated as
// "use a sane default" (60 s) so a buggy client cannot request a non-expiring
// slot.
func (r *PeerRegistry) Register(peerAddr, rack string, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[peerAddr]
	if !ok {
		entry = &peerRegistryEntry{peerAddr: peerAddr}
		r.entries[peerAddr] = entry
	}
	entry.rack = rack
	entry.lastSeen = now
	entry.expiry = now.Add(ttl)
}

// List returns all entries that have not yet expired, in no particular order.
// Expired entries encountered are evicted as a side effect.
func (r *PeerRegistry) List() []PeerInfo {
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]PeerInfo, 0, len(r.entries))
	for addr, entry := range r.entries {
		if !entry.expiry.After(now) {
			delete(r.entries, addr)
			continue
		}
		result = append(result, PeerInfo{
			PeerAddr:   entry.peerAddr,
			Rack:       entry.rack,
			LastSeenNs: entry.lastSeen.UnixNano(),
		})
	}
	return result
}

// Len returns the current entry count (including entries that may have
// expired but not yet been swept).
func (r *PeerRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Sweep removes expired entries. Safe to call periodically. Returns the
// number of entries evicted.
func (r *PeerRegistry) Sweep() int {
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	evicted := 0
	for addr, entry := range r.entries {
		if !entry.expiry.After(now) {
			delete(r.entries, addr)
			evicted++
		}
	}
	return evicted
}
