package filer

import (
	"sync"
	"time"
)

// maxMountPeerRegistryEntries caps the number of mounts the filer will track to
// prevent a burst of buggy or malicious clients from exhausting memory. At
// ~80 B per entry (map + struct + strings) a 10k cap costs ~1 MB, which is
// ample headroom for real fleets while being well under any filer's budget.
// A full registry silently rejects new registrations until expiry frees room.
const maxMountPeerRegistryEntries = 10000

// maxMountPeerRegistryTTL caps a single heartbeat's requested TTL. Prevents a
// misconfigured or malicious client from pinning an entry indefinitely.
const maxMountPeerRegistryTTL = time.Hour

// MountPeerRegistry is the in-memory mount-server registry (tier 1 of the peer
// chunk sharing design). The filer holds a map of mount-server address ->
// metadata with TTL-bounded entries refreshed by MountRegister heartbeats.
//
// The registry is small (O(fleet_size)) and slow-changing; fid-level state
// is NOT stored here — that lives on the mount fleet itself (tier 2).
//
// See design-weed-mount-peer-chunk-sharing.md §4.2.1.
type MountPeerRegistry struct {
	mu      sync.RWMutex
	entries map[string]*mountPeerRegistryEntry
	clock   func() time.Time // injectable for tests
}

type mountPeerRegistryEntry struct {
	peerAddr   string
	dataCenter string
	rack       string
	expiry     time.Time
	lastSeen   time.Time
}

// MountPeerInfo is the public view of a registered mount. DataCenter and
// Rack are carried as a two-level locality hierarchy: a peer in the same
// DC but a different rack is still a much better fetch target than a peer
// in a different DC, so both are worth distinguishing for ranking.
type MountPeerInfo struct {
	PeerAddr   string
	DataCenter string
	Rack       string
	LastSeenNs int64
}

// NewMountPeerRegistry constructs an empty registry using the real wall clock.
func NewMountPeerRegistry() *MountPeerRegistry {
	return newMountPeerRegistryWithClock(time.Now)
}

func newMountPeerRegistryWithClock(clock func() time.Time) *MountPeerRegistry {
	return &MountPeerRegistry{
		entries: make(map[string]*mountPeerRegistryEntry),
		clock:   clock,
	}
}

// Register inserts or renews an entry. A zero or negative ttl is treated as
// "use a sane default" (60 s); a ttl exceeding maxMountPeerRegistryTTL is capped.
// An empty peerAddr is rejected silently. When the registry is at capacity,
// a *new* entry is rejected; renewals of existing entries always succeed.
func (r *MountPeerRegistry) Register(peerAddr, dataCenter, rack string, ttl time.Duration) {
	if peerAddr == "" {
		return
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if ttl > maxMountPeerRegistryTTL {
		ttl = maxMountPeerRegistryTTL
	}
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[peerAddr]
	if !ok {
		if len(r.entries) >= maxMountPeerRegistryEntries {
			return
		}
		entry = &mountPeerRegistryEntry{peerAddr: peerAddr}
		r.entries[peerAddr] = entry
	}
	entry.dataCenter = dataCenter
	entry.rack = rack
	entry.lastSeen = now
	entry.expiry = now.Add(ttl)
}

// List returns all entries that have not yet expired, in no particular order.
// Expired entries encountered are evicted as a side effect.
func (r *MountPeerRegistry) List() []MountPeerInfo {
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]MountPeerInfo, 0, len(r.entries))
	for addr, entry := range r.entries {
		if !entry.expiry.After(now) {
			delete(r.entries, addr)
			continue
		}
		result = append(result, MountPeerInfo{
			PeerAddr:   entry.peerAddr,
			DataCenter: entry.dataCenter,
			Rack:       entry.rack,
			LastSeenNs: entry.lastSeen.UnixNano(),
		})
	}
	return result
}

// Len returns the current entry count (including entries that may have
// expired but not yet been swept).
func (r *MountPeerRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Sweep removes expired entries. Safe to call periodically. Returns the
// number of entries evicted.
func (r *MountPeerRegistry) Sweep() int {
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
