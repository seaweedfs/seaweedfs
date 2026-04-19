package mount

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// maxPeerDirectoryEntries caps the total (fid, holder) edges a single mount
// will hold in its shard of the chunk directory. Each edge is a
// (uint32, time.Time) map entry (~40 B with Go's map overhead), plus the
// per-peer metadata is interned — see peerMetadata below — so 500k edges
// come out to roughly 20 MB regardless of fleet size. A full directory
// rejects new announces until TTL frees room.
const maxPeerDirectoryEntries = 500000

// peerID is the interned identifier for a peer. Peer addresses and locality
// labels live once in the peers slice; the (fid -> holder) index references
// peers by this fixed-size id so we avoid paying for the ~48 B worth of
// string headers (addr, dc, rack) on every edge.
type peerID uint32

// peerMetadata is the per-peer data that used to be inlined on every
// holderEntry. Fields are updated in place on each announce so a peer
// reconfiguring its dc/rack is picked up without churn.
type peerMetadata struct {
	addr       string
	dataCenter string
	rack       string
}

// PeerDirectory is the tier-2 chunk directory: an in-memory
// `fid -> set<peer>` map, holding entries only for fids for which this
// mount is the HRW-assigned owner. Populated by inbound ChunkAnnounce RPCs,
// queried by inbound ChunkLookup RPCs. Entries expire on TTL.
//
// Peer addresses are interned into peerID. That way the hot index stores
// `fid -> (peerID -> expiry)`, a map of two fixed-size values, instead of
// a full (addr, dc, rack, expiry) struct per edge. The peer slice itself
// is bounded by fleet size (typically a few hundred), so the interning
// table's memory cost is negligible next to the per-edge savings.
//
// Lookup takes only a read lock; TTL eviction is the sole responsibility of
// the Sweep goroutine, which runs on a periodic ticker. This keeps the hot
// read path concurrent even under heavy announce traffic.
//
// See design-weed-mount-peer-chunk-sharing.md §4.2.2.
type PeerDirectory struct {
	mu sync.RWMutex

	// Interning table. peers[id] gives the peer's metadata; peerIDByAddr
	// maps an address back to its id when a new announce comes in.
	peers        []peerMetadata
	peerIDByAddr map[string]peerID

	// fid -> (peerID -> expiry). The inner map's value is a bare timestamp
	// — metadata is looked up via peers[id] on demand.
	index map[string]map[peerID]time.Time

	// entryCount is maintained in lock-step with map mutations (under
	// mu), so Stats() can report O(1) instead of scanning the index.
	entryCount int

	announces atomic.Int64
	lookups   atomic.Int64
	rejected  atomic.Int64
	evictions atomic.Int64

	clock func() time.Time
}

// NewPeerDirectory constructs a directory using the real wall clock.
func NewPeerDirectory() *PeerDirectory {
	return newPeerDirectoryWithClock(time.Now)
}

func newPeerDirectoryWithClock(clock func() time.Time) *PeerDirectory {
	return &PeerDirectory{
		index:        map[string]map[peerID]time.Time{},
		peerIDByAddr: map[string]peerID{},
		clock:        clock,
	}
}

// AnnounceResult reports how an inbound announce was handled per-fid: fids
// this receiver owns were recorded; others were rejected so the caller can
// retry the correct owner after refreshing its seed view.
type AnnounceResult struct {
	Rejected []string
}

// OwnerCheck is a predicate provided by the caller (backed by HRW on the
// current seed list) that says whether self is the owner for a given fid.
type OwnerCheck func(fid string) bool

// internPeer returns the interned id for peerAddr, allocating a new slot
// if this is the first announce from that peer. Must be called under mu
// (write). dc/rack are refreshed on each call so reconfig is picked up.
func (d *PeerDirectory) internPeer(addr, dc, rack string) peerID {
	if id, ok := d.peerIDByAddr[addr]; ok {
		p := &d.peers[id]
		p.dataCenter = dc
		p.rack = rack
		return id
	}
	id := peerID(len(d.peers))
	d.peers = append(d.peers, peerMetadata{addr: addr, dataCenter: dc, rack: rack})
	d.peerIDByAddr[addr] = id
	return id
}

// Announce records the caller as a holder for every accepted fid. Any fid
// for which ownerCheck returns false is rejected and surfaced back. A
// directory at maxPeerDirectoryEntries silently drops *new* edges
// (renewals of existing edges continue to succeed).
func (d *PeerDirectory) Announce(peerAddr, dataCenter, rack string, fids []string, ttl time.Duration, ownerCheck OwnerCheck) AnnounceResult {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	expiry := d.clock().Add(ttl)

	d.mu.Lock()
	defer d.mu.Unlock()

	pid := d.internPeer(peerAddr, dataCenter, rack)

	result := AnnounceResult{}
	for _, fid := range fids {
		if ownerCheck != nil && !ownerCheck(fid) {
			result.Rejected = append(result.Rejected, fid)
			d.rejected.Add(1)
			continue
		}
		holders, ok := d.index[fid]
		if !ok {
			if d.entryCount >= maxPeerDirectoryEntries {
				continue
			}
			holders = map[peerID]time.Time{}
			d.index[fid] = holders
		}
		if _, exists := holders[pid]; !exists {
			if d.entryCount >= maxPeerDirectoryEntries {
				continue
			}
			d.entryCount++
		}
		holders[pid] = expiry
		d.announces.Add(1)
	}
	return result
}

// LookupHolder is the public form of a holder entry. DataCenter and Rack
// are carried through so the fetcher can re-sort by its own locality
// before picking a peer to dial.
type LookupHolder struct {
	PeerAddr   string
	DataCenter string
	Rack       string
}

// maxLookupHolders caps how many holders a single ChunkLookup response
// returns per fid. The directory stores a holder per mount caching the
// fid, which on a large hot file can reach fleet size (hundreds). The
// fetcher typically only tries the first 1-3 before succeeding, so
// returning every entry is pure wire-overhead. LRU ordering ensures the
// freshest holders are the ones kept.
const maxLookupHolders = 16

// LookupResult holds the merged lookup response. Fids for which this
// receiver is not the owner are split out so the caller can retry.
type LookupResult struct {
	PeersByFid   map[string][]LookupHolder
	NotOwnerFids []string
}

// Lookup returns known holders for the requested fids. Expired edges are
// filtered out of the response but NOT deleted here — Sweep handles that
// under a write lock on its own schedule, so Lookup can stay read-only and
// concurrent. A fid owned by this receiver with no known holders returns
// an empty peer list.
//
// Holders are returned in LRU order (most recently announced first). Since
// each Announce stamps a fresh `now + ttl` expiry, sorting by expiry
// descending is equivalent to ordering by last-seen descending. Fetchers
// should try these in the returned order — the most recent announcer is
// the holder most likely to still have the chunk cached, given that each
// mount's chunk_cache is itself LRU.
func (d *PeerDirectory) Lookup(fids []string, ownerCheck OwnerCheck) LookupResult {
	now := d.clock()
	result := LookupResult{
		PeersByFid: map[string][]LookupHolder{},
	}

	// Collect candidate holders under the read lock, including the peer
	// metadata copies — Announce mutates peers[id] in place when an
	// existing peer re-announces, so reading peers[pid] outside the lock
	// would race with that write. Sorting happens after the unlock.
	type candidate struct {
		peerMetadata
		expiry time.Time
	}
	perFid := make(map[string][]candidate, len(fids))

	d.mu.RLock()
	for _, fid := range fids {
		d.lookups.Add(1)
		if ownerCheck != nil && !ownerCheck(fid) {
			result.NotOwnerFids = append(result.NotOwnerFids, fid)
			continue
		}
		holders, ok := d.index[fid]
		if !ok {
			result.PeersByFid[fid] = nil
			continue
		}
		cands := make([]candidate, 0, len(holders))
		for pid, expiry := range holders {
			if !expiry.After(now) {
				continue // Sweep will clean this up
			}
			cands = append(cands, candidate{peerMetadata: d.peers[pid], expiry: expiry})
		}
		perFid[fid] = cands
	}
	d.mu.RUnlock()

	for fid, cands := range perFid {
		sort.Slice(cands, func(i, j int) bool {
			if !cands[i].expiry.Equal(cands[j].expiry) {
				return cands[i].expiry.After(cands[j].expiry) // newest first
			}
			return cands[i].addr < cands[j].addr // deterministic tiebreak
		})
		// Cap response at maxLookupHolders. The fetcher typically tries 1-3
		// holders before succeeding; shipping every holder for hot fids is
		// wire-overhead without hit-rate gain.
		if len(cands) > maxLookupHolders {
			cands = cands[:maxLookupHolders]
		}
		out := make([]LookupHolder, len(cands))
		for i, c := range cands {
			out[i] = LookupHolder{PeerAddr: c.addr, DataCenter: c.dataCenter, Rack: c.rack}
		}
		result.PeersByFid[fid] = out
	}
	return result
}

// Sweep purges expired edges. Safe to call periodically. Peer slots are
// left in place — their count is bounded by fleet size (a few hundred at
// most) and never grows faster than the fleet itself, so reclaiming them
// isn't worth the bookkeeping.
func (d *PeerDirectory) Sweep() int {
	now := d.clock()
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for fid, holders := range d.index {
		for pid, expiry := range holders {
			if !expiry.After(now) {
				delete(holders, pid)
				d.entryCount--
				n++
			}
		}
		if len(holders) == 0 {
			delete(d.index, fid)
		}
	}
	d.evictions.Add(int64(n))
	return n
}

// Stats exposes counters for observability. O(1) — entryCount is kept in
// lock-step with map mutations.
func (d *PeerDirectory) Stats() (announces, lookups, rejected, evictions int64, entries int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.announces.Load(), d.lookups.Load(), d.rejected.Load(), d.evictions.Load(), d.entryCount
}
