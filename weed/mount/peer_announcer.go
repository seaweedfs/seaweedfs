package mount

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
)

// PeerAnnouncer batches and flushes ChunkAnnounce RPCs to the
// HRW-assigned owner mounts for each cached fid this mount holds.
//
// Shape:
//   * EnqueueAnnounce(fid) is a non-blocking push into an in-memory set.
//   * A background ticker flushes every announceInterval. Each flush
//     drains the set, adds fids due for TTL renewal, groups by owner
//     mount via HRW, and sends one ChunkAnnounce RPC per distinct owner.
//   * A successful send records announced_at[fid]. A rejected (not-owner)
//     fid is requeued so it will be retried after the seed view refreshes.
//
// See design-weed-mount-peer-chunk-sharing.md §4.4.
type PeerAnnouncer struct {
	selfAddr         string
	selfDataCenter   string
	selfRack         string
	ownerFor         func(fid string) string
	dialPeer         MountPeerDialer
	localDir         *PeerDirectory       // populated directly for fids whose HRW owner == self
	isCached         func(fid string) bool // residency check; set nil to disable
	announceInterval time.Duration
	announceTTL      time.Duration

	mu          sync.Mutex
	pending     map[string]struct{}
	announcedAt map[string]announceRecord

	stopCh  chan struct{}
	stopped atomic.Bool
	runWg   sync.WaitGroup // tracks the run goroutine so Stop can wait

	// flushCancelMu guards flushCancel, which is the cancel fn for the
	// currently in-flight flush (nil when idle). Stop takes the lock,
	// cancels the flush to unblock its RPCs fast, then waits on runWg.
	flushCancelMu sync.Mutex
	flushCancel   context.CancelFunc

	flushes      atomic.Int64
	sentFids     atomic.Int64
	rejectedFids atomic.Int64
	flushErrs    atomic.Int64

	clock func() time.Time
}

// MountPeerDialer opens a MountPeer gRPC client to a given peer. Tests
// inject a fake; production uses a real gRPC dial backed by a short
// connection cache.
type MountPeerDialer func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error)

// announceRecord is what we remember about a successful announce: who
// we told (the HRW owner at the time) and when. On each flush we
// recompute the HRW owner and, if it moved — typically because a new
// mount joined the seed list and the ring rebalanced — re-announce
// against the new owner. Without this, seed-view divergence during
// startup silently ages out fids on an owner that the reader never
// looks up.
type announceRecord struct {
	owner string
	at    time.Time
}

// NewPeerAnnouncer constructs an announcer. Caller must call Start.
// selfDataCenter and selfRack are the locality labels attached to every
// ChunkAnnounce so the receiving directory records holders with DC+rack
// and the fetcher can later re-rank by locality.
//
// localDir is optional: when non-nil, any fid whose HRW owner resolves
// to self is written directly into localDir instead of sending a
// self→self RPC (which the receiver would reject anyway). This is the
// only way a fid the writer itself owns becomes visible to peers'
// ChunkLookup calls.
func NewPeerAnnouncer(selfAddr, selfDataCenter, selfRack string, ownerFor func(fid string) string, dial MountPeerDialer, localDir *PeerDirectory) *PeerAnnouncer {
	return &PeerAnnouncer{
		selfAddr:         selfAddr,
		selfDataCenter:   selfDataCenter,
		selfRack:         selfRack,
		ownerFor:         ownerFor,
		dialPeer:         dial,
		localDir:         localDir,
		announceInterval: 15 * time.Second,
		announceTTL:      300 * time.Second,
		pending:          map[string]struct{}{},
		announcedAt:      map[string]announceRecord{},
		stopCh:           make(chan struct{}),
		clock:            time.Now,
	}
}

// EnqueueAnnounce marks a fid as needing announcement. Non-blocking;
// dedupes within a flush window.
func (a *PeerAnnouncer) EnqueueAnnounce(fid string) {
	a.mu.Lock()
	a.pending[fid] = struct{}{}
	a.mu.Unlock()
}

// SetCachePresence wires an optional residency check. When set, the
// announcer consults it right before dispatching a ChunkAnnounce RPC or
// writing a self-owned entry: if the cache no longer has the chunk
// (e.g. LRU-evicted under memory pressure between SetChunk and the
// next flush tick) we drop the announce rather than advertise bytes we
// can't serve back. nil disables the check.
func (a *PeerAnnouncer) SetCachePresence(isCached func(fid string) bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.isCached = isCached
}

// Start launches the background flush loop.
func (a *PeerAnnouncer) Start() {
	a.runWg.Add(1)
	go func() {
		defer a.runWg.Done()
		a.run()
	}()
}

// Stop halts the flush loop and waits for the current flush (if any) to
// finish. Safe to call multiple times. Callers that tear down shared
// dependencies (conn pool, local directory) can rely on no more
// sendTo/localDir.Announce goroutines being live after Stop returns.
func (a *PeerAnnouncer) Stop() {
	if a.stopped.Swap(true) {
		return
	}
	close(a.stopCh)
	// Cancel the in-flight flush's context so its RPCs return quickly
	// instead of running out the 10 s timeout.
	a.flushCancelMu.Lock()
	if a.flushCancel != nil {
		a.flushCancel()
	}
	a.flushCancelMu.Unlock()
	a.runWg.Wait()
}

// Stats exposes counters for observability.
func (a *PeerAnnouncer) Stats() (flushes, sentFids, rejectedFids, flushErrs int64) {
	return a.flushes.Load(), a.sentFids.Load(), a.rejectedFids.Load(), a.flushErrs.Load()
}

// announceFlushTimeout caps how long a single flush cycle can run. A slow
// or unresponsive owner peer would otherwise block a flush goroutine
// indefinitely, and since flushes fan out per owner in parallel a
// pileup would bleed into subsequent ticks and grow unbounded. The
// bound is set a bit under one flush interval so the next tick's flush
// doesn't chase the previous one's tail.
const announceFlushTimeout = 10 * time.Second

func (a *PeerAnnouncer) run() {
	t := time.NewTicker(a.announceInterval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), announceFlushTimeout)
			a.flushCancelMu.Lock()
			a.flushCancel = cancel
			a.flushCancelMu.Unlock()

			a.flushOnce(ctx)

			a.flushCancelMu.Lock()
			a.flushCancel = nil
			a.flushCancelMu.Unlock()
			cancel()
		}
	}
}

// flushOnce is exported via a wrapper for tests.
func (a *PeerAnnouncer) FlushForTest(ctx context.Context) {
	a.flushOnce(ctx)
}

func (a *PeerAnnouncer) flushOnce(ctx context.Context) {
	a.flushes.Add(1)
	now := a.clock()

	a.mu.Lock()
	// Swap pending in O(1) instead of copying.
	toAnnounce := a.pending
	a.pending = map[string]struct{}{}

	// Add renewals: (1) fids we announced long enough ago that they'll
	// expire on the owner side before our next flush tick; (2) fids
	// whose HRW owner has changed since we last announced them —
	// usually because a new mount joined the seed list and the ring
	// rebalanced. Also prune announcedAt entries that are so old
	// (> 2× TTL) that re-announcing them would be pointless.
	renewThreshold := now.Add(-a.announceTTL + 2*a.announceInterval)
	staleCutoff := now.Add(-2 * a.announceTTL)
	for fid, rec := range a.announcedAt {
		switch {
		case rec.at.Before(staleCutoff):
			delete(a.announcedAt, fid)
		case rec.at.Before(renewThreshold):
			toAnnounce[fid] = struct{}{}
		default:
			// Same fid, fresh timestamp — but owner may have moved.
			// Force a re-announce if HRW says a different mount is
			// the owner now.
			if a.ownerFor != nil {
				if cur := a.ownerFor(fid); cur != "" && cur != rec.owner {
					toAnnounce[fid] = struct{}{}
				}
			}
		}
	}
	a.mu.Unlock()

	if len(toAnnounce) == 0 {
		return
	}

	// Drop fids the cache no longer has. The write path hooks into
	// SetChunk + EnqueueAnnounce, but the chunk can be LRU-evicted in
	// the window between that and the next 15 s flush. Advertising a
	// chunk we can't actually serve just sends remote fetchers to our
	// FetchChunk, which then returns NOT_FOUND — a wasted round trip.
	// Also drop the announcedAt record so we don't keep renewing
	// evicted fids forever.
	a.mu.Lock()
	present := a.isCached
	a.mu.Unlock()
	if present != nil {
		for fid := range toAnnounce {
			if !present(fid) {
				delete(toAnnounce, fid)
				a.mu.Lock()
				delete(a.announcedAt, fid)
				a.mu.Unlock()
			}
		}
		if len(toAnnounce) == 0 {
			return
		}
	}

	// Classify each fid. Three buckets:
	//   byOwner — HRW owner is a remote mount; send a ChunkAnnounce RPC.
	//   selfOwned — HRW owner is us; write directly into our local
	//               directory (no RPC — we ARE the directory for this
	//               fid). Without this, fids we wrote AND HRW-own stay
	//               invisible to peer ChunkLookup forever.
	//   deferred — HRW owner unknown (empty seed view). Re-queue for
	//              the next flush once listOnce has populated seeds.
	byOwner := map[string][]string{}
	var selfOwned []string
	var deferred []string
	for fid := range toAnnounce {
		owner := ""
		if a.ownerFor != nil {
			owner = a.ownerFor(fid)
		}
		switch owner {
		case "":
			deferred = append(deferred, fid)
		case a.selfAddr:
			selfOwned = append(selfOwned, fid)
		default:
			byOwner[owner] = append(byOwner[owner], fid)
		}
	}
	if len(selfOwned) > 0 && a.localDir != nil {
		a.localDir.Announce(a.selfAddr, a.selfDataCenter, a.selfRack, selfOwned, a.announceTTL, nil)
		a.mu.Lock()
		for _, fid := range selfOwned {
			a.announcedAt[fid] = announceRecord{owner: a.selfAddr, at: now}
			a.sentFids.Add(1)
		}
		a.mu.Unlock()
	} else if len(selfOwned) > 0 {
		// No local directory wired (e.g. unit tests with nil). Re-queue
		// so a future flush with a possibly-different HRW owner can
		// send them.
		deferred = append(deferred, selfOwned...)
	}
	if len(deferred) > 0 {
		a.mu.Lock()
		for _, fid := range deferred {
			a.pending[fid] = struct{}{}
		}
		a.mu.Unlock()
	}

	// Diagnostic V(2): shape of the current flush. Helps post-mortem
	// debugging when an announce pipeline silently stops making progress
	// (e.g. seed view stuck at self-only, owner unreachable, etc).
	if len(byOwner) > 0 || len(selfOwned) > 0 || len(deferred) > 0 {
		glog.V(2).Infof("peer-announce flush: toAnnounce=%d byOwner=%d selfOwned=%d deferred=%d",
			len(toAnnounce), len(byOwner), len(selfOwned), len(deferred))
	}

	// Fan out one RPC per owner in parallel. A slow or unreachable
	// owner blocks only its own goroutine; other owners' announces
	// proceed immediately.
	var wg sync.WaitGroup
	for owner, fids := range byOwner {
		wg.Add(1)
		go func(owner string, fids []string) {
			defer wg.Done()
			a.sendTo(ctx, owner, fids, now)
		}(owner, fids)
	}
	wg.Wait()
}

func (a *PeerAnnouncer) sendTo(ctx context.Context, owner string, fids []string, now time.Time) {
	client, closeFn, err := a.dialPeer(ctx, owner)
	if err != nil {
		a.flushErrs.Add(1)
		// Requeue these fids: a future flush will retry.
		a.requeue(fids)
		glog.V(2).Infof("peer-announce dial %s: %v", owner, err)
		return
	}
	defer closeFn()

	resp, err := client.ChunkAnnounce(ctx, &mount_peer_pb.ChunkAnnounceRequest{
		FileIds:    fids,
		PeerAddr:   a.selfAddr,
		DataCenter: a.selfDataCenter,
		Rack:       a.selfRack,
		TtlSeconds: int32(a.announceTTL / time.Second),
	})
	if err != nil {
		a.flushErrs.Add(1)
		a.requeue(fids)
		glog.V(2).Infof("peer-announce %s: %v", owner, err)
		return
	}

	rejected := map[string]struct{}{}
	for _, f := range resp.RejectedFileIds {
		rejected[f] = struct{}{}
	}

	a.mu.Lock()
	for _, fid := range fids {
		if _, skip := rejected[fid]; skip {
			// Owner has changed or doesn't yet agree; retry later.
			a.pending[fid] = struct{}{}
			a.rejectedFids.Add(1)
			continue
		}
		a.announcedAt[fid] = announceRecord{owner: owner, at: now}
		a.sentFids.Add(1)
	}
	a.mu.Unlock()
}

func (a *PeerAnnouncer) requeue(fids []string) {
	a.mu.Lock()
	for _, fid := range fids {
		a.pending[fid] = struct{}{}
	}
	a.mu.Unlock()
}
