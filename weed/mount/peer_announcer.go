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
	announceInterval time.Duration
	announceTTL      time.Duration

	mu           sync.Mutex
	pending      map[string]struct{}
	announcedAt  map[string]time.Time

	stopCh  chan struct{}
	stopped atomic.Bool

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

// NewPeerAnnouncer constructs an announcer. Caller must call Start.
// selfDataCenter and selfRack are the locality labels attached to every
// ChunkAnnounce so the receiving directory records holders with DC+rack
// and the fetcher can later re-rank by locality.
func NewPeerAnnouncer(selfAddr, selfDataCenter, selfRack string, ownerFor func(fid string) string, dial MountPeerDialer) *PeerAnnouncer {
	return &PeerAnnouncer{
		selfAddr:         selfAddr,
		selfDataCenter:   selfDataCenter,
		selfRack:         selfRack,
		ownerFor:         ownerFor,
		dialPeer:         dial,
		announceInterval: 15 * time.Second,
		announceTTL:      300 * time.Second,
		pending:          map[string]struct{}{},
		announcedAt:      map[string]time.Time{},
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

// Start launches the background flush loop.
func (a *PeerAnnouncer) Start() {
	go a.run()
}

// Stop halts the flush loop. Safe to call multiple times.
func (a *PeerAnnouncer) Stop() {
	if a.stopped.Swap(true) {
		return
	}
	close(a.stopCh)
}

// Stats exposes counters for observability.
func (a *PeerAnnouncer) Stats() (flushes, sentFids, rejectedFids, flushErrs int64) {
	return a.flushes.Load(), a.sentFids.Load(), a.rejectedFids.Load(), a.flushErrs.Load()
}

func (a *PeerAnnouncer) run() {
	t := time.NewTicker(a.announceInterval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			a.flushOnce(context.Background())
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

	// Add renewals: fids we announced long enough ago that they'll
	// expire on the owner side before our next flush tick. Also prune
	// announcedAt entries that are so old (> 2× TTL) that re-announcing
	// them would be pointless — this bounds announcedAt memory at
	// roughly the peak cached-fid set of the mount.
	renewThreshold := now.Add(-a.announceTTL + 2*a.announceInterval)
	staleCutoff := now.Add(-2 * a.announceTTL)
	for fid, at := range a.announcedAt {
		switch {
		case at.Before(staleCutoff):
			delete(a.announcedAt, fid)
		case at.Before(renewThreshold):
			toAnnounce[fid] = struct{}{}
		}
	}
	a.mu.Unlock()

	if len(toAnnounce) == 0 {
		return
	}

	// Group by HRW owner. Fids whose current owner == self are skipped
	// (we are our own directory owner; no RPC needed — the directory
	// shard can be updated by the caller directly if wanted, but phase 1
	// keeps the hot path untouched for simplicity).
	byOwner := map[string][]string{}
	for fid := range toAnnounce {
		owner := ""
		if a.ownerFor != nil {
			owner = a.ownerFor(fid)
		}
		if owner == "" || owner == a.selfAddr {
			continue
		}
		byOwner[owner] = append(byOwner[owner], fid)
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
		a.announcedAt[fid] = now
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
