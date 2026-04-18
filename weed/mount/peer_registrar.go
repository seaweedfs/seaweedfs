package mount

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// PeerRegistrar maintains this mount's presence in the filer's mount
// registry and its local snapshot of the seed set. It runs two background
// tickers:
//
//   - MountRegister heartbeats, refreshing the filer's TTL entry for us.
//   - MountList polling, so OwnerFor() has an up-to-date view without
//     traversing the filer on every chunk operation.
//
// The registrar is single-filer in phase 1: calls go to whichever filer
// the WFS filer-client session happens to be talking to. Cross-filer
// convergence falls out of the TTL-bounded renewals; see design §4.2.1.
type PeerRegistrar struct {
	wfs               peerFilerClient
	selfPeerAddr      string
	selfDc            string
	selfRack          string
	registerInterval  time.Duration
	registerTTL       time.Duration
	listInterval      time.Duration

	mu    sync.RWMutex
	seeds []SeedPeer

	// stopCtx cancels when Stop() is called. Background RPCs scope their
	// deadline to this so unmount does not block on pending filer calls.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	stopped    atomic.Bool
}

// peerFilerClient is the subset of WFS the registrar depends on. Extracting
// it lets us feed a fake in unit tests.
type peerFilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

// NewPeerRegistrar constructs the registrar; Start launches the background
// loops.
func NewPeerRegistrar(wfs peerFilerClient, selfAddr, dc, rack string) *PeerRegistrar {
	ctx, cancel := context.WithCancel(context.Background())
	return &PeerRegistrar{
		wfs:              wfs,
		selfPeerAddr:     selfAddr,
		selfRack:         rack,
		selfDc:           dc,
		registerInterval: 30 * time.Second,
		registerTTL:      90 * time.Second,
		listInterval:     30 * time.Second,
		stopCtx:          ctx,
		stopCancel:       cancel,
	}
}

// Start does an initial register+list synchronously (so OwnerFor has a
// usable view immediately) and then kicks off the background loops.
func (r *PeerRegistrar) Start(ctx context.Context) error {
	if err := r.registerOnce(ctx); err != nil {
		glog.V(1).Infof("initial MountRegister: %v", err)
		// Do not fail startup — the mount must still serve reads even if
		// the filer doesn't know about us yet.
	}
	if err := r.listOnce(ctx); err != nil {
		glog.V(1).Infof("initial MountList: %v", err)
	}
	go r.loopRegister()
	go r.loopList()
	return nil
}

// Stop halts the background loops and cancels any in-flight RPCs they
// may have launched. Safe to call multiple times.
func (r *PeerRegistrar) Stop() {
	if r.stopped.Swap(true) {
		return
	}
	r.stopCancel()
}

// Seeds returns the currently-known seed set. Callers MUST NOT mutate the
// returned slice — it is shared with concurrent listOnce readers. Because
// listOnce atomically swaps to a brand-new slice on every refresh rather
// than mutating in place, returning the slice header directly is safe and
// avoids a per-call allocation on the read-hot OwnerFor path.
func (r *PeerRegistrar) Seeds() []SeedPeer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.seeds
}

// OwnerFor is a convenience wrapper that runs HRW against the current
// seed snapshot.
func (r *PeerRegistrar) OwnerFor(fid string) string {
	return OwnerFor(fid, r.Seeds())
}

func (r *PeerRegistrar) registerOnce(ctx context.Context) error {
	return r.wfs.WithFilerClient(false, func(c filer_pb.SeaweedFilerClient) error {
		_, err := c.MountRegister(ctx, &filer_pb.MountRegisterRequest{
			PeerAddr:   r.selfPeerAddr,
			Rack:       r.selfRack,
			DataCenter: r.selfDc,
			TtlSeconds: int32(r.registerTTL / time.Second),
		})
		return err
	})
}

func (r *PeerRegistrar) listOnce(ctx context.Context) error {
	return r.wfs.WithFilerClient(false, func(c filer_pb.SeaweedFilerClient) error {
		resp, err := c.MountList(ctx, &filer_pb.MountListRequest{})
		if err != nil {
			return err
		}
		next := make([]SeedPeer, 0, len(resp.Mounts))
		for _, m := range resp.Mounts {
			next = append(next, SeedPeer{PeerAddr: m.PeerAddr, Rack: m.Rack})
		}
		r.mu.Lock()
		r.seeds = next
		r.mu.Unlock()
		return nil
	})
}

func (r *PeerRegistrar) loopRegister() {
	t := time.NewTicker(r.registerInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCtx.Done():
			return
		case <-t.C:
			if err := r.registerOnce(r.stopCtx); err != nil {
				glog.V(2).Infof("MountRegister heartbeat: %v", err)
			}
		}
	}
}

func (r *PeerRegistrar) loopList() {
	t := time.NewTicker(r.listInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCtx.Done():
			return
		case <-t.C:
			if err := r.listOnce(r.stopCtx); err != nil {
				glog.V(2).Infof("MountList refresh: %v", err)
			}
		}
	}
}
