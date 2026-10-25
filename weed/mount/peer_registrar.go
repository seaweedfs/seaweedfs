package mount

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// PeerRegistrar maintains this mount's presence in every configured
// filer's mount registry and its local snapshot of the merged seed set.
// It runs two background tickers:
//
//   - MountRegister heartbeats, fanned out to every filer in parallel so
//     each filer keeps a fresh TTL entry for us. Mounts pointing at
//     different filers therefore still see each other once all filers
//     have been heartbeated.
//   - MountList polling, fanned out identically, merged by peer_addr
//     (newest LastSeenNs wins) so OwnerFor() has a view of the whole
//     fleet regardless of which filer each peer happens to heartbeat
//     through.
//
// An unreachable filer is tolerated: we log and continue as long as at
// least one filer succeeds. Entries cached on a permanently-gone filer
// fall out of the merged view on their own TTL.
type PeerRegistrar struct {
	filerAddrs       []pb.ServerAddress
	dialFiler        filerDialFn
	selfPeerAddr     string
	selfDc           string
	selfRack         string
	registerInterval time.Duration
	registerTTL      time.Duration
	listInterval     time.Duration

	mu    sync.RWMutex
	seeds []SeedPeer

	// stopCtx cancels when Stop() is called. Background RPCs scope their
	// deadline to this so unmount does not block on pending filer calls.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	stopped    atomic.Bool
}

// filerDialFn is how the registrar reaches one configured filer. The
// production wiring is pb.WithGrpcFilerClient; tests inject a fake.
type filerDialFn func(ctx context.Context, addr pb.ServerAddress, fn func(client filer_pb.SeaweedFilerClient) error) error

// NewPeerRegistrar constructs the registrar; Start launches the background
// loops. Callers must supply the full filer set so heartbeats and list
// polls reach every filer — otherwise mounts talking to different filers
// never observe each other.
func NewPeerRegistrar(filers []pb.ServerAddress, dial filerDialFn, selfAddr, dc, rack string) *PeerRegistrar {
	ctx, cancel := context.WithCancel(context.Background())
	return &PeerRegistrar{
		filerAddrs:       filers,
		dialFiler:        dial,
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
		// no filer yet knows about us.
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

// filerRPCTimeout bounds a single MountRegister / MountList call so a slow
// or partitioned filer can't wedge the background loops (which run on a
// 30 s cadence). 20 s gives room for TLS handshakes on cold connections
// while leaving headroom before the next tick.
const filerRPCTimeout = 20 * time.Second

// registerOnce fans a MountRegister out to every configured filer in
// parallel. Returns an error only if every filer failed; otherwise the
// best-effort semantics let the mount proceed when some filer is down.
func (r *PeerRegistrar) registerOnce(ctx context.Context) error {
	if len(r.filerAddrs) == 0 {
		return fmt.Errorf("no filers configured")
	}
	ctx, cancel := context.WithTimeout(ctx, filerRPCTimeout)
	defer cancel()
	req := &filer_pb.MountRegisterRequest{
		PeerAddr:   r.selfPeerAddr,
		Rack:       r.selfRack,
		DataCenter: r.selfDc,
		TtlSeconds: int32(r.registerTTL / time.Second),
	}
	var wg sync.WaitGroup
	var successes atomic.Int32
	for _, addr := range r.filerAddrs {
		wg.Add(1)
		go func(addr pb.ServerAddress) {
			defer wg.Done()
			err := r.dialFiler(ctx, addr, func(c filer_pb.SeaweedFilerClient) error {
				_, err := c.MountRegister(ctx, req)
				return err
			})
			if err != nil {
				glog.V(2).Infof("MountRegister %s: %v", addr, err)
				return
			}
			successes.Add(1)
		}(addr)
	}
	wg.Wait()
	if successes.Load() == 0 {
		return fmt.Errorf("MountRegister failed on all %d filer(s)", len(r.filerAddrs))
	}
	return nil
}

// listOnce polls MountList from every filer in parallel and merges the
// responses by peer_addr (newest LastSeenNs wins). This way two mounts
// heartbeating through different filers still end up in each other's
// seed view as soon as at least one filer has been listed on each side.
func (r *PeerRegistrar) listOnce(ctx context.Context) error {
	if len(r.filerAddrs) == 0 {
		r.mu.Lock()
		r.seeds = nil
		r.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, filerRPCTimeout)
	defer cancel()
	var (
		mu     sync.Mutex
		merged = map[string]*filer_pb.MountInfo{}
		fails  int
	)
	var wg sync.WaitGroup
	for _, addr := range r.filerAddrs {
		wg.Add(1)
		go func(addr pb.ServerAddress) {
			defer wg.Done()
			err := r.dialFiler(ctx, addr, func(c filer_pb.SeaweedFilerClient) error {
				resp, err := c.MountList(ctx, &filer_pb.MountListRequest{})
				if err != nil {
					return err
				}
				mu.Lock()
				for _, m := range resp.Mounts {
					if prev, ok := merged[m.PeerAddr]; !ok || m.LastSeenNs > prev.LastSeenNs {
						merged[m.PeerAddr] = m
					}
				}
				mu.Unlock()
				return nil
			})
			if err != nil {
				mu.Lock()
				fails++
				mu.Unlock()
				glog.V(2).Infof("MountList %s: %v", addr, err)
			}
		}(addr)
	}
	wg.Wait()

	if fails == len(r.filerAddrs) {
		return fmt.Errorf("MountList failed on all %d filer(s)", len(r.filerAddrs))
	}

	next := make([]SeedPeer, 0, len(merged))
	for _, m := range merged {
		next = append(next, SeedPeer{PeerAddr: m.PeerAddr, DataCenter: m.DataCenter, Rack: m.Rack})
	}
	r.mu.Lock()
	r.seeds = next
	r.mu.Unlock()
	return nil
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
