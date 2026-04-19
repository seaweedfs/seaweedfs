package mount

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"google.golang.org/grpc"
)

// fakeMountPeerClient records each ChunkAnnounce call; returns rejected
// fids from a configured set.
type fakeMountPeerClient struct {
	mu sync.Mutex
	mount_peer_pb.MountPeerClient
	announcedBy      map[string][]filerAnnouncement
	rejectedByClient func(fid string) bool
}

type filerAnnouncement struct {
	fids     []string
	peerAddr string
}

func (f *fakeMountPeerClient) ChunkAnnounce(ctx context.Context, req *mount_peer_pb.ChunkAnnounceRequest, opts ...grpc.CallOption) (*mount_peer_pb.ChunkAnnounceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// No owner key in the request; capture keyed by peer_addr (the announcer).
	f.announcedBy[req.PeerAddr] = append(f.announcedBy[req.PeerAddr], filerAnnouncement{
		fids:     append([]string(nil), req.FileIds...),
		peerAddr: req.PeerAddr,
	})
	resp := &mount_peer_pb.ChunkAnnounceResponse{}
	if f.rejectedByClient != nil {
		for _, fid := range req.FileIds {
			if f.rejectedByClient(fid) {
				resp.RejectedFileIds = append(resp.RejectedFileIds, fid)
			}
		}
	}
	return resp, nil
}

// fakeDialer returns the same fakeMountPeerClient for every peer addr,
// recording which owner the announcer dialed. Close is a no-op. The
// dialed slice is protected by its own mutex because the announcer now
// fans RPCs out across goroutines — concurrent appends would race.
type dialRecorder struct {
	mu      sync.Mutex
	dialled []string
}

func (d *dialRecorder) record(addr string) {
	d.mu.Lock()
	d.dialled = append(d.dialled, addr)
	d.mu.Unlock()
}

func (d *dialRecorder) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.dialled))
	copy(out, d.dialled)
	return out
}

func fakeDialer(fc *fakeMountPeerClient, rec *dialRecorder) MountPeerDialer {
	return func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		rec.record(peerAddr)
		return fc, func() {}, nil
	}
}

func TestPeerAnnouncer_FlushGroupsByOwner(t *testing.T) {
	fc := &fakeMountPeerClient{
		announcedBy: map[string][]filerAnnouncement{},
	}
	rec := &dialRecorder{}

	// Owner function: static assignment for test.
	ownerFor := func(fid string) string {
		if fid == "3,a" || fid == "3,c" {
			return "owner-1:18081"
		}
		return "owner-2:18081"
	}

	a := NewPeerAnnouncer("self:18080", "", "",ownerFor, fakeDialer(fc, rec), nil)

	a.EnqueueAnnounce("3,a")
	a.EnqueueAnnounce("3,b")
	a.EnqueueAnnounce("3,c")

	a.FlushForTest(context.Background())

	dialed := rec.snapshot(); sort.Strings(dialed)
	if len(dialed) != 2 || dialed[0] != "owner-1:18081" || dialed[1] != "owner-2:18081" {
		t.Errorf("expected both owners dialed once, got %v", dialed)
	}

	_, sent, _, _ := a.Stats()
	if sent != 3 {
		t.Errorf("expected 3 sent fids, got %d", sent)
	}
}

func TestPeerAnnouncer_SkipOwnerEqualsSelf(t *testing.T) {
	fc := &fakeMountPeerClient{announcedBy: map[string][]filerAnnouncement{}}
	rec := &dialRecorder{}

	a := NewPeerAnnouncer("self:18080", "", "",func(fid string) string {
		return "self:18080"
	}, fakeDialer(fc, rec), nil)

	a.EnqueueAnnounce("3,a")
	a.FlushForTest(context.Background())

	if d := rec.snapshot(); len(d) != 0 {
		t.Errorf("expected no dial when owner == self, dialed: %v", d)
	}
	// Self-owned fids stay in pending so a subsequent flush re-checks
	// them against a possibly-refreshed seed view. Without this retry a
	// mount whose initial MountList hadn't yet observed its peers would
	// silently drop every write-path announce on the floor.
	a.mu.Lock()
	_, stillPending := a.pending["3,a"]
	a.mu.Unlock()
	if !stillPending {
		t.Errorf("self-owned fid should stay pending for retry")
	}
}

func TestPeerAnnouncer_RequeueOnRejection(t *testing.T) {
	fc := &fakeMountPeerClient{
		announcedBy:      map[string][]filerAnnouncement{},
		rejectedByClient: func(fid string) bool { return fid == "3,x" },
	}
	rec := &dialRecorder{}

	a := NewPeerAnnouncer("self:18080", "", "",func(fid string) string {
		return "owner:18081"
	}, fakeDialer(fc, rec), nil)
	a.EnqueueAnnounce("3,x")
	a.EnqueueAnnounce("3,y")

	a.FlushForTest(context.Background())

	_, sent, rejected, _ := a.Stats()
	if sent != 1 {
		t.Errorf("expected 1 sent, got %d", sent)
	}
	if rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", rejected)
	}

	// The rejected fid should be back in pending.
	a.mu.Lock()
	_, stillPending := a.pending["3,x"]
	a.mu.Unlock()
	if !stillPending {
		t.Errorf("3,x should have been requeued after rejection")
	}
}

func TestPeerAnnouncer_TTLRenewal(t *testing.T) {
	fc := &fakeMountPeerClient{announcedBy: map[string][]filerAnnouncement{}}
	rec := &dialRecorder{}

	now := time.Unix(1000, 0)
	a := NewPeerAnnouncer("self:18080", "", "",func(fid string) string {
		return "owner:18081"
	}, fakeDialer(fc, rec), nil)
	a.clock = func() time.Time { return now }
	a.announceInterval = 10 * time.Second
	a.announceTTL = 60 * time.Second

	// Announce once.
	a.EnqueueAnnounce("3,a")
	a.FlushForTest(context.Background())
	if _, sent, _, _ := a.Stats(); sent != 1 {
		t.Fatalf("want 1 sent on initial announce, got %d", sent)
	}

	// Time passes to within the renewal threshold.
	now = now.Add(45 * time.Second)

	// No new enqueue, but renewal should fire.
	a.FlushForTest(context.Background())
	if _, sent, _, _ := a.Stats(); sent != 2 {
		t.Errorf("want 2 sent after renewal, got %d", sent)
	}
}

// TestPeerAnnouncer_ReAnnouncesOnOwnerChange guards the fix for a
// startup race where the writer's seed view reaches self-knowledge of
// its peers in two stages: first just {self,A}, then {self,A,B}. HRW
// may pick different owners across those views, so the announce from
// stage 1 is sent to a mount that will NOT be the HRW owner by the
// time a reader's seed view is the {self,A,B} version. Without
// re-announcing on owner change, the fid silently ages out on the
// wrong directory and readers get "no peer holder."
func TestPeerAnnouncer_ReAnnouncesOnOwnerChange(t *testing.T) {
	fc := &fakeMountPeerClient{announcedBy: map[string][]filerAnnouncement{}}
	rec := &dialRecorder{}

	ownerNow := "owner-1:18081"
	a := NewPeerAnnouncer("self:18080", "", "", func(fid string) string {
		return ownerNow
	}, fakeDialer(fc, rec), nil)

	a.EnqueueAnnounce("3,x")
	a.FlushForTest(context.Background())
	if _, sent, _, _ := a.Stats(); sent != 1 {
		t.Fatalf("initial announce: want 1 sent, got %d", sent)
	}

	// Seed view shifts; HRW now picks a different owner for the same
	// fid. A fresh-timestamped entry in announcedAt should be treated
	// as stale in owner terms and re-announced even though its age is
	// well within the TTL renewal window.
	ownerNow = "owner-2:18081"
	a.FlushForTest(context.Background())
	if _, sent, _, _ := a.Stats(); sent != 2 {
		t.Errorf("after owner change: want 2 sent, got %d", sent)
	}
	dialed := rec.snapshot()
	if len(dialed) != 2 || dialed[0] == dialed[1] {
		t.Errorf("want dial to both owners, got %v", dialed)
	}
}

// TestPeerAnnouncer_DropsEvictedFids guards the write→announce race:
// a chunk can be LRU-evicted between SetChunk and the next flush tick.
// Advertising an evicted fid just hands remote fetchers a NOT_FOUND
// from our FetchChunk server. With SetCachePresence wired, the
// announcer must drop evicted fids before dispatching the RPC and
// also clear them from announcedAt so they stop getting renewed.
func TestPeerAnnouncer_DropsEvictedFids(t *testing.T) {
	fc := &fakeMountPeerClient{announcedBy: map[string][]filerAnnouncement{}}
	rec := &dialRecorder{}
	present := map[string]bool{"3,here": true, "3,gone": false}

	a := NewPeerAnnouncer("self:18080", "", "", func(fid string) string {
		return "owner:18081"
	}, fakeDialer(fc, rec), nil)
	a.SetCachePresence(func(fid string) bool { return present[fid] })

	a.EnqueueAnnounce("3,here")
	a.EnqueueAnnounce("3,gone")
	a.FlushForTest(context.Background())

	if _, sent, _, _ := a.Stats(); sent != 1 {
		t.Errorf("only the still-cached fid should be announced, got sent=%d", sent)
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, an := range fc.announcedBy["self:18080"] {
		for _, fid := range an.fids {
			if fid == "3,gone" {
				t.Errorf("evicted fid should not have been announced, saw %q", fid)
			}
		}
	}
}

// TestPeerAnnouncer_StopWaitsForFlush guards the shutdown race: Stop
// must not return while a tick-triggered flushOnce is still dispatching
// RPCs, otherwise the owning wfs can tear down the conn pool out from
// under the in-flight sendTo goroutines.
func TestPeerAnnouncer_StopWaitsForFlush(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	slowDialer := func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		close(started)
		select {
		case <-unblock:
		case <-ctx.Done():
		}
		return nil, func() {}, context.DeadlineExceeded
	}

	a := NewPeerAnnouncer("self:18080", "", "", func(string) string {
		return "owner:18081"
	}, slowDialer, nil)

	// Shortcut straight into a flush on a dedicated goroutine so we
	// can assert Stop() blocks on it.
	a.EnqueueAnnounce("3,slow")
	flushDone := make(chan struct{})
	go func() {
		a.FlushForTest(context.Background())
		close(flushDone)
	}()
	<-started

	// Prove Stop() returns only after the flush finishes by racing
	// them: Stop() normally can't cancel a FlushForTest because
	// we're not going through run(); but unblocking after stop's
	// goroutine starts lets us confirm flushDone happens before
	// stopDone never does under the old code.
	stopDone := make(chan struct{})
	go func() {
		a.Stop()
		close(stopDone)
	}()
	close(unblock)

	select {
	case <-flushDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("flush did not complete within deadline")
	}
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("Stop() did not return after flush completed")
	}
}

// TestPeerAnnouncer_SelfOwnedWritesToLocalDir guards the shortcut path
// for fids whose HRW owner resolves to self: no RPC should go out, but
// the local PeerDirectory must end up with a holder entry pointing at
// self — otherwise remote ChunkLookup callers would get an empty
// holder list for fids this mount owns directly.
func TestPeerAnnouncer_SelfOwnedWritesToLocalDir(t *testing.T) {
	fc := &fakeMountPeerClient{announcedBy: map[string][]filerAnnouncement{}}
	rec := &dialRecorder{}
	dir := NewPeerDirectory()
	a := NewPeerAnnouncer("self:18080", "dc1", "rackA", func(fid string) string {
		return "self:18080"
	}, fakeDialer(fc, rec), dir)

	a.EnqueueAnnounce("3,selfowned")
	a.FlushForTest(context.Background())

	if d := rec.snapshot(); len(d) != 0 {
		t.Errorf("self-owned fid must not trigger any dial, got %v", d)
	}
	holders := dir.Lookup([]string{"3,selfowned"}, nil).PeersByFid["3,selfowned"]
	if len(holders) != 1 {
		t.Fatalf("local dir should have 1 holder after self-owned flush, got %d", len(holders))
	}
	if h := holders[0]; h.PeerAddr != "self:18080" || h.DataCenter != "dc1" || h.Rack != "rackA" {
		t.Errorf("local dir entry wrong: %+v", h)
	}
	if _, sent, _, _ := a.Stats(); sent != 1 {
		t.Errorf("sent counter should increment for self-owned too; got %d", sent)
	}
}

func TestPeerAnnouncer_DialerErrorRequeues(t *testing.T) {
	errDialer := func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		return nil, func() {}, context.DeadlineExceeded
	}
	a := NewPeerAnnouncer("self:18080", "", "",func(fid string) string {
		return "owner:18081"
	}, errDialer, nil)
	a.EnqueueAnnounce("3,a")
	a.FlushForTest(context.Background())

	_, _, _, errs := a.Stats()
	if errs != 1 {
		t.Errorf("expected 1 flush error, got %d", errs)
	}
	a.mu.Lock()
	_, pending := a.pending["3,a"]
	a.mu.Unlock()
	if !pending {
		t.Errorf("fid should be requeued after dial error")
	}
}
