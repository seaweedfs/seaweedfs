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

	a := NewPeerAnnouncer("self:18080", "", "",ownerFor, fakeDialer(fc, rec))

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
	}, fakeDialer(fc, rec))

	a.EnqueueAnnounce("3,a")
	a.FlushForTest(context.Background())

	if d := rec.snapshot(); len(d) != 0 {
		t.Errorf("expected no dial when owner == self, dialed: %v", d)
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
	}, fakeDialer(fc, rec))
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
	}, fakeDialer(fc, rec))
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

func TestPeerAnnouncer_DialerErrorRequeues(t *testing.T) {
	errDialer := func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		return nil, func() {}, context.DeadlineExceeded
	}
	a := NewPeerAnnouncer("self:18080", "", "",func(fid string) string {
		return "owner:18081"
	}, errDialer)
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
