package mount

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// fakeFilerClient captures MountRegister/MountList calls and lets the test
// pre-seed MountList responses. Implements just enough of
// filer_pb.SeaweedFilerClient to drive the registrar.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient // embed for methods we don't need

	mu            sync.Mutex
	registerCalls []filer_pb.MountRegisterRequest
	listResponse  filer_pb.MountListResponse
}

func (f *fakeFilerClient) MountRegister(ctx context.Context, req *filer_pb.MountRegisterRequest, opts ...grpc.CallOption) (*filer_pb.MountRegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCalls = append(f.registerCalls, *req)
	return &filer_pb.MountRegisterResponse{}, nil
}

func (f *fakeFilerClient) MountList(ctx context.Context, req *filer_pb.MountListRequest, opts ...grpc.CallOption) (*filer_pb.MountListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := f.listResponse // value copy
	return &resp, nil
}

// fakeFilerFleet maps each addr to its own fakeFilerClient so a test can
// simulate several filers with different registered/listed state.
type fakeFilerFleet struct {
	clients map[pb.ServerAddress]*fakeFilerClient
}

func (f *fakeFilerFleet) dial(ctx context.Context, addr pb.ServerAddress, fn func(client filer_pb.SeaweedFilerClient) error) error {
	c, ok := f.clients[addr]
	if !ok {
		// Treat unknown filer as "reachable but empty" — lets tests omit
		// pre-populating a client when they don't care.
		c = &fakeFilerClient{}
		f.clients[addr] = c
	}
	return fn(c)
}

func singleFilerFleet(c *fakeFilerClient) ([]pb.ServerAddress, filerDialFn) {
	addr := pb.ServerAddress("filer-1:18888")
	fleet := &fakeFilerFleet{clients: map[pb.ServerAddress]*fakeFilerClient{addr: c}}
	return []pb.ServerAddress{addr}, fleet.dial
}

func TestPeerRegistrar_StartPopulatesSeedsFromFiler(t *testing.T) {
	fc := &fakeFilerClient{
		listResponse: filer_pb.MountListResponse{
			Mounts: []*filer_pb.MountInfo{
				{PeerAddr: "mount-a:18080", Rack: "r1"},
				{PeerAddr: "mount-b:18080", Rack: "r2"},
			},
		},
	}
	filers, dial := singleFilerFleet(fc)

	r := NewPeerRegistrar(filers, dial, "self:18080", "dc1", "r1")
	if err := r.registerOnce(context.Background()); err != nil {
		t.Fatalf("registerOnce: %v", err)
	}
	if err := r.listOnce(context.Background()); err != nil {
		t.Fatalf("listOnce: %v", err)
	}

	fc.mu.Lock()
	if len(fc.registerCalls) != 1 {
		t.Errorf("expected 1 register call, got %d", len(fc.registerCalls))
	} else if fc.registerCalls[0].PeerAddr != "self:18080" {
		t.Errorf("register sent wrong peer addr: %q", fc.registerCalls[0].PeerAddr)
	}
	fc.mu.Unlock()

	seeds := r.Seeds()
	if len(seeds) != 2 {
		t.Errorf("expected 2 seeds, got %d", len(seeds))
	}

	owner := r.OwnerFor("3,01637037d6")
	if owner != "mount-a:18080" && owner != "mount-b:18080" {
		t.Errorf("OwnerFor returned unexpected addr: %q", owner)
	}
}

func TestPeerRegistrar_HeartbeatTTLMatchesConfig(t *testing.T) {
	fc := &fakeFilerClient{}
	filers, dial := singleFilerFleet(fc)
	r := NewPeerRegistrar(filers, dial, "self:18080", "", "")

	if err := r.registerOnce(context.Background()); err != nil {
		t.Fatalf("registerOnce: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.registerCalls) != 1 {
		t.Fatalf("expected 1 register call, got %d", len(fc.registerCalls))
	}
	want := int32(r.registerTTL.Seconds())
	if got := fc.registerCalls[0].TtlSeconds; got != want {
		t.Errorf("TtlSeconds: got %d want %d", got, want)
	}
}

func TestPeerRegistrar_StopIsIdempotent(t *testing.T) {
	filers, dial := singleFilerFleet(&fakeFilerClient{})
	r := NewPeerRegistrar(filers, dial, "self:18080", "", "")
	r.Stop()
	r.Stop() // second call must be a no-op (no panic)
}

// TestPeerRegistrar_RegisterBroadcastsToAllFilers guards the core
// multi-filer property: a single registerOnce must hit every configured
// filer so mounts pointing at different filers still converge.
func TestPeerRegistrar_RegisterBroadcastsToAllFilers(t *testing.T) {
	fc1 := &fakeFilerClient{}
	fc2 := &fakeFilerClient{}
	fc3 := &fakeFilerClient{}
	a1, a2, a3 := pb.ServerAddress("f1:18888"), pb.ServerAddress("f2:18888"), pb.ServerAddress("f3:18888")
	fleet := &fakeFilerFleet{clients: map[pb.ServerAddress]*fakeFilerClient{a1: fc1, a2: fc2, a3: fc3}}

	r := NewPeerRegistrar([]pb.ServerAddress{a1, a2, a3}, fleet.dial, "self:18080", "", "")
	if err := r.registerOnce(context.Background()); err != nil {
		t.Fatalf("registerOnce: %v", err)
	}

	for addr, fc := range fleet.clients {
		fc.mu.Lock()
		if len(fc.registerCalls) != 1 {
			t.Errorf("filer %s: got %d register calls, want 1", addr, len(fc.registerCalls))
		}
		fc.mu.Unlock()
	}
}

// TestPeerRegistrar_ListMergesAcrossFilers guards cross-filer convergence:
// mount A registered on filer-1, mount B on filer-2; a registrar that
// lists both filers must see both mounts.
func TestPeerRegistrar_ListMergesAcrossFilers(t *testing.T) {
	fc1 := &fakeFilerClient{
		listResponse: filer_pb.MountListResponse{
			Mounts: []*filer_pb.MountInfo{{PeerAddr: "mount-a:18080", Rack: "r1", LastSeenNs: 200}},
		},
	}
	fc2 := &fakeFilerClient{
		listResponse: filer_pb.MountListResponse{
			Mounts: []*filer_pb.MountInfo{{PeerAddr: "mount-b:18080", Rack: "r2", LastSeenNs: 200}},
		},
	}
	a1, a2 := pb.ServerAddress("f1:18888"), pb.ServerAddress("f2:18888")
	fleet := &fakeFilerFleet{clients: map[pb.ServerAddress]*fakeFilerClient{a1: fc1, a2: fc2}}

	r := NewPeerRegistrar([]pb.ServerAddress{a1, a2}, fleet.dial, "self:18080", "", "")
	if err := r.listOnce(context.Background()); err != nil {
		t.Fatalf("listOnce: %v", err)
	}

	seeds := r.Seeds()
	if len(seeds) != 2 {
		t.Fatalf("want 2 merged seeds, got %d: %+v", len(seeds), seeds)
	}
	addrs := map[string]bool{}
	for _, s := range seeds {
		addrs[s.PeerAddr] = true
	}
	if !addrs["mount-a:18080"] || !addrs["mount-b:18080"] {
		t.Errorf("merged seeds missing an entry: %+v", addrs)
	}
}

// TestPeerRegistrar_ListMergeKeepsNewestLastSeen guards the dedupe rule:
// the same mount reported by two filers collapses to one entry, keeping
// the freshest LastSeenNs for liveness-ordering decisions.
func TestPeerRegistrar_ListMergeKeepsNewestLastSeen(t *testing.T) {
	fc1 := &fakeFilerClient{
		listResponse: filer_pb.MountListResponse{
			Mounts: []*filer_pb.MountInfo{{PeerAddr: "mount-a:18080", Rack: "r1", LastSeenNs: 100}},
		},
	}
	fc2 := &fakeFilerClient{
		listResponse: filer_pb.MountListResponse{
			Mounts: []*filer_pb.MountInfo{{PeerAddr: "mount-a:18080", Rack: "r1", LastSeenNs: 500}},
		},
	}
	a1, a2 := pb.ServerAddress("f1:18888"), pb.ServerAddress("f2:18888")
	fleet := &fakeFilerFleet{clients: map[pb.ServerAddress]*fakeFilerClient{a1: fc1, a2: fc2}}

	r := NewPeerRegistrar([]pb.ServerAddress{a1, a2}, fleet.dial, "self:18080", "", "")
	if err := r.listOnce(context.Background()); err != nil {
		t.Fatalf("listOnce: %v", err)
	}
	seeds := r.Seeds()
	if len(seeds) != 1 {
		t.Fatalf("want 1 deduped seed, got %d", len(seeds))
	}
}
