package mount

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// fakeFilerClient captures MountRegister/MountList calls and lets the test
// pre-seed MountList responses. Implements just enough of
// filer_pb.SeaweedFilerClient to drive the registrar.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient // embed for methods we don't need

	mu              sync.Mutex
	registerCalls   []filer_pb.MountRegisterRequest
	listResponse    filer_pb.MountListResponse
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

type fakeWfs struct {
	client *fakeFilerClient
}

func (w *fakeWfs) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(w.client)
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
	w := &fakeWfs{client: fc}

	r := NewPeerRegistrar(w, "self:18080", "dc1", "r1")
	// Avoid starting the real tickers — use the synchronous initial
	// calls that Start does.
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
	w := &fakeWfs{client: fc}
	r := NewPeerRegistrar(w, "self:18080", "", "")

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
	r := NewPeerRegistrar(&fakeWfs{client: &fakeFilerClient{}}, "self:18080", "", "")
	r.Stop()
	r.Stop() // second call must be a no-op (no panic)
}
