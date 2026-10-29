package meta_cache

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The shared directory build behind EnsureVisited is a singleflight call: the
// first requester for a path owns the build, and every later requester for the
// same path waits on that same build. If the build runs on the owner's request
// context, the owner walking away (a FUSE request returning, the kernel
// interrupting it) kills the build for everybody else.
//
// A waiter whose own context is still alive then receives somebody else's
// cancellation. weed/mount/weedfs_dir_read.go classifies a canceled read as
// EINTR only when the *local* context is done, so the waiter falls through to
// fuse.EIO — the kernel hands EIO to readdirent, kubelet's applyFSGroup fails,
// and the volume never mounts. That is the 2026-10-28 gitea outage.
//
// The mock below reproduces it deterministically: the filer stream blocks
// mid-listing, the owner's context is canceled while the build is in flight,
// and a second requester joins that same build.

// ctxAwareListStream models a filer listing that fails with the request
// context's error once that context is canceled, the way a real gRPC stream
// does.
type ctxAwareListStream struct {
	ctx       context.Context
	responses []*filer_pb.ListEntriesResponse
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
	index     int
}

func (s *ctxAwareListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *ctxAwareListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *ctxAwareListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *ctxAwareListStream) CloseSend() error             { return nil }
func (s *ctxAwareListStream) Context() context.Context     { return s.ctx }
func (s *ctxAwareListStream) SendMsg(any) error            { return nil }
func (s *ctxAwareListStream) RecvMsg(any) error            { return nil }

// ctxAwareListClient counts how many listings were started. The shared build
// must be listed exactly once no matter how many requesters join it.
type ctxAwareListClient struct {
	filer_pb.SeaweedFilerClient
	stream *ctxAwareListStream

	mu    sync.Mutex
	calls int
}

func (c *ctxAwareListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	c.stream.ctx = ctx
	return c.stream, nil
}

func (c *ctxAwareListClient) listCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestEnsureVisitedSharedBuildSurvivesOwnerRequestCancel(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	entered := make(chan struct{})
	release := make(chan struct{})
	client := &ctxAwareListClient{
		stream: &ctxAwareListStream{
			responses: []*filer_pb.ListEntriesResponse{
				{
					Entry: &filer_pb.Entry{
						Name: "base.txt",
						Attributes: &filer_pb.FuseAttributes{
							Crtime:   1,
							Mtime:    1,
							FileMode: 0100644,
							FileSize: 3,
						},
					},
					SnapshotTsNs: 100,
				},
			},
			entered: entered,
			release: release,
		},
	}
	accessor := &buildFilerAccessor{client: client}

	// The owner starts the shared build.
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerErr := make(chan error, 1)
	go func() {
		ownerErr <- EnsureVisited(ownerCtx, mc, accessor, util.FullPath("/dir"), 0)
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("shared build never reached the filer listing")
	}

	// The owner's FUSE request goes away while the build is still in flight.
	cancelOwner()
	if err := <-ownerErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner error = %v, want context.Canceled", err)
	}

	// A second requester arrives and joins the still-running shared build.
	waiterErr := make(chan error, 1)
	go func() {
		waiterErr <- EnsureVisited(context.Background(), mc, accessor, util.FullPath("/dir"), 0)
	}()

	// Give the waiter time to join. If it started its own build instead, the
	// listing is requested twice — the repro would not be the production path.
	deadline := time.Now().Add(2 * time.Second)
	for client.listCalls() < 2 && time.Now().Before(deadline) {
		select {
		case err := <-waiterErr:
			t.Fatalf("waiter returned before the shared build finished (%v); it started a second listing (%d calls) instead of joining the shared build",
				err, client.listCalls())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if calls := client.listCalls(); calls != 1 {
		t.Fatalf("listing calls = %d, want 1 (the waiter must join the shared build)", calls)
	}

	// Let the build finish.
	close(release)

	select {
	case err := <-waiterErr:
		if err != nil {
			t.Fatalf("waiter inherited the owner's cancellation: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("waiter never returned")
	}

	if calls := client.listCalls(); calls != 1 {
		t.Fatalf("listing calls = %d, want 1", calls)
	}
	if !mc.IsDirectoryCached(util.FullPath("/dir")) {
		t.Fatal("directory /dir should be cached after the shared build completes")
	}
}
