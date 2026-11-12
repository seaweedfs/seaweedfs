package mount

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rejectingCreateFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	rejections int32
	calls      atomic.Int32
}

func (s *rejectingCreateFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if s.calls.Add(1) <= s.rejections {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

// A filer that answers with a refusal is not an outage, even when the file's
// own path reads like one: the deferred flush gives up after its bounded
// attempts instead of retrying until the refusal stops.
func TestFlushMetadataWithRetryStopsOnFilerRefusal(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {}

	wfs, _ := newCreateTestWFS(t)
	fake := &rejectingCreateFiler{rejections: 20}
	startFakeFiler(t, wfs, fake)

	const name = "unavailable-transport.log"
	out := &fuse.CreateOut{}
	if code := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_WRONLY | syscall.O_CREAT,
		Mode:     0o644,
	}, name, out); code != fuse.OK {
		t.Fatalf("Create status = %v, want OK", code)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if fh == nil {
		t.Fatal("Create did not register an open file handle")
	}

	wfs.flushMetadataWithRetry(fh, "/", name, util.FullPath("/").Child(name))

	if got := fake.calls.Load(); got != metadataFlushRetries+1 {
		t.Fatalf("CreateEntry calls = %d, want the bounded %d", got, metadataFlushRetries+1)
	}
	if !fh.dirtyMetadata {
		t.Fatal("refused flush cleared dirtyMetadata")
	}
}
