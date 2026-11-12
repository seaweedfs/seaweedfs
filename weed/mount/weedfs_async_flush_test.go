package mount

import (
	"context"
	"errors"
	"fmt"
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
	refuse     func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error)
	calls      atomic.Int32
}

func (s *rejectingCreateFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if s.calls.Add(1) <= s.rejections {
		return s.refuse(req)
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

	const name = "unavailable-transport.log"
	refusals := map[string]func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error){
		"grpc status": func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "denied")
		},
		"response error": func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			return &filer_pb.CreateEntryResponse{Error: "store write refused"}, nil
		},
	}
	for label, refuse := range refusals {
		t.Run(label, func(t *testing.T) {
			wfs, _ := newCreateTestWFS(t)
			fake := &rejectingCreateFiler{rejections: 20, refuse: refuse}
			startFakeFiler(t, wfs, fake)

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
		})
	}
}

func TestIsFilerUnreachable(t *testing.T) {
	path := "/unavailable/transport.log"
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"dial refused", fmt.Errorf("fh flush create %s: %w", path, fmt.Errorf("CreateEntry: %w",
			status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:18888: connect: connection refused"`))), true},
		{"call timed out", fmt.Errorf("fh flush create %s: %w", path, status.Error(codes.Unavailable, "filer 127.0.0.1:18888: call timed out")), true},
		{"read only", fmt.Errorf("fh flush create %s: %w", path, fmt.Errorf("CreateEntry %s: %w", path, filer_pb.ErrReadOnly)), false},
		{"message only", fmt.Errorf("fh flush create %s: %w", path, fmt.Errorf("CreateEntry: %w", errors.New("existing "+path+" is a directory"))), false},
		{"permission denied", fmt.Errorf("fh flush create %s: %w", path, status.Error(codes.PermissionDenied, "denied")), false},
		{"message too large", fmt.Errorf("fh flush create %s: %w", path, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")), false},
	}
	for _, c := range cases {
		if got := isFilerUnreachable(c.err); got != c.want {
			t.Errorf("%s: isFilerUnreachable(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}
