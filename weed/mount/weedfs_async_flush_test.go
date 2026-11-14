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

// Under writeback the kernel already moved EOF past the stored entry, so a
// drain that gives up must fence the inode whatever the filer answered.
func TestAbandonedWritebackFlushFencesInode(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {}

	for _, content := range []bool{true, false} {
		const name = "abandoned-writeback.log"
		wfs, _ := newCreateTestWFS(t)
		fake := &rejectingCreateFiler{rejections: 1 << 20, refuse: func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "denied")
		}}
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
		wfs.option.WritebackCache = true
		fh.writebackContentError = new(atomic.Bool)
		fh.dirtyMetadata = true
		fh.dirtyContent = content

		if err := wfs.flushMetadataWithRetry(fh, "/", name, util.FullPath("/").Child(name)); err == nil {
			t.Fatal("flush against a refusing filer succeeded")
		}
		if got := fh.writebackContentError.Load(); got != content {
			t.Fatalf("content=%v: fenced after abandoned writeback flush = %v, want %v", content, got, content)
		}
		if !content {
			continue
		}
		if err := wfs.checkContentPublication(fh, nil); !errors.Is(err, filer_pb.ErrPreconditionFailed) {
			t.Fatalf("checkContentPublication after abandonment = %v, want a precondition refusal", err)
		}
	}
}

func TestFailedFlockReleaseFencesInode(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {}

	for _, c := range []struct {
		name       string
		content    bool
		truncate   bool
		wantFenced bool
	}{
		{"content", true, false, true},
		{"metadata only", false, false, false},
		{"truncate", false, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			const name = "flock-release.log"
			wfs, _ := newCreateTestWFS(t)
			wfs.posixLocks = NewPosixLockTable()
			fake := &rejectingCreateFiler{rejections: 1 << 20, refuse: func(req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
				return nil, status.Error(codes.PermissionDenied, "denied")
			}}
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
			wfs.option.WritebackCache = true
			fenced := new(atomic.Bool)
			fh.writebackContentError = fenced
			fh.dirtyMetadata = true
			fh.dirtyContent = c.content
			if c.truncate {
				if code := wfs.SetAttr(make(chan struct{}), &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
					InHeader: fuse.InHeader{NodeId: out.NodeId},
					Valid:    fuse.FATTR_SIZE | fuse.FATTR_FH,
					Fh:       out.Fh,
					Size:     64,
				}}, &fuse.AttrOut{}); code != fuse.OK {
					t.Fatalf("SetAttr size = %v, want OK", code)
				}
			}

			wfs.Release(make(chan struct{}), &fuse.ReleaseIn{
				InHeader:     fuse.InHeader{NodeId: out.NodeId},
				Fh:           out.Fh,
				ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK,
			})
			if fake.calls.Load() == 0 {
				t.Fatal("synchronous release did not attempt the flush")
			}
			if got := fenced.Load(); got != c.wantFenced {
				t.Fatalf("fenced after failed release = %v, want %v", got, c.wantFenced)
			}
		})
	}
}

func TestReleaseFencesBeforeReopenSeesInode(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	wfs.option.WritebackCache = true
	const inode = 42
	entry := &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{}}
	fh, id, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, entry, 1, 0)
	fh.dirtyMetadata = true
	fh.dirtyContent = true

	released := wfs.fhMap.ReleaseByHandle(id)
	if released != fh {
		t.Fatal("last release did not hand back the handle")
	}
	reopened, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, entry, 1, 0)
	if reopened == fh {
		t.Fatal("reopen reused the released handle")
	}
	if err := wfs.checkContentPublication(reopened, nil); !errors.Is(err, filer_pb.ErrPreconditionFailed) {
		t.Fatalf("publication on a reopen after an abandoned release = %v, want a precondition refusal", err)
	}
	released.ReleaseHandle()
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
