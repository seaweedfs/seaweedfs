package mount

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// remoteReadFiler answers the download of an uncached remote file: stalled
// parks it until the caller's context ends, otherwise it fails at once.
type remoteReadFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	stalled bool
	started chan struct{}
}

func (s *remoteReadFiler) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	if !s.stalled {
		return nil, status.Error(codes.NotFound, "remote object gone")
	}
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, ctx.Err().Error())
}

// startRemoteRead opens /dir/file as an uncached remote entry and reads it
// in the background with the given FUSE cancel channel.
func startRemoteRead(t *testing.T, fake *remoteReadFiler, cancel chan struct{}) <-chan fuse.Status {
	t.Helper()
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, fake)

	root := util.FullPath("/")
	wfs.inodeToPath.MarkChildrenCached(root)
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 4096},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 4096},
	}, 0, 0)

	done := make(chan fuse.Status, 1)
	go func() {
		_, code := wfs.Read(cancel, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: inode},
			Fh:       uint64(fh.fh),
			Size:     4096,
		}, make([]byte, 4096))
		done <- code
	}()
	return done
}

// The kernel's FUSE contract answers an interrupted request with EINTR on the
// original request. Interrupts come from any handled signal, Go's preemption
// SIGURG included, so EIO here would fail healthy reads.
func TestReadInterruptedReturnsEINTR(t *testing.T) {
	fake := &remoteReadFiler{stalled: true, started: make(chan struct{})}
	cancel := make(chan struct{})
	done := startRemoteRead(t, fake, cancel)

	select {
	case <-fake.started:
	case <-time.After(10 * time.Second):
		t.Fatal("read never reached the filer")
	}
	close(cancel)

	select {
	case code := <-done:
		if code != fuse.EINTR {
			t.Fatalf("interrupted read returned %v, want EINTR", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Read still blocked after the FUSE request was interrupted")
	}
}

// A read that fails without an interrupt is a real I/O error and stays EIO.
func TestReadFailureWithoutInterruptReturnsEIO(t *testing.T) {
	fake := &remoteReadFiler{started: make(chan struct{})}
	done := startRemoteRead(t, fake, make(chan struct{}))

	select {
	case code := <-done:
		if code != fuse.EIO {
			t.Fatalf("failed read returned %v, want EIO", code)
		}
		select {
		case <-fake.started:
		default:
			t.Fatal("read failed before it reached the filer")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("failed read never returned")
	}
}
