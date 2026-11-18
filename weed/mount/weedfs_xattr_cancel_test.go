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

// stalledLookupFiler parks every LookupDirectoryEntry until the caller's
// context ends, the way a wedged filer holds an RPC forever.
type stalledLookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	lookupStarted chan struct{}
}

func (s *stalledLookupFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	select {
	case <-s.lookupStarted:
	default:
		close(s.lookupStarted)
	}
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, ctx.Err().Error())
}

// A FUSE interrupt must unwind a metadata lookup stuck on the filer; without
// it the op waits on the RPC forever and the caller — Samba issuing
// listxattr — never sees an answer.
func TestListXAttrCanceledWhileFilerStalled(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &stalledLookupFiler{lookupStarted: make(chan struct{})}
	startFakeFiler(t, wfs, fake)

	wfs.inodeToPath.EnsurePath(util.FullPath("/dir"), true)
	filePath := util.FullPath("/dir/f")
	inode := wfs.inodeToPath.AllocateInode(filePath, time.Now().Unix())
	wfs.inodeToPath.AddPath(inode, filePath)

	cancel := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() {
		_, code := wfs.ListXAttr(cancel, &fuse.InHeader{NodeId: inode}, nil)
		done <- code
	}()

	select {
	case <-fake.lookupStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("lookup never reached the filer")
	}
	close(cancel)

	select {
	case code := <-done:
		if code != fuse.EINTR {
			t.Fatalf("interrupted lookup returned %v, want EINTR", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ListXAttr still blocked after the FUSE request was interrupted")
	}
}

// Even without an interrupt the filer lookup has a deadline, so a hung RPC
// returns a filesystem error instead of parking the caller indefinitely.
func TestListXAttrTimesOutWhileFilerStalled(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 50 * time.Millisecond
	fake := &stalledLookupFiler{lookupStarted: make(chan struct{})}
	startFakeFiler(t, wfs, fake)

	wfs.inodeToPath.EnsurePath(util.FullPath("/dir"), true)
	filePath := util.FullPath("/dir/f")
	inode := wfs.inodeToPath.AllocateInode(filePath, time.Now().Unix())
	wfs.inodeToPath.AddPath(inode, filePath)

	done := make(chan fuse.Status, 1)
	go func() {
		_, code := wfs.ListXAttr(nil, &fuse.InHeader{NodeId: inode}, nil)
		done <- code
	}()

	select {
	case code := <-done:
		if code != fuse.EIO {
			t.Fatalf("stalled filer lookup returned %v, want EIO", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("ListXAttr still blocked after the filer lookup deadline")
	}
}
