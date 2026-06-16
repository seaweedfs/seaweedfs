package mount

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cacheRemoteTestServer broadcasts the matching invalidate event before
// returning the cached entry, so the apply loop is busy invalidating the same
// file handle the read still holds when downloadRemoteEntry applies its event.
type cacheRemoteTestServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	wfs               *WFS
	dir, name         string
	content           []byte
	invalidateStarted chan struct{}
}

func (s *cacheRemoteTestServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	cached := &filer_pb.Entry{
		Name:       s.name,
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(s.content))},
		Content:    s.content,
	}
	go s.wfs.metaCache.ApplyMetadataResponse(context.Background(), metadataUpdateEvent(s.dir, cached), meta_cache.SubscriberMetadataResponseApplyOptions)
	<-s.invalidateStarted
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry:         cached,
		MetadataEvent: metadataUpdateEvent(s.dir, cached),
	}, nil
}

// TestReadUncachedRemoteEntryDoesNotDeadlock guards the read of an uncached
// remote file against the apply-loop invalidate that needs its file-handle lock.
func TestReadUncachedRemoteEntryDoesNotDeadlock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	server := pb.NewGrpcServer()
	testServer := &cacheRemoteTestServer{
		dir:               "/dir",
		name:              "file",
		content:           []byte("hello remote world"),
		invalidateStarted: make(chan struct{}, 1),
	}
	filer_pb.RegisterSeaweedFilerServer(server, testServer)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}

	root := util.FullPath("/")
	wfs := &WFS{
		signature:         1,
		inodeToPath:       NewInodeToPath(root, 0),
		fhMap:             NewFileHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		hardLinkLockTable: util.NewLockTable[string](),
		option: &Option{
			ChunkSizeLimit:     1024,
			ConcurrentReaders:  1,
			VolumeServerAccess: "filerProxy",
			FilerAddresses: []pb.ServerAddress{
				pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port),
			},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	testServer.wfs = wfs

	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		// Mirror weedfs.go's invalidateFunc: take the file handle exclusive lock.
		func(path util.FullPath, _ *filer_pb.Entry) {
			inode, ok := wfs.inodeToPath.GetInode(path)
			if !ok {
				return
			}
			fh, ok := wfs.fhMap.FindFileHandle(inode)
			if !ok {
				return
			}
			select {
			case testServer.invalidateStarted <- struct{}{}:
			default:
			}
			lock := wfs.fhLockTable.AcquireLock("invalidateFunc", fh.fh, util.ExclusiveLock)
			wfs.fhLockTable.ReleaseLock(fh.fh, lock)
		},
		nil,
	)
	wfs.inodeToPath.MarkChildrenCached(root)
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))
	t.Cleanup(func() { wfs.metaCache.Shutdown() })

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(testServer.content))},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: int64(len(testServer.content))},
	})

	buff := make([]byte, len(testServer.content))
	done := make(chan fuse.Status, 1)
	go func() {
		_, status := wfs.Read(make(chan struct{}), &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: inode},
			Fh:       uint64(fh.fh),
			Offset:   0,
			Size:     uint32(len(buff)),
		}, buff)
		done <- status
	}()

	select {
	case status := <-done:
		if status != fuse.OK {
			t.Fatalf("Read status = %v, want OK", status)
		}
		if string(buff) != string(testServer.content) {
			t.Fatalf("Read content = %q, want %q", buff, testServer.content)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Read of an uncached remote entry deadlocked")
	}
}
