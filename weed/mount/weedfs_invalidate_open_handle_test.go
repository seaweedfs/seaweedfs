package mount

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newInvalidateTestWFS(t *testing.T) *WFS {
	t.Helper()

	// Map filer uid 2000 to local uid 1000 to verify the event entry gets the
	// same id translation a filer lookup would apply.
	uidGidMapper, err := meta_cache.NewUidGidMapper("1000:2000", "")
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
			// Nothing listens here: any secondary lookup during invalidation
			// fails, like the transient filer error that pins a handle.
			FilerAddresses: []pb.ServerAddress{
				pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1),
			},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			UidGidMapper:   uidGidMapper,
		},
	}

	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		wfs.invalidateOpenFileHandle,
		nil,
	)
	t.Cleanup(wfs.metaCache.Shutdown)

	return wfs
}

// An update event must refresh an open file handle from the entry the event
// itself carries. A second lookup can fail transiently or serve stale cached
// metadata, and the subscription cursor has already advanced, so a missed
// refresh leaves the handle pinned to the old entry until an unrelated event
// for the same path arrives.
func TestUpdateEventRefreshesOpenFileHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 180020, Uid: 2000},
				Chunks:     []*filer_pb.FileChunk{{FileId: "1,ab1", Size: 180020}},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply update event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	entry := fh.GetEntry().GetEntry()
	if entry.Attributes.FileSize != 180020 {
		t.Fatalf("open handle file size = %d, want 180020", entry.Attributes.FileSize)
	}
	if len(entry.GetChunks()) != 1 {
		t.Fatalf("open handle chunks = %d, want 1", len(entry.GetChunks()))
	}
	if entry.Attributes.Uid != 1000 {
		t.Fatalf("open handle uid = %d, want filer uid 2000 mapped to local 1000", entry.Attributes.Uid)
	}

	// A delete leaves the handle with its last entry so unlinked-but-open
	// reads keep working.
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 180020 {
		t.Fatalf("open handle file size after delete = %d, want 180020", size)
	}
}
