package mount

import (
	"context"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newLookupCacheTestWFS(t *testing.T, ttlSec int) *WFS {
	t.Helper()
	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}
	root := util.FullPath("/")
	wfs := &WFS{
		signature:         1,
		inodeToPath:       NewInodeToPath(root, ttlSec),
		fhMap:             NewFileHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		hardLinkLockTable: util.NewLockTable[string](),
		option: &Option{
			ChunkSizeLimit:     1024,
			ConcurrentReaders:  1,
			VolumeServerAccess: "filerProxy",
			FilerAddresses: []pb.ServerAddress{
				pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1),
			},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			UidGidMapper:   uidGidMapper,
		},
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper, root, false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		wfs.onEntryInvalidation, nil,
	)
	t.Cleanup(wfs.metaCache.Shutdown)
	return wfs
}

type lookupCacheTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries      []*filer_pb.Entry
	snapshotTsNs int64
	listCalls    atomic.Int32
	lookupCalls  atomic.Int32
}

func (s *lookupCacheTestFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.lookupCalls.Add(1)
	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: 123, FileMode: 0100644},
		},
	}, nil
}

func (s *lookupCacheTestFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	s.listCalls.Add(1)
	for _, e := range s.entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	if s.snapshotTsNs != 0 {
		stream.SetTrailer(metadata.Pairs(filer_pb.ListSnapshotTsNsTrailerKey, strconv.FormatInt(s.snapshotTsNs, 10)))
	}
	return nil
}

// expireDirCache marks a cached directory's TTL as elapsed without invalidating
// it, reproducing the state the kernel leaves the mount in when it still holds
// the directory listing (FOPEN_CACHE_DIR | FOPEN_KEEP_CACHE) past cacheMetaTtlSec.
func expireDirCache(t *testing.T, itp *InodeToPath, dir util.FullPath) {
	t.Helper()
	itp.Lock()
	defer itp.Unlock()
	d := itp.dirStateOf(dir)
	if d == nil {
		t.Fatalf("expireDirCache: no dirState for %s", dir)
	}
	if !d.isChildrenCached {
		t.Fatalf("expireDirCache: %s not cached", dir)
	}
	d.cachedExpiresTime = time.Now().Add(-time.Second)
}

// TestLookupRebuildsExpiredDirectoryCache reproduces issue #11262: after the
// metadata cache TTL elapses the kernel can still serve directory listings
// from its page cache, so ReadDir never runs and EnsureVisited is not called.
// Metadata-heavy listings (e.g. `ls --color`) then reach Lookup, which must
// rebuild the directory cache once instead of issuing one LookupEntry RPC
// per entry.
func TestLookupRebuildsExpiredDirectoryCache(t *testing.T) {
	wfs := newLookupCacheTestWFS(t, 60)
	dir := util.FullPath("/dir")
	wfs.inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)

	fake := &lookupCacheTestFiler{
		entries: []*filer_pb.Entry{
			{Name: "a", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644}},
			{Name: "b", Attributes: &filer_pb.FuseAttributes{FileSize: 2, FileMode: 0100644}},
			{Name: "c", Attributes: &filer_pb.FuseAttributes{FileSize: 3, FileMode: 0100644}},
		},
		snapshotTsNs: 5000,
	}
	startFakeFiler(t, wfs, fake)

	if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dir, 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	if !wfs.inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory should be cached after EnsureVisited")
	}

	expireDirCache(t, wfs.inodeToPath, dir)
	if wfs.inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory cache should be expired")
	}

	listCallsBefore := fake.listCalls.Load()
	lookupCallsBefore := fake.lookupCalls.Load()

	for _, name := range []string{"a", "b", "c"} {
		entry, _, status := wfs.lookupEntry(dir.Child(name))
		if status != fuse.OK {
			t.Fatalf("lookupEntry %s: %v", name, status)
		}
		if entry == nil || entry.Name() != name {
			t.Fatalf("lookupEntry %s: got %v", name, entry)
		}
	}

	listCalls := fake.listCalls.Load() - listCallsBefore
	lookupCalls := fake.lookupCalls.Load() - lookupCallsBefore
	if lookupCalls != 0 {
		t.Errorf("LookupDirectoryEntry RPCs = %d, want 0 (expired dir cache should be rebuilt, not read per-entry)", lookupCalls)
	}
	if listCalls != 1 {
		t.Errorf("ListEntries RPCs = %d, want 1 (expired dir cache should be rebuilt once)", listCalls)
	}
}
