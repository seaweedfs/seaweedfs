package mount

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
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
			// Nothing listens here: a transient filer failure at any point
			// must never leave a handle permanently stale.
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

type fakeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	listSnapshotTrailerTsNs int64 // when set, ListEntries returns empty with this trailer snapshot
	lookupSize              uint64
	lookupLogTsNs           int64
	lookupSignature         int32  // filer signature the lookup fence carries
	lookupSize2             uint64 // when set, served to the second and later lookups
	lookupLogTsNs2          int64
	cacheSize               uint64
	cacheLogTsNs            int64
	cacheUid                uint32 // filer-side uid the cache response carries
	updateEventless         bool   // UpdateEntry acks like a no-change update: no event, log position only
	updateLogTsNs           int64
	lookupCalls             atomic.Int32
	lookupStarted           chan struct{} // closed when the first lookup arrives
	lookupGate              chan struct{} // first lookup waits here when non-nil
}

func (s *fakeFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	call := s.lookupCalls.Add(1)
	if s.lookupGate != nil && call == 1 {
		close(s.lookupStarted)
		<-s.lookupGate
	}
	size, logTsNs := s.lookupSize, s.lookupLogTsNs
	if call > 1 && s.lookupSize2 != 0 {
		size, logTsNs = s.lookupSize2, s.lookupLogTsNs2
	}
	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: size, FileMode: 0100644},
		},
		LogTsNs:      logTsNs,
		LogSignature: s.lookupSignature,
	}, nil
}

func (s *fakeFilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// No MetadataEvent: the object was already cached by another client.
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: s.cacheSize, FileMode: 0100644, Uid: s.cacheUid},
		},
		LogTsNs: s.cacheLogTsNs,
	}, nil
}

func (s *fakeFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if s.listSnapshotTrailerTsNs != 0 {
		stream.SetTrailer(metadata.Pairs(filer_pb.ListSnapshotTsNsTrailerKey, strconv.FormatInt(s.listSnapshotTrailerTsNs, 10)))
	}
	return nil
}

func (s *fakeFilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	return &filer_pb.CreateEntryResponse{
		MetadataEvent: &filer_pb.SubscribeMetadataResponse{
			Directory: req.Directory,
			TsNs:      3000,
			EventNotification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: req.Entry.Name},
				NewEntry:      req.Entry,
				NewParentPath: req.Directory,
			},
		},
	}, nil
}

func (s *fakeFilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if s.updateEventless {
		return &filer_pb.UpdateEntryResponse{LogTsNs: s.updateLogTsNs}, nil
	}
	return &filer_pb.UpdateEntryResponse{
		MetadataEvent: &filer_pb.SubscribeMetadataResponse{
			Directory: req.Directory,
			TsNs:      2000,
			EventNotification: &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: req.Entry.Name},
				NewEntry:      req.Entry,
				NewParentPath: req.Directory,
			},
		},
	}, nil
}

// startFakeFiler serves fake on a local port and points wfs at it.
func startFakeFiler(t *testing.T, wfs *WFS, fake *fakeFilerServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, fake)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	wfs.option.FilerAddresses = []pb.ServerAddress{
		pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port),
	}
}

func updateEventFor(name string, size uint64, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      tsNs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: name},
			NewEntry: &filer_pb.Entry{
				Name:       name,
				Attributes: &filer_pb.FuseAttributes{FileSize: size},
			},
			NewParentPath: "/dir",
		},
	}
}

// An update event must refresh an open file handle from the entry the event
// itself carries: a second lookup can fail transiently, and with the
// subscription cursor already advanced, the handle would stay pinned to its
// old entry until an unrelated event arrives.
func TestUpdateEventRefreshesOpenFileHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
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
		TsNs:      1100,
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

// A queued invalidation must not roll the handle back over newer state a
// local flush installed while the event sat in the queue: for a cached
// directory the store entry is the ordered merge of both, and its version
// outranks the event's.
func TestQueuedEventDoesNotRollBackNewerLocalState(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	// Hold the handle lock so the queued invalidation cannot apply yet.
	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A local flush lands after the event was queued: newer state goes into
	// the handle and, via the local apply, into the local store.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 2000), meta_cache.LocalMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply local event: %v", err)
	}
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (queued size-100 event must not roll back the newer local state)", size)
	}
}

// During a directory build, an event touching the building directory is
// buffered: its store write is deferred while its invalidation runs against
// a mid-build store. Build completion versions the directory at the listing
// snapshot and re-invalidates, so the handle lands on the completed state.
func TestBufferedBuildEventReinvalidatesOnCompletion(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}

	// Covered by the upcoming listing snapshot (TsNs 900 <= snapshot 1000);
	// its immediate invalidation runs while the directory is read-through,
	// so the handle picks up the event's state.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 900), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply buffered event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 100 {
		t.Fatalf("open handle file size mid-build = %d, want 100 (event state)", size)
	}

	// The listing then inserts the newer entry the snapshot already covers,
	// through the same batch path a real build uses.
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 300,
		},
	}, 2000); err != nil {
		t.Fatalf("insert listing entry: %v", err)
	}

	if err := wfs.metaCache.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 1000); err != nil {
		t.Fatalf("complete build: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 300 {
		t.Fatalf("open handle file size after build completion = %d, want 300 (snapshot-covered event must re-invalidate)", size)
	}
}

// A store hit only resolves an invalidation when the parent directory is
// cached. An uncached parent receives no store writes, so a leftover entry
// there is stale and must not mask the event.
func TestUncachedDirStaleStoreEntryDoesNotMaskEvent(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	// Leftover store entry under a parent that is not children-cached.
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 88,
		},
	}, 0); err != nil {
		t.Fatalf("insert stale entry: %v", err)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 180020, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply update event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 180020 {
		t.Fatalf("open handle file size = %d, want 180020 (stale store entry must not mask the event)", size)
	}
}

// In a read-through directory neither a local flush nor the event reaches the
// local store, so ordering falls to the versions: an event at or before the
// handle's last filer-acknowledged mutation is old news and must not roll the
// handle back.
func TestQueuedEventOlderThanFlushedStateIsIgnored(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	// Hold the handle lock so the queued invalidation cannot apply yet.
	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A local flush lands: the filer acknowledged it with a later log
	// timestamp than the queued event.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	fh.advanceEntryVersion(2000, 0)
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (event at TsNs 1000 predates the flush at 2000)", size)
	}
}

// saveEntry (truncate, setattr) must advance the open handle's version from
// the acknowledged mutation's log timestamp, or an older queued event rolls
// the mutation back in a read-through directory.
func TestSaveEntryKeepsOpenHandleAheadOfOlderEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A truncate-style mutation: the filer acknowledges it at TsNs 2000 and
	// installs the acknowledged state into the handle. Whichever order the
	// queued event and the acknowledgment reach the handle, the newer
	// acknowledged state wins.
	saved := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	if code := wfs.saveEntry(util.FullPath("/dir/file"), saved); code != fuse.OK {
		t.Fatalf("saveEntry status = %v, want OK", code)
	}

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (saveEntry at TsNs 2000 outranks the queued event at 1000)", size)
	}
}

// A handle opened while an older event sits in the invalidation queue takes
// its version from the lookup response's log position, which covers every
// event the filer had committed — including the queued one.
func TestQueuedEventDoesNotRollBackHandleOpenedAfterEnqueue(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{lookupSize: 200, lookupLogTsNs: 2000})

	// Stall the single invalidation worker on an unrelated handle's lock so
	// queued events outlive the open below.
	blockerInode := wfs.inodeToPath.Lookup(util.FullPath("/other/blocker"), time.Now().Unix(), false, false, 0, false)
	blockerFh, _ := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	}, 0, 0)
	blockerLock := wfs.fhLockTable.AcquireLock("test", blockerFh.fh, util.ExclusiveLock)
	blockerReleased := false
	releaseBlocker := func() {
		if !blockerReleased {
			blockerReleased = true
			wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		}
	}
	defer releaseBlocker()
	blockerEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/other",
		TsNs:      500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "blocker"},
			NewEntry: &filer_pb.Entry{
				Name:       "blocker",
				Attributes: &filer_pb.FuseAttributes{FileSize: 2},
			},
			NewParentPath: "/other",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), blockerEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply blocker event: %v", err)
	}

	// The event predates the open below.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply subscriber event: %v", err)
	}

	// Open now: the lookup reaches the filer, which serves the newer
	// size-200 state versioned at log position 2000.
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("opened handle file size = %d, want 200", size)
	}

	releaseBlocker()
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (event queued before the open must not roll it back)", size)
	}
}

// An event buffered for a building directory keeps its queued invalidation
// even when the build is aborted. A handle opened after the abort is fenced
// by its lookup response's log position, which covers the committed event.
func TestAbortedBuildEventDoesNotRollBackLaterOpen(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{lookupSize: 200, lookupLogTsNs: 2000})

	blockerInode := wfs.inodeToPath.Lookup(util.FullPath("/other/blocker"), time.Now().Unix(), false, false, 0, false)
	blockerFh, _ := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	}, 0, 0)
	blockerLock := wfs.fhLockTable.AcquireLock("test", blockerFh.fh, util.ExclusiveLock)
	blockerReleased := false
	releaseBlocker := func() {
		if !blockerReleased {
			blockerReleased = true
			wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		}
	}
	defer releaseBlocker()
	blockerEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/other",
		TsNs:      500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "blocker"},
			NewEntry: &filer_pb.Entry{
				Name:       "blocker",
				Attributes: &filer_pb.FuseAttributes{FileSize: 2},
			},
			NewParentPath: "/other",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), blockerEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply blocker event: %v", err)
	}

	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply buffered event: %v", err)
	}
	if err := wfs.metaCache.AbortDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("abort build: %v", err)
	}

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}

	releaseBlocker()
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (aborted-build event must not roll back the open)", size)
	}
}

// An event applied while the open's lookup is in flight is committed on the
// filer before the lookup is served, so the response's log position covers
// it and the fresh handle is not rolled back.
func TestEventDuringOpenLookupIsFenced(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{
		lookupSize:    200,
		lookupLogTsNs: 2000,
		lookupStarted: make(chan struct{}),
		lookupGate:    make(chan struct{}),
	}
	startFakeFiler(t, wfs, fake)

	blockerInode := wfs.inodeToPath.Lookup(util.FullPath("/other/blocker"), time.Now().Unix(), false, false, 0, false)
	blockerFh, _ := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	}, 0, 0)
	blockerLock := wfs.fhLockTable.AcquireLock("test", blockerFh.fh, util.ExclusiveLock)
	blockerReleased := false
	releaseBlocker := func() {
		if !blockerReleased {
			blockerReleased = true
			wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		}
	}
	defer releaseBlocker()
	blockerEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/other",
		TsNs:      500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "blocker"},
			NewEntry: &filer_pb.Entry{
				Name:       "blocker",
				Attributes: &filer_pb.FuseAttributes{FileSize: 2},
			},
			NewParentPath: "/other",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), blockerEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply blocker event: %v", err)
	}

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	type openResult struct {
		fh     *FileHandle
		status fuse.Status
	}
	opened := make(chan openResult, 1)
	go func() {
		fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
		opened <- openResult{fh, status}
	}()

	// While the open's lookup is blocked in the filer, an event lands and
	// its invalidation is queued behind the blocker.
	<-fake.lookupStarted
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply mid-lookup event: %v", err)
	}
	close(fake.lookupGate)

	result := <-opened
	if result.status != fuse.OK {
		t.Fatalf("AcquireHandle status = %v, want OK", result.status)
	}

	releaseBlocker()
	wfs.metaCache.WaitForEntryInvalidations()

	if size := result.fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (mid-lookup event must be fenced)", size)
	}
}

// The remote-cache response versions the freshly loaded state by the caching
// event or, for an already-cached object, by the response's log position —
// covering events committed but not yet delivered, regardless of which filer
// answered after a failover.
func TestRemoteCacheResponseVersionFencesUndeliveredEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{cacheSize: 200, cacheLogTsNs: 2000}
	startFakeFiler(t, wfs, fake)
	// First filer is unreachable; WithFilerClient fails over to the fake.
	live := wfs.option.FilerAddresses[0]
	wfs.option.FilerAddresses = []pb.ServerAddress{
		pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1),
		live,
	}

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("downloaded file size = %d, want 200", size)
	}

	// An event committed before the download (TsNs 1500 < 2000) but
	// delivered only now must not roll the handle back.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply late event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (response-versioned state must fence the undelivered event)", size)
	}
}

// A no-change update returns success without an event; the response's log
// position must still fence the handle, or a delayed event already reflected
// by the confirmed state rolls it back.
func TestEventlessSaveAckStillFencesOlderEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{updateEventless: true, updateLogTsNs: 2000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply subscriber event: %v", err)
	}

	// The save confirms the handle's state without changing it: no event,
	// only the acknowledged log position, which installs the confirmed
	// state over whatever the queued event may have applied first.
	saved := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	if code := wfs.saveEntry(util.FullPath("/dir/file"), saved); code != fuse.OK {
		t.Fatalf("saveEntry status = %v, want OK", code)
	}

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (no-op ack at log position 2000 outranks the queued event at 1000)", size)
	}
}

// A local mutation ack for one path must not inflate the version of store
// reads for other paths: the subscription may still owe those paths older
// events, and an inflated fence would discard them permanently.
func TestLocalAckDoesNotFenceUnrelatedDelayedEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 88,
		},
	}, 0); err != nil {
		t.Fatalf("insert cached entry: %v", err)
	}

	// A local flush of an unrelated file acknowledges filer position 2000.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("other", 10, 2000), meta_cache.LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply local event: %v", err)
	}
	// Open the file: the cached-store read must not claim position 2000 —
	// the local ack versioned only its own path.
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}

	// The delayed subscription event for this file must still land.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 150, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delayed event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 150 {
		t.Fatalf("open handle file size = %d, want 150 (unrelated local ack must not fence this file's event)", size)
	}
}

// Two concurrent first opens race: the slower opener's older lookup result
// must not overwrite the newer entry the faster opener installed, while the
// monotonic version keeps the newer timestamp — entry and version are one
// decision under the handle map lock.
func TestSlowerConcurrentOpenDoesNotOverwriteNewerHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{
		lookupSize:     100,
		lookupLogTsNs:  1000,
		lookupSize2:    200,
		lookupLogTsNs2: 2000,
		lookupStarted:  make(chan struct{}),
		lookupGate:     make(chan struct{}),
	}
	startFakeFiler(t, wfs, fake)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	type openResult struct {
		fh     *FileHandle
		status fuse.Status
	}
	slow := make(chan openResult, 1)
	go func() {
		fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
		slow <- openResult{fh, status}
	}()
	<-fake.lookupStarted

	// The faster opener completes with newer state while the slow lookup is
	// still in flight.
	fastFh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("fast AcquireHandle status = %v, want OK", status)
	}
	if size := fastFh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("fast open file size = %d, want 200", size)
	}

	close(fake.lookupGate)
	result := <-slow
	if result.status != fuse.OK {
		t.Fatalf("slow AcquireHandle status = %v, want OK", result.status)
	}

	if size := fastFh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (slower opener's older lookup must not overwrite)", size)
	}
	if got := fastFh.entryVersionTsNs.Load(); got != 2000 {
		t.Fatalf("open handle version = %d, want 2000", got)
	}
}

// An event at or below a directory's listing floor is already reflected in
// the snapshot state; applying it would roll the store back while the floor
// keeps claiming the snapshot version, fencing out the correcting events.
func TestFloorProtectsSnapshotStateFromDelayedEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)

	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 300,
		},
	}, 2000); err != nil {
		t.Fatalf("insert listing entry: %v", err)
	}
	if err := wfs.metaCache.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 2000); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	// Delayed event the snapshot already covers: must not touch the store.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply covered event: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil {
		t.Fatalf("find entry: %v", err)
	}
	if entry.FileSize != 300 {
		t.Fatalf("store file size = %d, want 300 (event at 1500 is covered by the floor at 2000)", entry.FileSize)
	}

	// A genuinely new event still applies.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 400, 2500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply new event: %v", err)
	}
	entry, _, err = wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil {
		t.Fatalf("find entry: %v", err)
	}
	if entry.FileSize != 400 {
		t.Fatalf("store file size = %d, want 400 (event above the floor must apply)", entry.FileSize)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A fence is a lower bound: a listing or lookup can include a mutation whose
// event is delivered afterwards. Such an event carries state the handle
// already holds — it must advance the version without destroying dirty
// pages, or local writes are lost for a no-op.
func TestAlreadyReflectedEventDoesNotDestroyDirtyPages(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)
	pagesBefore := fh.dirtyPages

	// The event re-delivers exactly the state the handle already reflects.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages were destroyed for an already-reflected event")
	}
	if got := fh.entryVersionTsNs.Load(); got != 1500 {
		t.Fatalf("handle version = %d, want 1500 (the no-op event still advances the version)", got)
	}
}

// A slower opener's install must not land on a dirty handle (local writes
// would be lost) and unversioned lookup results cannot outrank anything.
func TestSlowerOpenRejectedWhenHandleDirtyOrLookupUnversioned(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{
		lookupSize:     100,
		lookupLogTsNs:  3000, // newer than the fast open, but the handle is dirty
		lookupSize2:    200,
		lookupLogTsNs2: 2000,
		lookupStarted:  make(chan struct{}),
		lookupGate:     make(chan struct{}),
	}
	startFakeFiler(t, wfs, fake)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	type openResult struct {
		fh     *FileHandle
		status fuse.Status
	}
	slow := make(chan openResult, 1)
	go func() {
		fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
		slow <- openResult{fh, status}
	}()
	<-fake.lookupStarted

	fastFh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("fast AcquireHandle status = %v, want OK", status)
	}
	fastFh.dirtyMetadata = true

	close(fake.lookupGate)
	result := <-slow
	if result.status != fuse.OK {
		t.Fatalf("slow AcquireHandle status = %v, want OK", result.status)
	}

	if size := fastFh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (install on a dirty handle must be rejected)", size)
	}
	if !fastFh.dirtyMetadata {
		t.Fatal("dirtyMetadata was cleared by the rejected install")
	}
}

// Legacy filers return no version; two racing opens both at version zero must
// not overwrite each other — the first install stands.
func TestUnversionedSlowerOpenDoesNotOverwrite(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{
		lookupSize:    100, // slower, unversioned
		lookupSize2:   200, // faster, unversioned
		lookupStarted: make(chan struct{}),
		lookupGate:    make(chan struct{}),
	}
	startFakeFiler(t, wfs, fake)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	type openResult struct {
		fh     *FileHandle
		status fuse.Status
	}
	slow := make(chan openResult, 1)
	go func() {
		fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
		slow <- openResult{fh, status}
	}()
	<-fake.lookupStarted

	fastFh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("fast AcquireHandle status = %v, want OK", status)
	}

	close(fake.lookupGate)
	result := <-slow
	if result.status != fuse.OK {
		t.Fatalf("slow AcquireHandle status = %v, want OK", result.status)
	}

	if size := fastFh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (an unversioned response cannot outrank the installed entry)", size)
	}
}

// The no-op judgment must use the immutable base snapshot, not the live
// entry: local writes diverge the live entry from the base, and an event
// re-delivering the base would otherwise look like a foreign change —
// destroying the dirty pages and rolling the entry back over nothing.
func TestAlreadyReflectedEventPreservesDirtyWrites(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)

	// A local write grows the live entry past the base.
	fh.UpdateEntry(func(entry *filer_pb.Entry) {
		entry.Attributes.FileSize = 205
	})
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	// The delayed event re-delivers the base the handle was opened with.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages were destroyed by an event re-delivering the handle's base state")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 205 {
		t.Fatalf("live entry file size = %d, want 205 (local write must survive the base re-delivery)", size)
	}
	if got := fh.entryVersionTsNs.Load(); got != 1500 {
		t.Fatalf("handle version = %d, want 1500", got)
	}
}

// A versioned deletion is a fact about the path with no entry left to carry
// it. Without a tombstone, a delayed older event resurrects the deleted path
// permanently — the deletion's own redelivery is dedup-suppressed.
func TestDeleteTombstoneBlocksResurrection(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      2000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	// Delayed update the deletion supersedes: must not resurrect the path.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 150, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delayed update: %v", err)
	}
	if entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file")); err == nil {
		t.Fatalf("deleted path resurrected by a delayed event: %+v", entry)
	}

	// A genuinely newer create still applies over the tombstone.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 250, 2500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer create: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil || entry.FileSize != 250 {
		t.Fatalf("entry after newer create = %+v, %v; want size 250", entry, err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A server-side copy installs the copied entry into the destination handle;
// it must enroll in the versioned-base protocol, or the copy's own event
// differs from the stale pre-copy base and destroys writes made to the
// destination after the copy.
func TestServerSideCopyInstallEnrollsInBase(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
	}, 0, 0)

	copied := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), copied, entryVersion{}, 200)

	// Writes land on the destination before the copy's event arrives.
	fh.UpdateEntry(func(entry *filer_pb.Entry) {
		entry.Attributes.FileSize = 205
	})
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	// The copy's own event carries the copied state — a no-op for this base.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply copy event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages destroyed by the copy's own event")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 205 {
		t.Fatalf("live entry file size = %d, want 205 (post-copy write must survive)", size)
	}
}

// A completed listing proves absences as well as presences: a delayed
// create for a name the snapshot omitted re-creates something the listing
// already saw deleted.
func TestAbsenceFloorBlocksGhostCreate(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)

	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/other",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 1,
		},
	}, 0); err != nil {
		t.Fatalf("insert listing entry: %v", err)
	}
	if err := wfs.metaCache.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 2000); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("ghost", 100, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply ghost create: %v", err)
	}
	if entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/ghost")); err == nil {
		t.Fatalf("name absent at snapshot 2000 resurrected by event at 1500: %+v", entry)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("ghost", 250, 2500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer ghost create: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/ghost"))
	if err != nil || entry.FileSize != 250 {
		t.Fatalf("ghost after newer create = %+v, %v; want size 250", entry, err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A deletion is a fact about the path, not about what the cache happened to
// hold: even when the store has no entry to delete, the versioned delete
// must leave a tombstone, or a delayed older event recreates the path.
func TestVersionedDeleteOfMissingEntryLeavesTombstone(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	// No entry inserted: the delete finds nothing to remove.
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      2000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 150, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delayed update: %v", err)
	}
	if entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file")); err == nil {
		t.Fatalf("path deleted at 2000 recreated by an event at 1500: %+v", entry)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 250, 2500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer create: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil || entry.FileSize != 250 {
		t.Fatalf("entry after newer create = %+v, %v; want size 250", entry, err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A committed copy whose readback failed installs a synthesized base with
// local timestamps; the copy's real event legitimately differs from it and
// must be adopted as the base without invalidating writes made since — the
// event is ours, not a foreign change.
func TestCommittedCopyWithFailedReadbackAdoptsItsEvent(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
	}, 0, 0)

	// Readback failed: nil entry forces the synthesized fallback.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// Writes land on the destination before the copy's event arrives.
	fh.UpdateEntry(func(entry *filer_pb.Entry) {
		entry.Attributes.FileSize = 205
	})
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	// The real copy event differs from the synthesized base in timestamps.
	copyEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 200, Mtime: 999},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), copyEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply copy event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages destroyed by the committed copy's own event")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 205 {
		t.Fatalf("live entry file size = %d, want 205 (post-copy write must survive)", size)
	}
	if got := fh.entryVersionTsNs.Load(); got != 1500 {
		t.Fatalf("handle version = %d, want 1500", got)
	}

	// The adoption is one-shot: a genuinely foreign event still invalidates.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 300, 1600), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply foreign event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 300 {
		t.Fatalf("live entry file size = %d, want 300 (foreign event after adoption must install)", size)
	}
	if fh.dirtyPages == pagesBefore {
		t.Fatal("foreign event after adoption must invalidate dirty pages")
	}
}

// A directory snapshot newer than a tombstone confirms the name is still
// absent at the newer position; an event between the two must be fenced by
// the floor even though an older record exists.
func TestNewerAbsenceFloorOverridesOlderTombstone(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	// A later listing confirms the name is still absent as of 3000.
	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := wfs.metaCache.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 3000); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	// Newer than the tombstone, older than the snapshot: still fenced.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 150, 2000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply mid event: %v", err)
	}
	if entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file")); err == nil {
		t.Fatalf("name absent at snapshot 3000 recreated by an event at 2000: %+v", entry)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 350, 3500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer create: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil || entry.FileSize != 350 {
		t.Fatalf("entry after newer create = %+v, %v; want size 350", entry, err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A flush acknowledgment supersedes a pending copy-event adoption: the copy's
// event is version gated after the ack, so a surviving adoption flag would
// misfire on the next genuinely foreign event, silently swallowing it.
func TestFlushAckCancelsPendingCopyEventAdoption(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
	}, 0, 0)

	// Committed copy, failed readback: adoption pending on a synthesized base.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// A local flush lands: the ack at 3000 becomes the authoritative base.
	fh.dirtyMetadata = true
	if status := wfs.doFlush(context.Background(), fh, 0, 0, false); status != fuse.OK {
		t.Fatalf("doFlush status = %v, want OK", status)
	}

	// The copy's own delayed event is version gated by the ack.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply copy event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	// A genuinely foreign event must install normally, not be adopted.
	pagesBefore := fh.dirtyPages
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 300, 3500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply foreign event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 300 {
		t.Fatalf("live entry file size = %d, want 300 (foreign event must install, not be silently adopted)", size)
	}
	if fh.dirtyPages == pagesBefore {
		t.Fatal("foreign event must invalidate dirty pages, not be silently adopted")
	}
}

// A version must never advance without its value: a handle opened while a
// setattr was in flight holds the pre-mutation entry, and stamping it with
// the acknowledgment's version would fence out the events carrying the state
// it lacks. The acknowledged entry is installed with the version instead.
func TestAckedSaveInstallsIntoRacingOpenHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{})

	// The handle opened after the setattr path found none, before the ack.
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	saved := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	if code := wfs.saveEntry(util.FullPath("/dir/file"), saved); code != fuse.OK {
		t.Fatalf("saveEntry status = %v, want OK", code)
	}

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (the acknowledged state must be installed with its version)", size)
	}
	if got := fh.entryVersionTsNs.Load(); got != 2000 {
		t.Fatalf("handle version = %d, want 2000", got)
	}

	// A dirty handle is left alone entirely: neither entry nor version.
	dirtyInode := wfs.inodeToPath.Lookup(util.FullPath("/dir/dirty"), time.Now().Unix(), false, false, 0, false)
	dirtyFh, _ := wfs.fhMap.AcquireFileHandle(wfs, dirtyInode, &filer_pb.Entry{
		Name:       "dirty",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)
	dirtyFh.dirtyMetadata = true
	if code := wfs.saveEntry(util.FullPath("/dir/dirty"), &filer_pb.Entry{
		Name:       "dirty",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}); code != fuse.OK {
		t.Fatalf("saveEntry status = %v, want OK", code)
	}
	if size := dirtyFh.GetEntry().GetEntry().Attributes.FileSize; size != 88 {
		t.Fatalf("dirty handle file size = %d, want 88 (local writes supersede the ack)", size)
	}
	if got := dirtyFh.entryVersionTsNs.Load(); got != 0 {
		t.Fatalf("dirty handle version = %d, want 0 (no version without its value)", got)
	}
}

// An empty listing carries its snapshot in the stream trailer, so empty
// directories still gain an absence floor — and their stale tombstones
// still get pruned — instead of accumulating forever.
func TestEmptyListingTrailerSnapshotSetsAbsenceFloor(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{listSnapshotTrailerTsNs: 4000})

	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, util.FullPath("/dir")); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}

	// Absent at the trailer snapshot: an older create must be fenced.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("ghost", 100, 3000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply covered create: %v", err)
	}
	if entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/ghost")); err == nil {
		t.Fatalf("name absent at trailer snapshot 4000 created by event at 3000: %+v", entry)
	}

	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("ghost", 450, 4500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer create: %v", err)
	}
	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/ghost"))
	if err != nil || entry.FileSize != 450 {
		t.Fatalf("entry after newer create = %+v, %v; want size 450", entry, err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// A foreign delete of a file with unflushed local writes must not destroy the
// dirty pages: POSIX lets a process keep writing to an unlinked-but-open file,
// and the writes were already acknowledged.
func TestVacateEventPreservesDirtyPages(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages destroyed by a foreign delete of an open file with unflushed writes")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size after delete = %d, want 200", size)
	}
}

// A committed copy whose readback failed adopts only the copy's own event
// (same content). A foreign write to the destination that arrives first has
// different content and must install normally, not be swallowed by the
// pending adoption.
func TestCopyAdoptRejectsForeignEvent(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
	}, 0, 0)

	// Readback failed: synthesized base at size 200, adoption pending.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// A foreign write to the destination arrives before the copy's own event:
	// different content (size 500), so it must install and not be adopted.
	foreign := updateEventFor("file", 500, 1500)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), foreign, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply foreign event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 500 {
		t.Fatalf("live entry file size = %d, want 500 (foreign write must install, not be swallowed by the copy adoption)", size)
	}
}

// downloadRemoteEntry stores the handle's base in local uid/gid form, so a
// later re-delivery of unchanged content compares equal and does not
// force-destroy dirty pages under a non-identity UidGidMapper.
func TestRemoteDownloadBaseMappedToLocal(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// The test mapper maps filer uid 2000 -> local 1000; the cache response
	// carries filer uid 2000.
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 200, cacheUid: 2000, cacheLogTsNs: 1000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 200},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 200},
	}, 0, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}
	// The base — and the live entry — must be in local uid form.
	if uid := fh.GetEntry().GetEntry().Attributes.Uid; uid != 1000 {
		t.Fatalf("live entry uid = %d, want filer 2000 mapped to local 1000", uid)
	}

	// The user writes to the handle (dirty pages, not flushed).
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	// A re-delivery of the same content (local-form candidate, uid 2000 in the
	// event mapped to local 1000) must read as a no-op and preserve the writes.
	reDeliver := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100644, Uid: 2000}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), reDeliver, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply re-delivery: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages destroyed by an unchanged re-delivery (base was not mapped to local form)")
	}
}

// A foreign delete of a dirty open file must mark the handle deleted, so a
// later flush does not recreate the remotely-unlinked name.
func TestForeignDeleteMarksHandleDeleted(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)
	fh.dirtyMetadata = true

	del := &filer_pb.SubscribeMetadataResponse{
		Directory:         "/dir",
		TsNs:              1500,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "file"}},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), del, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if !fh.isDeleted {
		t.Fatal("handle not marked deleted after a foreign delete; a flush would recreate the unlinked name")
	}
}

// A no-event acknowledgment (log fence only) must version the cache entry, so
// an older subscriber event cannot roll it back.
func TestEventlessSaveVersionsCacheEntry(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{updateEventless: true, updateLogTsNs: 2000})

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	if code := wfs.saveEntry(util.FullPath("/dir/file"), &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100644},
	}); code != fuse.OK {
		t.Fatalf("saveEntry status = %v, want OK", code)
	}

	// An older subscriber event must be fenced out by the ack's log position.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1500), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply older event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil || entry.FileSize != 200 {
		t.Fatalf("cache entry = %+v, %v; want size 200 (older event must not roll back the no-event ack)", entry, err)
	}
}

// A remote download response that lands after the handle was already
// populated must not roll the entry back: it is older than what the handle
// holds, and the monotonic version would keep the newer value.
func TestStaleRemoteDownloadDoesNotRollBack(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 100, cacheLogTsNs: 1000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 200},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 200},
	}, 0, 0)
	// While the download was in flight the handle was populated at a newer
	// version — it now has local chunks and no longer needs the response.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,ab1", Size: 200}},
	})
	fh.advanceEntryVersion(3000, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("handle file size = %d, want 200 (a download at version 1000 must not overwrite the version-3000 handle)", size)
	}
	if got := fh.entryVersionTsNs.Load(); got != 3000 {
		t.Fatalf("handle version = %d, want 3000", got)
	}
}

// A still-remote-only handle must take an older or unversioned response
// anyway — without it there are no local chunks to read — but must not claim
// the response's log position.
func TestRemoteOnlyHandleTakesUnversionedDownload(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 100})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 100},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 100},
	}, 0, 0)
	fh.advanceEntryVersion(3000, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}

	if fh.GetEntry().GetEntry().IsInRemoteOnly() {
		t.Fatal("remote-only handle did not take the download; reads would have no chunks")
	}
	if got := fh.entryVersionTsNs.Load(); got != 3000 {
		t.Fatalf("handle version = %d, want 3000 (an unversioned response must not claim a position)", got)
	}
}

// A foreign metadata-only change (chmod) with unchanged content must not be
// mistaken for a committed copy's own event and adopted; it must install.
func TestCopyAdoptRejectsForeignMetadataChange(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100644},
	}, 0, 0)

	// Readback failed: synthesized base at size 200, mode 0644; adoption pending.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// A foreign chmod: same content (size 200) but mode 0600.
	chmod := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100600}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), chmod, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply chmod: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if mode := fh.GetEntry().GetEntry().Attributes.FileMode; mode != 0100600 {
		t.Fatalf("live entry mode = %o, want 0100600 (foreign chmod must install, not be swallowed by copy adoption)", mode)
	}
}

// A foreign rename must not mark an open handle deleted: the file still
// exists at its new path, and marking it silently drops later writes through
// the already-open descriptor.
func TestForeignRenameDoesNotMarkHandleDeleted(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}, 0, 0)
	fh.dirtyMetadata = true

	rename := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "renamed", Attributes: &filer_pb.FuseAttributes{FileSize: 200}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), rename, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.isDeleted {
		t.Fatal("handle marked deleted by a foreign rename; later writes would be silently dropped")
	}
	// The handle follows the file to its new name, so a flush updates the
	// renamed file instead of recreating the old one.
	if got := fh.FullPath(); got != util.FullPath("/dir/renamed") {
		t.Fatalf("handle path = %q, want /dir/renamed after the rename", got)
	}
	if name := fh.GetEntry().GetEntry().Name; name != "renamed" {
		t.Fatalf("handle entry name = %q, want renamed", name)
	}

	// A real delete of the file at its current name still marks the handle.
	del := &filer_pb.SubscribeMetadataResponse{
		Directory:         "/dir",
		TsNs:              2500,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "renamed"}},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), del, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	if !fh.isDeleted {
		t.Fatal("handle not marked deleted by a real foreign delete")
	}
}

// A foreign touch arriving before a committed copy's own event must apply its
// timestamps to a clean handle, and must not consume anything the copy's own
// event still needs — the copy's event carries the same content, so it never
// destroys the post-copy writes.
func TestCopyAdoptAppliesForeignTouchToCleanHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200, Mtime: 100},
	}, 0, 0)

	// Readback failed: synthesized base, adoption pending.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// A foreign touch: identical content, new mtime.
	touch := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, Mtime: 999}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), touch, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply touch: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if mtime := fh.GetEntry().GetEntry().Attributes.Mtime; mtime != 999 {
		t.Fatalf("live entry mtime = %d, want 999 (foreign touch must apply to a clean handle, not be swallowed)", mtime)
	}
}

// An unversioned response (pre-upgrade filer) must not overwrite versioned
// state on a handle that already has local content.
func TestUnversionedRemoteDownloadDoesNotOverwriteVersioned(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// No cacheLogTsNs and no metadata event: the response carries no version.
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 100})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 200},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 200},
	}, 0, 0)
	// Populated at version 3000 while the download was in flight.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,ab1", Size: 200}},
	})
	fh.advanceEntryVersion(3000, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("handle file size = %d, want 200 (an unversioned response must not overwrite version-3000 state)", size)
	}
}

// A fence stamped by one filer says nothing about an event another filer
// logged: their clocks are independent. Skipping across that boundary would
// leave the handle stale for good, so the event is applied instead — a
// re-apply the base check absorbs when it turns out to be redundant.
func TestEventFromAnotherFilerIsNotFencedByForeignClock(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// Filer A answers the open-time lookup and stamps its fence at 5000.
	startFakeFiler(t, wfs, &fakeFilerServer{lookupSize: 200, lookupLogTsNs: 5000, lookupSignature: 11})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}
	if got := fh.entryVersionSignature.Load(); got != 11 {
		t.Fatalf("handle fence signature = %d, want 11 from the lookup", got)
	}

	// Filer B logged this event at 3000 on its own clock — below A's fence,
	// but the two positions are not comparable.
	fromOtherFiler := updateEventFor("file", 900, 3000)
	fromOtherFiler.EventNotification.Signatures = []int32{22}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), fromOtherFiler, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply cross-filer event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 900 {
		t.Fatalf("handle file size = %d, want 900 (an event from another filer must not be fenced by this filer's clock)", size)
	}

	// The same filer's own older event is still fenced: one clock, real order.
	sameFiler := updateEventFor("file", 700, 4000)
	sameFiler.EventNotification.Signatures = []int32{11}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), sameFiler, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply same-filer event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size == 700 {
		t.Fatal("an older event from the fencing filer was applied; within one clock domain it must be fenced")
	}
}

// A foreign touch landing before a committed copy's own event must not leave
// the copy's event to destroy the post-copy writes.
func TestTouchBeforeCopyEventKeepsPostCopyWrites(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100, Mtime: 100},
	}, 0, 0)

	// Committed copy whose readback failed: the base is synthesized.
	wfs.applyServerSideWholeFileCopyResult(fh, fh, util.FullPath("/dir/file"), nil, entryVersion{}, 200)

	// Post-copy writes.
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	// A foreign touch arrives first: same content, new mtime.
	touch := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, Mtime: 555}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), touch, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply touch: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	// Then the copy's own event, with the server's timestamps.
	copyEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1600,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, Mtime: 777}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), copyEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply copy event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("post-copy writes destroyed: a timestamp-only event must not invalidate the dirty overlay")
	}
}

// A rejected download must not publish its state to the metadata cache either.
func TestRejectedDownloadDoesNotRollBackCache(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// Unversioned response carrying older content.
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 100})

	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/"))
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	// The cache holds the current state at version 3000.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 200, 3000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,ab1", Size: 200}},
	}, 0, 0)
	fh.advanceEntryVersion(3000, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}
	// The download publishes asynchronously; a synchronous apply behind it
	// drains the FIFO so the assertion sees the final state either way.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("other", 1, 4000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("flush apply loop: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	entry, versionTsNs, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil {
		t.Fatalf("find cache entry: %v", err)
	}
	if entry.FileSize != 200 || versionTsNs != 3000 {
		t.Fatalf("cache size/version = %d/%d, want 200/3000 (a rejected download must not publish)", entry.FileSize, versionTsNs)
	}
}

// A remote-only handle takes an unversioned response because it cannot read
// without chunks, but a response that is merely older is refused: its content
// predates the state the handle already reflects.
func TestRemoteOnlyHandleRefusesKnownOlderDownload(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// Versioned at 1000 — older than the handle's 3000.
	startFakeFiler(t, wfs, &fakeFilerServer{cacheSize: 100, cacheLogTsNs: 1000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:        "file",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 200},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 200},
	}, 0, 0)
	fh.advanceEntryVersion(3000, 0)

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("handle file size = %d, want 200 (a known-older response must be refused even when remote-only)", size)
	}
}

// A metadata-only foreign change (chmod) must not invalidate the dirty-page
// overlay: pages overlay content, and the content did not change.
func TestForeignChmodPreservesDirtyPages(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100644},
	}, 0, 0)
	fh.dirtyMetadata = true
	pagesBefore := fh.dirtyPages

	chmod := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200, FileMode: 0100600}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), chmod, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply chmod: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if fh.dirtyPages != pagesBefore {
		t.Fatal("dirty pages destroyed by a metadata-only change")
	}
}

// A rename over an existing file destroys that file; its open handle must be
// marked deleted so a later flush cannot resurrect it over the renamed source.
func TestRenameOverExistingMarksReplacedHandleDeleted(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	srcInode := wfs.inodeToPath.Lookup(util.FullPath("/dir/src"), time.Now().Unix(), false, false, 0, false)
	srcFh, _ := wfs.fhMap.AcquireFileHandle(wfs, srcInode, &filer_pb.Entry{
		Name:       "src",
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
	}, 0, 0)
	dstInode := wfs.inodeToPath.Lookup(util.FullPath("/dir/dst"), time.Now().Unix(), false, false, 0, false)
	dstFh, _ := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name:       "dst",
		Attributes: &filer_pb.FuseAttributes{FileSize: 900},
	}, 0, 0)
	dstFh.dirtyMetadata = true

	rename := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "src"},
			NewEntry:      &filer_pb.Entry{Name: "dst", Attributes: &filer_pb.FuseAttributes{FileSize: 100}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), rename, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if !dstFh.isDeleted {
		t.Fatal("replaced destination handle not marked deleted; its flush could resurrect it over the renamed source")
	}
	if srcFh.isDeleted {
		t.Fatal("renamed source handle must stay live")
	}
}

// An acknowledgment from one filer must not be dropped behind a fence another
// filer stamped: their positions are not comparable, and dropping it leaves
// the handle holding exactly the state the mutation replaced.
func TestAckFromAnotherFilerIsNotDroppedBehindForeignFence(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)
	// Filer A's fence at 5000.
	fh.advanceEntryVersion(5000, 11)

	// Filer B acknowledges our mutation at 1000 on its own clock.
	acked := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 200}}
	fh.installAckedEntry(acked, 1000, 22)

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("handle file size = %d, want 200 (a cross-filer ack must not be dropped behind a foreign fence)", size)
	}

	// The same filer's own older ack is still refused.
	older := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 300}}
	fh.installAckedEntry(older, 500, fh.entryVersionSignature.Load())
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size == 300 {
		t.Fatal("an older ack from the fencing filer was installed; within one clock domain it must be refused")
	}
}
