package mount

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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
	lookupSize      uint64
	lookupLogTsNs   int64
	lookupSize2     uint64 // when set, served to the second and later lookups
	lookupLogTsNs2  int64
	cacheSize       uint64
	cacheLogTsNs    int64
	updateEventless bool // UpdateEntry acks like a no-change update: no event, log position only
	updateLogTsNs   int64
	lookupCalls     atomic.Int32
	lookupStarted   chan struct{} // closed when the first lookup arrives
	lookupGate      chan struct{} // first lookup waits here when non-nil
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
		LogTsNs: logTsNs,
	}, nil
}

func (s *fakeFilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	// No MetadataEvent: the object was already cached by another client.
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: s.cacheSize, FileMode: 0100644},
		},
		LogTsNs: s.cacheLogTsNs,
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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

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

	// The listing then inserts the newer entry the snapshot already covers.
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 300,
		},
	}); err != nil {
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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	// Leftover store entry under a parent that is not children-cached.
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 88,
		},
	}); err != nil {
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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

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
	fh.advanceEntryVersionTsNs(2000)
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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A truncate-style mutation: the filer acknowledges it at TsNs 2000 and
	// the handle takes the new entry.
	saved := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	if code := wfs.saveEntry(util.FullPath("/dir/file"), saved); code != fuse.OK {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("saveEntry status = %v, want OK", code)
	}
	fh.SetEntry(saved)
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

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
	blockerFh := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	})
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
	blockerFh := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	})
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
	blockerFh := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	})
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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

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
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})

	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("file", 100, 1000), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// The save confirms the handle's state without changing it: no event,
	// only the acknowledged log position.
	saved := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	}
	if code := wfs.saveEntry(util.FullPath("/dir/file"), saved); code != fuse.OK {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("saveEntry status = %v, want OK", code)
	}
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

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
	}); err != nil {
		t.Fatalf("insert cached entry: %v", err)
	}

	// A local flush of an unrelated file acknowledges filer position 2000.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEventFor("other", 10, 2000), meta_cache.LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply local event: %v", err)
	}
	if got := wfs.metaCache.LatestEventTsNs(); got != 0 {
		t.Fatalf("LatestEventTsNs = %d, want 0 (local acks must not advance the subscription cursor)", got)
	}

	// Open the file: the cached-store read must not claim position 2000.
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
