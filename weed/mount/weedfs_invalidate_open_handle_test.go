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

// A queued invalidation must not roll the handle back over newer state a
// local flush installed while the event sat in the queue. The local store is
// ordered by the apply loop, so it resolves the refresh for cached
// directories.
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

	updateEvent := func(size uint64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "file"},
				NewEntry: &filer_pb.Entry{
					Name:       "file",
					Attributes: &filer_pb.FuseAttributes{FileSize: size},
				},
				NewParentPath: "/dir",
			},
		}
	}

	// Hold the handle lock so the queued invalidation cannot apply yet.
	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEvent(100), meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A local flush lands after the event was queued: newer state goes into
	// the handle and, via the local apply, into the local store.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateEvent(200), meta_cache.LocalMetadataResponseApplyOptions); err != nil {
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
// buffered: its store write is deferred to build completion while its
// invalidation runs immediately, against a store that may not reflect the
// listing yet. Build completion must re-invalidate every buffered event —
// including snapshot-covered ones — so the handle lands on the completed
// directory's state.
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
	// its immediate invalidation runs before the listing inserts the newer
	// entry, so the handle picks up the event's state.
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      900,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), event, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
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

// A hit in the local store only resolves an invalidation when the parent
// directory is cached. An uncached parent receives no store writes, so a
// leftover entry there is stale and must not mask the event.
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

	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 180020},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), updateResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply update event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 180020 {
		t.Fatalf("open handle file size = %d, want 180020 (stale store entry must not mask the event)", size)
	}
}

// In a read-through directory neither a local flush nor the event reaches the
// local store, so ordering falls to the filer log timestamps: an event at or
// before the handle's last filer-acknowledged local mutation is old news and
// must not roll the handle back.
func TestQueuedEventOlderThanFlushedStateIsIgnored(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	// Hold the handle lock so the queued invalidation cannot apply yet.
	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	older := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), older, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A local flush lands: the filer acknowledged it with a later log
	// timestamp than the queued event.
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	fh.advanceLocalEntryTs(2000)
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (event at TsNs 1000 predates the flush at 2000)", size)
	}
}

type fakeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	lookupSize    uint64
	pingTsNs      int64
	cacheSize     uint64
	cacheLogTsNs  int64
	lookupCalls   atomic.Int32
	lookupStarted chan struct{} // closed when the first lookup arrives
	lookupGate    chan struct{} // first lookup waits here when non-nil
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

func (s *fakeFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if s.lookupGate != nil && s.lookupCalls.Add(1) == 1 {
		close(s.lookupStarted)
		<-s.lookupGate
	}
	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileSize: s.lookupSize, FileMode: 0100644},
		},
	}, nil
}

func (s *fakeFilerServer) Ping(ctx context.Context, req *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	return &filer_pb.PingResponse{StartTimeNs: s.pingTsNs}, nil
}

func (s *fakeFilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
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

// saveEntry (truncate, setattr) must advance the open handle's watermark from
// the acknowledged mutation's log timestamp, or an older queued event rolls
// the mutation back in a read-through directory.
func TestSaveEntryKeepsOpenHandleAheadOfOlderEvents(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, &fakeFilerServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	wfs := newInvalidateTestWFS(t)
	wfs.option.FilerAddresses = []pb.ServerAddress{
		pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port),
	}

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	older := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), older, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
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

// When a filer response carries no metadata event, the watermark falls back
// to the latest filer log timestamp already seen: the filer served state at
// least that new, so queued events at or before it must not roll the fresh
// handle state back.
func TestNilAckEventFallsBackToLatestSeenTs(t *testing.T) {
	wfs := newInvalidateTestWFS(t)

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	// An unrelated event advances the mount's known filer log position.
	unrelated := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "other",
				Attributes: &filer_pb.FuseAttributes{FileSize: 1},
			},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), unrelated, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply unrelated event: %v", err)
	}
	if got := wfs.metaCache.LatestEventTsNs(); got != 1500 {
		t.Fatalf("LatestEventTsNs = %d, want 1500", got)
	}

	testLock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	older := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), older, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(fh.fh, testLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// A filer RPC that returned no event (remote cache already populated,
	// server-side copy) installs fresh state; the baseline captured before
	// the RPC stands in for the missing event timestamp.
	baselineTsNs := wfs.metaCache.LatestEventTsNs()
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	fh.noteFilerAck(baselineTsNs, nil)
	wfs.fhLockTable.ReleaseLock(fh.fh, testLock)

	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (queued event at TsNs 1000 predates the known log position 1500)", size)
	}
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

// A handle opened while an older event sits in the invalidation queue is
// fenced at open time: its entry came from a lookup that reflects every
// event applied so far, so the queued event must not replace it.
func TestQueuedEventDoesNotRollBackHandleOpenedAfterEnqueue(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{lookupSize: 200})

	// Stall the single invalidation worker on an unrelated handle's lock so
	// queued events outlive the open below.
	blockerInode := wfs.inodeToPath.Lookup(util.FullPath("/dir/blocker"), time.Now().Unix(), false, false, 0, false)
	blockerFh := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	})
	blockerLock := wfs.fhLockTable.AcquireLock("test", blockerFh.fh, util.ExclusiveLock)
	blockerEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "blocker"},
			NewEntry: &filer_pb.Entry{
				Name:       "blocker",
				Attributes: &filer_pb.FuseAttributes{FileSize: 2},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), blockerEvent, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("apply blocker event: %v", err)
	}

	// The event for the file predates the open below.
	older := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), older, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("apply subscriber event: %v", err)
	}

	// Open the file now: the lookup reaches the filer, which already serves
	// the newer size-200 state.
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("opened handle file size = %d, want 200", size)
	}

	wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (event queued before the open must not roll it back)", size)
	}
}

// The delivered-event cursor misses events already committed on the filer but
// not yet delivered to the subscription. A pre-RPC filer self-ping reads the
// clock those events are stamped with, so state fetched after the ping fences
// them out.
func TestFilerBarrierCoversUndeliveredEvents(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{pingTsNs: 2000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	// The copy/remote-cache pattern: barrier, then the operation's result.
	// The event at TsNs 1500 is committed but not yet delivered, so only the
	// filer clock (2000) can cover it.
	baselineTsNs := wfs.filerBarrierTsNs()
	if baselineTsNs != 2000 {
		t.Fatalf("filerBarrierTsNs = %d, want 2000 from the filer ping", baselineTsNs)
	}
	fh.SetEntry(&filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
	})
	fh.advanceLocalEntryTs(baselineTsNs)

	late := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply late event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (undelivered-at-barrier event must not roll back)", size)
	}
}

// An event buffered for a building directory must be covered by the open-time
// cursor even if it never reaches a store write: aborting the build drops the
// buffered events while their invalidations stay queued, so an open fenced
// below the event would be rolled back.
func TestAbortedBuildEventStillCoveredByOpenFence(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	startFakeFiler(t, wfs, &fakeFilerServer{lookupSize: 200})

	// Stall the invalidation worker on an unrelated handle's lock.
	blockerInode := wfs.inodeToPath.Lookup(util.FullPath("/other/blocker"), time.Now().Unix(), false, false, 0, false)
	blockerFh := wfs.fhMap.AcquireFileHandle(wfs, blockerInode, &filer_pb.Entry{
		Name:       "blocker",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1},
	})
	blockerLock := wfs.fhLockTable.AcquireLock("test", blockerFh.fh, util.ExclusiveLock)
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
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("apply blocker event: %v", err)
	}

	if err := wfs.metaCache.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("begin build: %v", err)
	}
	buffered := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), buffered, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("apply buffered event: %v", err)
	}
	if got := wfs.metaCache.LatestEventTsNs(); got != 1000 {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("LatestEventTsNs = %d, want 1000 (buffered event must advance the cursor)", got)
	}
	if err := wfs.metaCache.AbortDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("abort build: %v", err)
	}

	// Open after the abort: the lookup reaches the filer, which serves the
	// newer size-200 state; the fence must cover the still-queued event.
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, status := wfs.AcquireHandle(inode, 0, 0, 0)
	if status != fuse.OK {
		wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
		t.Fatalf("AcquireHandle status = %v, want OK", status)
	}

	wfs.fhLockTable.ReleaseLock(blockerFh.fh, blockerLock)
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (aborted-build event must not roll back the open)", size)
	}
}

// During filer failover, WithFilerClient retries the callback against another
// filer while the current-filer index still points at the failed one. The
// barrier must ping through the callback's client, or it silently degrades to
// the delivered-event cursor and reopens the undelivered-event rollback.
func TestRemoteCacheBarrierFollowsFailover(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{pingTsNs: 2000, cacheSize: 200}
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

	// An event committed before the download (TsNs 1500 < ping 2000) but
	// delivered only now must not roll the handle back.
	late := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply late event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (barrier must come from the failover filer)", size)
	}
}

// An event applied while the open's lookup is in flight can already be
// reflected in the returned entry while sitting above the pre-lookup cursor.
// The open re-reads until the cursor is stable across the lookup so the fence
// covers such events.
func TestEventDuringOpenLookupIsFenced(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &fakeFilerServer{
		lookupSize:    200,
		lookupStarted: make(chan struct{}),
		lookupGate:    make(chan struct{}),
	}
	startFakeFiler(t, wfs, fake)

	// Stall the invalidation worker on an unrelated handle's lock.
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

	// While the open's lookup is blocked in the filer, an event lands and its
	// invalidation is queued behind the blocker.
	<-fake.lookupStarted
	during := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), during, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
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

// A pre-RPC ping cannot cover an event committed during the RPC itself. The
// cache response carries a log timestamp stamped before the filer read the
// entry, causally fencing everything the returned entry reflects.
func TestCacheResponseLogTsFencesEventsCommittedDuringRPC(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	// The ping (100) predates the event (1500); only the response's log
	// timestamp (2000) can cover it.
	startFakeFiler(t, wfs, &fakeFilerServer{pingTsNs: 100, cacheSize: 200, cacheLogTsNs: 2000})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	})

	if err := fh.downloadRemoteEntry(fh.GetEntry()); err != nil {
		t.Fatalf("downloadRemoteEntry: %v", err)
	}

	late := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 100},
			},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply late event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 200 {
		t.Fatalf("open handle file size = %d, want 200 (response log ts must fence the mid-RPC event)", size)
	}
}
