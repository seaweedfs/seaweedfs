package mount

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
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
	listErr      error
	listCalls    atomic.Int32
	lookupCalls  atomic.Int32
	mu           sync.Mutex
	listGate     chan struct{}
	startedCh    chan struct{}
	startedOnce  sync.Once
}

func (s *lookupCacheTestFiler) setListGate(gate, started chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listGate = gate
	s.startedCh = started
	s.startedOnce = sync.Once{}
}

func (s *lookupCacheTestFiler) signalStarted() {
	s.startedOnce.Do(func() {
		if s.startedCh != nil {
			close(s.startedCh)
		}
	})
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
	s.mu.Lock()
	gate := s.listGate
	s.mu.Unlock()
	if gate != nil {
		s.signalStarted()
		<-gate
	}
	if s.listErr != nil {
		return s.listErr
	}
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

// TestLookupRebuildDeduplicatesConcurrentRebuilds exercises the singleflight in
// EnsureVisited: concurrent lookups into the same expired directory share one
// rebuild rather than each issuing its own ListEntries.
func TestLookupRebuildDeduplicatesConcurrentRebuilds(t *testing.T) {
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
	expireDirCache(t, wfs.inodeToPath, dir)

	started := make(chan struct{})
	gate := make(chan struct{})
	fake.setListGate(gate, started)

	listCallsBefore := fake.listCalls.Load()
	names := []string{"a", "b", "c"}
	var wg sync.WaitGroup
	errs := make([]error, len(names))
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			if _, _, status := wfs.lookupEntry(dir.Child(name)); status != fuse.OK {
				errs[i] = fmt.Errorf("lookupEntry %s: %v", name, status)
			}
		}(i, name)
	}

	<-started
	// Hold the rebuild so the other lookups reach the singleflight while it
	// is in flight, exercising the deduplication rather than serializing.
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := fake.listCalls.Load() - listCallsBefore; got != 1 {
		t.Errorf("rebuild ListEntries RPCs = %d, want 1 (concurrent lookups should share one rebuild)", got)
	}
	if got := fake.lookupCalls.Load(); got != 0 {
		t.Errorf("LookupDirectoryEntry RPCs = %d, want 0", got)
	}
}

// TestLookupRebuildCooldownAfterFailure guards the failure path: a failed
// rebuild records the attempt so an immediate retry is suppressed (per-entry
// RPC fallback), and once the cooldown elapses and the filer recovers a later
// lookup rebuilds the cache again.
func TestLookupRebuildCooldownAfterFailure(t *testing.T) {
	wfs := newLookupCacheTestWFS(t, 60)
	dir := util.FullPath("/dir")
	wfs.inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)

	fake := &lookupCacheTestFiler{
		entries: []*filer_pb.Entry{
			{Name: "a", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644}},
		},
		snapshotTsNs: 5000,
	}
	startFakeFiler(t, wfs, fake)

	if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dir, 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	expireDirCache(t, wfs.inodeToPath, dir)

	// First lookup: rebuild fails, falls through to the per-entry RPC.
	fake.listErr = errors.New("persistent listing failure")
	listCallsBefore := fake.listCalls.Load()
	lookupCallsBefore := fake.lookupCalls.Load()
	if entry, _, status := wfs.lookupEntry(dir.Child("a")); status != fuse.OK || entry == nil || entry.Name() != "a" {
		t.Fatalf("lookupEntry a after failed rebuild: status=%v entry=%v", status, entry)
	}
	if got := fake.lookupCalls.Load() - lookupCallsBefore; got != 1 {
		t.Errorf("first lookup LookupDirectoryEntry RPCs = %d, want 1 (fall through to per-entry RPC)", got)
	}
	if got := fake.listCalls.Load() - listCallsBefore; got == 0 {
		t.Error("first lookup should have attempted a rebuild")
	}

	// Immediate retry: cooldown suppresses the rebuild, falls through again.
	listCallsBefore = fake.listCalls.Load()
	lookupCallsBefore = fake.lookupCalls.Load()
	if _, _, status := wfs.lookupEntry(dir.Child("a")); status != fuse.OK {
		t.Fatalf("second lookupEntry a: %v", status)
	}
	if got := fake.listCalls.Load() - listCallsBefore; got != 0 {
		t.Errorf("second lookup ListEntries RPCs = %d, want 0 (cooldown suppresses rebuild)", got)
	}
	if got := fake.lookupCalls.Load() - lookupCallsBefore; got != 1 {
		t.Errorf("second lookup LookupDirectoryEntry RPCs = %d, want 1", got)
	}

	// Filer recovers and the cooldown elapses: the next lookup rebuilds.
	fake.listErr = nil
	advanceRebuildAttempt(t, wfs.inodeToPath, dir, expiredDirRebuildCooldown+time.Second)
	listCallsBefore = fake.listCalls.Load()
	lookupCallsBefore = fake.lookupCalls.Load()
	if _, _, status := wfs.lookupEntry(dir.Child("a")); status != fuse.OK {
		t.Fatalf("third lookupEntry a: %v", status)
	}
	if got := fake.listCalls.Load() - listCallsBefore; got != 1 {
		t.Errorf("third lookup ListEntries RPCs = %d, want 1 (rebuild after cooldown)", got)
	}
	if got := fake.lookupCalls.Load() - lookupCallsBefore; got != 0 {
		t.Errorf("third lookup LookupDirectoryEntry RPCs = %d, want 0 (served from rebuilt cache)", got)
	}
}

// advanceRebuildAttempt moves a directory's last rebuild attempt back by d,
// simulating the cooldown having elapsed without sleeping.
func advanceRebuildAttempt(t *testing.T, itp *InodeToPath, dir util.FullPath, d time.Duration) {
	t.Helper()
	itp.Lock()
	defer itp.Unlock()
	state := itp.dirStateOf(dir)
	if state == nil {
		t.Fatalf("advanceRebuildAttempt: no dirState for %s", dir)
	}
	state.lastRebuildAttempt = time.Now().Add(-d)
}
