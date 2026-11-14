//go:build !freebsd

package mount

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// hardLinkFiler stores the entries of "/" the way the filer stores hard
// links: a name record pointing at a shared blob that holds the content and
// the link count. Removing a link lowers the count and stamps the ctime.
type hardLinkFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	records map[string]*filer_pb.Entry
	blobs   map[string]*filer_pb.Entry
	stamp   int64
	// failLookups fails that many lookups of failName once a delete ran.
	failName    string
	failLookups int
	deleted     bool
}

func (s *hardLinkFiler) resolve(name string) *filer_pb.Entry {
	record := s.records[name]
	if record == nil {
		return nil
	}
	entry := record
	if len(record.HardLinkId) > 0 {
		entry = s.blobs[string(record.HardLinkId)]
	}
	entry = proto.Clone(entry).(*filer_pb.Entry)
	entry.Name = name
	return entry
}

func (s *hardLinkFiler) store(entry *filer_pb.Entry) *filer_pb.Entry {
	s.stamp++
	stored := filer.FromPbEntry("/", entry).ToProtoEntry()
	stored.CommitTsNs = s.stamp
	if old := s.records[entry.Name]; old != nil && len(old.HardLinkId) > 0 && string(old.HardLinkId) != string(entry.HardLinkId) {
		s.unlinkBlob(old.HardLinkId)
	}
	if len(entry.HardLinkId) > 0 {
		s.blobs[string(entry.HardLinkId)] = stored
		s.records[entry.Name] = &filer_pb.Entry{Name: entry.Name, HardLinkId: entry.HardLinkId}
	} else {
		s.records[entry.Name] = stored
	}
	return s.resolve(entry.Name)
}

func (s *hardLinkFiler) unlinkBlob(id []byte) {
	blob := s.blobs[string(id)]
	if blob.HardLinkCounter--; blob.HardLinkCounter <= 0 {
		delete(s.blobs, string(id))
		return
	}
	now := time.Now()
	blob.Attributes.Ctime, blob.Attributes.CtimeNs = now.Unix(), int32(now.Nanosecond())
}

func (s *hardLinkFiler) LookupDirectoryEntry(ctx context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleted && r.Name == s.failName && s.failLookups > 0 {
		s.failLookups--
		return nil, status.Error(codes.Internal, "store read failed")
	}
	entry := s.resolve(r.Name)
	if entry == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (s *hardLinkFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.resolve(r.Entry.Name)
	for _, c := range r.GetCondition().GetClauses() {
		matches := false
		switch c.Kind {
		case filer_pb.WriteCondition_IF_ENTRY_EQUAL:
			expected := proto.Clone(c.ExpectedEntry).(*filer_pb.Entry)
			filer_pb.BeforeEntrySerialization(expected.Chunks)
			if current != nil {
				filer_pb.BeforeEntrySerialization(current.Chunks)
			}
			matches = current != nil && proto.Equal(expected, current)
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			matches = current == nil
		}
		if !matches {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	stored := s.store(r.Entry)
	return &filer_pb.CreateEntryResponse{LogTsNs: stored.CommitTsNs, MetadataEvent: &filer_pb.SubscribeMetadataResponse{
		Directory:         r.Directory,
		TsNs:              stored.CommitTsNs,
		EventNotification: &filer_pb.EventNotification{OldEntry: current, NewEntry: stored, NewParentPath: r.Directory},
	}}, nil
}

func (s *hardLinkFiler) UpdateEntry(ctx context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.resolve(r.Entry.Name)
	if current == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	stored := s.store(r.Entry)
	return &filer_pb.UpdateEntryResponse{MetadataEvent: &filer_pb.SubscribeMetadataResponse{
		Directory:         r.Directory,
		TsNs:              stored.CommitTsNs,
		EventNotification: &filer_pb.EventNotification{OldEntry: current, NewEntry: stored, NewParentPath: r.Directory},
	}}, nil
}

func (s *hardLinkFiler) DeleteEntry(ctx context.Context, r *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[r.Name]
	if record == nil {
		return &filer_pb.DeleteEntryResponse{}, nil
	}
	delete(s.records, r.Name)
	s.deleted = true
	if len(record.HardLinkId) > 0 {
		s.unlinkBlob(record.HardLinkId)
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (s *hardLinkFiler) stored(name string) *filer_pb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolve(name)
}

// failSurvivorReads fails every lookup of "final" once a delete ran.
func failSurvivorReads(fake *hardLinkFiler, fail bool) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.failName, fake.failLookups = "final", 0
	if fail {
		fake.failLookups = 1 << 30
	}
}

func linkHandleChunk(key uint64, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: 100, ModifiedTsNs: int64(key)}
}

// openLinkTestFile stores "/tmp" as the filer holds a flushed file and opens
// a clean handle on it, the way a create, write and fsync leave one.
func openLinkTestFile(t *testing.T) (*WFS, *hardLinkFiler, *FileHandle, uint64) {
	t.Helper()
	wfs := newInvalidateTestWFS(t)
	wfs.option.FilerMountRootPath = "/"
	fake := &hardLinkFiler{records: map[string]*filer_pb.Entry{}, blobs: map[string]*filer_pb.Entry{}}
	startFakeFiler(t, wfs, fake)
	stored := fake.store(&filer_pb.Entry{
		Name:       "tmp",
		Chunks:     []*filer_pb.FileChunk{linkHandleChunk(1, 0)},
		Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0o600, Uid: 2000, Gid: 20, FileSize: 100, Crtime: 1, Mtime: 1, Ctime: 1},
	})
	inode := wfs.inodeToPath.Lookup("/tmp", 1, false, false, 42, true)
	wfs.mapPbIdFromFilerToLocal(stored)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, stored, stored.CommitTsNs, 0)
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.RememberPath("/tmp")
	return wfs, fake, fh, inode
}

func appendAndFlush(t *testing.T, wfs *WFS, fh *FileHandle, key uint64) fuse.Status {
	t.Helper()
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(key, int64(len(e.Chunks))*100))
		e.Attributes.FileSize = uint64(len(e.Chunks)) * 100
	})
	fh.dirtyMetadata = true
	return wfs.doFlush(context.Background(), fh, 1000, 20, false, 0)
}

// queueClose closes fh the way a writeback mount does. Its flush starts once
// the handle is marked deleted, or a moment later, so callers that do not wait
// for it run first.
func queueClose(wfs *WFS, fh *FileHandle) {
	fh.asyncFlushPending = true
	wfs.pendingAsyncFlush = map[uint64]chan struct{}{}
	wfs.asyncFlushCh = make(chan *asyncFlushItem, 1)
	wfs.ReleaseHandle(fh.fh)
	markedDeleted := func() bool {
		lock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.SharedLock)
		defer wfs.fhLockTable.ReleaseLock(fh.fh, lock)
		return fh.isDeleted
	}
	go func() {
		item := <-wfs.asyncFlushCh
		for deadline := time.Now().Add(200 * time.Millisecond); !markedDeleted() && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		wfs.processAsyncFlushItem(item)
	}()
}

func linkInRoot(t *testing.T, wfs *WFS, inode uint64, name string) {
	t.Helper()
	in := &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 20}}}, Oldnodeid: inode}
	if code := wfs.Link(make(chan struct{}), in, name, &fuse.EntryOut{}); code != fuse.OK {
		t.Fatalf("link %s: %v", name, code)
	}
}

func unlinkInRoot(t *testing.T, wfs *WFS, name string) {
	t.Helper()
	header := &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 20}}}
	if code := wfs.Unlink(make(chan struct{}), header, name); code != fuse.OK {
		t.Fatalf("unlink %s: %v", name, code)
	}
}

func wantStored(t *testing.T, fake *hardLinkFiler, name string, chunks int, links int32) {
	t.Helper()
	entry := fake.stored(name)
	if entry == nil {
		t.Fatalf("%s is gone", name)
	}
	if len(entry.Chunks) != chunks || entry.HardLinkCounter != links {
		t.Fatalf("%s holds %d chunks and %d links, want %d and %d", name, len(entry.Chunks), entry.HardLinkCounter, chunks, links)
	}
}

// A file still open from its creation gets a second name; writes through the
// open handle must reach both names.
func TestLinkKeepsOpenHandlePublishing(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush after link: %v", code)
	}
	wantStored(t, fake, "tmp", 2, 2)
	wantStored(t, fake, "final", 2, 2)
}

// The temp-file publish pattern: write tmp, link it to its final name, unlink
// tmp, keep appending through the handle. Every fsync must reach the filer
// under the surviving name, and the removed name must stay removed.
func TestUnlinkedAliasKeepsOpenHandlePublishing(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "dirty"}[dirty], func(t *testing.T) {
			wfs, fake, fh, inode := openLinkTestFile(t)
			linkInRoot(t, wfs, inode, "final")
			if dirty {
				fh.UpdateEntry(func(e *filer_pb.Entry) {
					e.Chunks = append(e.Chunks, linkHandleChunk(9, 100))
					e.Attributes.FileSize = 200
				})
				fh.dirtyMetadata = true
			}
			unlinkInRoot(t, wfs, "tmp")
			if fh.isDeleted {
				t.Fatal("the handle of a file that still has a name is marked deleted")
			}
			unlinked := fake.stored("final").Attributes
			if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
				t.Fatalf("flush after unlinking the alias: %v", code)
			}
			chunks := 2
			if dirty {
				chunks = 3
			}
			wantStored(t, fake, "final", chunks, 1)
			if got := fake.stored("final").Attributes; got.Ctime != unlinked.Ctime || got.CtimeNs != unlinked.CtimeNs {
				t.Fatalf("the flush moved the ctime from the unlink's %d.%09d back to %d.%09d", unlinked.Ctime, unlinked.CtimeNs, got.Ctime, got.CtimeNs)
			}
			if fake.stored("tmp") != nil {
				t.Fatal("the flush recreated the unlinked name")
			}
		})
	}
}

// The read of the surviving entry right after the delete can fail; the next
// flush has to read it again rather than publish over the stale base forever.
func TestUnlinkedAliasRereadsLostEntryAtFlush(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	failSurvivorReads(fake, true)
	unlinkInRoot(t, wfs, "tmp")
	failSurvivorReads(fake, false)
	if fh.linkRefresh == nil {
		t.Fatal("a failed read of the surviving entry was not left for the next flush")
	}
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush after the lost read: %v", code)
	}
	wantStored(t, fake, "final", 2, 1)
	if fh.linkRefresh != nil {
		t.Fatal("the pending read survived a successful flush")
	}
}

// A flush that resolved the alias before the unlink removed it reaches the
// surviving name, and still reads it again when the unlink could not.
func TestFlushResolvedBeforeAliasUnlinkReachesSurvivor(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	failSurvivorReads(fake, true)
	unlinkInRoot(t, wfs, "tmp")
	failSurvivorReads(fake, false)
	lock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	err := wfs.adoptStoredLink(context.Background(), fh, "/tmp", fh.linkRefresh)
	wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	if err != nil || fh.linkRefresh == nil {
		t.Fatalf("reading the removed name dropped the pending read: %v", err)
	}
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	if err := wfs.flushMetadataToFiler(context.Background(), fh, "/", "tmp", 1000, 20, nil); err != nil {
		t.Fatalf("flush resolved before the unlink: %v", err)
	}
	wantStored(t, fake, "final", 2, 1)
	if fake.stored("tmp") != nil || fh.linkRefresh != nil {
		t.Fatal("the flush recreated the unlinked name or left the read pending")
	}
}

// A flush that picked the alias and waits for its publication lock while the
// alias is unlinked publishes through the surviving name.
func TestFlushWaitingThroughAliasUnlinkReachesSurvivor(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	table := wfs.metadataPublicationTable()
	held := table.AcquireLock("test", "/tmp", util.SharedLock)
	done := make(chan error, 1)
	go func() { done <- wfs.flushMetadataToFiler(context.Background(), fh, "/", "tmp", 1000, 20, nil) }()
	waitForPublicationWaiter(t, table, "/tmp")
	unlinkInRoot(t, wfs, "tmp")
	table.ReleaseLock("/tmp", held)
	if err := <-done; err != nil {
		t.Fatalf("flush that waited through the unlink: %v", err)
	}
	wantStored(t, fake, "final", 2, 1)
	if fake.stored("tmp") != nil {
		t.Fatal("the flush recreated the unlinked name")
	}
}

// A flush whose name is unlinked during every lock wait gives up with a
// retryable error instead of publishing to a name it has not checked.
func TestFlushWhoseNameKeepsChangingRetries(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	for _, name := range []string{"a", "b", "c", "final"} {
		linkInRoot(t, wfs, inode, name)
	}
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	table := wfs.metadataPublicationTable()
	current := util.FullPath("/tmp")
	held := table.AcquireLock("test", current, util.SharedLock)
	done := make(chan error, 1)
	go func() { done <- wfs.flushMetadataToFiler(context.Background(), fh, "/", "tmp", 1000, 20, nil) }()
	var removed []string
	for i := 0; i < 4; i++ {
		waitForPublicationWaiter(t, table, current)
		_, name := current.DirAndName()
		unlinkInRoot(t, wfs, name)
		removed = append(removed, name)
		next := fh.FullPath()
		nextHeld := table.AcquireLock("test", next, util.SharedLock)
		table.ReleaseLock(current, held)
		current, held = next, nextHeld
	}
	err := <-done
	table.ReleaseLock(current, held)
	if current != "/final" {
		t.Fatalf("the surviving name is %s, want /final", current)
	}
	if err == nil || !isRetryableFilerError(err) {
		t.Fatalf("flush whose name changed during every lock wait: %v, want a retryable error", err)
	}
	if err := wfs.flushMetadataToFiler(context.Background(), fh, "/", "tmp", 1000, 20, nil); err != nil {
		t.Fatalf("retried flush: %v", err)
	}
	wantStored(t, fake, "final", 2, 1)
	for _, name := range removed {
		if fake.stored(name) != nil {
			t.Fatalf("the flush recreated the unlinked name %s", name)
		}
	}
}

// waitForPublicationWaiter returns once a writer is queued behind the shared
// publication lock the test holds on path.
func waitForPublicationWaiter(t *testing.T, table *util.LockTable[util.FullPath], path util.FullPath) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		probe, free := table.TryAcquireLock("probe", path, util.SharedLock)
		if !free {
			return
		}
		table.ReleaseLock(path, probe)
		if time.Now().After(deadline) {
			t.Fatalf("the flush never waited for the publication lock on %s", path)
		}
	}
}

// With writeback caching close only queues the flush. Unlinking the alias,
// found in a listed directory, before that flush runs must not drop the
// content the other link keeps.
func TestUnlinkedAliasPublishesQueuedClose(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	wfs.inodeToPath.MarkChildrenCached("/")
	linkInRoot(t, wfs, inode, "final")
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	queueClose(wfs, fh)
	unlinkInRoot(t, wfs, "tmp")
	wantStored(t, fake, "final", 2, 1)
	if fake.stored("tmp") != nil {
		t.Fatal("the queued flush recreated the unlinked name")
	}
}

// With writeback caching the filer create of a new file waits for its queued
// close, so a link made right after the close has to wait for it too.
func TestLinkWaitsForQueuedClose(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	wfs.inodeToPath.MarkChildrenCached("/")
	fake.mu.Lock()
	delete(fake.records, "tmp")
	fake.mu.Unlock()
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", fh.GetEntry().GetEntry()), 0); err != nil {
		t.Fatalf("cache the local entry: %v", err)
	}
	fh.baseEntry.Store(nil)
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	queueClose(wfs, fh)
	linkInRoot(t, wfs, inode, "final")
	wantStored(t, fake, "tmp", 2, 2)
	wantStored(t, fake, "final", 2, 2)
}

// Unlinking the last name still stops the handle from writing it back.
func TestUnlinkLastLinkKeepsOpenHandleDeleted(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	unlinkInRoot(t, wfs, "tmp")
	unlinkInRoot(t, wfs, "final")
	if !fh.isDeleted {
		t.Fatal("the handle of a file with no names left is not marked deleted")
	}
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush after the last unlink: %v", code)
	}
	for _, name := range []string{"tmp", "final"} {
		if fake.stored(name) != nil {
			t.Fatalf("the flush recreated %s", name)
		}
	}
}

// adoptLinkChange must not paper over a peer's write: a stored entry that
// differs from the base in more than the link fields leaves the base alone.
func TestAdoptLinkChangeKeepsBaseOnPeerWrite(t *testing.T) {
	wfs, _, fh, _ := openLinkTestFile(t)
	base := fh.baseEntry.Load()
	stored := proto.Clone(base).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(stored)
	stored.HardLinkId, stored.HardLinkCounter = []byte("link"), 2
	stored.Chunks = append(stored.Chunks, linkHandleChunk(7, 100))
	fh.adoptLinkChange(stored, 99, 0)
	if fh.baseEntry.Load() != base || len(fh.GetEntry().GetEntry().HardLinkId) != 0 {
		t.Fatal("a peer's chunk change was adopted as the handle's base")
	}
	stored.Chunks = stored.Chunks[:1]
	fh.adoptLinkChange(stored, 99, 0)
	if got := fh.GetEntry().GetEntry(); string(got.HardLinkId) != "link" || got.HardLinkCounter != 2 {
		t.Fatalf("handle did not adopt the link: %v", got)
	}
}

// A link change no newer than what the handle holds must not move its base
// back; one stamped by another filer's clock says nothing about it and lands.
func TestAdoptLinkChangeSkipsOlderVersion(t *testing.T) {
	wfs, _, fh, _ := openLinkTestFile(t)
	base := fh.baseEntry.Load()
	fh.advanceEntryVersion(100, 5)
	stored := proto.Clone(base).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(stored)
	stored.HardLinkId, stored.HardLinkCounter = []byte("link"), 2
	fh.adoptLinkChange(stored, 100, 5)
	if fh.baseEntry.Load() != base || len(fh.GetEntry().GetEntry().HardLinkId) != 0 {
		t.Fatal("a link change older than the handle's version was adopted")
	}
	fh.adoptLinkChange(stored, 99, 7)
	if got := fh.GetEntry().GetEntry(); string(got.HardLinkId) != "link" || got.HardLinkCounter != 2 {
		t.Fatalf("a link change from another filer's clock was not adopted: %v", got)
	}
}
