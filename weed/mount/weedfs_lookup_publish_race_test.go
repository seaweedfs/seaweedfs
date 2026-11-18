package mount

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// publishRaceFiler is a filer whose next lookup of a held name reads the
// store when the request arrives but answers only once the test releases it:
// a read served from a snapshot taken before a concurrent create commits.
type publishRaceFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[util.FullPath]*filer_pb.Entry
	lookups map[util.FullPath]int

	holdName string
	arrived  chan struct{}
	release  chan struct{}

	// The next create of holdCreateName waits for createRelease before it
	// is stored and acknowledged: a publish in flight.
	holdCreateName string
	createArrived  chan struct{}
	createRelease  chan struct{}

	// dropCreates acknowledges creates without keeping them, as if a peer
	// deleted each file right after it was published.
	dropCreates bool
	creates     atomic.Int32
}

func newPublishRaceFiler() *publishRaceFiler {
	return &publishRaceFiler{
		entries: map[util.FullPath]*filer_pb.Entry{},
		lookups: map[util.FullPath]int{},
	}
}

func (s *publishRaceFiler) hold(name string) (arrived, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdName, s.arrived, s.release = name, make(chan struct{}), make(chan struct{})
	return s.arrived, s.release
}

func (s *publishRaceFiler) holdCreate(name string) (arrived, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdCreateName, s.createArrived, s.createRelease = name, make(chan struct{}), make(chan struct{})
	return s.createArrived, s.createRelease
}

func (s *publishRaceFiler) lookupCount(p util.FullPath) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups[p]
}

func (s *publishRaceFiler) remove(p util.FullPath) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, p)
}

func (s *publishRaceFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	p := util.NewFullPath(r.Directory, r.Name)
	s.mu.Lock()
	s.lookups[p]++
	stored := s.entries[p]
	var arrived, release chan struct{}
	if s.holdName != "" && r.Name == s.holdName {
		arrived, release = s.arrived, s.release
		s.holdName = ""
	}
	s.mu.Unlock()
	if arrived != nil {
		close(arrived)
		<-release
	}
	if stored == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(stored).(*filer_pb.Entry)}, nil
}

func (s *publishRaceFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.creates.Add(1)
	s.mu.Lock()
	var arrived, release chan struct{}
	if s.holdCreateName != "" && r.Entry.GetName() == s.holdCreateName {
		arrived, release = s.createArrived, s.createRelease
		s.holdCreateName = ""
	}
	s.mu.Unlock()
	if arrived != nil {
		close(arrived)
		<-release
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dropCreates {
		s.entries[util.NewFullPath(r.Directory, r.Entry.Name)] = proto.Clone(r.Entry).(*filer_pb.Entry)
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *publishRaceFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[util.NewFullPath(r.Directory, r.Entry.Name)] = proto.Clone(r.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

// newPublishRaceWFS mounts a directory the mount has not listed, so lookups
// under it go to the filer, the state a checkpoint directory is in when its
// cached listing has expired.
func newPublishRaceWFS(t *testing.T) (*WFS, *publishRaceFiler, util.FullPath, uint64) {
	t.Helper()
	wfs, _ := newCreateTestWFS(t)
	fake := newPublishRaceFiler()
	startFakeFiler(t, wfs, fake)
	dir := util.FullPath("/checkpoints")
	fake.mu.Lock()
	fake.entries[dir] = &filer_pb.Entry{
		Name:        "checkpoints",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(0o755 | 1<<31), Uid: 123, Gid: 456, Mtime: 1, Crtime: 1},
	}
	fake.mu.Unlock()
	dirInode := wfs.inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)
	if wfs.metaCache.IsDirectoryCached(dir) {
		t.Fatalf("%s must not be cached for lookups to reach the filer", dir)
	}
	return wfs, fake, dir, dirInode
}

func createDeferredFile(t *testing.T, wfs *WFS, dirInode uint64, name string) *fuse.CreateOut {
	t.Helper()
	out := &fuse.CreateOut{}
	if st := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: dirInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Flags:    syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL,
		Mode:     0o600,
	}, name, out); st != fuse.OK {
		t.Fatalf("Create %s = %v, want OK", name, st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if fh == nil || !fh.dirtyMetadata {
		t.Fatalf("Create %s should leave a dirty handle holding a deferred create", name)
	}
	return out
}

func flushHandle(wfs *WFS, out *fuse.CreateOut) fuse.Status {
	return wfs.Flush(make(chan struct{}), &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: out.NodeId, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Fh:       out.Fh,
	})
}

// lookupWhilePublishing starts a lookup of path, lets the filer read its store
// before the deferred create is published, publishes it with a flush (as the
// FLUSH a forked child's close sends), and only then lets the answer return.
func lookupWhilePublishing(t *testing.T, wfs *WFS, fake *publishRaceFiler, out *fuse.CreateOut, path util.FullPath) fuse.Status {
	t.Helper()
	_, name := path.DirAndName()
	arrived, release := fake.hold(name)
	done := make(chan fuse.Status, 1)
	go func() {
		_, _, st := wfs.lookupEntry(context.Background(), path)
		done <- st
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("lookup never reached the filer")
	}
	if st := flushHandle(wfs, out); st != fuse.OK {
		close(release)
		t.Fatalf("Flush = %v, want OK", st)
	}
	if fh := wfs.GetHandle(FileHandleId(out.Fh)); fh == nil || fh.dirtyMetadata {
		close(release)
		t.Fatal("the flush should publish the deferred create and leave the handle clean")
	}
	close(release)
	select {
	case st := <-done:
		return st
	case <-time.After(10 * time.Second):
		t.Fatal("lookup did not return")
	}
	return fuse.EIO
}

// safetensors (and Python's tempfile) write a checkpoint to a temp name and
// rename it over the target while the file is still open. The rename looks up
// the temp name first. When a flush publishes the deferred create while that
// lookup is in flight, the filer can answer from a read taken before the
// publish committed, and the handle is already clean by the time the answer
// arrives. The file exists; the rename must not fail with ENOENT.
func TestLookupFindsDeferredCreatePublishedDuringLookup(t *testing.T) {
	wfs, fake, dir, dirInode := newPublishRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpA1b2C3")
	path := dir.Child(".tmpA1b2C3")
	before := fake.lookupCount(path)

	if st := lookupWhilePublishing(t, wfs, fake, out, path); st != fuse.OK {
		t.Fatalf("lookupEntry = %v, want OK: the file was published while the lookup was in flight", st)
	}
	if got := fake.creates.Load(); got != 1 {
		t.Fatalf("CreateEntry calls = %d, want 1", got)
	}
	if got := fake.lookupCount(path) - before; got != 2 {
		t.Fatalf("LookupDirectoryEntry calls = %d, want 2 (the stale answer, then one after the publish)", got)
	}
}

// Asking again must not resurrect a file the filer no longer has: here a peer
// removed it right after the publish, so the second answer stands.
func TestLookupReportsFileRemovedAfterPublishDuringLookup(t *testing.T) {
	wfs, fake, dir, dirInode := newPublishRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpGone")
	path := dir.Child(".tmpGone")
	before := fake.lookupCount(path)
	fake.mu.Lock()
	fake.dropCreates = true
	fake.mu.Unlock()

	if st := lookupWhilePublishing(t, wfs, fake, out, path); st != fuse.ENOENT {
		t.Fatalf("lookupEntry = %v, want ENOENT: the filer no longer has the file", st)
	}
	if got := fake.lookupCount(path) - before; got != 2 {
		t.Fatalf("LookupDirectoryEntry calls = %d, want 2", got)
	}
}

// A clean handle has nothing unpublished, so a not-found answer is final and
// is not asked twice.
func TestLookupNotFoundForCleanHandleAsksOnce(t *testing.T) {
	wfs, fake, dir, dirInode := newPublishRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, "published")
	path := dir.Child("published")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	fake.remove(path) // a peer deleted it
	before := fake.lookupCount(path)

	if _, _, st := wfs.lookupEntry(context.Background(), path); st != fuse.ENOENT {
		t.Fatalf("lookupEntry = %v, want ENOENT", st)
	}
	if got := fake.lookupCount(path) - before; got != 1 {
		t.Fatalf("LookupDirectoryEntry calls = %d, want 1", got)
	}
}

// A deferred create that stays unpublished for the whole lookup is answered
// from the handle, with one filer call.
func TestLookupNotFoundForDirtyHandleUsesLocalEntry(t *testing.T) {
	wfs, fake, dir, dirInode := newPublishRaceWFS(t)
	createDeferredFile(t, wfs, dirInode, "local-only")
	path := dir.Child("local-only")
	before := fake.lookupCount(path)

	entry, _, st := wfs.lookupEntry(context.Background(), path)
	if st != fuse.OK || entry == nil {
		t.Fatalf("lookupEntry = %v, want the deferred entry", st)
	}
	if got := fake.lookupCount(path) - before; got != 1 {
		t.Fatalf("LookupDirectoryEntry calls = %d, want 1", got)
	}
	if got := fake.creates.Load(); got != 0 {
		t.Fatalf("CreateEntry calls = %d, want 0 before any flush", got)
	}
}

// The not-found answer can also arrive while the publish is still in flight:
// sent, but not yet acknowledged. The handle is then still the only copy the
// mount can vouch for, so it answers, whenever the flush completes.
func TestLookupAnswersFromHandleWhilePublishInFlight(t *testing.T) {
	wfs, fake, dir, dirInode := newPublishRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpSlow")
	path := dir.Child(".tmpSlow")
	before := fake.lookupCount(path)

	lookupArrived, lookupRelease := fake.hold(".tmpSlow")
	createArrived, createRelease := fake.holdCreate(".tmpSlow")
	done := make(chan fuse.Status, 1)
	go func() {
		_, _, st := wfs.lookupEntry(context.Background(), path)
		done <- st
	}()
	waitClosed(t, lookupArrived, "lookup never reached the filer")
	flushed := make(chan fuse.Status, 1)
	go func() { flushed <- flushHandle(wfs, out) }()
	waitClosed(t, createArrived, "flush never sent its create")

	close(lookupRelease)
	select {
	case st := <-done:
		if st != fuse.OK {
			close(createRelease)
			t.Fatalf("lookupEntry = %v, want OK: the file is being published", st)
		}
	case <-time.After(10 * time.Second):
		close(createRelease)
		t.Fatal("lookup did not return while the publish was in flight")
	}
	close(createRelease)
	select {
	case st := <-flushed:
		if st != fuse.OK {
			t.Fatalf("Flush = %v, want OK", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flush did not return")
	}
	if got := fake.lookupCount(path) - before; got != 1 {
		t.Fatalf("LookupDirectoryEntry calls = %d, want 1", got)
	}
}

func waitClosed(t *testing.T, ch chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
	}
}
