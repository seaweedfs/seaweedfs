package mount

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// renameRaceFiler answers lookups, creates and updates from its entries and
// moves entries on rename. Each write records whether a rename was in flight
// when it arrived: on TiKV, a write to the source then refuses the rename's
// commit. holdRename and holdCreate keep the next rename or create of a name
// in flight until the test releases it.
type renameRaceFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu       sync.Mutex
	entries  map[util.FullPath]*filer_pb.Entry
	writes   []renameRaceWrite
	renaming int

	renameArrived, renameRelease chan struct{}

	holdCreateName               string
	createArrived, createRelease chan struct{}
}

type renameRaceWrite struct {
	op           string
	path         util.FullPath
	duringRename bool
}

func (s *renameRaceFiler) holdRename(t *testing.T) (arrived chan struct{}, release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renameArrived, s.renameRelease = make(chan struct{}), make(chan struct{})
	release = closeOnce(s.renameRelease)
	t.Cleanup(release)
	return s.renameArrived, release
}

func (s *renameRaceFiler) holdCreate(t *testing.T, name string) (arrived chan struct{}, release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdCreateName, s.createArrived, s.createRelease = name, make(chan struct{}), make(chan struct{})
	release = closeOnce(s.createRelease)
	t.Cleanup(release)
	return s.createArrived, release
}

func (s *renameRaceFiler) recorded() []renameRaceWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]renameRaceWrite(nil), s.writes...)
}

func (s *renameRaceFiler) stored(p util.FullPath) *filer_pb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.entries[p]; entry != nil {
		return proto.Clone(entry).(*filer_pb.Entry)
	}
	return nil
}

func (s *renameRaceFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if stored := s.stored(util.NewFullPath(r.Directory, r.Name)); stored != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: stored}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (s *renameRaceFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	p := util.NewFullPath(r.Directory, r.Entry.GetName())
	s.mu.Lock()
	s.writes = append(s.writes, renameRaceWrite{op: "create", path: p, duringRename: s.renaming > 0})
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
	s.entries[p] = proto.Clone(r.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

// UpdateEntry refuses an update of a name that holds no entry, as the filer
// refuses one whose condition expected an entry there.
func (s *renameRaceFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	p := util.NewFullPath(r.Directory, r.Entry.GetName())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, renameRaceWrite{op: "update", path: p, duringRename: s.renaming > 0})
	if s.entries[p] == nil {
		return nil, status.Error(codes.FailedPrecondition, "no entry to update")
	}
	s.entries[p] = proto.Clone(r.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (s *renameRaceFiler) StreamRenameEntry(r *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) error {
	s.mu.Lock()
	s.renaming++
	arrived, release := s.renameArrived, s.renameRelease
	s.renameArrived, s.renameRelease = nil, nil
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.renaming--
		s.mu.Unlock()
	}()
	if arrived != nil {
		close(arrived)
		<-release
	}

	oldPath, newPath := util.NewFullPath(r.OldDirectory, r.OldName), util.NewFullPath(r.NewDirectory, r.NewName)
	s.mu.Lock()
	moved := s.entries[oldPath]
	if moved == nil {
		s.mu.Unlock()
		return status.Errorf(codes.NotFound, "%s not found", oldPath)
	}
	moved = proto.Clone(moved).(*filer_pb.Entry)
	moved.Name = r.NewName
	s.entries[newPath] = moved
	delete(s.entries, oldPath)
	s.mu.Unlock()

	if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
		Directory: r.OldDirectory,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: r.OldName},
			NewEntry:      proto.Clone(moved).(*filer_pb.Entry),
			NewParentPath: r.NewDirectory,
		},
		TsNs: time.Now().UnixNano(),
	}); err != nil {
		return err
	}
	return stream.Send(&filer_pb.StreamRenameEntryResponse{
		Directory:         r.OldDirectory,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: r.OldName}},
		TsNs:              time.Now().UnixNano(),
	})
}

// newRenameRaceWFS mounts /checkpoints and /final, neither listed by the
// mount, so lookups under them go to the filer.
func newRenameRaceWFS(t *testing.T) (*WFS, *renameRaceFiler, uint64, uint64) {
	t.Helper()
	wfs, _ := newCreateTestWFS(t)
	fake := &renameRaceFiler{entries: map[util.FullPath]*filer_pb.Entry{}}
	startFakeFiler(t, wfs, fake)
	var inodes []uint64
	for _, dir := range []util.FullPath{"/checkpoints", "/final"} {
		_, name := dir.DirAndName()
		fake.entries[dir] = &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(0o777 | 1<<31), Uid: 123, Gid: 456, Mtime: 1, Crtime: 1},
		}
		inodes = append(inodes, wfs.inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true))
	}
	return wfs, fake, inodes[0], inodes[1]
}

func renameFile(wfs *WFS, oldDirInode, newDirInode uint64, oldName, newName string) fuse.Status {
	return wfs.Rename(make(chan struct{}), &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: oldDirInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Newdir:   newDirInode,
	}, oldName, newName)
}

// An unsupported flag must not fall through to a plain rename over the target.
func TestRenameRefusesFlagsItDoesNotImplement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags uint32
	}{
		{"exchange", RenameExchange},
		{"whiteout", RenameWhiteout},
		{"noreplace-and-exchange", RenameNoReplace | RenameExchange},
		{"unknown", 1 << 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs, fake, dirInode, _ := newRenameRaceWFS(t)
			file := func(name string, inode uint64) *filer_pb.Entry {
				return &filer_pb.Entry{
					Name:       name,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Uid: 123, Gid: 456, Mtime: 1, Crtime: 1, Inode: inode},
				}
			}
			fake.mu.Lock()
			fake.entries["/checkpoints/a"] = file("a", 11)
			fake.entries["/checkpoints/b"] = file("b", 22)
			fake.mu.Unlock()

			code := wfs.Rename(make(chan struct{}), &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: dirInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
				Newdir:   dirInode,
				Flags:    tc.flags,
			}, "a", "b")
			if code != fuse.EINVAL {
				t.Fatalf("rename with flags %#x: %v, want EINVAL", tc.flags, code)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			a, b := fake.entries["/checkpoints/a"], fake.entries["/checkpoints/b"]
			if a.GetAttributes().GetInode() != 11 || b.GetAttributes().GetInode() != 22 {
				t.Fatalf("after the refused rename a=%v b=%v, want both where they were", a, b)
			}
		})
	}
}

func waitFor(t *testing.T, what string, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never happened", what)
	}
}

func waitStatus(t *testing.T, what string, ch chan fuse.Status) fuse.Status {
	t.Helper()
	select {
	case st := <-ch:
		return st
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
	return fuse.EIO
}

// stillWaiting reports a call that has not returned after a while: one that
// waits for a rename in flight.
func stillWaiting(ch chan fuse.Status) bool {
	select {
	case <-ch:
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}

func checkNoWriteDuringRename(t *testing.T, fake *renameRaceFiler) {
	t.Helper()
	for _, w := range fake.recorded() {
		if w.duringRename {
			t.Fatalf("%s of %s reached the filer while the rename was in flight: on TiKV it refuses the rename's commit (all writes %+v)", w.op, w.path, fake.recorded())
		}
	}
}

// A flush that waited for the locks while another flush published the same
// handle finds nothing left to publish. Sending the entry again rewrote the
// stored file for nothing.
func TestFlushOfPublishedHandleSendsNothing(t *testing.T) {
	wfs, fake, dirInode, _ := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpA1b2C3")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	before := len(fake.recorded())

	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if err := wfs.flushMetadataToFiler(context.Background(), fh, "/checkpoints", ".tmpA1b2C3", 123, 456, nil); err != nil {
		t.Fatalf("flushMetadataToFiler = %v, want nil", err)
	}
	if writes := fake.recorded(); len(writes) != before {
		t.Fatalf("a flush of a published handle wrote again: %+v", writes[before:])
	}
}

// The incident of 11-19: safetensors renames its temp file while the file is
// still open. The rename publishes the deferred create first; a FLUSH from a
// forked child's close of the same file, queued behind that publication,
// published it a second time while the filer moved the file, and TiKV refused
// the rename's commit: EIO. The deferred create is published once, and nothing
// reaches the filer while it moves the file.
func TestRenameOfDeferredCreateWithQueuedFlush(t *testing.T) {
	wfs, fake, dirInode, _ := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpTdjXVI")
	createArrived, releaseCreate := fake.holdCreate(t, ".tmpTdjXVI")
	renameArrived, releaseRename := fake.holdRename(t)

	renamed := make(chan fuse.Status, 1)
	go func() { renamed <- renameFile(wfs, dirInode, dirInode, ".tmpTdjXVI", "ema.safetensors.tmp") }()
	waitFor(t, "the rename's publication of the deferred create", createArrived)

	// The FLUSH has looked at the handle while it was still unpublished and
	// waits for its locks: doFlush from there on. (Calling Flush itself would
	// add doFlush's unlocked first look at the flag to the race report.)
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	started := make(chan struct{})
	flushed := make(chan fuse.Status, 1)
	go func() {
		close(started)
		if err := wfs.flushMetadataToFiler(context.Background(), fh, "/checkpoints", ".tmpTdjXVI", 123, 456, nil); err != nil {
			flushed <- fuse.EIO
			return
		}
		flushed <- fuse.OK
	}()
	waitFor(t, "the queued flush starting", started)
	// It waits on the locks the rename's publication holds, so it cannot
	// return before that publication does; let it reach them.
	time.Sleep(200 * time.Millisecond)
	select {
	case st := <-flushed:
		t.Fatalf("the queued flush returned %v while the rename's publication held the locks", st)
	default:
	}
	releaseCreate()

	waitFor(t, "the rename reaching the filer", renameArrived)
	// A queued publication that is coming arrives while the filer moves the file.
	time.Sleep(200 * time.Millisecond)
	releaseRename()

	if st := waitStatus(t, "Rename", renamed); st != fuse.OK {
		t.Fatalf("Rename = %v, want OK", st)
	}
	if st := waitStatus(t, "Flush", flushed); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	checkNoWriteDuringRename(t, fake)
	var creates int
	for _, w := range fake.recorded() {
		if w.op == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("CreateEntry calls = %d, want the deferred create published once (writes %+v)", creates, fake.recorded())
	}
	if fake.stored("/checkpoints/ema.safetensors.tmp") == nil || fake.stored("/checkpoints/.tmpTdjXVI") != nil {
		t.Fatal("the file must end up under its new name only")
	}
}

// markWritten does to the handle what a WRITE does: new content to publish.
func markWritten(wfs *WFS, out *fuse.CreateOut, size uint64) {
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	lock := wfs.fhLockTable.AcquireLock("test write", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	fh.UpdateEntry(func(entry *filer_pb.Entry) { entry.Attributes.FileSize = size })
	fh.dirtyMetadata = true
}

// A close that publishes new content while a rename of the file is in flight
// waits for the rename, then publishes under the new name.
func TestFlushDuringRenamePublishesUnderNewName(t *testing.T) {
	wfs, fake, dirInode, finalInode := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpB")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	renameArrived, releaseRename := fake.holdRename(t)

	renamed := make(chan fuse.Status, 1)
	go func() { renamed <- renameFile(wfs, dirInode, finalInode, ".tmpB", "b.bin") }()
	waitFor(t, "the rename reaching the filer", renameArrived)

	markWritten(wfs, out, 4096)
	flushed := make(chan fuse.Status, 1)
	go func() { flushed <- flushHandle(wfs, out) }()
	if !stillWaiting(flushed) {
		t.Fatal("the flush returned while the rename was in flight")
	}
	checkNoWriteDuringRename(t, fake)
	releaseRename()

	if st := waitStatus(t, "Rename", renamed); st != fuse.OK {
		t.Fatalf("Rename = %v, want OK", st)
	}
	if st := waitStatus(t, "Flush", flushed); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	var published, recreated bool
	for _, w := range fake.recorded() {
		published = published || w.op == "create" && w.path == "/final/b.bin"
		recreated = recreated || w.op == "create" && w.path == "/checkpoints/.tmpB" && published
	}
	if !published || recreated {
		t.Fatalf("writes = %+v, want the flush publishing /final/b.bin", fake.recorded())
	}
	if stored := fake.stored("/final/b.bin"); stored == nil || stored.GetAttributes().GetFileSize() != 4096 {
		t.Fatalf("/final/b.bin = %v, want the flushed size 4096", stored)
	}
	if fake.stored("/checkpoints/.tmpB") != nil {
		t.Fatal("the flush recreated the old name")
	}
}

// A chmod through an open file while a rename moves it to another directory
// waits for the rename, then applies to the file under its new name.
func TestChmodDuringRenameAppliesToNewName(t *testing.T) {
	wfs, fake, dirInode, finalInode := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, ".tmpC")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	renameArrived, releaseRename := fake.holdRename(t)

	renamed := make(chan fuse.Status, 1)
	go func() { renamed <- renameFile(wfs, dirInode, finalInode, ".tmpC", "c.bin") }()
	waitFor(t, "the rename reaching the filer", renameArrived)

	chmodded := make(chan fuse.Status, 1)
	go func() {
		in := &fuse.SetAttrIn{}
		in.NodeId, in.Fh, in.Valid, in.Mode = out.NodeId, out.Fh, fuse.FATTR_MODE|fuse.FATTR_FH, 0o640
		in.Caller = fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}
		chmodded <- wfs.SetAttr(make(chan struct{}), in, &fuse.AttrOut{})
	}()
	if !stillWaiting(chmodded) {
		t.Fatal("the chmod returned while the rename was in flight")
	}
	checkNoWriteDuringRename(t, fake)
	releaseRename()

	if st := waitStatus(t, "Rename", renamed); st != fuse.OK {
		t.Fatalf("Rename = %v, want OK", st)
	}
	if st := waitStatus(t, "SetAttr", chmodded); st != fuse.OK {
		t.Fatalf("SetAttr = %v, want OK", st)
	}
	stored := fake.stored("/final/c.bin")
	if stored == nil || stored.GetAttributes().GetFileMode()&0o777 != 0o640 {
		t.Fatalf("/final/c.bin = %v, want mode 0640", stored)
	}
	if fake.stored("/checkpoints/.tmpC") != nil {
		t.Fatal("the chmod recreated the old name")
	}
}

// Two renames between the same two names, in opposite directions, take their
// locks in the same order and both finish.
func TestOppositeRenamesDoNotDeadlock(t *testing.T) {
	wfs, _, dirInode, finalInode := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, "x")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	// Closed: only the names' locks are at stake here.
	wfs.Release(make(chan struct{}), &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: out.NodeId, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Fh:       out.Fh,
	})
	var wg sync.WaitGroup
	results := make(chan fuse.Status, 40)
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); results <- renameFile(wfs, dirInode, finalInode, "x", "x") }()
		go func() { defer wg.Done(); results <- renameFile(wfs, finalInode, dirInode, "x", "x") }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("renames in opposite directions deadlocked")
	}
	close(results)
	for st := range results {
		if st != fuse.OK && st != fuse.Status(syscall.ENOENT) {
			t.Fatalf("Rename = %v, want OK or ENOENT", st)
		}
	}
}

// One open file renamed back and forth between two directories while other
// goroutines chmod it through its handle: no change of the file reaches the
// filer while a rename moves it, every chmod lands on the file wherever it is,
// and nothing deadlocks.
func TestRenameStressWithAttributeChanges(t *testing.T) {
	wfs, fake, dirInode, finalInode := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, "s")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	stop := make(chan struct{})
	failed := make(chan fuse.Status, 2)
	var wg sync.WaitGroup
	for _, mode := range []uint32{0o600, 0o640} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				in := &fuse.SetAttrIn{}
				in.NodeId, in.Valid, in.Mode = out.NodeId, fuse.FATTR_MODE, mode
				in.Caller = fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}
				// EAGAIN: renamed again every time the chmod waited
				if st := wfs.SetAttr(make(chan struct{}), in, &fuse.AttrOut{}); st != fuse.OK && st != fuse.Status(syscall.EAGAIN) {
					failed <- st
					return
				}
			}
		}()
	}
	dirs := [2]uint64{dirInode, finalInode}
	for i := 0; i < 40; i++ {
		if st := renameFile(wfs, dirs[i%2], dirs[(i+1)%2], "s", "s"); st != fuse.OK {
			close(stop)
			wg.Wait()
			t.Fatalf("rename %d = %v, want OK", i, st)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case st := <-failed:
		t.Fatalf("SetAttr = %v, want OK", st)
	default:
	}
	for _, w := range fake.recorded() {
		if w.duringRename && (w.path == "/checkpoints/s" || w.path == "/final/s") {
			t.Fatalf("%s of %s reached the filer while a rename moved the file", w.op, w.path)
		}
	}
	if fake.stored("/checkpoints/s") == nil || fake.stored("/final/s") != nil {
		t.Fatal("after an even number of renames the file must be back in /checkpoints only")
	}
}

// Renames back and forth while other goroutines run this mount's flush paths
// on the same open file (a close's flush, the periodic metadata flush) and
// chmod it: the rename's locks, the flushes' and the attribute changes' are
// taken in one order, so nothing deadlocks, and nothing reaches the filer for
// the file's names while a rename moves it. The handle has nothing new to
// publish, so the flushes stop at their check under the locks.
func TestRenameStressWithFlushes(t *testing.T) {
	wfs, fake, dirInode, finalInode := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, "f")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	before := len(fake.recorded())

	stop := make(chan struct{})
	failed := make(chan string, 3)
	var wg sync.WaitGroup
	run := func(what string, step func() string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if msg := step(); msg != "" {
					failed <- what + ": " + msg
					return
				}
			}
		}()
	}
	run("close flush", func() string {
		dir, name := fh.FullPath().DirAndName()
		if err := wfs.flushMetadataToFiler(context.Background(), fh, dir, name, 123, 456, nil); err != nil && !strings.Contains(err.Error(), "name changed while waiting") {
			return err.Error()
		}
		return ""
	})
	run("periodic flush", func() string {
		if err := wfs.flushFileMetadata(fh); err != nil && !strings.Contains(err.Error(), "name changed while waiting") {
			return err.Error()
		}
		return ""
	})
	run("chmod", func() string {
		in := &fuse.SetAttrIn{}
		in.NodeId, in.Valid, in.Mode = out.NodeId, fuse.FATTR_MODE, 0o640
		in.Caller = fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}
		if st := wfs.SetAttr(make(chan struct{}), in, &fuse.AttrOut{}); st != fuse.OK && st != fuse.Status(syscall.EAGAIN) {
			return st.String()
		}
		return ""
	})

	done := make(chan string, 1)
	go func() {
		dirs := [2]uint64{dirInode, finalInode}
		for i := 0; i < 40; i++ {
			if st := renameFile(wfs, dirs[i%2], dirs[(i+1)%2], "f", "f"); st != fuse.OK {
				done <- fmt.Sprintf("rename %d = %v", i, st)
				return
			}
		}
		done <- ""
	}()
	select {
	case msg := <-done:
		close(stop)
		if msg != "" {
			wg.Wait()
			t.Fatal(msg)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("renames with concurrent flushes did not finish: deadlock")
	}
	wg.Wait()
	select {
	case msg := <-failed:
		t.Fatal(msg)
	default:
	}
	for _, w := range fake.recorded()[before:] {
		if w.duringRename && (w.path == "/checkpoints/f" || w.path == "/final/f") {
			t.Fatalf("%s of %s reached the filer while a rename moved the file", w.op, w.path)
		}
		if w.op == "create" {
			t.Fatalf("a flush of an unchanged handle published it: %+v", w)
		}
	}
	if fake.stored("/checkpoints/f") == nil || fake.stored("/final/f") != nil {
		t.Fatal("after an even number of renames the file must be back in /checkpoints only")
	}
}

// A chmod whose file is moved again each time it gets the name's lock gives
// up with EAGAIN instead of writing a name the file has left.
func TestChmodOfAFileThatKeepsMovingGivesUp(t *testing.T) {
	wfs, fake, dirInode, _ := newRenameRaceWFS(t)
	out := createDeferredFile(t, wfs, dirInode, "f0")
	if st := flushHandle(wfs, out); st != fuse.OK {
		t.Fatalf("Flush = %v, want OK", st)
	}
	writes := len(fake.recorded())
	table := wfs.metadataPublicationTable()
	// Held shared, a name's lock still stops the chmod, and a shared try
	// fails only while the chmod is queued for it.
	waitQueued := func(p util.FullPath) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			lock, acquired := table.TryAcquireLock("test probe", p, util.SharedLock)
			if !acquired {
				return
			}
			table.ReleaseLock(p, lock)
			if time.Now().After(deadline) {
				t.Fatalf("the chmod never waited for %s", p)
			}
			time.Sleep(time.Millisecond)
		}
	}

	path := util.FullPath("/checkpoints/f0")
	held := table.AcquireLock("test", path, util.SharedLock)
	chmodded := make(chan fuse.Status, 1)
	go func() {
		in := &fuse.SetAttrIn{}
		in.NodeId, in.Fh, in.Valid, in.Mode = out.NodeId, out.Fh, fuse.FATTR_MODE|fuse.FATTR_FH, 0o640
		in.Caller = fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}
		chmodded <- wfs.SetAttr(make(chan struct{}), in, &fuse.AttrOut{})
	}()
	for i := 1; i <= 4; i++ {
		waitQueued(path)
		next := util.FullPath(fmt.Sprintf("/checkpoints/f%d", i))
		wfs.inodeToPath.MovePath(path, next)
		nextHeld := table.AcquireLock("test", next, util.SharedLock)
		table.ReleaseLock(path, held)
		path, held = next, nextHeld
	}
	st := waitStatus(t, "SetAttr", chmodded)
	table.ReleaseLock(path, held)
	if st != fuse.Status(syscall.EAGAIN) {
		t.Fatalf("SetAttr = %v, want EAGAIN", st)
	}
	if got := fake.recorded(); len(got) != writes {
		t.Fatalf("writes = %+v, want none after the flush", got[writes:])
	}
}
