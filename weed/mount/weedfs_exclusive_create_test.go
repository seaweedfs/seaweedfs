package mount

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// exclusiveCreateFiler is one filer shared by several mounts. Like the real
// filer, it refuses an OExcl create or an IF_NOT_EXISTS publish of a name it
// already stores, and otherwise replaces the stored entry.
type exclusiveCreateFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[util.FullPath]*filer_pb.Entry
	creates int
}

func newExclusiveCreateFiler() *exclusiveCreateFiler {
	return &exclusiveCreateFiler{entries: map[util.FullPath]*filer_pb.Entry{}}
}

func (s *exclusiveCreateFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	p := util.NewFullPath(r.Directory, r.Entry.Name)
	stored := s.entries[p]
	if stored != nil && r.OExcl {
		return &filer_pb.CreateEntryResponse{Error: "entry already exists", ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, nil
	}
	for _, c := range r.GetCondition().GetClauses() {
		if c.Kind == filer_pb.WriteCondition_IF_NOT_EXISTS && stored != nil ||
			c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && !proto.Equal(c.ExpectedEntry, stored) {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	s.entries[p] = proto.Clone(r.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *exclusiveCreateFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := s.entries[util.NewFullPath(r.Directory, r.Name)]
	if stored == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(stored).(*filer_pb.Entry)}, nil
}

func (s *exclusiveCreateFiler) stored(p util.FullPath) *filer_pb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[p]
}

// peerMount is a mount of the shared filer whose cached root listing does not
// show what other mounts created yet, as before their events arrive.
func peerMount(t *testing.T, fake *exclusiveCreateFiler) *WFS {
	t.Helper()
	wfs, _ := newCreateTestWFS(t)
	startFakeFiler(t, wfs, fake)
	return wfs
}

func createIn(flags uint32) *fuse.CreateIn {
	return &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Flags:    flags,
		Mode:     0o644,
	}
}

func storePeerFile(t *testing.T, fake *exclusiveCreateFiler, name string, size uint64) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.entries[util.FullPath("/").Child(name)] = &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: size, Inode: 777, Crtime: 1, Mtime: 1, Uid: 123, Gid: 456},
	}
}

// A file created at the filer replaces nothing a peer stored there: open
// without O_EXCL opens the peer's file, and mknod fails.
func TestCreateAtFilerKeepsPeerFile(t *testing.T) {
	fake := newExclusiveCreateFiler()
	storePeerFile(t, fake, "shared", 5)

	wfs := peerMount(t, fake)
	wfs.option.EagerFilerCreate = true
	out := &fuse.CreateOut{}
	if st := wfs.Create(make(chan struct{}), createIn(syscall.O_WRONLY|syscall.O_CREAT), "shared", out); st != fuse.OK {
		t.Fatalf("Create = %v, want OK on the peer's file", st)
	}
	if out.Attr.Size != 5 {
		t.Fatalf("opened size = %d, want the peer's 5", out.Attr.Size)
	}
	if got := fake.stored("/shared"); got.GetAttributes().GetFileSize() != 5 {
		t.Fatalf("stored size = %d, want the peer's file kept", got.GetAttributes().GetFileSize())
	}

	storePeerFile(t, fake, "node", 3)
	if st := wfs.Mknod(make(chan struct{}), &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Mode:     syscall.S_IFREG | 0o644,
	}, "node", &fuse.EntryOut{}); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Mknod = %v, want EEXIST", st)
	}
	if got := fake.stored("/node"); got.GetAttributes().GetFileSize() != 3 {
		t.Fatalf("stored size = %d, want the peer's file kept", got.GetAttributes().GetFileSize())
	}
}

// Two mounts open the same new name with O_CREAT|O_EXCL before either closes:
// the first one stores the file at open, so the second is refused there.
func TestExclusiveCreateFailsOnNameAnotherMountCreated(t *testing.T) {
	fake := newExclusiveCreateFiler()
	a, b := peerMount(t, fake), peerMount(t, fake)
	excl := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL)

	outA := &fuse.CreateOut{}
	if st := a.Create(make(chan struct{}), createIn(excl), "lock", outA); st != fuse.OK {
		t.Fatalf("first Create = %v, want OK", st)
	}
	if fake.stored("/lock") == nil {
		t.Fatal("an exclusive create should be stored at open")
	}
	if fh := a.GetHandle(FileHandleId(outA.Fh)); fh == nil || fh.dirtyMetadata || fh.baseEntry.Load() == nil {
		t.Fatal("the stored create should leave a clean handle over the stored entry")
	}
	if st := b.Create(make(chan struct{}), createIn(excl), "lock", &fuse.CreateOut{}); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("second Create = %v, want EEXIST", st)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.creates != 2 {
		t.Fatalf("CreateEntry calls = %d, want 2", fake.creates)
	}
}
