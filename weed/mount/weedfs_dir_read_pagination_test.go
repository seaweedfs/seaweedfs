package mount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// pagingSink collects names, accepting at most limit per round the way a kernel
// reply buffer does.
type pagingSink struct {
	limit   int
	names   []string
	lastOff uint64
	round   int
}

func (s *pagingSink) AddEntry(entry fuse.DirEntry) bool {
	if s.round >= s.limit {
		return false
	}
	s.round++
	s.names = append(s.names, entry.Name)
	s.lastOff = entry.Off
	return true
}

func (s *pagingSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut { return nil }
func (s *pagingSink) TakesLookupRef() bool                            { return true }

func newPagingWFS(tb testing.TB, dir util.FullPath, names []string, ttlSec int32) *WFS {
	tb.Helper()
	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		tb.Fatalf("uid/gid mapper: %v", err)
	}
	root := util.FullPath("/")
	wfs := &WFS{
		option: &Option{
			ChunkSizeLimit: 1024, ConcurrentReaders: 1, VolumeServerAccess: "filerProxy",
			FilerAddresses:     []pb.ServerAddress{pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1)},
			GrpcDialOption:     grpc.WithTransportCredentials(insecure.NewCredentials()),
			FilerMountRootPath: "/", MountUid: 99, MountGid: 100, MountMode: 0o777,
			MountMtime: time.Now(), MountCtime: time.Now(), UidGidMapper: uidGidMapper,
		},
		signature: 1, inodeToPath: NewInodeToPath(root, 0),
		fhMap: NewFileHandleToInode(), dhMap: NewDirectoryHandleToInode(),
		fhLockTable: util.NewLockTable[FileHandleId](), hardLinkLockTable: util.NewLockTable[string](),
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(tb.TempDir(), "meta"), uidGidMapper, root, false,
		func(p util.FullPath) { wfs.inodeToPath.MarkChildrenCached(p) },
		func(p util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(p) },
		func(meta_cache.EntryInvalidation) {}, nil,
	)
	tb.Cleanup(wfs.metaCache.Shutdown)

	now := time.Now()
	ctx := context.Background()
	if err := wfs.metaCache.InsertEntry(ctx, &filer.Entry{
		FullPath: dir,
		Attr:     filer.Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now},
	}, 0); err != nil {
		tb.Fatalf("insert dir: %v", err)
	}
	for _, name := range names {
		child := dir.Child(name)
		attr := filer.Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 1, Inode: child.AsInode(now.Unix())}
		if ttlSec > 0 {
			// Expired well in the past, so the meta cache drops it after the
			// store has already counted it against the limit.
			attr.TtlSec = ttlSec
			attr.Crtime = now.Add(-time.Duration(ttlSec+60) * time.Second)
		}
		if err := wfs.metaCache.InsertEntry(ctx, &filer.Entry{FullPath: child, Attr: attr}, 0); err != nil {
			tb.Fatalf("insert %s: %v", name, err)
		}
	}
	wfs.inodeToPath.MarkChildrenCached(root)
	wfs.inodeToPath.Lookup(dir, now.Unix(), true, false, 0, true)
	wfs.inodeToPath.MarkChildrenCached(dir)
	return wfs
}

// TestReadDirResumePastShrunkDirectory covers a client resuming a fresh handle
// at a cookie from an earlier, larger listing. The directory has since shrunk
// past that offset, and the readdir used to fall back to listing from the first
// child again, handing back names the client had already consumed.
func TestReadDirResumePastShrunkDirectory(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := 0; i < 100; i++ {
		names = append(names, fmt.Sprintf("f%03d", i))
	}
	wfs := newPagingWFS(t, dir, names, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)

	dhid, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)

	sink := &pagingSink{limit: 4096}
	status := wfs.doReadDirectory(&fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirInode},
		Fh:       uint64(dhid),
		Offset:   152, // past the end of a directory that now holds 100
		Size:     1 << 20,
	}, sink, false)
	if status != fuse.OK {
		t.Fatalf("readdir: %v", status)
	}
	if len(sink.names) != 0 {
		t.Errorf("resuming past the end returned %d entries (first %q), want none", len(sink.names), sink.names[0])
	}
}

// TestReadDirWithExpiredEntries covers a batch the store fills to the limit but
// whose entries the meta cache then drops as expired. The post-filter count used
// to be read as end-of-directory, silently hiding every later child.
func TestReadDirWithExpiredEntries(t *testing.T) {
	dir := util.FullPath("/d")
	// One full store batch of entries that all expire, then live ones behind
	// them. Nothing survives the first batch, which is the case that latched.
	var names []string
	for i := 0; i < batchSize; i++ {
		names = append(names, fmt.Sprintf("a%05d", i))
	}
	wfs := newPagingWFS(t, dir, names, 30)

	live := []string{"z001", "z002", "z003"}
	now := time.Now()
	for _, name := range live {
		child := dir.Child(name)
		if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
			FullPath: child,
			Attr:     filer.Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 1, Inode: child.AsInode(now.Unix())},
		}, 0); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	dhid, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)

	sink := &pagingSink{limit: 4096}
	var offset uint64
	for round := 0; round < 20; round++ {
		sink.round = 0
		status := wfs.doReadDirectory(&fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirInode},
			Fh:       uint64(dhid),
			Offset:   offset,
			Size:     1 << 20,
		}, sink, false)
		if status != fuse.OK {
			t.Fatalf("readdir: %v", status)
		}
		if sink.lastOff <= offset {
			break
		}
		offset = sink.lastOff
	}

	var seen []string
	for _, n := range sink.names {
		if n != "." && n != ".." {
			seen = append(seen, n)
		}
	}
	if len(seen) != len(live) {
		t.Fatalf("listed %d entries %v, want the %d live ones %v", len(seen), seen, len(live), live)
	}
}

// TestReadDirTrimsConsumedEntries checks that a walk does not accumulate the
// whole directory in the handle. Offsets index into the stream from
// entryStreamOffset, so the two have to advance together or the listing
// silently misaligns.
func TestReadDirTrimsConsumedEntries(t *testing.T) {
	dir := util.FullPath("/d")
	const total = 5000
	var names []string
	for i := 0; i < total; i++ {
		names = append(names, fmt.Sprintf("f%05d", i))
	}
	wfs := newPagingWFS(t, dir, names, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)

	dhid, dh := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)

	// A small sink forces many rounds, which is when the stream would grow.
	sink := &pagingSink{limit: 64}
	var offset uint64
	var seen []string
	peak := 0
	for round := 0; round < 500; round++ {
		sink.round = 0
		before := len(sink.names)
		status := wfs.doReadDirectory(&fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirInode},
			Fh:       uint64(dhid),
			Offset:   offset,
			Size:     1 << 20,
		}, sink, false)
		if status != fuse.OK {
			t.Fatalf("readdir: %v", status)
		}
		if n := len(dh.entryStream); n > peak {
			peak = n
		}
		if len(sink.names) == before || sink.lastOff <= offset {
			break
		}
		offset = sink.lastOff
	}
	for _, n := range sink.names {
		if n != "." && n != ".." {
			seen = append(seen, n)
		}
	}

	if len(seen) != total {
		t.Fatalf("listed %d entries, want %d", len(seen), total)
	}
	for i, name := range seen {
		if want := fmt.Sprintf("f%05d", i); name != want {
			t.Fatalf("entry %d is %q, want %q -- trimming misaligned the offsets", i, name, want)
		}
	}
	// Without trimming the handle ends up holding every entry.
	if peak >= total {
		t.Errorf("handle held %d entries at peak, want well under the %d in the directory", peak, total)
	}
}

// listingFilerServer serves a fixed, sorted child list the way the filer
// paginates one.
type listingFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	names []string
}

func (s *listingFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	var sent uint32
	for _, name := range s.names {
		if name < req.StartFromFileName || (name == req.StartFromFileName && !req.InclusiveStartFrom) {
			continue
		}
		if req.Limit > 0 && sent >= req.Limit {
			break
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 1},
		}}); err != nil {
			return err
		}
		sent++
	}
	return nil
}

// TestReadDirDirectTrimsConsumedEntries is the same check for a directory read
// through to the filer. That path is taken precisely for directories too big to
// cache, so holding the whole walk in the handle is where a listing turns into
// gigabytes of resident memory.
func TestReadDirDirectTrimsConsumedEntries(t *testing.T) {
	dir := util.FullPath("/d")
	const total = 5000
	var names []string
	for i := 0; i < total; i++ {
		names = append(names, fmt.Sprintf("f%05d", i))
	}
	wfs := newPagingWFS(t, dir, nil, 0)
	startFakeFiler(t, wfs, &listingFilerServer{names: names})
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	if !wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now()) {
		t.Fatal("directory did not enter read-through mode")
	}

	dhid, dh := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)

	sink := &pagingSink{limit: 64}
	var offset uint64
	var seen []string
	peak := 0
	for round := 0; round < 500; round++ {
		sink.round = 0
		before := len(sink.names)
		status := wfs.doReadDirectory(&fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirInode},
			Fh:       uint64(dhid),
			Offset:   offset,
			Size:     1 << 20,
		}, sink, false)
		if status != fuse.OK {
			t.Fatalf("readdir: %v", status)
		}
		if n := len(dh.entryStream); n > peak {
			peak = n
		}
		if len(sink.names) == before || sink.lastOff <= offset {
			break
		}
		offset = sink.lastOff
	}
	for _, n := range sink.names {
		if n != "." && n != ".." {
			seen = append(seen, n)
		}
	}

	if len(seen) != total {
		t.Fatalf("listed %d entries, want %d", len(seen), total)
	}
	for i, name := range seen {
		if want := fmt.Sprintf("f%05d", i); name != want {
			t.Fatalf("entry %d is %q, want %q -- trimming misaligned the offsets", i, name, want)
		}
	}
	if peak >= total {
		t.Errorf("handle held %d entries at peak, want well under the %d in the directory", peak, total)
	}
}
