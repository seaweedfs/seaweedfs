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
