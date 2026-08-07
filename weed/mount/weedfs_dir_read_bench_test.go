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

const benchDirEntryCount = 200000

// benchSink drains a readdir the way a front end does. sinkLimit is how many
// entries one round accepts, standing in for the kernel reply buffer or the
// WinFsp adapter's batch.
type benchSink struct {
	plus      bool
	takesRef  bool
	sinkLimit int
	count     int
	inodes    []uint64
	lastOff   uint64
	attrs     []fuse.EntryOut
}

func (s *benchSink) reset() {
	s.count = 0
	s.inodes = s.inodes[:0]
	s.attrs = s.attrs[:0]
}

func (s *benchSink) AddEntry(entry fuse.DirEntry) bool {
	if s.count >= s.sinkLimit {
		return false
	}
	s.count++
	s.lastOff = entry.Off
	s.inodes = append(s.inodes, entry.Ino)
	return true
}

func (s *benchSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	if s.count >= s.sinkLimit {
		return nil
	}
	s.count++
	s.lastOff = entry.Off
	s.inodes = append(s.inodes, entry.Ino)
	s.attrs = append(s.attrs, fuse.EntryOut{})
	return &s.attrs[len(s.attrs)-1]
}

func (s *benchSink) TakesLookupRef() bool { return s.takesRef }

func inodeTableSize(i *InodeToPath) int {
	i.RLock()
	defer i.RUnlock()
	return len(i.inode2path)
}

func newBenchWFS(tb testing.TB, dir util.FullPath, n int) *WFS {
	tb.Helper()

	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		tb.Fatalf("uid/gid mapper: %v", err)
	}

	root := util.FullPath("/")
	option := &Option{
		ChunkSizeLimit:     1024,
		ConcurrentReaders:  1,
		VolumeServerAccess: "filerProxy",
		FilerAddresses:     []pb.ServerAddress{pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1)},
		GrpcDialOption:     grpc.WithTransportCredentials(insecure.NewCredentials()),
		FilerMountRootPath: "/",
		MountUid:           99,
		MountGid:           100,
		MountMode:          0o777,
		MountMtime:         time.Now(),
		MountCtime:         time.Now(),
		UidGidMapper:       uidGidMapper,
	}

	wfs := &WFS{
		option:            option,
		signature:         1,
		inodeToPath:       NewInodeToPath(root, 0),
		fhMap:             NewFileHandleToInode(),
		dhMap:             NewDirectoryHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		hardLinkLockTable: util.NewLockTable[string](),
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(tb.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		func(meta_cache.EntryInvalidation) {},
		nil,
	)
	tb.Cleanup(wfs.metaCache.Shutdown)

	now := time.Now()
	ctx := context.Background()
	if err := wfs.metaCache.InsertEntry(ctx, &filer.Entry{
		FullPath: dir,
		Attr:     filer.Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now, Uid: 99, Gid: 100},
	}, 0); err != nil {
		tb.Fatalf("insert dir: %v", err)
	}
	for i := 0; i < n; i++ {
		child := dir.Child(fmt.Sprintf("image-%08d.jpg", i))
		if err := wfs.metaCache.InsertEntry(ctx, &filer.Entry{
			FullPath: child,
			// The filer stamps an inode on every entry it stores, so a listing
			// arrives with one and never has to derive its own.
			Attr: filer.Attr{Mode: 0o644, Mtime: now, Crtime: now, Uid: 99, Gid: 100, FileSize: 4 << 20, Inode: child.AsInode(now.Unix())},
			// A real file has chunks, and building them is most of what decoding
			// an entry costs.
			Chunks: []*filer_pb.FileChunk{{
				FileId: "3,01637037d6", Size: 4 << 20, ModifiedTsNs: now.UnixNano(),
				ETag: "1a2b3c4d5e6f7890",
				Fid:  &filer_pb.FileId{VolumeId: 3, FileKey: uint64(i), Cookie: 0x1637037d},
			}},
		}, 0); err != nil {
			tb.Fatalf("insert entry %d: %v", i, err)
		}
	}

	// Mark the tree cached so the listing is served from the meta cache and no
	// filer client is dialled.
	wfs.inodeToPath.MarkChildrenCached(root)
	wfs.inodeToPath.Lookup(dir, now.Unix(), true, false, 0, true)
	wfs.inodeToPath.MarkChildrenCached(dir)

	return wfs
}

// walkOnce enumerates the whole directory the way a front end does: repeated
// rounds against one handle until the listing runs dry, returning whatever
// references the round took.
func walkOnce(tb testing.TB, wfs *WFS, dirInode uint64, sink *benchSink, forgets bool) int {
	dhid, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)

	total := 0
	offset := uint64(0)
	for {
		sink.reset()
		status := wfs.doReadDirectory(&fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirInode},
			Fh:       uint64(dhid),
			Offset:   offset,
			Size:     1 << 20,
		}, sink, sink.plus)
		if status != fuse.OK {
			tb.Fatalf("readdir: %v", status)
		}
		if sink.count == 0 {
			return total
		}
		total += sink.count
		if forgets {
			// HasInode keeps this to the entries that really hold a reference,
			// so a listing that took none is not charged for a bogus Forget.
			for _, ino := range sink.inodes {
				if wfs.inodeToPath.HasInode(ino) {
					wfs.Forget(ino, 1)
				}
			}
		}
		if sink.lastOff <= offset {
			return total
		}
		offset = sink.lastOff
	}
}

var benchCases = []struct {
	name string
	plus bool
	// ref mirrors the sink's TakesLookupRef. The kernel's reports true whatever
	// the mode, exactly as fuseDirEntryList does; it is the mode that decides
	// whether a reference is actually granted.
	ref bool
	// forgets is what the front end does after a round: the kernel returns one
	// FORGET per readdirplus entry and none for a plain readdir, and the WinFsp
	// adapter hands back everything a round gave it.
	forgets bool
}{
	{"kernel_readdir", false, true, false},
	{"kernel_readdirplus", true, true, true},
	{"winfsp_readdirplus", true, false, true},
}

func BenchmarkReadDirectory(b *testing.B) {
	dir := util.FullPath("/images")
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			wfs := newBenchWFS(b, dir, benchDirEntryCount)
			dirInode, _ := wfs.inodeToPath.GetInode(dir)
			sink := &benchSink{plus: tc.plus, takesRef: tc.ref, sinkLimit: 4096}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := walkOnce(b, wfs, dirInode, sink, tc.forgets); got != benchDirEntryCount+2 {
					b.Fatalf("listed %d entries, want %d", got, benchDirEntryCount+2)
				}
			}
			b.StopTimer()
			// What the listing left in the inode table, over the root and the
			// directory itself.
			b.ReportMetric(float64(inodeTableSize(wfs.inodeToPath)), "inodes_left")
		})
	}
}
