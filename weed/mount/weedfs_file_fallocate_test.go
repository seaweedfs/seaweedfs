package mount

import (
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newOpenFileHandle builds a WFS holding one open handle on a file of the given
// size, the state a descriptor is in after an open and before any write.
func newOpenFileHandle(t *testing.T, fileSize uint64) (*WFS, *FileHandle) {
	t.Helper()

	wfs := newCopyRangeTestWFS()
	path := util.FullPath("/file.txt")
	inode := wfs.inodeToPath.Lookup(path, 1, false, false, 0, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name: "file.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			FileSize: fileSize,
			Inode:    inode,
		},
	}, 0, 0)
	fh.RememberPath(path)

	return wfs, fh
}

// TestFallocateWithinFile covers the posix_fallocate() case that used to fall
// back to a glibc emulation reading through a write-only descriptor: the range
// is already inside the file, so the file must be left alone and answered OK.
func TestFallocateWithinFile(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 10)

	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 1}
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate inside the file: got %v, want OK", status)
	}
	if got := fh.GetEntry().GetEntry().GetAttributes().GetFileSize(); got != 10 {
		t.Fatalf("file size: got %d, want 10", got)
	}
	if fh.dirtyMetadata {
		t.Fatal("Fallocate inside the file marked the handle dirty")
	}
}

func TestFallocateGrowsFile(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 10)

	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 4096, Length: 4096}
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate past the end: got %v, want OK", status)
	}
	if got := fh.GetEntry().GetEntry().GetAttributes().GetFileSize(); got != 8192 {
		t.Fatalf("file size: got %d, want 8192", got)
	}
	if !fh.dirtyMetadata {
		t.Fatal("Fallocate past the end did not mark the handle dirty")
	}
}

func TestFallocateKeepSize(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 10)

	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 4096, Mode: FALLOC_FL_KEEP_SIZE}
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate with FALLOC_FL_KEEP_SIZE: got %v, want OK", status)
	}
	if got := fh.GetEntry().GetEntry().GetAttributes().GetFileSize(); got != 10 {
		t.Fatalf("file size: got %d, want 10", got)
	}
}

// TestFallocateUnsupportedMode pins the status for a mode we cannot honor:
// ENOSYS would make the kernel stop sending FUSE_FALLOCATE altogether.
func TestFallocateUnsupportedMode(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 10)

	const punchHole = 0x02
	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 4, Mode: punchHole | FALLOC_FL_KEEP_SIZE}
	if status := wfs.Fallocate(nil, in); status != fuse.ENOTSUP {
		t.Fatalf("Fallocate with FALLOC_FL_PUNCH_HOLE: got %v, want ENOTSUP", status)
	}
}

func TestFallocateUnknownHandle(t *testing.T) {
	wfs, _ := newOpenFileHandle(t, 10)

	in := &fuse.FallocateIn{Fh: 12345, Offset: 0, Length: 1}
	if status := wfs.Fallocate(nil, in); status != fuse.EBADF {
		t.Fatalf("Fallocate on an unknown handle: got %v, want EBADF", status)
	}
}

// TestFallocateNoOpIgnoresQuotaAndWorm covers the two guards a request that
// allocates nothing must not trip: it reserves no space and rewrites no entry.
func TestFallocateNoOpIgnoresQuotaAndWorm(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 10)
	wfs.option.Quota = 1
	wfs.IsOverQuota = true
	wfs.FilerConf = filer.NewFilerConf()
	if err := wfs.FilerConf.AddLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/",
		Worm:           proto.Bool(true),
	}); err != nil {
		t.Fatalf("AddLocationConf: %v", err)
	}
	fh.GetEntry().GetEntry().WormEnforcedAtTsNs = time.Now().UnixNano()

	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 1}
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate inside the file: got %v, want OK", status)
	}

	in = &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 4096}
	if status := wfs.Fallocate(nil, in); status != fuse.Status(syscall.ENOSPC) {
		t.Fatalf("Fallocate past the end over quota: got %v, want ENOSPC", status)
	}

	wfs.option.Quota = 0
	wfs.IsOverQuota = false
	if status := wfs.Fallocate(nil, in); status != fuse.EPERM {
		t.Fatalf("Fallocate past the end under worm: got %v, want EPERM", status)
	}
}

// TestFallocateChargesTheGrowth pins the quota accounting: the writes that fill
// a pre-extended range do not grow the file, so they charge nothing and the
// growth has to be counted where it happens.
func TestFallocateChargesTheGrowth(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)
	wfs, fh := newOpenFileHandle(t, 10)
	wfs.option.Quota = 100 << 20

	in := &fuse.FallocateIn{Fh: uint64(fh.fh), Offset: 0, Length: 8192}
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate past the end: got %v, want OK", status)
	}
	if got := wfs.GetUncommittedBytes(); got != 8192-10 {
		t.Fatalf("uncommitted bytes: got %d, want %d", got, 8192-10)
	}

	// A second request inside the now-larger file allocates nothing more.
	if status := wfs.Fallocate(nil, in); status != fuse.OK {
		t.Fatalf("Fallocate inside the file: got %v, want OK", status)
	}
	if got := wfs.GetUncommittedBytes(); got != 8192-10 {
		t.Fatalf("uncommitted bytes after a no-op: got %d, want %d", got, 8192-10)
	}
}
