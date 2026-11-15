//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// A write the swap file cannot buffer fails, and must not grow the file: the
// next flush would publish that size with a hole where its data should be. A
// write that buffers only its first chunk is a short write of those bytes, or
// an error for kernel writeback, which would drop the rest silently.
func TestFailedWriteDoesNotGrowFile(t *testing.T) {
	t.Run("short write", func(t *testing.T) { testFailedWriteDoesNotGrowFile(t, 0) })
	t.Run("kernel writeback", func(t *testing.T) { testFailedWriteDoesNotGrowFile(t, fuse.WRITE_CACHE) })
}

func testFailedWriteDoesNotGrowFile(t *testing.T, writeFlags uint32) {
	base := attributeAppendEntry(1)
	base.CommitTsNs = 100
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	peer.blobs = map[string][]byte{base.Chunks[0].GetFileIdString(): bytes.Repeat([]byte{'O'}, 256)}

	w := newInvalidateTestWFS(t)
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0600); err != nil {
		t.Fatal(err)
	}
	w.option.uniqueCacheDirForWrite = filepath.Join(notADir, "swap")
	w.option.DefaultPermissions = true
	w.posixLocks = NewPosixLockTable()
	w.option.ConcurrentWriters = 1
	w.concurrentWriters = util.NewLimitedConcurrentExecutor(1)
	startFakeFiler(t, w, peer)
	w.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
	w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
	w.inodeToPath.MarkChildrenCached("/dir")
	if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", proto.Clone(base).(*filer_pb.Entry)), 1); err != nil {
		t.Fatal(err)
	}
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_WRONLY}, out); code != fuse.OK {
		t.Fatal(code)
	}
	fh := w.GetHandle(FileHandleId(out.Fh))
	g6AppendUploader(t, fh, 100, peer)

	write := func(offset uint64, size int, value byte) (uint32, fuse.Status) {
		return w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh, Offset: offset, Size: uint32(size), WriteFlags: writeFlags}, bytes.Repeat([]byte{value}, size))
	}
	if _, code := write(256, 256, 'A'); code != fuse.OK {
		t.Fatalf("buffered write: %v", code)
	}
	// The only writable chunk slot is taken, so this write needs the swap file.
	if _, code := write(4096, 256, 'B'); code == fuse.OK {
		t.Fatal("write through an unusable swap directory succeeded")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 512 {
		t.Fatalf("file size after the failed write = %d, want 512", size)
	}
	// The first 24 bytes fit the existing chunk; the next chunk needs the swap file.
	n, code := write(1000, 48, 'C')
	if writeFlags == 0 && (code != fuse.OK || n != 24) {
		t.Fatalf("boundary-crossing write = %d, %v; want a short write of 24", n, code)
	}
	if writeFlags != 0 && code == fuse.OK {
		t.Fatalf("boundary-crossing writeback = %d, OK; want an error", n)
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 1024 {
		t.Fatalf("file size after the short write = %d, want 1024", size)
	}
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: out.Fh}); code != fuse.OK {
		t.Fatal(code)
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
	want = append(want, make([]byte, 1000-512)...)
	want = append(want, bytes.Repeat([]byte{'C'}, 24)...)
	if got := g6StoredBytes(t, peer); peer.entry.Attributes.FileSize != 1024 || !bytes.Equal(got, want) {
		t.Fatalf("stored size %d, want 1024 with the original and buffered bytes", peer.entry.Attributes.FileSize)
	}
}
