package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A chunk merge rewrites the whole file into new chunks, and the filer deletes
// the chunks it replaced once the merge is committed. A handle that stays open
// across the merge must read the new chunks: after a vacuum the replaced ones
// are gone, and a read view still naming them sends reads to missing needles.
func TestMergeMovesReadViewOffReplacedChunks(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)

	chunk := func(volumeId uint32, key uint64, offset int64, size int, ts int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: volumeId, FileKey: key, Cookie: 1}, Offset: offset, Size: uint64(size), ModifiedTsNs: ts}
	}
	// A bloated file: b rewrote the tail of a.
	a := chunk(1, 1, 0, 8, 1)
	b := chunk(1, 2, 4, 8, 2)
	// The merge's rewrite, stamped as the oldest data it holds.
	merged := chunk(2, 1, 0, 12, 1)
	// A write that was attached while the merge ran.
	late := chunk(3, 1, 10, 4, 3)

	entry := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, FileSize: 12}, Chunks: []*filer_pb.FileChunk{a, b}}
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, entry, 0, 0)
	snapshot := fh.GetEntry().GetChunks()

	// Only the merge's chunk and the late write are still stored: a and b went
	// with the merge's commit and a vacuum.
	cache := newFakeChunkCache()
	cache.Put(merged.GetFileIdString(), []byte("aaaabbbbbbbb"))
	cache.Put(late.GetFileIdString(), []byte("dddd"))
	group, err := filer.NewChunkGroup(func(ctx context.Context, fileId string) ([]string, error) {
		return nil, errors.New("needle " + fileId + " was vacuumed")
	}, cache, []*filer_pb.FileChunk{a, b}, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fh.entryChunkGroup = group
	fh.AddChunks([]*filer_pb.FileChunk{late})
	if err := fh.entryChunkGroup.AddChunk(late); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 14)
	if _, _, err := fh.entryChunkGroup.ReadDataAt(context.Background(), int64(len(buf)), buf, 0); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("a read through the replaced chunks succeeded; the test would not notice a stale read view")
	}

	fh.installRewrittenChunks(snapshot, []*filer_pb.FileChunk{merged})

	var got []string
	for _, c := range fh.GetEntry().GetChunks() {
		got = append(got, c.GetFileIdString())
	}
	if want := []string{merged.GetFileIdString(), late.GetFileIdString()}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks after the merge: %v, want the rewrite and the late write %v", got, want)
	}

	n, _, err := fh.entryChunkGroup.ReadDataAt(context.Background(), int64(len(buf)), buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read through the open handle after the merge: %v", err)
	}
	if got := string(buf[:n]); got != "aaaabbbbbbdddd" {
		t.Fatalf("read %q after the merge, want %q", got, "aaaabbbbbbdddd")
	}
}

// An upload that fails while a flush drains the pages before a merge is a
// write error: it maps the way doFlush maps its own FlushData failure, and the
// metadata flush does not retry it as a filer error.
func TestPageUploadErrorIsAWriteError(t *testing.T) {
	full := &pageUploadError{err: fmt.Errorf("flush data: %w", errors.New("assign volume: no free volumes left"))}
	if got := flushErrorToFuseStatus(fmt.Errorf("flush /dir/file: %w", full)); got != fuse.Status(syscall.ENOSPC) {
		t.Fatalf("a full cluster during the drain maps to %v, want ENOSPC", got)
	}
	if !isPageUploadError(fmt.Errorf("flush /dir/file: %w", full)) {
		t.Fatal("a wrapped upload failure is not recognized, so the metadata flush would retry it")
	}
	if got := flushErrorToFuseStatus(errors.New("fh flush create /dir/file: refused")); got != fuse.EIO {
		t.Fatalf("other flush failures map to %v, want EIO as before", got)
	}
}
