//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const inlineTestContent = "0123456789"

// openInlineFile opens /dir/file, which the filer keeps inside its entry, on a
// mount of a filer that stores what the mount sends it.
func openInlineFile(t *testing.T) (*WFS, uint64, *g6ConditionalPeer) {
	t.Helper()
	base := &filer_pb.Entry{
		Name:    "file",
		Content: []byte(inlineTestContent),
		Attributes: &filer_pb.FuseAttributes{
			Inode: 42, Uid: 2000, Gid: 20, FileMode: 0644, FileSize: uint64(len(inlineTestContent)), Mtime: 10,
		},
		CommitTsNs: 100,
	}
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry), blobs: make(map[string][]byte)}

	w := newInvalidateTestWFS(t)
	w.option.CacheDirForWrite = t.TempDir()
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
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_RDWR}, out); code != fuse.OK {
		t.Fatalf("open: %v", code)
	}
	g6AppendUploader(t, w.GetHandle(FileHandleId(out.Fh)), 100, peer)
	return w, out.Fh, peer
}

// storedInlineFile flushes the handle and returns the file as the filer has
// it then: its content while that is all of it, else its chunks, the later
// over the earlier.
func storedInlineFile(t *testing.T, w *WFS, handle uint64, peer *g6ConditionalPeer) []byte {
	t.Helper()
	if code := w.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code != fuse.OK {
		t.Fatalf("fsync: %v", code)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	entry := peer.entry
	size := entry.GetAttributes().GetFileSize()
	if len(entry.Content) > 0 {
		if len(entry.Chunks) > 0 || uint64(len(entry.Content)) != size {
			t.Fatalf("stored with %d bytes of content next to %d chunks and a size of %d", len(entry.Content), len(entry.Chunks), size)
		}
		return entry.Content
	}
	chunks := append([]*filer_pb.FileChunk(nil), entry.Chunks...)
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].ModifiedTsNs < chunks[j].ModifiedTsNs })
	stored := make([]byte, size)
	for _, c := range chunks {
		body, ok := peer.blobs[c.GetFileIdString()]
		if !ok || uint64(len(body)) < c.Size || c.Offset < 0 || uint64(c.Offset)+c.Size > size {
			t.Fatalf("stored chunk %v of a file of %d bytes has %d bytes behind it", c, size, len(body))
		}
		copy(stored[c.Offset:], body[:c.Size])
	}
	return stored
}

func writeInlineFile(t *testing.T, w *WFS, handle, offset uint64, data string) {
	t.Helper()
	n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: offset, Size: uint32(len(data))}, []byte(data))
	if code != fuse.OK || n != uint32(len(data)) {
		t.Fatalf("write %q at %d: %d %v", data, offset, n, code)
	}
}

func resizeInlineFile(t *testing.T, w *WFS, size uint64) {
	t.Helper()
	in := &fuse.SetAttrIn{}
	in.NodeId, in.Valid, in.Size = 42, fuse.FATTR_SIZE, size
	if code := w.SetAttr(nil, in, &fuse.AttrOut{}); code != fuse.OK {
		t.Fatalf("set size %d: %v", size, code)
	}
}

// A write to a file whose bytes are inside its entry used to drop them all:
// the entry is published with the write's chunk and without its content, so
// what the write did not cover read as zeros.
func TestWriteToInlineFileKeepsTheBytesItLeavesAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset uint64
		data   string
		want   string
	}{
		{"an append", 10, "XY", "0123456789XY"},
		{"in the middle", 3, "AB", "012AB56789"},
		{"at the start", 0, "Z", "Z123456789"},
		{"past the end", 12, "Q", "0123456789\x00\x00Q"},
		{"all of it", 0, "abcdefghijkl", "abcdefghijkl"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, handle, peer := openInlineFile(t)
			writeInlineFile(t, w, handle, tc.offset, tc.data)

			// read back through the handle before anything is stored
			buff := make([]byte, len(tc.want))
			if n, err := readDataByFileHandle(buff, w.GetHandle(FileHandleId(handle)), 0); err != nil || string(buff[:n]) != tc.want {
				t.Fatalf("the handle reads %q (%v), want %q", buff[:n], err, tc.want)
			}
			if got := storedInlineFile(t, w, handle, peer); string(got) != tc.want {
				t.Fatalf("stored %q, want %q", got, tc.want)
			}
		})
	}
}

// Content shorter than the file reads no further than itself, on the mount
// and through the filer, so a file that grows has it stored as a chunk.
func TestFallocateOfInlineFileKeepsItsContent(t *testing.T) {
	w, handle, peer := openInlineFile(t)
	if code := w.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 0, Length: 16}); code != fuse.OK {
		t.Fatalf("fallocate: %v", code)
	}
	want := inlineTestContent + "\x00\x00\x00\x00\x00\x00"
	if got := storedInlineFile(t, w, handle, peer); string(got) != want {
		t.Fatalf("stored %q, want %q", got, want)
	}
}

// A size below the content cuts it. Left as it was, the bytes cut off came
// back when the file grew again.
func TestShrinkingAnInlineFileCutsItsContent(t *testing.T) {
	w, handle, peer := openInlineFile(t)
	resizeInlineFile(t, w, 4)
	if got := storedInlineFile(t, w, handle, peer); string(got) != "0123" {
		t.Fatalf("stored %q, want %q", got, "0123")
	}

	writeInlineFile(t, w, handle, 8, "W")
	if got, want := storedInlineFile(t, w, handle, peer), "0123\x00\x00\x00\x00W"; string(got) != want {
		t.Fatalf("stored %q after a write past the cut, want %q", got, want)
	}

	resizeInlineFile(t, w, 0)
	if got := storedInlineFile(t, w, handle, peer); len(got) != 0 {
		t.Fatalf("stored %q after a cut to nothing", got)
	}
}

// The bytes a copy does not reach stay, in a destination kept inside its entry.
func TestCopyFileRangeIntoInlineFileKeepsItsContent(t *testing.T) {
	wfs := newCopyRangeTestWFS()
	open := func(name, content string) *FileHandle {
		path := util.FullPath("/" + name)
		inode := wfs.inodeToPath.Lookup(path, 1, false, false, 0, true)
		fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
			Name:       name,
			Content:    []byte(content),
			Attributes: &filer_pb.FuseAttributes{FileMode: 0100644, FileSize: uint64(len(content)), Inode: inode},
		}, 0, 0)
		fh.RememberPath(path)
		t.Cleanup(fh.dirtyPages.Destroy)
		return fh
	}
	src, dst := open("src", "abcdef"), open("dst", inlineTestContent)

	written, code := wfs.CopyFileRange(nil, &fuse.CopyFileRangeIn{FhIn: uint64(src.fh), FhOut: uint64(dst.fh), OffIn: 1, OffOut: 4, Len: 3})
	if code != fuse.OK || written != 3 {
		t.Fatalf("copy: %d %v", written, code)
	}
	buff := make([]byte, len(inlineTestContent))
	if n, err := readDataByFileHandle(buff, dst, 0); err != nil || string(buff[:n]) != "0123bcd789" {
		t.Fatalf("the destination reads %q (%v), want %q", buff[:n], err, "0123bcd789")
	}
	if content := dst.GetEntry().GetEntry().GetContent(); len(content) != 0 {
		t.Fatalf("the destination's entry still has %q inside it", content)
	}
}

func TestResizeInlineContent(t *testing.T) {
	entry := func(content string, size uint64) *filer_pb.Entry {
		return &filer_pb.Entry{Name: "file", Content: []byte(content), Attributes: &filer_pb.FuseAttributes{FileSize: size}}
	}
	var saved []string
	save := func(r io.Reader, _ string, offset, tsNs int64, expected uint64) (*filer_pb.FileChunk, error) {
		body, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		if uint64(len(body)) != expected {
			t.Fatalf("saving %d bytes announced as %d", len(body), expected)
		}
		saved = append(saved, string(body))
		return &filer_pb.FileChunk{FileId: "3,01", Offset: offset, Size: uint64(len(body)), ModifiedTsNs: tsNs}, nil
	}
	for _, tc := range []struct {
		name        string
		entry       *filer_pb.Entry
		size        uint64
		wantContent string
		wantSaved   []string
	}{
		{"cut", entry(inlineTestContent, 10), 4, "0123", nil},
		{"cut to nothing", entry(inlineTestContent, 10), 0, "", nil},
		{"the same size", entry(inlineTestContent, 10), 10, inlineTestContent, nil},
		{"grown", entry(inlineTestContent, 10), 16, "", []string{inlineTestContent}},
		// an older mount cut the size and left the content
		{"grown after an old cut", entry(inlineTestContent, 4), 16, "", []string{"0123"}},
		{"the size of the content after an old cut", entry(inlineTestContent, 4), 10, "", []string{"0123"}},
		{"grown from nothing", entry(inlineTestContent, 0), 16, "", nil},
		{"no content", entry("", 10), 16, "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved = nil
			chunked, err := resizeInlineContent(tc.entry, tc.size, 7, save)
			if err != nil {
				t.Fatal(err)
			}
			if string(tc.entry.Content) != tc.wantContent {
				t.Fatalf("content %q, want %q", tc.entry.Content, tc.wantContent)
			}
			if len(saved) != len(tc.wantSaved) || len(saved) == 1 && saved[0] != tc.wantSaved[0] {
				t.Fatalf("saved %q as chunks, want %q", saved, tc.wantSaved)
			}
			if chunked != (len(tc.wantSaved) > 0) || len(tc.entry.Chunks) != len(tc.wantSaved) {
				t.Fatalf("chunked=%v with %d chunks, want %d", chunked, len(tc.entry.Chunks), len(tc.wantSaved))
			}
			if chunked && (tc.entry.Chunks[0].Offset != 0 || tc.entry.Chunks[0].ModifiedTsNs != 7) {
				t.Fatalf("chunk %v, want it at 0 with the time given", tc.entry.Chunks[0])
			}
		})
	}

	t.Run("not saved", func(t *testing.T) {
		e := entry(inlineTestContent, 10)
		refused := errors.New("no volume to write to")
		_, err := resizeInlineContent(e, 16, 7, func(io.Reader, string, int64, int64, uint64) (*filer_pb.FileChunk, error) {
			return nil, refused
		})
		if !errors.Is(err, refused) {
			t.Fatalf("err %v, want the save's", err)
		}
		if !bytes.Equal(e.Content, []byte(inlineTestContent)) || len(e.Chunks) != 0 {
			t.Fatalf("content %q and %d chunks after a save that failed", e.Content, len(e.Chunks))
		}
	})
}
