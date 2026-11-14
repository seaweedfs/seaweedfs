//go:build !freebsd

package mount

import (
	"bytes"
	"io"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// A FLUSH from another fd of the open file uploads its pages, then a WRITE
// lands before it commits. The commit carries the new size but not the
// write's chunk, so the next flush must still publish that chunk.
func TestFlushRacingWriteKeepsHandleDirty(t *testing.T) {
	base := attributeAppendEntry(1)
	base.CommitTsNs = 100
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 100)
	fh := w.GetHandle(FileHandleId(handle))
	write := func(offset uint64, value byte) {
		t.Helper()
		n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: offset, Size: 256}, bytes.Repeat([]byte{value}, 256))
		if n != 256 || code != fuse.OK {
			t.Fatalf("write at %d: %d %v", offset, n, code)
		}
	}
	flush := func() fuse.Status {
		return w.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle})
	}

	uploading, resume := make(chan struct{}), make(chan struct{})
	upload := fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk
	first := true
	fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk = func(p util.FullPath) filer.SaveDataAsChunkFunctionType {
		save := upload(p)
		return func(r io.Reader, name string, offset, ts int64, ttl uint64) (*filer_pb.FileChunk, error) {
			if first {
				first = false
				close(uploading)
				<-resume
			}
			return save(r, name, offset, ts, ttl)
		}
	}

	write(256, 'A')
	racing := make(chan fuse.Status, 1)
	go func() { racing <- flush() }()
	<-uploading
	write(512, 'B')
	close(resume)
	if code := <-racing; code != fuse.OK {
		t.Fatalf("racing flush: %v", code)
	}
	if code := flush(); code != fuse.OK {
		t.Fatalf("owner flush: %v", code)
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()
	want := append(bytes.Repeat([]byte{'O'}, 256), bytes.Repeat([]byte{'A'}, 256)...)
	want = append(want, bytes.Repeat([]byte{'B'}, 256)...)
	if got := g6StoredBytes(t, peer); !bytes.Equal(got, want) {
		t.Fatalf("stored size %d chunks %v: the racing write's bytes were not published", peer.entry.Attributes.FileSize, peer.entry.Chunks)
	}
}
