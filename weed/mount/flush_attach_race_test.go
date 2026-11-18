//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// attachOnAssignPeer runs onAssign when the flush asks for a volume, which a
// handle with many small chunks does while it folds them into a manifest.
type attachOnAssignPeer struct {
	*g6ConditionalPeer
	once     sync.Once
	onAssign func()
}

func (p *attachOnAssignPeer) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	p.once.Do(p.onAssign)
	return &filer_pb.AssignVolumeResponse{Error: "no volume in this test"}, nil
}

// An upload attaches its chunk without the handle lock, so it can land while a
// flush is between taking its chunk snapshot and installing the compacted
// list. The chunk must stay on the handle and reach the filer.
func TestFlushKeepsChunkAttachedDuringCommit(t *testing.T) {
	base := attributeAppendEntry(256)
	base.CommitTsNs = 100
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 1000)
	fh := w.GetHandle(FileHandleId(handle))

	late := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: 1, Cookie: 1}, Offset: 0, Size: 256, ModifiedTsNs: 500}
	startFakeFiler(t, w, &attachOnAssignPeer{g6ConditionalPeer: peer, onAssign: func() {
		fh.AddChunks([]*filer_pb.FileChunk{late})
		fh.entryChunkGroup.AddChunk(late)
	}})

	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 256 * 256, Size: 256}, bytes.Repeat([]byte{'W'}, 256)); n != 256 || code != fuse.OK {
		t.Fatalf("write: %d %v", n, code)
	}
	if code := w.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle}); code != fuse.OK {
		t.Fatalf("flush: %v", code)
	}

	has := func(chunks []*filer_pb.FileChunk) bool {
		for _, c := range chunks {
			if contentFileIdKey(c) == contentFileIdKey(late) {
				return true
			}
		}
		return false
	}
	if !has(fh.GetEntry().GetChunks()) {
		t.Fatal("the handle lost the chunk an upload attached during the flush")
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if !has(peer.entry.Chunks) {
		t.Fatal("the flush did not publish the chunk an upload attached during it")
	}
}

// Uploads keep attaching chunks while a flush clones its request entry, so the
// clone must hold the entry lock. Run with -race.
func TestFlushClonesRequestWhileUploadsAttach(t *testing.T) {
	base := attributeAppendEntry(1)
	base.CommitTsNs = 100
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 1000)
	fh := w.GetHandle(FileHandleId(handle))

	attached := make(chan struct{})
	go func() {
		defer close(attached)
		for key := uint64(1); key <= 2000; key++ {
			fh.AddChunks([]*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: key, Cookie: 1}, Offset: 0, Size: 256, ModifiedTsNs: int64(key)}})
			time.Sleep(20 * time.Microsecond)
		}
	}()
	for {
		if err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil); err != nil {
			t.Fatalf("flush: %v", err)
		}
		select {
		case <-attached:
			return
		default:
		}
	}
}

// bloatedHandleWithPendingUpload opens a handle over chunks bloated enough to
// merge and starts uploading a write past them. The upload blocks until
// release is closed; drained reports its result.
func bloatedHandleWithPendingUpload(t *testing.T) (w *WFS, fh *FileHandle, peer *g6ConditionalPeer, base *filer_pb.Entry, release chan struct{}, drained chan error) {
	t.Helper()
	base = attributeAppendEntry(1)
	base.Chunks = nil
	for i := 0; i < 8; i++ {
		base.Chunks = append(base.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i * 64), Size: 256, ModifiedTsNs: int64(i + 1)})
	}
	base.Attributes.FileSize = 7*64 + 256
	base.CommitTsNs = 100
	if _, _, bloated := shouldMergeChunks(base.Chunks, nil); !bloated {
		t.Fatal("the test entry is not bloated enough to merge")
	}
	peer = &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 1000)
	fh = w.GetHandle(FileHandleId(handle))

	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).saveDataAsChunk = func(util.FullPath) filer.SaveDataAsChunkFunctionType {
		return func(r io.Reader, _ string, offset, ts int64, _ uint64) (*filer_pb.FileChunk, error) {
			once.Do(func() { close(started) })
			<-release
			body, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 2, FileKey: 1, Cookie: 1}, Offset: offset, Size: uint64(len(body)), ModifiedTsNs: ts}, nil
		}
	}
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: base.Attributes.FileSize, Size: 256}, bytes.Repeat([]byte{'W'}, 256)); n != 256 || code != fuse.OK {
		t.Fatalf("write: %d %v", n, code)
	}
	drained = make(chan error, 1)
	go func() { drained <- fh.dirtyPages.FlushData() }()
	<-started
	return w, fh, peer, base, release, drained
}

// A flush that finds its chunks bloated while an upload is still in flight
// leaves the merge to a later flush instead of waiting for the upload under
// the handle and publication locks: a writeback upload retries until storage
// recovers, and every operation on the handle would wait with it.
func TestFlushLeavesMergeWhileUploadPending(t *testing.T) {
	w, fh, peer, base, release, drained := bloatedHandleWithPendingUpload(t)

	flushed := make(chan error, 1)
	go func() { flushed <- w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil) }()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		<-flushed
		t.Fatal("the flush waited for a pending upload under the handle lock")
	}
	close(release)
	if err := <-drained; err != nil {
		t.Fatalf("drain: %v", err)
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.entry.Chunks) != len(base.Chunks) {
		t.Fatalf("published %d chunks, want the %d unmerged ones", len(peer.entry.Chunks), len(base.Chunks))
	}
}

// A flush that publishes while an upload is pending must not adopt a peer's
// inline content: the upload attaches its chunk next to it, and reads stop at
// the inline bytes, so the completed write would never be read.
func TestFlushWithUploadPendingRefusesPeerInlineContent(t *testing.T) {
	w, fh, peer, _, release, drained := bloatedHandleWithPendingUpload(t)
	defer func() {
		close(release)
		if err := <-drained; err != nil {
			t.Fatalf("drain: %v", err)
		}
	}()

	inline := bytes.Repeat([]byte{'P'}, int(filer.FileSize(fh.GetEntry().GetEntry())))
	peer.mu.Lock()
	peer.entry.Chunks = nil
	peer.entry.Content = inline
	peer.entry.Attributes.FileSize = uint64(len(inline))
	peer.entry.CommitTsNs = 200
	peer.mu.Unlock()

	if err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil); !errors.Is(err, filer_pb.ErrPreconditionFailed) {
		t.Fatalf("flush over a peer's inline content with an upload pending: %v, want a refused merge", err)
	}
	if content := fh.GetEntry().GetEntry().GetContent(); len(content) != 0 {
		t.Fatalf("the handle adopted %d bytes of the peer's inline content", len(content))
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if !bytes.Equal(peer.entry.Content, inline) || len(peer.entry.Chunks) != 0 {
		t.Fatal("the flush published over the peer's inline content")
	}
}
