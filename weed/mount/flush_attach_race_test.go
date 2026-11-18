//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
