//go:build !freebsd

package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestAsyncConflictRetainsPendingReferenceProbe(t *testing.T) {
	w := newInvalidateTestWFS(t)
	base := flushConditionEntry(1)
	peer := flushConditionEntry(1, 2)
	server := &contentFlushConditionFiler{entry: proto.Clone(peer).(*filer_pb.Entry)}
	startFakeFiler(t, w, server)
	w.mapPbIdFromFilerToLocal(base)
	fh := newFileHandle(w, 1, 42, base)
	t.Cleanup(fh.dirtyPages.Destroy)
	fh.RememberPath(util.FullPath("/dir/file"))
	pending := flushConditionEntry(1, 3).Chunks[1]
	pending.Offset = 2
	fh.UpdateEntry(func(e *filer_pb.Entry) { e.Chunks = append(e.Chunks, pending) })
	fh.dirtyMetadata = true
	fh.asyncFlushPending = true
	fh.asyncFlushUid = 1000
	fh.asyncFlushGid = 20
	w.option.CacheDirForWrite = t.TempDir()
	w.inodeToPath.Lookup(util.FullPath("/dir/file"), 0, false, false, fh.inode, true)
	w.fhMap.inode2fh[fh.inode] = fh
	done := make(chan struct{})
	w.pendingAsyncFlush = map[uint64]chan struct{}{fh.inode: done}
	w.asyncFlushWg.Add(1)
	w.processAsyncFlushItem(&asyncFlushItem{fh: fh, done: done})
	select {
	case <-done:
	default:
		t.Fatal("no async completion")
	}
	server.mu.Lock()
	unchanged := proto.Equal(peer, server.entry)
	server.mu.Unlock()
	if !unchanged {
		t.Fatal("refused async flush changed peer entry")
	}
	if _, kept := w.fhMap.FindFileHandle(fh.inode); kept {
		return
	}
	files, err := globFailedContent(t, w)
	if err != nil || len(files) == 0 {
		t.Fatalf("async conflict completed, removed handle and retained no pending-reference recovery: %v %v", files, err)
	}
}
