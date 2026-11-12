//go:build !freebsd

package mount

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

type contentFlushConditionFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu    sync.Mutex
	entry *filer_pb.Entry
	calls int
}

func (s *contentFlushConditionFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	for _, c := range r.GetCondition().GetClauses() {
		matches := false
		switch c.Kind {
		case filer_pb.WriteCondition_IF_ENTRY_EQUAL:
			matches = proto.Equal(c.ExpectedEntry, s.entry)
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			matches = s.entry == nil
		}
		if !matches {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{LogTsNs: 2000}, nil
}
func flushConditionEntry(ids ...uint64) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{Inode: 42, FileMode: 0644, Uid: 2000, Gid: 20, FileSize: 3}}
	for i, id := range ids {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: id, Cookie: 1}, Offset: int64(i), Size: 1, ModifiedTsNs: int64(id)})
	}
	return e
}

// Exercise the actual mount content-publication caller. A peer's successful
// append or replacement is authoritative; refusing our stale flush must retain
// our dirty entry, instead of refreshing a guard and replaying stale content.
func TestContentFlushPreservesPeerChunks(t *testing.T) {
	for _, name := range []string{"single-writer", "peer-append", "replaced", "deleted", "deferred-create", "deferred-create-race"} {
		t.Run(name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			observed := flushConditionEntry(1)
			live := flushConditionEntry(1, 3)
			live.Chunks[1].Offset = 2
			current := flushConditionEntry(1)
			wantError := false
			switch name {
			case "peer-append":
				current = flushConditionEntry(1, 2)
				wantError = true
			case "replaced":
				current = flushConditionEntry(4)
				current.Attributes.Inode = 99
				wantError = true
			case "deleted":
				current = nil
				wantError = true
			case "deferred-create":
				observed = nil
				current = nil
			case "deferred-create-race":
				observed = nil
				current = flushConditionEntry(4)
				current.Attributes.Inode = 99
				wantError = true
			}
			if observed != nil {
				wfs.mapPbIdFromFilerToLocal(observed)
			}
			wfs.mapPbIdFromFilerToLocal(live)
			fake := &contentFlushConditionFiler{entry: current}
			startFakeFiler(t, wfs, fake)
			fh := newFileHandle(wfs, 1, 42, live)
			t.Cleanup(func() { fh.dirtyPages.Destroy() })
			fh.baseEntry.Store(observed)
			fh.dirtyMetadata = true
			fh.RememberPath(util.FullPath("/dir/file"))
			before := proto.Clone(current)
			code := wfs.doFlush(context.Background(), fh, 1000, 20, false)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if wantError {
				if code == fuse.OK {
					t.Fatal("stale content flush acknowledged after a peer changed the entry")
				}
				if !proto.Equal(fake.entry, before) {
					t.Fatalf("peer entry changed: %v", fake.entry)
				}
				if !fh.dirtyMetadata {
					t.Fatal("refused flush lost dirty metadata")
				}
				if len(fh.entry.Entry.Chunks) != 2 || fh.entry.Entry.Chunks[1].GetFileIdString() != live.Chunks[1].GetFileIdString() {
					t.Fatal("refused flush lost pending content")
				}
			} else {
				if code != fuse.OK {
					t.Fatal(code)
				}
				if fh.dirtyMetadata {
					t.Fatal("successful flush remains dirty")
				}
				if len(fake.entry.Chunks) != 2 || fake.entry.Chunks[1].Offset != 2 || fake.entry.Chunks[1].Size != 1 {
					t.Fatalf("normal flush lost content: %v", fake.entry)
				}
			}
			if fake.calls != 1 {
				t.Fatalf("unexpected replay: %d calls", fake.calls)
			}
		})
	}
}
