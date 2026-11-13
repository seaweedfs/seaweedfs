//go:build !freebsd

package mount

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// Like the filer, stamp the stored version and announce what was stored.
	old := s.entry
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	s.entry.CommitTsNs = 1000 + int64(s.calls)
	return &filer_pb.CreateEntryResponse{LogTsNs: 2000, MetadataEvent: &filer_pb.SubscribeMetadataResponse{
		Directory: r.Directory,
		TsNs:      s.entry.CommitTsNs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: old,
			NewEntry: proto.Clone(s.entry).(*filer_pb.Entry),
		},
	}}, nil
}
func (s *contentFlushConditionFiler) LookupDirectoryEntry(ctx context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entry == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(s.entry).(*filer_pb.Entry)}, nil
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

// Each acknowledged flush must leave the handle a base the next flush on the
// same handle can match: the stored entry, with the filer's own commit stamp.
func TestContentFlushRepeatsOnOneHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	stored := flushConditionEntry(1)
	stored.CommitTsNs = 7
	fake := &contentFlushConditionFiler{entry: proto.Clone(stored).(*filer_pb.Entry)}
	startFakeFiler(t, wfs, fake)
	observed := proto.Clone(stored).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(observed)
	fh := newFileHandle(wfs, 1, 42, proto.Clone(observed).(*filer_pb.Entry))
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.RememberPath(util.FullPath("/dir/file"))
	for i := uint64(2); i <= 3; i++ {
		fh.UpdateEntry(func(e *filer_pb.Entry) {
			e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: i, Cookie: 1}, Offset: int64(i - 1), Size: 1, ModifiedTsNs: int64(i)})
			e.Attributes.FileSize = i
		})
		fh.dirtyMetadata = true
		if code := wfs.doFlush(context.Background(), fh, 1000, 20, false); code != fuse.OK {
			t.Fatalf("flush %d: %v", i-1, code)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.entry.Chunks) != 3 || fake.calls != 2 {
		t.Fatalf("stored %d chunks after %d calls", len(fake.entry.Chunks), fake.calls)
	}
}

// The periodic metadata flush publishes the same whole entry, so it must not
// overwrite a peer's append either.
func TestPeriodicMetadataFlushPreservesPeerChunks(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	current := flushConditionEntry(1, 2)
	fake := &contentFlushConditionFiler{entry: proto.Clone(current).(*filer_pb.Entry)}
	startFakeFiler(t, wfs, fake)
	observed := flushConditionEntry(1)
	wfs.mapPbIdFromFilerToLocal(observed)
	live := flushConditionEntry(1, 3)
	live.Chunks[1].Offset = 2
	wfs.mapPbIdFromFilerToLocal(live)
	fh := newFileHandle(wfs, 1, 42, live)
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.baseEntry.Store(observed)
	fh.dirtyMetadata = true
	fh.RememberPath(util.FullPath("/dir/file"))
	if err := wfs.flushFileMetadata(fh); err == nil {
		t.Fatal("periodic flush overwrote a peer's append")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !proto.Equal(fake.entry, current) || fake.calls != 1 {
		t.Fatalf("peer entry changed after %d calls: %v", fake.calls, fake.entry)
	}
	if !fh.dirtyMetadata {
		t.Fatal("refused periodic flush lost dirty metadata")
	}
}

// A miss on state no peer changed is republished once against the stored
// entry: a rename while open, or a filer uid the local id mapping cannot
// reproduce (newInvalidateTestWFS maps local 1000 to filer 2000).
func TestContentFlushRepublishesUnchangedEntry(t *testing.T) {
	for _, name := range []string{"renamed", "colliding-uid"} {
		t.Run(name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			stored := flushConditionEntry(1)
			stored.CommitTsNs = 7
			observed := proto.Clone(stored).(*filer_pb.Entry)
			path := util.FullPath("/dir/file")
			switch name {
			case "renamed":
				wfs.mapPbIdFromFilerToLocal(observed)
				stored.Name, stored.CommitTsNs = "moved", 8
				path = "/dir/moved"
			case "colliding-uid":
				stored.Attributes.Uid, observed.Attributes.Uid = 1000, 1000
			}
			fake := &contentFlushConditionFiler{entry: proto.Clone(stored).(*filer_pb.Entry)}
			startFakeFiler(t, wfs, fake)
			live := proto.Clone(observed).(*filer_pb.Entry)
			live.Name = stored.Name
			live.Chunks = append(live.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 3, Cookie: 1}, Offset: 1, Size: 1, ModifiedTsNs: 3})
			fh := newFileHandle(wfs, 1, 42, live)
			t.Cleanup(func() { fh.dirtyPages.Destroy() })
			fh.baseEntry.Store(observed)
			fh.dirtyMetadata = true
			fh.RememberPath(path)
			if code := wfs.doFlush(context.Background(), fh, 1000, 20, false); code != fuse.OK {
				t.Fatal(code)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.entry.Chunks) != 2 || fake.calls != 2 {
				t.Fatalf("stored %d chunks after %d calls", len(fake.entry.Chunks), fake.calls)
			}
		})
	}
}
