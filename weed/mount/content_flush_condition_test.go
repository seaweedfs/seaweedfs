package mount

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
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
func (s *contentFlushConditionFiler) UpdateEntry(ctx context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clauses := r.GetCondition().GetClauses(); len(clauses) > 0 && !proto.Equal(clauses[0].GetExpectedEntry(), s.entry) {
		return nil, status.Error(codes.FailedPrecondition, "entry changed")
	}
	s.calls++
	s.entry = proto.Clone(r.Entry).(*filer_pb.Entry)
	s.entry.CommitTsNs = 1000 + int64(s.calls)
	return &filer_pb.UpdateEntryResponse{LogTsNs: 2000}, nil
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

// Exercise the actual mount content-publication caller. A peer's append is
// merged with ours; a replaced or deleted file is not ours to merge into, so
// the refused flush keeps our dirty entry instead of overwriting the peer.
func TestContentFlushPreservesPeerChunks(t *testing.T) {
	for _, name := range []string{"single-writer", "peer-append", "replaced", "deleted", "deferred-create", "deferred-create-race"} {
		t.Run(name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			observed := flushConditionEntry(1)
			live := flushConditionEntry(1, 3)
			live.Chunks[1].Offset = 2
			current := flushConditionEntry(1)
			wantError, wantCalls := false, 1
			switch name {
			case "peer-append":
				current = flushConditionEntry(1, 2)
				wantCalls = 2
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
			code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0)
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
				want := []uint64{1, 3}
				if name == "peer-append" {
					want = []uint64{1, 2, 3}
				}
				if got := chunkFileKeys(fake.entry); !slices.Equal(got, want) {
					t.Fatalf("stored chunks %v, want %v", got, want)
				}
				if got := chunkFileKeys(fh.GetEntry().GetEntry()); !slices.Equal(got, want) {
					t.Fatalf("handle chunks %v, want %v", got, want)
				}
			}
			if fake.calls != wantCalls {
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
		if code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
			t.Fatalf("flush %d: %v", i-1, code)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.entry.Chunks) != 3 || fake.calls != 2 {
		t.Fatalf("stored %d chunks after %d calls", len(fake.entry.Chunks), fake.calls)
	}
}

type refusedThenStalledFiler struct {
	*contentFlushConditionFiler
}

func (s *refusedThenStalledFiler) LookupDirectoryEntry(ctx context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// A refused create stays refused when reading the stored entry stalls: the
// flush must not resend the create as if the filer had timed out.
func TestContentFlushRefusalWithStalledLookup(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 200 * time.Millisecond
	fake := &contentFlushConditionFiler{entry: flushConditionEntry(1, 2)}
	startFakeFiler(t, wfs, &refusedThenStalledFiler{fake})
	oldSleep := metadataFlushSleep
	metadataFlushSleep = func(context.Context, time.Duration) {}
	t.Cleanup(func() { metadataFlushSleep = oldSleep })
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
	start := time.Now()
	code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0)
	elapsed := time.Since(start)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if code == fuse.OK || !fh.dirtyMetadata {
		t.Fatalf("refused flush reported %v, dirty=%v", code, fh.dirtyMetadata)
	}
	if fake.calls != 1 || elapsed > 2*time.Second {
		t.Fatalf("refused create sent %d times over %v", fake.calls, elapsed)
	}
	if got := chunkFileKeys(fake.entry); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("stored chunks %v", got)
	}
}

func chunkFileKeys(e *filer_pb.Entry) (keys []uint64) {
	for _, c := range e.GetChunks() {
		keys = append(keys, c.GetFid().GetFileKey())
	}
	return keys
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
	if err := wfs.flushFileMetadata(fh); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := chunkFileKeys(fake.entry); !slices.Equal(got, []uint64{1, 2, 3}) || fake.calls != 2 {
		t.Fatalf("stored chunks %v after %d calls", got, fake.calls)
	}
	if got := chunkFileKeys(fh.GetEntry().GetEntry()); !slices.Equal(got, []uint64{1, 2, 3}) {
		t.Fatalf("handle chunks %v", got)
	}
	if !fh.dirtyMetadata {
		t.Fatal("periodic flush cleared dirty metadata")
	}
}

// Every merged flush is followed by a flush that matches what it stored, so
// the peer's chunk is not dropped by the next publication from this handle.
func TestContentFlushMergeThenRepeat(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fake := &contentFlushConditionFiler{entry: flushConditionEntry(1, 2)}
	startFakeFiler(t, wfs, fake)
	observed := flushConditionEntry(1)
	wfs.mapPbIdFromFilerToLocal(observed)
	live := flushConditionEntry(1, 3)
	live.Chunks[1].Offset = 2
	wfs.mapPbIdFromFilerToLocal(live)
	fh := newFileHandle(wfs, 1, 42, live)
	t.Cleanup(func() { fh.dirtyPages.Destroy() })
	fh.baseEntry.Store(observed)
	fh.RememberPath(util.FullPath("/dir/file"))
	for i := uint64(4); i <= 5; i++ {
		fh.dirtyMetadata = true
		if code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
			t.Fatalf("flush before chunk %d: %v", i, code)
		}
		fh.UpdateEntry(func(e *filer_pb.Entry) {
			e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: i, Cookie: 1}, Offset: int64(i - 1), Size: 1, ModifiedTsNs: int64(i)})
			e.Attributes.FileSize = i
		})
	}
	fh.dirtyMetadata = true
	if code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
		t.Fatal(code)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := chunkFileKeys(fake.entry); !slices.Equal(got, []uint64{1, 2, 3, 4, 5}) || fake.calls != 4 {
		t.Fatalf("stored chunks %v after %d calls", got, fake.calls)
	}
}

// The merge keeps a peer's attribute and extended changes the handle did not
// make, applies the ones it did, and lets a local truncation clip the peer's
// chunks past the new end.
func TestMergeContentEntry(t *testing.T) {
	base := flushConditionEntry(1, 2)
	base.Extended = map[string][]byte{"user.a": []byte("1"), "user.b": []byte("1")}
	stored := proto.Clone(base).(*filer_pb.Entry)
	stored.Chunks = append(stored.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 9, Cookie: 1}, Offset: 2, Size: 4})
	stored.Attributes.FileMode, stored.Attributes.FileSize = 0600, 6
	stored.Extended["user.b"] = []byte("peer")
	stored.CommitTsNs = 77

	local := proto.Clone(base).(*filer_pb.Entry)
	local.Attributes.Mtime = 123
	local.Extended["user.a"] = []byte("mine")
	delete(local.Extended, "user.b")
	local.Chunks = append(local.Chunks, &filer_pb.FileChunk{FileId: "1,0300000001", Offset: 2, Size: 1, ModifiedTsNs: 3})

	merged, ok := mergeContentEntry(stored, base, local, local, -1, false)
	if !ok {
		t.Fatal("not merged")
	}
	if got := len(merged.Chunks); got != 4 || merged.Attributes.FileSize != 6 {
		t.Fatalf("merged %d chunks, size %d", got, merged.Attributes.FileSize)
	}
	if merged.Attributes.FileMode != 0600 || merged.Attributes.Mtime != 123 || merged.CommitTsNs != 77 {
		t.Fatalf("attributes %v", merged.Attributes)
	}
	if string(merged.Extended["user.a"]) != "mine" || merged.Extended["user.b"] != nil {
		t.Fatalf("extended %v", merged.Extended)
	}

	truncated := proto.Clone(base).(*filer_pb.Entry)
	truncated.Chunks = truncated.Chunks[:1]
	truncated.Attributes.FileSize = 1
	merged, ok = mergeContentEntry(stored, base, truncated, truncated, -1, false)
	if !ok {
		t.Fatal("truncation not merged")
	}
	if got := chunkFileKeys(merged); !slices.Equal(got, []uint64{1}) || merged.Attributes.FileSize != 1 {
		t.Fatalf("truncated merge kept %v, size %d", got, merged.Attributes.FileSize)
	}

	// Truncated to 1, then written at 7: the peer chunk at 2 stays cut.
	regrown := proto.Clone(truncated).(*filer_pb.Entry)
	regrown.Chunks = append(regrown.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 7, Cookie: 1}, Offset: 7, Size: 1, ModifiedTsNs: 7})
	regrown.Attributes.FileSize = 8
	merged, ok = mergeContentEntry(stored, base, regrown, regrown, 1, false)
	if !ok {
		t.Fatal("truncate then write not merged")
	}
	if got := chunkFileKeys(merged); !slices.Equal(got, []uint64{1, 7}) || merged.Attributes.FileSize != 8 {
		t.Fatalf("truncate then write kept %v, size %d", got, merged.Attributes.FileSize)
	}

	// Shortening a manifest reference leaves its members whole.
	peerManifest := proto.Clone(base).(*filer_pb.Entry)
	peerManifest.Chunks = append(peerManifest.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 5, Cookie: 1}, Offset: 0, Size: 10, IsChunkManifest: true})
	if _, ok := mergeContentEntry(peerManifest, base, regrown, regrown, 1, false); ok {
		t.Fatal("merged a peer manifest across the truncate")
	}

	// A peer truncate stands when the handle only overwrote inside it.
	peerTruncated := proto.Clone(base).(*filer_pb.Entry)
	peerTruncated.Chunks, peerTruncated.Attributes.FileSize = peerTruncated.Chunks[:1], 1
	overwrote := proto.Clone(base).(*filer_pb.Entry)
	overwrote.Chunks = append(overwrote.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 6, Cookie: 1}, Size: 1, ModifiedTsNs: 6})
	merged, ok = mergeContentEntry(peerTruncated, base, overwrote, overwrote, -1, false)
	if !ok {
		t.Fatal("overwrite over a peer truncate not merged")
	}
	if got := chunkFileKeys(merged); !slices.Equal(got, []uint64{1, 6}) || merged.Attributes.FileSize != 1 {
		t.Fatalf("overwrite over a peer truncate kept %v, size %d", got, merged.Attributes.FileSize)
	}

	// A rewrite folded into a manifest is still a rewrite.
	rewritten := proto.Clone(base).(*filer_pb.Entry)
	rewritten.Chunks = []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 8, Cookie: 1}, Size: 2, IsChunkManifest: true}}
	if _, ok := mergeContentEntry(stored, base, rewritten, rewritten, -1, true); ok {
		t.Fatal("merged a stale whole-file rewrite over a peer append")
	}
	if _, ok := mergeContentEntry(stored, base, rewritten, rewritten, -1, false); !ok {
		t.Fatal("refused new chunks the handle did not rewrite")
	}
	if _, ok := mergeContentEntry(proto.Clone(base).(*filer_pb.Entry), base, rewritten, rewritten, -1, true); !ok {
		t.Fatal("refused a rewrite with no peer chunk change")
	}

	stored.Attributes.Inode = 99
	if _, ok := mergeContentEntry(stored, base, local, local, -1, false); ok {
		t.Fatal("merged into a replaced file")
	}
	unstamped := flushConditionEntry(4)
	unstamped.Chunks[0].Size, unstamped.Chunks[0].ModifiedTsNs = 3, 0
	shortened := proto.Clone(unstamped).(*filer_pb.Entry)
	shortened.Chunks[0].Size, shortened.Attributes.FileSize = 2, 2
	peerAppend := proto.Clone(unstamped).(*filer_pb.Entry)
	peerAppend.Chunks = append(peerAppend.Chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 9, Cookie: 1}, Offset: 3, Size: 1, ModifiedTsNs: 9})
	if _, ok := mergeContentEntry(peerAppend, unstamped, shortened, shortened, 2, false); !ok {
		t.Fatal("refused a truncate that shortened an unstamped base chunk")
	}

	noInode := proto.Clone(base).(*filer_pb.Entry)
	noInode.Attributes.Inode, noInode.Attributes.Crtime = 0, 5
	sameFile := proto.Clone(noInode).(*filer_pb.Entry)
	sameFile.Chunks = append(sameFile.Chunks, stored.Chunks[2])
	if _, ok := mergeContentEntry(sameFile, noInode, local, local, -1, false); !ok {
		t.Fatal("refused an inode-less file with the same creation time")
	}
	sameFile.Attributes.Crtime = 6
	if _, ok := mergeContentEntry(sameFile, noInode, local, local, -1, false); ok {
		t.Fatal("merged into an inode-less replacement")
	}

	empty := flushConditionEntry()
	inline := proto.Clone(empty).(*filer_pb.Entry)
	inline.Content = []byte("peer")
	appended := proto.Clone(empty).(*filer_pb.Entry)
	appended.Chunks = flushConditionEntry(5).Chunks
	if _, ok := mergeContentEntry(inline, empty, appended, appended, -1, false); ok {
		t.Fatal("merged chunks next to inline content")
	}
	if _, ok := mergeContentEntry(appended, empty, inline, inline, -1, false); ok {
		t.Fatal("merged inline content next to chunks")
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
			if code := wfs.doFlush(context.Background(), fh, 1000, 20, false, 0); code != fuse.OK {
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

// Rebasing keeps a size the handle reached after the flush was taken, not the
// size it sent: a peer's truncate merged in must stand.
func TestRebaseEntryKeepsMergedSize(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, flushConditionEntry(1, 2), 0, 0)
	sent := proto.Clone(fh.GetEntry().GetEntry()).(*filer_pb.Entry)
	merged := flushConditionEntry(1)
	merged.Attributes.FileSize = 1

	fh.rebaseEntry(merged, sent)
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 1 {
		t.Fatalf("size %d after rebasing onto a truncate, want 1", size)
	}

	fh.GetEntry().GetEntry().Attributes.FileSize = 5
	fh.rebaseEntry(merged, sent)
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != 5 {
		t.Fatalf("size %d after a write grew the handle, want 5", size)
	}
}

// A merged entry whose manifest cannot be resolved yet leaves the read view
// marked, and the next read resolves it again instead of failing for good.
func TestRebaseEntryRetriesUnresolvedChunkView(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, flushConditionEntry(1), 0, 0)
	lookupDown := true
	group, err := filer.NewChunkGroup(func(ctx context.Context, fileId string) ([]string, error) {
		if lookupDown {
			return nil, errors.New("volume lookup down")
		}
		return nil, nil
	}, nil, nil, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fh.entryChunkGroup = group

	sent := proto.Clone(fh.GetEntry().GetEntry()).(*filer_pb.Entry)
	merged := flushConditionEntry(1, 2)
	merged.Chunks[1].IsChunkManifest = true
	fh.rebaseEntry(merged, sent)
	if !fh.chunkViewStale.Load() {
		t.Fatal("unresolved manifest not marked")
	}

	lookupDown = false
	fh.UpdateEntry(func(e *filer_pb.Entry) { e.Chunks = e.Chunks[:1] })
	fh.refreshChunkView()
	if fh.chunkViewStale.Load() {
		t.Fatal("read view still marked after the chunks resolved")
	}
}
