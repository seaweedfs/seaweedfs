package weed_server

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type scopedEntryRun struct {
	err     error
	got     *filer.Entry
	called  bool
	deleted []string
}

func scopedUpdateRequest(t *testing.T, entry *filer_pb.Entry) *filer_pb.UpdateEntryRequest {
	t.Helper()
	auth := make(map[string]string, len(entry.GetChunks()))
	for _, chunk := range entry.GetChunks() {
		if chunk == nil {
			continue
		}
		fid := chunk.GetFileIdString()
		if fid != "" {
			auth[fid] = scopedChunkProof(t, "retention", fid, time.Hour)
		}
	}
	return &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: entry, ChunkAuth: auth}
}

func runScopedUpdateState(t *testing.T, initial *filer.Entry, requested *filer_pb.Entry, late func(*filer.Entry)) scopedEntryRun {
	t.Helper()
	path := scopeJoin(aliceWS, requested.GetName())
	store := newRenameTestStore()
	store.entries[path] = initial
	fs, scope := newScopeRetentionServer(t, store)
	req := scopedUpdateRequest(t, requested)
	run := scopedEntryRun{}
	_, run.err = scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
		FullMethod: scopeFilerMethodPrefix + "UpdateEntry",
	}, func(handlerCtx context.Context, handlerReq any) (any, error) {
		run.called = true
		if late != nil {
			late(store.entries[path])
		}
		return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
	})
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		run.deleted = append(run.deleted, ids...)
	})
	var findErr error
	run.got, findErr = store.FindEntry(context.Background(), util.FullPath(path))
	if findErr != nil {
		t.Fatalf("find entry after scoped update: %v", findErr)
	}
	return run
}

func runScopedCreateState(t *testing.T, initial *filer.Entry, requested *filer_pb.Entry, late func(*filer.Entry)) scopedEntryRun {
	t.Helper()
	path := scopeJoin(aliceWS, requested.GetName())
	store := newRenameTestStore()
	store.entries[path] = initial
	fs, scope := newScopeRetentionServer(t, store)
	update := scopedUpdateRequest(t, requested)
	req := &filer_pb.CreateEntryRequest{
		Directory:                aliceWS,
		Entry:                    update.Entry,
		ChunkAuth:                update.ChunkAuth,
		SkipCheckParentDirectory: true,
	}
	run := scopedEntryRun{}
	_, run.err = scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
		FullMethod: scopeFilerMethodPrefix + "CreateEntry",
	}, func(handlerCtx context.Context, handlerReq any) (any, error) {
		run.called = true
		if late != nil {
			late(store.entries[path])
		}
		return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
	})
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		run.deleted = append(run.deleted, ids...)
	})
	var findErr error
	run.got, findErr = store.FindEntry(context.Background(), util.FullPath(path))
	if findErr != nil {
		t.Fatalf("find entry after scoped create: %v", findErr)
	}
	return run
}

func scopedEntrySnapshot(entry *filer.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	out := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
	for _, chunk := range out.GetChunks() {
		if chunk != nil {
			chunk.FileId = chunk.GetFileIdString()
			chunk.Fid = nil
		}
	}
	return out
}

func scopedRequestedSnapshot(entry *filer_pb.Entry) *filer_pb.Entry {
	out := proto.Clone(entry).(*filer_pb.Entry)
	for _, chunk := range out.GetChunks() {
		if chunk != nil {
			chunk.FileId = chunk.GetFileIdString()
			chunk.Fid = nil
		}
	}
	out.CommitTsNs = 0
	return out
}

func scopedEntryClone(entry *filer.Entry) *filer.Entry {
	if entry == nil {
		return nil
	}
	clone := *entry
	clone.Chunks = proto.Clone(&filer_pb.Entry{Chunks: entry.Chunks}).(*filer_pb.Entry).Chunks
	clone.Content = append([]byte(nil), entry.Content...)
	clone.HardLinkId = append(filer.HardLinkId(nil), entry.HardLinkId...)
	if entry.Remote != nil {
		clone.Remote = proto.Clone(entry.Remote).(*filer_pb.RemoteEntry)
	}
	if entry.Extended != nil {
		clone.Extended = make(map[string][]byte, len(entry.Extended))
		for key, value := range entry.Extended {
			clone.Extended[key] = append([]byte(nil), value...)
		}
	}
	return &clone
}

func assertScopedEntryMatchesRequested(t *testing.T, got *filer.Entry, requested *filer_pb.Entry) {
	t.Helper()
	gotSnapshot := scopedEntrySnapshot(got)
	// UpdateEntry preserves object identity and creation time, and the filer
	// fills an initial access time when the request leaves it unset.
	gotSnapshot.Attributes.Crtime = requested.GetAttributes().GetCrtime()
	gotSnapshot.Attributes.Atime = requested.GetAttributes().GetAtime()
	gotSnapshot.Attributes.Inode = requested.GetAttributes().GetInode()
	gotSnapshot.CommitTsNs = 0
	if !proto.Equal(gotSnapshot, scopedRequestedSnapshot(requested)) {
		t.Fatalf("stored entry differs from requested replacement: got=%v want=%v", gotSnapshot, requested)
	}
}

func assertScopedEntryUnchanged(t *testing.T, run scopedEntryRun, want *filer.Entry) {
	t.Helper()
	if !proto.Equal(scopedEntrySnapshot(run.got), scopedEntrySnapshot(want)) {
		t.Fatalf("stored entry changed: got=%v want=%v", run.got.ToProtoEntry(), want.ToProtoEntry())
	}
	if len(run.deleted) != 0 {
		t.Fatalf("refused update queued garbage chunks: %v", run.deleted)
	}
}

func provenUpdateEntry(name string, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
		Chunks:     chunks,
	}
}

func scopedChunk(fileID string, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileID, Offset: offset, Size: size}
}

func TestScopedUpdateRejectsStoredManifestAtAdmission(t *testing.T) {
	const name = "stored-manifest-admission"
	initial := newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000060", Size: 10, IsChunkManifest: true}}
	initial.FileSize = 10
	want := scopedEntryClone(initial)
	requested := provenUpdateEntry(name, scopedChunk("5,0c00000061", 0, 10))

	run := runScopedUpdateState(t, initial, requested, nil)
	if status.Code(run.err) != codes.PermissionDenied || run.called {
		t.Fatalf("stored manifest update err=%v called=%v, want admission PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, want)

	initial = newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000061", Size: 10}}
	initial.FileSize = 10
	want = scopedEntryClone(initial)
	want.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000062", Size: 10, IsChunkManifest: true}}
	run = runScopedUpdateState(t, initial, requested, func(entry *filer.Entry) {
		entry.Chunks = want.Chunks
	})
	if status.Code(run.err) != codes.PermissionDenied || !run.called {
		t.Fatalf("late stored manifest update err=%v called=%v, want locked PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, want)
}

func TestScopedCreateRejectsStoredManifestAtAdmissionAndUnderLock(t *testing.T) {
	const name = "stored-manifest-create"
	requested := provenUpdateEntry(name, scopedChunk("5,0c00000062", 0, 10))

	initial := newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000063", Size: 10, IsChunkManifest: true}}
	initial.FileSize = 10
	run := runScopedCreateState(t, initial, requested, nil)
	if status.Code(run.err) != codes.PermissionDenied || run.called {
		t.Fatalf("stored manifest create err=%v called=%v, want admission PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, scopedEntryClone(initial))

	initial = newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000064", Size: 10}}
	initial.FileSize = 10
	expected := scopedEntryClone(initial)
	expected.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000065", Size: 10, IsChunkManifest: true}}
	expected.FileSize = 10
	run = runScopedCreateState(t, initial, requested, func(entry *filer.Entry) {
		entry.Chunks = expected.Chunks
		entry.FileSize = expected.FileSize
	})
	if status.Code(run.err) != codes.PermissionDenied || !run.called {
		t.Fatalf("late stored manifest create err=%v called=%v, want locked PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, expected)
}

func TestScopedUpdateRejectsMixedInlineAndProvenChunks(t *testing.T) {
	const name = "mixed-inline-chunks"
	initial := newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000066", Size: 10}}
	initial.FileSize = 10
	want := scopedEntryClone(initial)
	requested := provenUpdateEntry(name, scopedChunk("5,0c00000067", 0, 10))
	requested.Content = []byte("inline and chunks")

	run := runScopedUpdateState(t, initial, requested, nil)
	if status.Code(run.err) != codes.PermissionDenied || run.called {
		t.Fatalf("mixed update err=%v called=%v, want admission PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, want)
}

func TestScopedUpdateRejectsMalformedStoredChunkRanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		entryName string
		chunk     *filer_pb.FileChunk
	}{
		{name: "negative offset", entryName: "malformed-stored-negative", chunk: scopedChunk("5,0c00000068", -1, 1)},
		{name: "offset overflow", entryName: "malformed-stored-overflow", chunk: scopedChunk("5,0c00000069", math.MaxInt64, 1)},
		{name: "nil chunk", entryName: "malformed-stored-nil", chunk: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := newFileEntry(scopeJoin(aliceWS, tc.entryName), 1)
			initial.Chunks = []*filer_pb.FileChunk{tc.chunk}
			want := scopedEntryClone(initial)
			for _, requested := range []*filer_pb.Entry{
				provenUpdateEntry(tc.entryName, scopedChunk("5,0c0000006a", 0, 10)),
				provenUpdateEntry(tc.entryName),
			} {
				if tc.chunk == nil {
					stored := initial.ToProtoEntry()
					if err := scopeRetainsData(stored, requested, func(string) bool { return true }); err == nil {
						t.Fatalf("malformed nil stored update requestedChunks=%d was admitted", len(requested.GetChunks()))
					}
					if !proto.Equal(initial.ToProtoEntry(), want.ToProtoEntry()) {
						t.Fatal("direct malformed nil stored check changed the entry")
					}
					continue
				}
				run := runScopedUpdateState(t, initial, requested, nil)
				wantHandler := len(requested.GetChunks()) == 0
				if status.Code(run.err) != codes.PermissionDenied || run.called != wantHandler {
					t.Fatalf("malformed stored update requestedChunks=%d err=%v called=%v, want PermissionDenied with handler called=%v", len(requested.GetChunks()), run.err, run.called, wantHandler)
				}
				assertScopedEntryUnchanged(t, run, want)
			}
		})
	}
}

func TestScopedUpdateRejectsMalformedRequestedChunkRanges(t *testing.T) {
	const name = "malformed-requested-range"
	for _, tc := range []struct {
		name string
		edit func(*filer_pb.Entry)
	}{
		{name: "negative offset", edit: func(entry *filer_pb.Entry) { entry.Chunks[0].Offset = -1 }},
		{name: "offset overflow", edit: func(entry *filer_pb.Entry) { entry.Chunks[0].Offset = math.MaxInt64; entry.Chunks[0].Size = 1 }},
		{name: "file size overflow", edit: func(entry *filer_pb.Entry) { entry.Attributes.FileSize = uint64(math.MaxInt64) + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := newFileEntry(scopeJoin(aliceWS, name), 1)
			initial.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c0000006b", Size: 10}}
			initial.FileSize = 10
			want := scopedEntryClone(initial)
			requested := provenUpdateEntry(name, scopedChunk("5,0c0000006c", 0, 10))
			tc.edit(requested)
			run := runScopedUpdateState(t, initial, requested, nil)
			if status.Code(run.err) != codes.PermissionDenied || run.called {
				t.Fatalf("malformed requested update err=%v called=%v, want admission PermissionDenied", run.err, run.called)
			}
			assertScopedEntryUnchanged(t, run, want)
		})
	}
}

func TestScopedUpdatePersistsSafeSparseAndReplacementLayouts(t *testing.T) {
	const name = "safe-layouts"
	tests := []struct {
		name       string
		initial    *filer.Entry
		requested  *filer_pb.Entry
		wantSize   uint64
		wantDelete string
	}{
		{
			name: "explicit shorter replacement",
			initial: func() *filer.Entry {
				entry := newFileEntry(scopeJoin(aliceWS, name), 1)
				entry.Chunks = []*filer_pb.FileChunk{scopedChunk("5,0c0000006d", 0, 10)}
				entry.FileSize = 100
				return entry
			}(),
			requested: func() *filer_pb.Entry {
				entry := provenUpdateEntry(name, scopedChunk("5,0c0000006e", 0, 5))
				entry.Attributes.FileSize = 5
				return entry
			}(),
			wantSize:   5,
			wantDelete: "5,0c0000006d",
		},
		{
			name: "sparse same length",
			initial: func() *filer.Entry {
				entry := newFileEntry(scopeJoin(aliceWS, name), 1)
				entry.Chunks = []*filer_pb.FileChunk{scopedChunk("5,0c0000006f", 0, 10)}
				entry.FileSize = 100
				return entry
			}(),
			requested: func() *filer_pb.Entry {
				entry := provenUpdateEntry(name,
					scopedChunk("5,0c0000006f", 0, 10),
					scopedChunk("5,0c00000070", 10, 10),
				)
				entry.Attributes.FileSize = 100
				return entry
			}(),
			wantSize: 100,
		},
		{
			name: "sparse tail extension",
			initial: func() *filer.Entry {
				entry := newFileEntry(scopeJoin(aliceWS, name), 1)
				entry.Chunks = []*filer_pb.FileChunk{scopedChunk("5,0c00000080", 0, 10)}
				entry.FileSize = 100
				return entry
			}(),
			requested: func() *filer_pb.Entry {
				entry := provenUpdateEntry(name,
					scopedChunk("5,0c00000080", 0, 10),
					scopedChunk("5,0c00000081", 100, 10),
				)
				entry.Attributes.FileSize = 110
				return entry
			}(),
			wantSize: 110,
		},
		{
			name: "hole and overlap",
			initial: func() *filer.Entry {
				entry := newFileEntry(scopeJoin(aliceWS, name), 1)
				entry.Chunks = []*filer_pb.FileChunk{
					scopedChunk("5,0c00000071", 0, 10),
					scopedChunk("5,0c00000072", 20, 10),
				}
				entry.FileSize = 30
				return entry
			}(),
			requested: func() *filer_pb.Entry {
				entry := provenUpdateEntry(name,
					scopedChunk("5,0c00000071", 0, 10),
					scopedChunk("5,0c00000072", 20, 10),
					scopedChunk("5,0c00000073", 30, 10),
				)
				entry.Attributes.FileSize = 40
				return entry
			}(),
			wantSize: 40,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := runScopedUpdateState(t, tc.initial, tc.requested, nil)
			if run.err != nil {
				t.Fatalf("safe scoped update: %v", run.err)
			}
			if run.got.FileSize != tc.wantSize {
				t.Fatalf("stored logical size = %d, want %d", run.got.FileSize, tc.wantSize)
			}
			if len(run.got.GetChunks()) == 0 {
				t.Fatal("safe scoped update lost all chunks")
			}
			if tc.wantDelete != "" && !containsString(run.deleted, tc.wantDelete) {
				t.Fatalf("deleted chunks = %v, want %s", run.deleted, tc.wantDelete)
			}
			if tc.wantDelete == "" && len(run.deleted) != 0 {
				t.Fatalf("safe retained update queued garbage chunks: %v", run.deleted)
			}
			assertScopedEntryMatchesRequested(t, run.got, tc.requested)
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestScopedUpdateAllowsCompleteInlineAndRemoteReplacements(t *testing.T) {
	const name = "complete-replacements"
	inline := newFileEntry(scopeJoin(aliceWS, name), 1)
	inline.Content = []byte("old inline")
	requestedInline := provenUpdateEntry(name, scopedChunk("5,0c00000074", 0, 10))
	requestedInline.Attributes.FileSize = 10
	run := runScopedUpdateState(t, inline, requestedInline, nil)
	if run.err != nil {
		t.Fatalf("inline replacement: %v", run.err)
	}
	assertScopedEntryMatchesRequested(t, run.got, requestedInline)
	if !bytes.Equal(run.got.Content, nil) || len(run.got.GetChunks()) != 1 {
		t.Fatalf("inline replacement content=%q chunks=%v", run.got.Content, run.got.GetChunks())
	}

	remote := newFileEntry(scopeJoin(aliceWS, name), 1)
	remote.Remote = &filer_pb.RemoteEntry{StorageName: "remote", RemoteSize: 10, RemoteMtime: 1700000000}
	requestedRemote := provenUpdateEntry(name, scopedChunk("5,0c00000075", 0, 10))
	requestedRemote.Attributes.FileSize = 10
	run = runScopedUpdateState(t, remote, requestedRemote, nil)
	if run.err != nil {
		t.Fatalf("remote replacement: %v", run.err)
	}
	assertScopedEntryMatchesRequested(t, run.got, requestedRemote)
	if run.got.Remote != nil || len(run.got.GetChunks()) != 1 {
		t.Fatalf("remote replacement remote=%v chunks=%v", run.got.Remote, run.got.GetChunks())
	}
}

func TestScopedUpdateRechecksSharedHardLinkUnderPathLock(t *testing.T) {
	const name = "shared-link-update"
	id := []byte("shared-hard-link")
	initial := newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.HardLinkId = id
	initial.HardLinkCounter = 1
	initial.Chunks = []*filer_pb.FileChunk{scopedChunk("5,0c00000076", 0, 10)}
	initial.FileSize = 10
	requested := provenUpdateEntry(name,
		scopedChunk("5,0c00000076", 0, 10),
		scopedChunk("5,0c00000077", 10, 10),
	)
	requested.HardLinkId = id
	requested.HardLinkCounter = 1
	expected := scopedEntryClone(initial)
	expected.HardLinkCounter = 2

	run := runScopedUpdateState(t, initial, requested, func(entry *filer.Entry) {
		entry.HardLinkCounter = 2
	})
	if status.Code(run.err) != codes.PermissionDenied || !run.called {
		t.Fatalf("late shared link update err=%v called=%v, want locked PermissionDenied", run.err, run.called)
	}
	assertScopedEntryUnchanged(t, run, expected)
}

func TestScopedUpdateAllowsSingleHardLinkReplacement(t *testing.T) {
	const name = "single-link-update"
	id := []byte("single-hard-link")
	initial := newFileEntry(scopeJoin(aliceWS, name), 1)
	initial.HardLinkId = id
	initial.HardLinkCounter = 1
	initial.Chunks = []*filer_pb.FileChunk{scopedChunk("5,0c00000078", 0, 10)}
	initial.FileSize = 10
	requested := provenUpdateEntry(name,
		scopedChunk("5,0c00000078", 0, 10),
		scopedChunk("5,0c00000079", 10, 10),
	)
	requested.Attributes.FileSize = 20
	requested.HardLinkId = id
	requested.HardLinkCounter = 1

	run := runScopedUpdateState(t, initial, requested, nil)
	if run.err != nil {
		t.Fatalf("single hard-link update: %v", run.err)
	}
	if !bytes.Equal(run.got.HardLinkId, id) || run.got.HardLinkCounter != 1 {
		t.Fatalf("single hard-link state = id %x count %d, want id %x count 1", run.got.HardLinkId, run.got.HardLinkCounter, id)
	}
	assertScopedEntryMatchesRequested(t, run.got, requested)
}
