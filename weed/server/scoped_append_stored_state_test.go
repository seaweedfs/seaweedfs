package weed_server

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func scopedAppendStateChunk(fileID string, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileID, Offset: offset, Size: size}
}

func runScopedAppendState(t *testing.T, entry *filer.Entry, requested []*filer_pb.FileChunk, mutate func(*filer.Entry)) (error, *filer.Entry, *filer_pb.AppendToEntryRequest) {
	t.Helper()
	fs, store := newAppendLockServer(t)
	close(store.release)
	store.entries[appendLockTestPath] = entry

	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, fs)
	chunkAuth := make(map[string]string, len(requested))
	for _, chunk := range requested {
		chunkAuth[chunk.GetFileIdString()] = scopedChunkProof(t, "append", chunk.GetFileIdString(), time.Hour)
	}
	req := &filer_pb.AppendToEntryRequest{
		Directory: "/data",
		EntryName: "app.log",
		Chunks:    requested,
		ChunkAuth: chunkAuth,
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken("append", time.Hour, []string{"/data"}, nil),
	))
	_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AppendToEntry"}, func(handlerCtx context.Context, handlerReq any) (any, error) {
		if mutate != nil {
			mutate(store.entries[appendLockTestPath])
		}
		return fs.AppendToEntry(handlerCtx, handlerReq.(*filer_pb.AppendToEntryRequest))
	})
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		if len(ids) > 0 {
			t.Fatalf("append queued live chunks for deletion: %v", ids)
		}
	})
	got, findErr := store.FindEntry(context.Background(), appendLockTestPath)
	if findErr != nil {
		t.Fatalf("find entry after scoped append: %v", findErr)
	}
	return err, got, req
}

func TestScopedAppendRejectsNilStoredChunkRange(t *testing.T) {
	err := scopeCheckChunkRanges([]*filer_pb.FileChunk{nil})
	if err == nil {
		t.Fatal("scope retention accepted a nil stored chunk")
	}
}

func TestScopeRetainsDataRefusesMalformedStoredRangesWithoutRequestedChunks(t *testing.T) {
	tests := []struct {
		name   string
		chunks []*filer_pb.FileChunk
	}{
		{name: "nil", chunks: []*filer_pb.FileChunk{nil}},
		{name: "negative", chunks: []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000088", -1, 1)}},
		{name: "overflow", chunks: []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000089", math.MaxInt64, 1)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stored := &filer_pb.Entry{Chunks: tc.chunks}
			if err := scopeRetainsData(stored, &filer_pb.Entry{}, nil); err == nil {
				t.Fatalf("retention check accepted malformed stored range: %v", tc.chunks)
			}
		})
	}
}

func TestScopedEmptyAppendRefusesMalformedStoredRangesUnderPathLock(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*filer.Entry)
	}{
		{
			name: "negative",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c0000008a", -1, 1)}
			},
		},
		{
			name: "overflow",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c0000008b", math.MaxInt64, 1)}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := newFileEntry(appendLockTestPath, 1)
			var expected *filer_pb.Entry
			err, got, req := runScopedAppendState(t, entry, nil, func(entry *filer.Entry) {
				tc.mutate(entry)
				expected = proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
			})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("empty append error = %v, want PermissionDenied", err)
			}
			if len(req.GetChunks()) != 0 {
				t.Fatalf("empty append request gained chunks: %v", req.GetChunks())
			}
			if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(expected)) {
				t.Fatalf("stored entry changed after empty append refusal: got=%v want=%v", got.ToProtoEntry(), expected)
			}
		})
	}
}

func normalizedScopedAppendEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	clone := proto.Clone(entry).(*filer_pb.Entry)
	for _, chunk := range clone.GetChunks() {
		if chunk != nil {
			chunk.FileId = chunk.GetFileIdString()
			chunk.Fid = nil
		}
	}
	return clone
}

func TestScopedAppendRefusesUnsafeStoredStatesUnderPathLock(t *testing.T) {
	const requestedID = "5,0c00000070"
	tests := []struct {
		name   string
		mutate func(*filer.Entry)
	}{
		{
			name: "chunkless sparse tail",
			mutate: func(entry *filer.Entry) {
				entry.FileSize = 100
			},
		},
		{
			name: "sparse tail beyond chunk extent",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000071", 0, 10)}
				entry.FileSize = 20
			},
		},
		{
			name: "stored manifest",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000072", Offset: 0, Size: 10, IsChunkManifest: true}}
				entry.FileSize = 10
			},
		},
		{
			name: "negative stored offset",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000073", -1, 1)}
			},
		},
		{
			name: "stored range overflow",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000074", math.MaxInt64, 1)}
			},
		},
		{
			name: "directory",
			mutate: func(entry *filer.Entry) {
				entry.Attr.Mode = os.ModeDir | 0755
			},
		},
		{
			name: "symlink",
			mutate: func(entry *filer.Entry) {
				entry.Attr.Mode = os.ModeSymlink | 0777
				entry.Attr.SymlinkTarget = "/target"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := newFileEntry(appendLockTestPath, 1)
			requested := []*filer_pb.FileChunk{scopedAppendStateChunk(requestedID, 0, 10)}
			var expected *filer_pb.Entry
			err, got, req := runScopedAppendState(t, entry, requested, func(entry *filer.Entry) {
				tc.mutate(entry)
				expected = proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
			})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("scoped append error = %v, want PermissionDenied", err)
			}
			if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(expected)) {
				t.Fatalf("stored entry changed after refusal: got=%v want=%v", got.ToProtoEntry(), expected)
			}
			if req.GetChunks()[0].GetOffset() != 0 {
				t.Fatalf("request chunk offset changed after refusal: %v", req.GetChunks())
			}
			for _, chunk := range got.GetChunks() {
				if chunk.GetFileIdString() == requestedID {
					t.Fatalf("requested chunk persisted after refusal: %v", got.GetChunks())
				}
			}
		})
	}
}

func TestScopedAppendRefusesUnsafeStoredStatesAtAdmission(t *testing.T) {
	const requestedID = "5,0c00000085"
	tests := []struct {
		name   string
		mutate func(*filer.Entry)
	}{
		{
			name: "sparse tail",
			mutate: func(entry *filer.Entry) {
				entry.FileSize = 100
			},
		},
		{
			name: "stored manifest",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000086", Offset: 0, Size: 10, IsChunkManifest: true}}
				entry.FileSize = 10
			},
		},
		{
			name: "stored range overflow",
			mutate: func(entry *filer.Entry) {
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000087", math.MaxInt64, 1)}
			},
		},
		{
			name: "directory",
			mutate: func(entry *filer.Entry) {
				entry.Attr.Mode = os.ModeDir | 0755
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := newFileEntry(appendLockTestPath, 1)
			tc.mutate(entry)
			expected := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
			fs, store := newAppendLockServer(t)
			close(store.release)
			store.entries[appendLockTestPath] = entry
			scope := newFilerGrpcScope(scopeConfig{
				signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
			}, fs)
			chunk := scopedAppendStateChunk(requestedID, 0, 10)
			req := &filer_pb.AppendToEntryRequest{
				Directory: "/data", EntryName: "app.log", Chunks: []*filer_pb.FileChunk{chunk},
				ChunkAuth: map[string]string{requestedID: scopedChunkProof(t, "append", requestedID, time.Hour)},
			}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				"authorization", "Bearer "+scopedToken("append", time.Hour, []string{"/data"}, nil),
			))
			called := false
			_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AppendToEntry"}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				called = true
				return fs.AppendToEntry(handlerCtx, handlerReq.(*filer_pb.AppendToEntryRequest))
			})
			if status.Code(err) != codes.PermissionDenied || called {
				t.Fatalf("admission error=%v called=%v, want PermissionDenied before handler", err, called)
			}
			if req.GetChunks()[0].GetOffset() != 0 {
				t.Fatalf("request chunk offset changed at admission refusal: %v", req.GetChunks())
			}
			fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
				if len(ids) > 0 {
					t.Fatalf("admission refusal queued live chunks for deletion: %v", ids)
				}
			})
			got, findErr := store.FindEntry(context.Background(), appendLockTestPath)
			if findErr != nil {
				t.Fatalf("find entry after admission refusal: %v", findErr)
			}
			if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(expected)) {
				t.Fatalf("stored entry changed after admission refusal: got=%v want=%v", got.ToProtoEntry(), expected)
			}
		})
	}
}

func TestScopedAppendRefusesRequestedRangeOverflowBeforeMutation(t *testing.T) {
	first := scopedAppendStateChunk("5,0c00000075", 0, math.MaxInt64)
	second := scopedAppendStateChunk("5,0c00000076", 0, 1)
	err, got, req := runScopedAppendState(t, newFileEntry(appendLockTestPath, 1), []*filer_pb.FileChunk{first, second}, nil)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped append error = %v, want PermissionDenied", err)
	}
	if len(got.GetChunks()) != 0 {
		t.Fatalf("chunks after refused overflow append = %v, want none", got.GetChunks())
	}
	if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(newFileEntry(appendLockTestPath, 1).ToProtoEntry())) {
		t.Fatalf("stored entry changed after overflow refusal: %v", got.ToProtoEntry())
	}
	if req.GetChunks()[0].GetOffset() != 0 || req.GetChunks()[1].GetOffset() != 0 {
		t.Fatalf("request chunks were mutated before overflow refusal: %v", req.GetChunks())
	}
}

func TestScopedAppendAllowsSafeStoredStates(t *testing.T) {
	const requestedID = "5,0c00000077"
	tests := []struct {
		name     string
		entry    func() *filer.Entry
		wantLast int64
	}{
		{
			name: "internal hole",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.Chunks = []*filer_pb.FileChunk{
					scopedAppendStateChunk("5,0c00000078", 0, 10),
					scopedAppendStateChunk("5,0c00000079", 20, 10),
				}
				entry.FileSize = 30
				return entry
			},
			wantLast: 30,
		},
		{
			name: "overlapping chunks",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.Chunks = []*filer_pb.FileChunk{
					scopedAppendStateChunk("5,0c0000007a", 0, 20),
					scopedAppendStateChunk("5,0c0000007b", 10, 20),
				}
				entry.FileSize = 30
				return entry
			},
			wantLast: 30,
		},
		{
			name: "file size equals extent",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c0000007c", 0, 10)}
				entry.FileSize = 10
				return entry
			},
			wantLast: 10,
		},
		{
			name: "compressed and encrypted chunk",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.Chunks = []*filer_pb.FileChunk{{
					FileId: "5,0c0000007d", Offset: 0, Size: 10,
					CipherKey: []byte{1, 2, 3}, IsCompressed: true,
				}}
				entry.FileSize = 10
				return entry
			},
			wantLast: 10,
		},
		{
			name: "cached remote entry",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000081", 0, 10)}
				entry.FileSize = 10
				entry.Remote = &filer_pb.RemoteEntry{StorageName: "remote-store", RemoteSize: 10, RemoteMtime: entry.Mtime.Unix()}
				return entry
			},
			wantLast: 10,
		},
		{
			name: "single hard link",
			entry: func() *filer.Entry {
				entry := newFileEntry(appendLockTestPath, 1)
				entry.HardLinkId = []byte("single-hard-link")
				entry.HardLinkCounter = 1
				entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c0000007e", 0, 10)}
				entry.FileSize = 10
				return entry
			},
			wantLast: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := tc.entry()
			before := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
			err, got, _ := runScopedAppendState(t, entry, []*filer_pb.FileChunk{scopedAppendStateChunk(requestedID, 0, 10)}, nil)
			if err != nil {
				t.Fatalf("scoped append: %v", err)
			}
			chunks := got.GetChunks()
			if len(chunks) != len(before.GetChunks())+1 {
				t.Fatalf("chunks after append = %v, want %d chunks", chunks, len(before.GetChunks())+1)
			}
			for i, want := range before.GetChunks() {
				if !proto.Equal(normalizedScopedAppendEntry(&filer_pb.Entry{Chunks: []*filer_pb.FileChunk{chunks[i]}}), normalizedScopedAppendEntry(&filer_pb.Entry{Chunks: []*filer_pb.FileChunk{want}})) {
					t.Fatalf("stored chunk %d changed: got=%v want=%v", i, chunks[i], want)
				}
			}
			if len(chunks) == 0 || chunks[len(chunks)-1].GetFileIdString() != requestedID || chunks[len(chunks)-1].GetOffset() != tc.wantLast {
				t.Fatalf("chunks after append = %v, want new chunk %s at offset %d", chunks, requestedID, tc.wantLast)
			}
			if got.Size() != uint64(tc.wantLast+10) {
				t.Fatalf("logical size after append = %d, want %d", got.Size(), tc.wantLast+10)
			}
		})
	}
}

func TestScopedAppendStillRefusesSharedHardLink(t *testing.T) {
	entry := newFileEntry(appendLockTestPath, 1)
	entry.HardLinkId = []byte("shared-hard-link")
	entry.HardLinkCounter = 2
	entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c0000007f", 0, 10)}
	entry.FileSize = 10
	err, got, _ := runScopedAppendState(t, entry, []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000080", 0, 10)}, nil)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped shared hard-link append error = %v, want PermissionDenied", err)
	}
	if len(got.GetChunks()) != 1 || got.GetChunks()[0].GetFileIdString() != "5,0c0000007f" {
		t.Fatalf("shared hard-link chunks after refusal = %v", got.GetChunks())
	}
}

func TestScopedAppendRechecksSharedHardLinkCountUnderPathLock(t *testing.T) {
	entry := newFileEntry(appendLockTestPath, 1)
	entry.HardLinkId = []byte("single-hard-link")
	entry.HardLinkCounter = 1
	entry.Chunks = []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000082", 0, 10)}
	entry.FileSize = 10
	var expected *filer_pb.Entry
	err, got, _ := runScopedAppendState(t, entry, []*filer_pb.FileChunk{scopedAppendStateChunk("5,0c00000083", 0, 10)}, func(entry *filer.Entry) {
		entry.HardLinkCounter = 2
		expected = proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped late shared hard-link append error = %v, want PermissionDenied", err)
	}
	if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(expected)) {
		t.Fatalf("stored hard-link entry changed after refusal: got=%v want=%v", got.ToProtoEntry(), expected)
	}
}
