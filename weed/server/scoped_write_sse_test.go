package weed_server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestScopeHasSSEUsesTheReadPathMarkerAllowlist(t *testing.T) {
	for _, key := range scopeSSEExtendedKeys {
		t.Run(key, func(t *testing.T) {
			entry := &filer_pb.Entry{Extended: map[string][]byte{key: []byte("marker")}}
			if !scopeHasSSE(entry) {
				t.Fatalf("scopeHasSSE did not recognize %q", key)
			}
		})
	}

	for _, tc := range []struct {
		name  string
		entry *filer_pb.Entry
		want  bool
	}{
		{name: "nil entry", entry: nil},
		{name: "plain chunk metadata", entry: &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", CipherKey: []byte{1}, IsCompressed: true}}}},
		{name: "SSE chunk type", entry: &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", SseType: filer_pb.SSEType_SSE_C}}}, want: true},
		{name: "SSE chunk metadata", entry: &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", SseMetadata: []byte("metadata")}}}, want: true},
		{name: "unrelated extended attribute", entry: &filer_pb.Entry{Extended: map[string][]byte{"x-amz-server-side-encryption-unknown": []byte("marker")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := scopeHasSSE(tc.entry); got != tc.want {
				t.Fatalf("scopeHasSSE = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScopeCheckSSEStateComparesPresenceAndBytes(t *testing.T) {
	for _, key := range scopeSSEExtendedKeys {
		t.Run("add/"+key, func(t *testing.T) {
			requested := &filer_pb.Entry{Extended: map[string][]byte{key: []byte("marker")}}
			if err := scopeCheckSSEState(nil, requested, false); err == nil {
				t.Fatalf("scopeCheckSSEState allowed adding %q", key)
			}
		})
		t.Run("remove/"+key, func(t *testing.T) {
			stored := &filer_pb.Entry{Extended: map[string][]byte{key: []byte("marker")}}
			if err := scopeCheckSSEState(stored, &filer_pb.Entry{}, false); err == nil {
				t.Fatalf("scopeCheckSSEState allowed removing %q", key)
			}
		})
		t.Run("empty-presence/"+key, func(t *testing.T) {
			requested := &filer_pb.Entry{Extended: map[string][]byte{key: {}}}
			if err := scopeCheckSSEState(nil, requested, false); err == nil {
				t.Fatalf("scopeCheckSSEState allowed adding empty %q", key)
			}
		})
	}

	stored := &filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.SeaweedFSSSES3Key: []byte("same"),
	}}
	identical := proto.Clone(stored).(*filer_pb.Entry)
	if err := scopeCheckSSEState(stored, identical, false); err != nil {
		t.Fatalf("scopeCheckSSEState rejected identical metadata: %v", err)
	}

	changed := proto.Clone(stored).(*filer_pb.Entry)
	changed.Extended[s3_constants.SeaweedFSSSES3Key] = []byte("changed")
	if err := scopeCheckSSEState(stored, changed, false); err == nil {
		t.Fatal("scopeCheckSSEState allowed changed metadata")
	}

	unknown := &filer_pb.Entry{Extended: map[string][]byte{"x-custom": []byte("changed")}}
	if err := scopeCheckSSEState(nil, unknown, false); err != nil {
		t.Fatalf("scopeCheckSSEState rejected unrelated metadata: %v", err)
	}
}

func TestScopedSSEMetadataAdmissionRules(t *testing.T) {
	const storedFid = "5,0c00000060"
	tests := []struct {
		name        string
		method      string
		stored      *filer_pb.Entry
		requested   *filer_pb.Entry
		wantDenied  bool
		wantHandler bool
	}{
		{
			name:   "chunkless add to plain update",
			method: "UpdateEntry",
			stored: entryWith("metadata", storedFid),
			requested: &filer_pb.Entry{
				Name:     "metadata",
				Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)},
			},
			wantDenied: true,
		},
		{
			name:   "chunk-bearing add to plain update",
			method: "UpdateEntry",
			stored: entryWith("metadata", storedFid),
			requested: &filer_pb.Entry{
				Name:     "metadata",
				Chunks:   []*filer_pb.FileChunk{{FileId: storedFid, Size: 10}},
				Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)},
			},
			wantDenied: true,
		},
		{
			name:   "change on SSE update",
			method: "UpdateEntry",
			stored: &filer_pb.Entry{Name: "metadata", Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}},
			requested: &filer_pb.Entry{
				Name:     "metadata",
				Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmKMS)},
			},
			wantDenied: true,
		},
		{
			name:   "removal from SSE update",
			method: "UpdateEntry",
			stored: &filer_pb.Entry{Name: "metadata", Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}},
			requested: &filer_pb.Entry{
				Name: "metadata",
			},
			wantDenied:  true,
			wantHandler: true,
		},
		{
			name:        "removal from SSE create overwrite",
			method:      "CreateEntry",
			stored:      &filer_pb.Entry{Name: "metadata", Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}},
			requested:   &filer_pb.Entry{Name: "metadata"},
			wantDenied:  true,
			wantHandler: true,
		},
		{
			name:   "identical metadata update",
			method: "UpdateEntry",
			stored: &filer_pb.Entry{Name: "metadata", Extended: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256),
				"x-custom":                           []byte("old"),
			}},
			requested: &filer_pb.Entry{
				Name: "metadata",
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256),
					"x-custom":                           []byte("new"),
				},
			},
		},
		{
			name:   "new create chunkless SSE metadata",
			method: "CreateEntry",
			requested: &filer_pb.Entry{
				Name:     "metadata",
				Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)},
			},
			wantDenied: true,
		},
		{
			name:   "new create chunked SSE metadata",
			method: "CreateEntry",
			requested: &filer_pb.Entry{
				Name:     "metadata",
				Chunks:   []*filer_pb.FileChunk{{FileId: "5,0c00000061", Size: 10}},
				Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)},
			},
			wantDenied: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newRenameTestStore()
			store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
			path := util.FullPath(scopeJoin(aliceWS, tc.requested.GetName()))
			if tc.stored != nil {
				if err := store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, proto.Clone(tc.stored).(*filer_pb.Entry))); err != nil {
					t.Fatalf("seed stored entry: %v", err)
				}
			}
			var before *filer_pb.Entry
			if tc.stored != nil {
				seeded, findErr := store.FindEntry(context.Background(), path)
				if findErr != nil {
					t.Fatalf("snapshot stored entry: %v", findErr)
				}
				before = seeded.ToProtoEntry()
			}
			fs, scope := newScopeRetentionServer(t, store)
			backend := &scopeCountingBackend{scopeBackend: scope.backend}
			scope.backend = backend
			var req any
			if tc.method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: tc.requested, SkipCheckParentDirectory: true}
			} else {
				req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: tc.requested}
			}
			for _, chunk := range tc.requested.GetChunks() {
				if chunk.GetFileIdString() != "" {
					proof := scopedChunkProof(t, "retention", chunk.GetFileIdString(), time.Hour)
					switch typed := req.(type) {
					case *filer_pb.CreateEntryRequest:
						typed.ChunkAuth = map[string]string{chunk.GetFileIdString(): proof}
					case *filer_pb.UpdateEntryRequest:
						typed.ChunkAuth = map[string]string{chunk.GetFileIdString(): proof}
					}
				}
			}
			called := false
			_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + tc.method}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				called = true
				if tc.wantHandler && backend.lookups != 0 {
					t.Fatalf("chunkless SSE removal made %d admission lookups, want zero", backend.lookups)
				}
				if tc.method == "CreateEntry" {
					return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
				}
				return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
			})
			if tc.wantDenied {
				if status.Code(err) != codes.PermissionDenied || called != tc.wantHandler {
					t.Fatalf("error=%v called=%v, want PermissionDenied with handler called=%v", err, called, tc.wantHandler)
				}
				got, findErr := store.FindEntry(context.Background(), path)
				if before == nil {
					if findErr == nil {
						t.Fatalf("refused create left an entry: %#v", got.ToProtoEntry())
					}
				} else if findErr != nil || !proto.Equal(got.ToProtoEntry(), before) {
					t.Fatalf("stored entry changed after refusal: got=%#v want=%#v err=%v", got.ToProtoEntry(), before, findErr)
				}
				return
			}
			if err != nil || !called {
				t.Fatalf("identical metadata update error=%v called=%v, want admitted handler", err, called)
			}
			got, findErr := store.FindEntry(context.Background(), util.FullPath(scopeJoin(aliceWS, tc.requested.GetName())))
			if findErr != nil {
				t.Fatalf("find identical metadata entry: %v", findErr)
			}
			if !bytes.Equal(got.Extended[s3_constants.AmzServerSideEncryption], tc.requested.Extended[s3_constants.AmzServerSideEncryption]) {
				t.Fatalf("identical SSE metadata = %q, want %q", got.Extended[s3_constants.AmzServerSideEncryption], tc.requested.Extended[s3_constants.AmzServerSideEncryption])
			}
			if !bytes.Equal(got.Extended["x-custom"], []byte("new")) {
				t.Fatalf("unrelated metadata = %q, want new", got.Extended["x-custom"])
			}
		})
	}
}

func TestScopedSSEMetadataRecheckedUnderPathLock(t *testing.T) {
	const name = "metadata-race"
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		t.Run(method, func(t *testing.T) {
			store := newRenameTestStore()
			store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
			plain := &filer_pb.Entry{Name: name}
			if err := store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, plain)); err != nil {
				t.Fatalf("seed plain entry: %v", err)
			}
			fs, scope := newScopeRetentionServer(t, store)
			requested := &filer_pb.Entry{Name: name}
			var req any
			if method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: requested, SkipCheckParentDirectory: true}
			} else {
				req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested}
			}
			called := false
			_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + method,
			}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				called = true
				changed := &filer_pb.Entry{Name: name, Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}}
				if updateErr := store.UpdateEntry(context.Background(), filer.FromPbEntry(aliceWS, changed)); updateErr != nil {
					t.Fatalf("inject SSE metadata change: %v", updateErr)
				}
				if method == "CreateEntry" {
					return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
				}
				return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
			})
			if status.Code(err) != codes.PermissionDenied || !called {
				t.Fatalf("under-lock metadata change error=%v called=%v, want PermissionDenied after handler entry", err, called)
			}
			got, findErr := store.FindEntry(context.Background(), util.FullPath(scopeJoin(aliceWS, name)))
			if findErr != nil {
				t.Fatalf("find injected SSE entry: %v", findErr)
			}
			if !bytes.Equal(got.Extended[s3_constants.AmzServerSideEncryption], []byte(s3_constants.SSEAlgorithmAES256)) {
				t.Fatalf("stored SSE metadata after refusal = %q, want AES256", got.Extended[s3_constants.AmzServerSideEncryption])
			}
		})
	}
}

func TestScopedSSEChunkMarkersRefusedAtAdmission(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setSSEType bool
		metadata   []byte
	}{
		{name: "chunk type", setSSEType: true},
		{name: "chunk metadata", metadata: []byte("metadata")},
	} {
		for _, method := range []string{"CreateEntry", "UpdateEntry"} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				h := newScopeHarness(t)
				h.fake.putDir(aliceWS)
				name := "sse-marked"
				fid := "5,0c00000056"
				if method == "UpdateEntry" {
					h.fake.put(aliceWS+"/"+name, entryWith(name, "5,0c00000055"))
				}
				chunk := &filer_pb.FileChunk{FileId: fid, Size: 10, SseMetadata: tc.metadata}
				if tc.setSSEType {
					chunk.SseType = filer_pb.SSEType_SSE_C
				}
				entry := &filer_pb.Entry{Name: name, Chunks: []*filer_pb.FileChunk{chunk}}
				proof := scopedChunkProof(t, "alice", fid, time.Hour)
				expectScopeDenied(t, h, method, tc.name, func() error {
					if method == "CreateEntry" {
						_, err := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil)).CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
							Directory: aliceWS, Entry: entry, SkipCheckParentDirectory: true, ChunkAuth: map[string]string{fid: proof},
						})
						return err
					}
					_, err := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil)).UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
						Directory: aliceWS, Entry: entry, ChunkAuth: map[string]string{fid: proof},
					})
					return err
				})
			})
		}
	}
}

func TestScopedSSEChunkMarkersRefusedOnAppend(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setSSEType bool
		metadata   []byte
	}{
		{name: "chunk type", setSSEType: true},
		{name: "chunk metadata", metadata: []byte("metadata")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, store := newAppendLockServer(t)
			close(store.release)
			store.entries[appendLockTestPath] = newFileEntry(appendLockTestPath, 1)
			before := proto.Clone(store.entries[appendLockTestPath].ToProtoEntry()).(*filer_pb.Entry)
			const fid = "5,0c00000059"
			chunk := &filer_pb.FileChunk{FileId: fid, Size: 10, SseMetadata: tc.metadata}
			if tc.setSSEType {
				chunk.SseType = filer_pb.SSEType_SSE_C
			}
			req := &filer_pb.AppendToEntryRequest{
				Directory: "/data", EntryName: "app.log", Chunks: []*filer_pb.FileChunk{chunk},
				ChunkAuth: map[string]string{fid: scopedChunkProof(t, "append", fid, time.Hour)},
			}
			called := false
			_, err := newFilerGrpcScope(scopeConfig{
				signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
			}, fs).unary(scopedRetentionContextForAppend(), req, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + "AppendToEntry",
			}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				called = true
				return fs.AppendToEntry(handlerCtx, handlerReq.(*filer_pb.AppendToEntryRequest))
			})
			if status.Code(err) != codes.PermissionDenied || called {
				t.Fatalf("append SSE marker error=%v called=%v, want PermissionDenied before handler", err, called)
			}
			if got := store.entries[appendLockTestPath].ToProtoEntry(); !proto.Equal(got, before) {
				t.Fatalf("stored entry changed after refused append: got=%v want=%v", got, before)
			}
		})
	}
}

func scopedRetentionContextForAppend() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+scopedToken("append", time.Hour, []string{"/data"}, nil),
	))
}

func TestGrpcScopeTokenlessSSEMetadataUpdateRemainsAllowed(t *testing.T) {
	h := newScopeHarness(t)
	_, err := h.client("").UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
		Directory: aliceWS,
		Entry: &filer_pb.Entry{
			Name:     "tokenless-sse",
			Extended: map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)},
		},
	})
	if err != nil {
		t.Fatalf("tokenless SSE metadata update: %v", err)
	}
	if got := len(h.fake.received("UpdateEntry")); got != 1 {
		t.Fatalf("tokenless SSE metadata update reached handler %d times, want 1", got)
	}
}

func scopedSSEEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
		Chunks: []*filer_pb.FileChunk{{
			FileId:      "5,0c00000055",
			Size:        10,
			SseType:     filer_pb.SSEType_SSE_S3,
			SseMetadata: []byte("stored-sse3-chunk"),
		}},
	}
}

func TestScopedStoredSSEChunksRefusedAtAdmissionAndUnderLock(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, late := range []bool{false, true} {
			caseName := "early"
			if late {
				caseName = "late"
			}
			t.Run(method+"/"+caseName, func(t *testing.T) {
				store := newRenameTestStore()
				store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
				name := "stored-sse"
				path := scopeJoin(aliceWS, name)
				plain := &filer_pb.Entry{
					Name:       name,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
					Chunks:     []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
				}
				storedSSE := scopedSSEEntry(name)
				if late {
					store.entries[path] = filer.FromPbEntry(aliceWS, plain)
				} else {
					store.entries[path] = filer.FromPbEntry(aliceWS, storedSSE)
				}
				expected := proto.Clone(store.entries[path].ToProtoEntry()).(*filer_pb.Entry)
				fs, scope := newScopeRetentionServer(t, store)
				fid := "5,0c00000056"
				requested := &filer_pb.Entry{
					Name:       name,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
					Chunks:     []*filer_pb.FileChunk{{FileId: fid, Size: 10}},
				}
				var req any
				if method == "CreateEntry" {
					req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: requested, SkipCheckParentDirectory: true, ChunkAuth: map[string]string{fid: scopedChunkProof(t, "retention", fid, time.Hour)}}
				} else {
					req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested, ChunkAuth: map[string]string{fid: scopedChunkProof(t, "retention", fid, time.Hour)}}
				}

				called := false
				_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(handlerCtx context.Context, handlerReq any) (any, error) {
					called = true
					if late {
						if updateErr := store.UpdateEntry(context.Background(), filer.FromPbEntry(aliceWS, proto.Clone(storedSSE).(*filer_pb.Entry))); updateErr != nil {
							t.Fatalf("inject stored SSE state: %v", updateErr)
						}
						expected = proto.Clone(store.entries[path].ToProtoEntry()).(*filer_pb.Entry)
					}
					if method == "CreateEntry" {
						return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
					}
					return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
				})
				if status.Code(err) != codes.PermissionDenied || called != late {
					t.Fatalf("%s late=%v err=%v called=%v, want PermissionDenied and called=%v", method, late, err, called, late)
				}
				got, findErr := store.FindEntry(context.Background(), util.FullPath(path))
				if findErr != nil {
					t.Fatalf("find stored entry: %v", findErr)
				}
				if !proto.Equal(normalizedScopedAppendEntry(got.ToProtoEntry()), normalizedScopedAppendEntry(expected)) {
					t.Fatalf("stored SSE entry changed after refusal: got=%v want=%v", got.ToProtoEntry(), expected)
				}
			})
		}
	}
}

func TestScopedStoredSSEAppendRefused(t *testing.T) {
	t.Run("admission", func(t *testing.T) {
		fs, store := newAppendLockServer(t)
		close(store.release)
		entry := newFileEntry(appendLockTestPath, 1)
		entry.Extended = map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}
		store.entries[appendLockTestPath] = entry
		before := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)

		scope := newFilerGrpcScope(scopeConfig{
			signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
		}, fs)
		newFid := "5,0c00000057"
		_, err := scope.unary(scopedRetentionContextForAppend(), &filer_pb.AppendToEntryRequest{
			Directory: "/data", EntryName: "app.log",
			Chunks:    []*filer_pb.FileChunk{{FileId: newFid, Size: 10}},
			ChunkAuth: map[string]string{newFid: scopedChunkProof(t, "append", newFid, time.Hour)},
		}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AppendToEntry"}, func(handlerCtx context.Context, req any) (any, error) {
			return fs.AppendToEntry(handlerCtx, req.(*filer_pb.AppendToEntryRequest))
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("scoped append to stored SSE entry error = %v, want PermissionDenied", err)
		}
		stored, findErr := store.FindEntry(context.Background(), appendLockTestPath)
		if findErr != nil {
			t.Fatalf("find stored SSE append entry: %v", findErr)
		}
		if !proto.Equal(stored.ToProtoEntry(), before) {
			t.Fatalf("stored entry changed after refused SSE append: got=%v want=%v", stored.ToProtoEntry(), before)
		}
	})

	t.Run("under lock", func(t *testing.T) {
		const newFid = "5,0c0000005a"
		entry := newFileEntry(appendLockTestPath, 1)
		expected := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
		expected.Extended = map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}
		err, stored, _ := runScopedAppendState(t, entry, []*filer_pb.FileChunk{{FileId: newFid, Size: 10}}, func(entry *filer.Entry) {
			entry.Extended = map[string][]byte{s3_constants.AmzServerSideEncryption: []byte(s3_constants.SSEAlgorithmAES256)}
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("scoped append after stored SSE race error = %v, want PermissionDenied", err)
		}
		if !proto.Equal(stored.ToProtoEntry(), expected) {
			t.Fatalf("stored entry changed after under-lock SSE refusal: got=%v want=%v", stored.ToProtoEntry(), expected)
		}
	})
}

func TestScopedChunkCipherAndCompressionMetadataRemainAllowed(t *testing.T) {
	const fid = "5,0c00000058"
	chunk := &filer_pb.FileChunk{FileId: fid, Size: 10, CipherKey: []byte{1, 2, 3}, IsCompressed: true}
	if err := scopeCheckEntryShape(&filer_pb.Entry{Chunks: []*filer_pb.FileChunk{proto.Clone(chunk).(*filer_pb.FileChunk)}}); err != nil {
		t.Fatalf("plain cipher/compression metadata rejected: %v", err)
	}

	stored := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{proto.Clone(chunk).(*filer_pb.FileChunk)}}
	requested := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{proto.Clone(chunk).(*filer_pb.FileChunk)}}
	if err := scopeRetainsData(stored, requested, nil); err != nil {
		t.Fatalf("retained cipher/compression metadata rejected: %v", err)
	}

	store := newRenameTestStore()
	store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
	name := "ciphered"
	fs, scope := newScopeRetentionServer(t, store)
	request := &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{proto.Clone(chunk).(*filer_pb.FileChunk)}}
	_, err := scope.unary(scopedRetentionContext(), &filer_pb.CreateEntryRequest{
		Directory: aliceWS, Entry: request, SkipCheckParentDirectory: true,
		ChunkAuth: map[string]string{fid: scopedChunkProof(t, "retention", fid, time.Hour)},
	}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "CreateEntry"}, func(handlerCtx context.Context, req any) (any, error) {
		return fs.CreateEntry(handlerCtx, req.(*filer_pb.CreateEntryRequest))
	})
	if err != nil {
		t.Fatalf("scoped ciphered/compressed CreateEntry: %v", err)
	}
	got, findErr := store.FindEntry(context.Background(), util.FullPath(scopeJoin(aliceWS, name)))
	if findErr != nil {
		t.Fatalf("find ciphered entry: %v", findErr)
	}
	if len(got.GetChunks()) != 1 || !bytes.Equal(got.GetChunks()[0].GetCipherKey(), chunk.GetCipherKey()) || !got.GetChunks()[0].GetIsCompressed() {
		t.Fatalf("cipher/compression metadata changed: %#v", got.GetChunks())
	}
}
