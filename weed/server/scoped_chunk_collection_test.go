package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	collectionA = "scoped-collection-a"
	collectionB = "scoped-collection-b"
)

func setScopedCollection(t *testing.T, fs *FilerServer, prefix, collection string) {
	t.Helper()
	fs.filer.DirBucketsPath = "/buckets"
	if err := fs.filer.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: prefix,
		Collection:     collection,
	}); err != nil {
		t.Fatalf("set collection rule for %s: %v", prefix, err)
	}
}

func collectionScopeContext(holder string, prefixes ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken(holder, time.Hour, prefixes, nil),
	))
}

func collectionProof(t *testing.T, holder, fid, collection string) string {
	t.Helper()
	proof := security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, fid, holder, 1, collection)
	if proof == "" {
		t.Fatalf("mint assignment proof for %s in %q", fid, collection)
	}
	return string(proof)
}

func collectionChunk(fileID string, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileID, Offset: offset, Size: 10}
}

func collectionEntrySnapshot(entry *filer.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return collectionNormalizedEntry(entry.ToProtoEntry())
}

func assertCollectionEntryEqual(t *testing.T, got *filer.Entry, want *filer_pb.Entry) {
	t.Helper()
	if !proto.Equal(collectionEntrySnapshot(got), collectionNormalizedEntry(want)) {
		t.Fatalf("stored entry changed: got=%v want=%v", collectionEntrySnapshot(got), want)
	}
}

func collectionNormalizedEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	out := proto.Clone(entry).(*filer_pb.Entry)
	for _, chunk := range out.GetChunks() {
		if chunk != nil {
			chunk.FileId = chunk.GetFileIdString()
			chunk.Fid = nil
		}
	}
	return out
}

func TestScopedProofCollectionCreateUpdateAppend(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry", "AppendToEntry"} {
		for _, boundary := range []string{"same collection", "admission mismatch", "locked mismatch"} {
			t.Run(method+"/"+boundary, func(t *testing.T) {
				store := newRenameTestStore()
				var fs *FilerServer
				if method == "AppendToEntry" {
					var appendStore *appendHoldStore
					fs, appendStore = newAppendLockServer(t)
					close(appendStore.release)
					store = appendStore.renameTestStore
				} else {
					fs, _ = newScopeRetentionServer(t, store)
				}
				scope := newFilerGrpcScope(scopeConfig{
					signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
				}, fs)
				const dir, name = "/data", "app.log"
				const oldFID, newFID = "5,0c00000091", "5,0c00000092"
				path := util.NewFullPath(dir, name)
				initial := newFileEntry(string(path), 1)
				initial.Chunks = []*filer_pb.FileChunk{collectionChunk(oldFID, 0)}
				initial.FileSize = 10
				store.entries[string(path)] = initial
				before := collectionEntrySnapshot(initial)
				setScopedCollection(t, fs, dir, collectionA)
				proofCollection := collectionA
				if boundary == "admission mismatch" {
					proofCollection = collectionB
				}
				chunk := collectionChunk(newFID, 10)
				auth := map[string]string{newFID: collectionProof(t, "writer", newFID, proofCollection)}
				entry := &filer_pb.Entry{
					Name: name, Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 20},
					Chunks: []*filer_pb.FileChunk{collectionChunk(oldFID, 0), chunk},
				}
				var req any
				switch method {
				case "CreateEntry":
					req = &filer_pb.CreateEntryRequest{Directory: dir, Entry: entry, ChunkAuth: auth, SkipCheckParentDirectory: true}
				case "UpdateEntry":
					req = &filer_pb.UpdateEntryRequest{Directory: dir, Entry: entry, ChunkAuth: auth}
				case "AppendToEntry":
					// A successful append moves this to 10; refusal must leave 7.
					chunk.Offset = 7
					req = &filer_pb.AppendToEntryRequest{Directory: dir, EntryName: name, Chunks: []*filer_pb.FileChunk{chunk}, ChunkAuth: auth}
				}
				called := false
				resp, err := scope.unary(collectionScopeContext("writer", dir), req, &grpc.UnaryServerInfo{
					FullMethod: scopeFilerMethodPrefix + method,
				}, func(ctx context.Context, request any) (any, error) {
					called = true
					if boundary == "locked mismatch" {
						setScopedCollection(t, fs, dir, collectionB)
					}
					switch r := request.(type) {
					case *filer_pb.CreateEntryRequest:
						return fs.CreateEntry(ctx, r)
					case *filer_pb.UpdateEntryRequest:
						return fs.UpdateEntry(ctx, r)
					default:
						return fs.AppendToEntry(ctx, request.(*filer_pb.AppendToEntryRequest))
					}
				})
				if called != (boundary != "admission mismatch") {
					t.Fatalf("handler called=%v at %s", called, boundary)
				}
				stored, findErr := store.FindEntry(context.Background(), path)
				if findErr != nil {
					t.Fatal(findErr)
				}
				if boundary == "same collection" {
					if err != nil {
						t.Fatal(err)
					}
					if result, ok := resp.(*filer_pb.CreateEntryResponse); ok && (result.GetError() != "" || result.GetErrorCode() != filer_pb.FilerError_OK) {
						t.Fatalf("create response: %v", result)
					}
					chunks := stored.GetChunks()
					if len(chunks) != 2 || chunks[0].GetFileIdString() != oldFID || chunks[1].GetFileIdString() != newFID || chunks[1].Offset != 10 {
						t.Fatalf("successful write did not retain old bytes and add new bytes: %v", chunks)
					}
				} else {
					if status.Code(err) != codes.PermissionDenied {
						t.Fatalf("collection mismatch: %v", err)
					}
					assertCollectionEntryEqual(t, stored, before)
					if method == "AppendToEntry" && chunk.Offset != 7 {
						t.Fatalf("refused append changed offset to %d", chunk.Offset)
					}
				}
				fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
					if len(ids) != 0 {
						t.Fatalf("write queued retained chunks for deletion: %v", ids)
					}
				})
			})
		}
	}
}

func TestScopedProofCollectionChecksNewGrantsAndCollection(t *testing.T) {
	for _, targetCollection := range []string{collectionA, collectionB} {
		t.Run(targetCollection, func(t *testing.T) {
			store := newRenameTestStore()
			fs, scope := newScopeRetentionServer(t, store)
			sourceDir := aliceWS + "/collection-a"
			targetDir := aliceWS + "/collection-b"
			setScopedCollection(t, fs, sourceDir, collectionA)
			setScopedCollection(t, fs, targetDir, targetCollection)
			const fid = "5,0c0000009d"
			const holder = "same-jti-holder"
			// The two scope tokens keep the same holder id while changing its path grant.
			oldGrant := scopedToken(holder, time.Hour, []string{sourceDir}, nil)
			newGrant := scopedToken(holder, time.Hour, []string{targetDir}, nil)
			if oldGrant == newGrant {
				t.Fatal("scope tokens with changed grants unexpectedly identical")
			}
			assignResp, err := scope.unary(
				metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+oldGrant)),
				&filer_pb.AssignVolumeRequest{Path: scopeJoin(sourceDir, "file"), Count: 1},
				&grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AssignVolume"},
				func(context.Context, any) (any, error) {
					return &filer_pb.AssignVolumeResponse{FileId: fid, Count: 1, Collection: collectionA}, nil
				},
			)
			if err != nil {
				t.Fatalf("issue source collection assignment proof: %v", err)
			}
			issued, ok := assignResp.(*filer_pb.AssignVolumeResponse)
			if !ok || issued.GetAssignAuth() == "" {
				t.Fatalf("issue source collection assignment proof response=%#v", assignResp)
			}
			req := &filer_pb.CreateEntryRequest{
				Directory:                targetDir,
				SkipCheckParentDirectory: true,
				Entry:                    &filer_pb.Entry{Name: "changed-grant", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{collectionChunk(fid, 0)}},
				ChunkAuth:                map[string]string{fid: issued.GetAssignAuth()},
			}
			// Even a valid same-collection proof cannot extend the old token's grants.
			_, denied := scope.unary(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+oldGrant)), req,
				&grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "CreateEntry"}, func(context.Context, any) (any, error) {
					t.Fatal("old grant reached the destination handler")
					return nil, nil
				})
			if status.Code(denied) != codes.PermissionDenied {
				t.Fatalf("old grant accepted outside its path: %v", denied)
			}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+newGrant))
			called := false
			_, err = scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "CreateEntry"}, func(ctx context.Context, request any) (any, error) {
				called = true
				return fs.CreateEntry(ctx, request.(*filer_pb.CreateEntryRequest))
			})
			stored, findErr := store.FindEntry(context.Background(), util.NewFullPath(targetDir, "changed-grant"))
			if targetCollection == collectionA {
				if err != nil || !called || findErr != nil {
					t.Fatalf("same-collection changed-grant write: err=%v called=%v find=%v", err, called, findErr)
				}
				if len(stored.GetChunks()) != 1 || stored.GetChunks()[0].GetFileIdString() != fid {
					t.Fatalf("same-collection changed-grant write lost the proven chunk: %v", stored.GetChunks())
				}
			} else {
				if status.Code(err) != codes.PermissionDenied || called {
					t.Fatalf("same-holder changed-collection write err=%v called=%v", err, called)
				}
				if findErr != filer_pb.ErrNotFound {
					t.Fatalf("refused write persisted entry: %v", findErr)
				}
			}
		})
	}
}

func TestScopedProofCollectionRejectsSaveAfterCrossCollectionRename(t *testing.T) {
	store := newRenameTestStore()
	sourceDir := aliceWS + "/collection-a"
	targetDir := aliceWS + "/collection-b"
	const name = "renamed"
	sourcePath := scopeJoin(sourceDir, name)
	targetPath := scopeJoin(targetDir, name)
	initial := newFileEntry(sourcePath, 1)
	initial.Chunks = []*filer_pb.FileChunk{collectionChunk("5,0c0000009e", 0)}
	initial.FileSize = 10
	store.entries[sourceDir] = newDirectoryEntry(sourceDir, 2)
	store.entries[targetDir] = newDirectoryEntry(targetDir, 3)
	store.entries[sourcePath] = initial
	fs, scope := newScopeRetentionServer(t, store)
	setScopedCollection(t, fs, sourceDir, collectionA)
	setScopedCollection(t, fs, targetDir, collectionB)
	ctx := collectionScopeContext("rename-holder", aliceWS)
	_, err := scope.unary(ctx, &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: sourceDir,
		OldName:      name,
		NewDirectory: targetDir,
		NewName:      name,
	}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AtomicRenameEntry"}, func(handlerCtx context.Context, request any) (any, error) {
		return fs.AtomicRenameEntry(handlerCtx, request.(*filer_pb.AtomicRenameEntryRequest))
	})
	if err != nil {
		t.Fatalf("cross-collection rename: %v", err)
	}
	if _, findErr := store.FindEntry(context.Background(), util.FullPath(sourcePath)); findErr != filer_pb.ErrNotFound {
		t.Fatalf("source survived cross-collection rename: %v", findErr)
	}
	moved, findErr := store.FindEntry(context.Background(), util.FullPath(targetPath))
	if findErr != nil {
		t.Fatalf("find renamed destination: %v", findErr)
	}
	beforeSave := collectionEntrySnapshot(moved)
	const fid = "5,0c0000009f"
	request := &filer_pb.UpdateEntryRequest{
		Directory: targetDir,
		Entry: &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 20},
			Chunks:     []*filer_pb.FileChunk{collectionChunk("5,0c0000009e", 0), collectionChunk(fid, 10)},
		},
		ChunkAuth: map[string]string{fid: collectionProof(t, "rename-holder", fid, collectionA)},
	}
	called := false
	_, err = scope.unary(ctx, request, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "UpdateEntry"}, func(handlerCtx context.Context, handlerRequest any) (any, error) {
		called = true
		return fs.UpdateEntry(handlerCtx, handlerRequest.(*filer_pb.UpdateEntryRequest))
	})
	if status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("save after cross-collection rename err=%v called=%v, want admission PermissionDenied", err, called)
	}
	afterSave, findErr := store.FindEntry(context.Background(), util.FullPath(targetPath))
	if findErr != nil {
		t.Fatalf("find destination after refused save: %v", findErr)
	}
	assertCollectionEntryEqual(t, afterSave, beforeSave)
	if request.GetEntry().GetChunks()[1].GetOffset() != 10 {
		t.Fatalf("refused save changed requested offset: %v", request.GetEntry().GetChunks())
	}
}
