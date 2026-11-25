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
	"google.golang.org/grpc/status"
)

func TestScopedChunkProofRefusesPhysicalIdentityAliases(t *testing.T) {
	const suffix = "5,0c00000055_1"
	const canonical = "5,0d00000055"
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, storedID := range []string{suffix, canonical} {
			for _, afterAdmission := range []bool{false, true} {
				stage := "admission"
				if afterAdmission {
					stage = "locked handler"
				}
				t.Run(method+"/"+storedID+"/"+stage, func(t *testing.T) {
					store := newRenameTestStore()
					store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
					fs, scope := newScopeRetentionServer(t, store)
					initial := &filer_pb.Entry{Name: "alias", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{{FileId: storedID, Size: 10}}}
					if !afterAdmission {
						if err := store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, initial)); err != nil {
							t.Fatal(err)
						}
					} else if method == "UpdateEntry" {
						store.entries[aliceWS+"/alias"] = newFileEntry(aliceWS+"/alias", 2)
					}
					chunk := &filer_pb.FileChunk{FileId: suffix, Size: 10}
					fid := suffix
					if storedID == suffix {
						chunk.FileId = ""
						chunk.Fid = &filer_pb.FileId{VolumeId: 5, FileKey: 13, Cookie: 85}
						fid = canonical
					}
					requested := &filer_pb.Entry{Name: "alias", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{chunk}}
					proof := security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, "5,0c00000055", "retention", 2, "")
					auth := map[string]string{fid: string(proof)}
					var req any = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested, ChunkAuth: auth}
					if method == "CreateEntry" {
						req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: requested, ChunkAuth: auth, SkipCheckParentDirectory: true}
					}
					called := false
					_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(ctx context.Context, req any) (any, error) {
						called = true
						if afterAdmission {
							if err := store.InsertEntry(ctx, filer.FromPbEntry(aliceWS, initial)); err != nil {
								t.Fatal(err)
							}
						}
						if method == "CreateEntry" {
							return fs.CreateEntry(ctx, req.(*filer_pb.CreateEntryRequest))
						}
						return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
					})
					if status.Code(err) != codes.PermissionDenied || called != afterAdmission {
						t.Errorf("err=%v called=%v, want refusal at %s", err, called, stage)
					}
					stored, err := store.FindEntry(context.Background(), util.NewFullPath(aliceWS, "alias"))
					if err != nil || !retentionEntryDataEqual(stored, initial) {
						t.Errorf("stored reference changed: entry=%v err=%v", stored, err)
					}
					fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
						if len(ids) > 0 {
							t.Errorf("live needle queued for deletion: %v", ids)
						}
					})
				})
			}
		}
	}
}

func TestScopedChunkProofRefusesAliasesWithinNewEntry(t *testing.T) {
	store := newRenameTestStore()
	store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
	fs, scope := newScopeRetentionServer(t, store)
	proof := string(security.GenJwtForAssign(security.SigningKey(scopeTestKey), time.Hour, "5,0c00000055", "retention", 2, ""))
	req := &filer_pb.CreateEntryRequest{
		Directory: aliceWS, SkipCheckParentDirectory: true,
		Entry: &filer_pb.Entry{Name: "aliases", Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Chunks: []*filer_pb.FileChunk{
			{FileId: "5,0c00000055_1", Size: 10},
			{Fid: &filer_pb.FileId{VolumeId: 5, FileKey: 13, Cookie: 85}, Offset: 10, Size: 10},
		}},
		ChunkAuth: map[string]string{"5,0c00000055_1": proof, "5,0d00000055": proof},
	}
	_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "CreateEntry"}, func(ctx context.Context, req any) (any, error) {
		return fs.CreateEntry(ctx, req.(*filer_pb.CreateEntryRequest))
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("aliased chunk list error=%v, want PermissionDenied", err)
	}
	if _, err := store.FindEntry(context.Background(), util.NewFullPath(aliceWS, "aliases")); err != filer_pb.ErrNotFound {
		t.Fatalf("aliased chunk list persisted: %v", err)
	}
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		if len(ids) > 0 {
			t.Errorf("refused create queued live chunks: %v", ids)
		}
	})
}

func TestScopedChunkProofIdentityBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested string
	}{
		{"same spelling", "5,0c00000055_1"},
		{"different volume", "6,0d00000055"},
		{"different cookie", "5,0d00000056"},
		{"different needle", "5,0e00000055"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stored := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055_1", Size: 10}}}
			requested := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{
				{FileId: tc.requested, Size: 10}, {FileId: tc.requested, Offset: 10, Size: 10},
			}}
			if err := scopeRetainsData(stored, requested, func(string) bool { return true }); err != nil {
				t.Fatalf("unchanged spelling or distinct physical chunk refused: %v", err)
			}
		})
	}
}
