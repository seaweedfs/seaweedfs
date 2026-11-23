package weed_server

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestScopeWritesCannotChangeAnotherHardLinkName(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, change := range []string{"truncate", "rewrite layout", "metadata", "detach with inline data", "drop link and data", "single remaining link"} {
			t.Run(method+"/"+change, func(t *testing.T) {
				store := newRenameTestStore()
				store.kv = make(map[string][]byte)
				fs, scope := newScopeRetentionServer(t, store)
				original := &filer_pb.Entry{
					Name: "f", HardLinkId: []byte("shared-hard-link"), HardLinkCounter: 2,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
					Chunks:     []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
				}
				if change == "detach with inline data" {
					original.Chunks = nil
					original.Content = []byte("existing data")
				}
				if change == "single remaining link" {
					original.HardLinkCounter = 1
				}
				dirs := []string{aliceWS, bobWS}
				if change == "single remaining link" {
					dirs = dirs[:1]
				}
				for _, dir := range dirs {
					if err := fs.filer.Store.InsertEntry(context.Background(), filer.FromPbEntry(dir, original)); err != nil {
						t.Fatal(err)
					}
				}
				requested := proto.Clone(original).(*filer_pb.Entry)
				switch change {
				case "truncate", "single remaining link":
					requested.Chunks = nil
				case "rewrite layout":
					requested.Chunks[0].Offset = 100
				case "metadata":
					requested.Attributes.FileMode = 0600
				case "detach with inline data":
					requested.HardLinkId = nil
					requested.HardLinkCounter = 0
				case "drop link and data":
					requested.HardLinkId = nil
					requested.HardLinkCounter = 0
					requested.Chunks = nil
				}
				var req any = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested}
				if method == "CreateEntry" {
					req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: requested, SkipCheckParentDirectory: true}
				}
				resp, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(ctx context.Context, req any) (any, error) {
					if method == "CreateEntry" {
						return fs.CreateEntry(ctx, req.(*filer_pb.CreateEntryRequest))
					}
					return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
				})
				allowed := change == "drop link and data" || change == "single remaining link"
				if allowed {
					if err != nil {
						t.Fatalf("allowed mutation: %v", err)
					}
					if created, ok := resp.(*filer_pb.CreateEntryResponse); ok && created.GetError() != "" {
						t.Fatal(created.GetError())
					}
				} else if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("mutation error = %v, want PermissionDenied", err)
				}
				got, err := fs.filer.FindEntry(context.Background(), util.FullPath(aliceWS+"/f"))
				if err != nil {
					t.Fatal(err)
				}
				want := original
				if allowed {
					want = requested
				}
				if !retentionEntryDataEqual(got, want) || !bytes.Equal(got.HardLinkId, want.HardLinkId) || uint32(got.Mode) != want.Attributes.FileMode {
					t.Fatalf("target changed unexpectedly: %v", got.ToProtoEntry())
				}
				if change != "single remaining link" {
					other, err := fs.filer.FindEntry(context.Background(), util.FullPath(bobWS+"/f"))
					if err != nil {
						t.Fatal(err)
					}
					if !retentionEntryDataEqual(other, original) || uint32(other.Mode) != original.Attributes.FileMode {
						t.Fatalf("out-of-scope name changed: %v", other.ToProtoEntry())
					}
				}
			})
		}
	}
}

func TestScopeAppendCannotChangeSharedHardLink(t *testing.T) {
	fs, store := newAppendLockServer(t)
	close(store.release)
	store.kv = make(map[string][]byte)
	entry := &filer.Entry{FullPath: appendLockTestPath, HardLinkId: []byte("shared-hard-link"), HardLinkCounter: 2,
		Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}}}
	if err := fs.filer.Store.InsertEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	_, err := fs.AppendToEntry(withScopeKeepOnly(context.Background()), &filer_pb.AppendToEntryRequest{
		Directory: "/data", EntryName: "app.log", Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("append error = %v, want PermissionDenied", err)
	}
	got, err := fs.filer.FindEntry(context.Background(), appendLockTestPath)
	if err != nil || len(got.GetChunks()) != 1 {
		t.Fatalf("append changed shared data: entry=%v err=%v", got, err)
	}
}

func TestScopeSharedHardLinkIsRecheckedAfterAdmission(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		t.Run(method, func(t *testing.T) {
			store := newRenameTestStore()
			store.kv = make(map[string][]byte)
			fs, scope := newScopeRetentionServer(t, store)
			entry := &filer_pb.Entry{Name: "f", HardLinkId: []byte("shared-hard-link"), HardLinkCounter: 1,
				Content: []byte("existing data"), Attributes: &filer_pb.FuseAttributes{FileMode: 0644}}
			if err := fs.filer.Store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, entry)); err != nil {
				t.Fatal(err)
			}
			requested := proto.Clone(entry).(*filer_pb.Entry)
			requested.HardLinkCounter = 2
			requested.Content = nil
			var req any = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested}
			if method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: requested, SkipCheckParentDirectory: true}
			}
			called := false
			_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(ctx context.Context, req any) (any, error) {
				called = true
				// A trusted writer adds another name after scope admission. The
				// requested count now matches, but truncation would affect that name.
				entry.HardLinkCounter = 2
				if err := fs.filer.Store.InsertEntry(ctx, filer.FromPbEntry(bobWS, entry)); err != nil {
					t.Fatal(err)
				}
				if method == "CreateEntry" {
					return fs.CreateEntry(ctx, req.(*filer_pb.CreateEntryRequest))
				}
				return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
			})
			if !called || status.Code(err) != codes.PermissionDenied {
				t.Fatalf("handler reached=%v error=%v, want locked refusal", called, err)
			}
			other, err := fs.filer.FindEntry(context.Background(), util.FullPath(bobWS+"/f"))
			if err != nil || !bytes.Equal(other.Content, entry.Content) {
				t.Fatalf("other name's data changed: entry=%v error=%v", other, err)
			}
		})
	}
}
