package weed_server

import (
	"bytes"
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

func TestScopeRetainsData(t *testing.T) {
	const storedFileID = "5,0c00000055"
	cases := []struct {
		name      string
		stored    *filer_pb.Entry
		requested *filer_pb.Entry
		wantErr   bool
	}{
		{
			name:      "unchanged inline content",
			stored:    &filer_pb.Entry{Content: []byte("stored")},
			requested: &filer_pb.Entry{Content: []byte("stored")},
		},
		{
			name:      "changed inline content",
			stored:    &filer_pb.Entry{Content: []byte("stored")},
			requested: &filer_pb.Entry{Content: []byte("new")},
			wantErr:   true,
		},
		{
			name:      "cleared inline content",
			stored:    &filer_pb.Entry{Content: []byte("stored")},
			requested: &filer_pb.Entry{},
		},
		{
			name:      "unchanged chunk",
			stored:    entryWith("stored", storedFileID),
			requested: entryWith("stored", storedFileID),
		},
		{
			name:      "new chunk",
			stored:    entryWith("stored", storedFileID),
			requested: entryWith("stored", "5,0c00000056"),
			wantErr:   true,
		},
		{
			name:      "cleared chunks",
			stored:    entryWith("stored", storedFileID),
			requested: &filer_pb.Entry{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scopeRetainsData(tc.stored, tc.requested)
			if (err != nil) != tc.wantErr {
				t.Fatalf("scopeRetainsData err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

type scopeRetentionCase struct {
	name        string
	initial     *filer_pb.Entry
	replacement *filer_pb.Entry
}

func scopeRetentionCases() []scopeRetentionCase {
	return []scopeRetentionCase{
		{
			name:        "inline",
			initial:     &filer_pb.Entry{Name: "retained", Content: []byte("A")},
			replacement: &filer_pb.Entry{Name: "retained", Content: []byte("B")},
		},
		{
			name:        "chunks",
			initial:     entryWith("retained", "5,0c00000055"),
			replacement: entryWith("retained", "5,0c00000056"),
		},
	}
}

func newScopeRetentionServer(t *testing.T, store *renameTestStore) (*FilerServer, *filerGrpcScope) {
	t.Helper()
	fs := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, fs)
	return fs, scope
}

func scopedRetentionContext() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken("retention", time.Hour, []string{aliceWS}, nil),
	))
}

func retentionEntryDataEqual(got *filer.Entry, want *filer_pb.Entry) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	// The store may populate the parsed Fid cache from FileId. Compare the
	// retained reference and layout, allowing that derived representation.
	normalized := func(chunks []*filer_pb.FileChunk) *filer_pb.Entry {
		e := proto.Clone(&filer_pb.Entry{Chunks: chunks}).(*filer_pb.Entry)
		for _, chunk := range e.Chunks {
			chunk.FileId = chunk.GetFileIdString()
			chunk.Fid = nil
		}
		return e
	}
	return bytes.Equal(got.Content, want.Content) && proto.Equal(normalized(got.GetChunks()), normalized(want.GetChunks()))
}

func TestScopeRetentionRechecksCreateAndUpdateUnderPathLock(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, tc := range scopeRetentionCases() {
			t.Run(method+"/stale "+tc.name, func(t *testing.T) {
				store := newRenameTestStore()
				path := scopeJoin(aliceWS, tc.initial.GetName())
				store.entries[path] = filer.FromPbEntry(aliceWS, proto.Clone(tc.initial).(*filer_pb.Entry))
				fs, scope := newScopeRetentionServer(t, store)
				var req any
				if method == "CreateEntry" {
					req = &filer_pb.CreateEntryRequest{Directory: aliceWS, SkipCheckParentDirectory: true, Entry: proto.Clone(tc.initial).(*filer_pb.Entry)}
				} else {
					req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: proto.Clone(tc.initial).(*filer_pb.Entry)}
				}

				called := false
				_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
					FullMethod: scopeFilerMethodPrefix + method,
				}, func(handlerCtx context.Context, handlerReq any) (any, error) {
					called = true
					if updateErr := store.UpdateEntry(context.Background(), filer.FromPbEntry(aliceWS, proto.Clone(tc.replacement).(*filer_pb.Entry))); updateErr != nil {
						t.Fatalf("replace stored entry: %v", updateErr)
					}
					switch method {
					case "CreateEntry":
						return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
					case "UpdateEntry":
						return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
					default:
						t.Fatalf("unexpected method %q", method)
						return nil, nil
					}
				})
				if status.Code(err) != codes.PermissionDenied || !called {
					t.Fatalf("stale %s err=%v called=%v, want handler recheck PermissionDenied", method, err, called)
				}
				got, findErr := store.FindEntry(context.Background(), util.FullPath(path))
				if findErr != nil {
					t.Fatalf("find replacement: %v", findErr)
				}
				if !retentionEntryDataEqual(got, tc.replacement) {
					t.Fatalf("rejected stale %s changed replacement data: got content=%q chunks=%v, want content=%q chunks=%v", method, got.Content, got.GetChunks(), tc.replacement.Content, tc.replacement.GetChunks())
				}
			})

			t.Run(method+"/unchanged "+tc.name, func(t *testing.T) {
				store := newRenameTestStore()
				path := scopeJoin(aliceWS, tc.initial.GetName())
				store.entries[path] = filer.FromPbEntry(aliceWS, proto.Clone(tc.initial).(*filer_pb.Entry))
				fs, scope := newScopeRetentionServer(t, store)
				var req any
				if method == "CreateEntry" {
					req = &filer_pb.CreateEntryRequest{Directory: aliceWS, SkipCheckParentDirectory: true, Entry: proto.Clone(tc.initial).(*filer_pb.Entry)}
				} else {
					req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: proto.Clone(tc.initial).(*filer_pb.Entry)}
				}

				called := false
				_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
					FullMethod: scopeFilerMethodPrefix + method,
				}, func(handlerCtx context.Context, handlerReq any) (any, error) {
					called = true
					switch method {
					case "CreateEntry":
						return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
					case "UpdateEntry":
						return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
					default:
						t.Fatalf("unexpected method %q", method)
						return nil, nil
					}
				})
				if err != nil || !called {
					t.Fatalf("unchanged %s err=%v called=%v, want successful handler", method, err, called)
				}
				got, findErr := store.FindEntry(context.Background(), util.FullPath(path))
				if findErr != nil {
					t.Fatalf("find retained entry: %v", findErr)
				}
				if !retentionEntryDataEqual(got, tc.initial) {
					t.Fatalf("successful unchanged %s changed retained data: got content=%q chunks=%v, want content=%q chunks=%v", method, got.Content, got.GetChunks(), tc.initial.Content, tc.initial.GetChunks())
				}
			})
		}
	}
}
