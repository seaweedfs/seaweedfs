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
			err := scopeRetainsData(tc.stored, tc.requested, nil)
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
	staleErr    bool
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
			staleErr:    true,
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
				resp, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
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
				if tc.staleErr {
					if status.Code(err) != codes.PermissionDenied || !called {
						t.Fatalf("stale %s err=%v called=%v, want handler recheck PermissionDenied", method, err, called)
					}
				} else {
					if err != nil || !called {
						t.Fatalf("stale %s err=%v called=%v, want inline write admitted", method, err, called)
					}
					if method == "CreateEntry" {
						createResp, ok := resp.(*filer_pb.CreateEntryResponse)
						if !ok || createResp == nil || createResp.GetError() != "" || createResp.GetErrorCode() != filer_pb.FilerError_OK {
							t.Fatalf("stale inline CreateEntry response = %#v, want empty error and FilerError_OK", resp)
						}
					}
				}
				got, findErr := store.FindEntry(context.Background(), util.FullPath(path))
				if findErr != nil {
					t.Fatalf("find replacement: %v", findErr)
				}
				want := tc.replacement
				if !tc.staleErr {
					want = tc.initial
				}
				if !retentionEntryDataEqual(got, want) {
					t.Fatalf("stale %s stored content=%q chunks=%v, want content=%q chunks=%v", method, got.Content, got.GetChunks(), want.Content, want.GetChunks())
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
				resp, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
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
				if method == "CreateEntry" {
					createResp, ok := resp.(*filer_pb.CreateEntryResponse)
					if !ok || createResp == nil || createResp.GetError() != "" || createResp.GetErrorCode() != filer_pb.FilerError_OK {
						t.Fatalf("unchanged CreateEntry response = %#v, want empty error and FilerError_OK", resp)
					}
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

func TestScopeKeepsHardLink(t *testing.T) {
	const storedFileID = "5,0c00000055"
	id := []byte("shared-hard-link")
	linked := &filer_pb.Entry{HardLinkId: id, HardLinkCounter: 2, Chunks: []*filer_pb.FileChunk{{FileId: storedFileID}}}
	cases := []struct {
		name      string
		stored    *filer_pb.Entry
		requested *filer_pb.Entry
		wantErr   bool
	}{
		{name: "unlinked entry", stored: entryWith("f", storedFileID), requested: entryWith("f", storedFileID)},
		{name: "same link and count", stored: linked, requested: &filer_pb.Entry{HardLinkId: id, HardLinkCounter: 2, Chunks: linked.Chunks}},
		{name: "lowered count", stored: linked, requested: &filer_pb.Entry{HardLinkId: id, HardLinkCounter: 1, Chunks: linked.Chunks}, wantErr: true},
		{name: "lowered count without chunks", stored: linked, requested: &filer_pb.Entry{HardLinkId: id, HardLinkCounter: 1}, wantErr: true},
		{name: "chunks detached from the link", stored: linked, requested: entryWith("f", storedFileID), wantErr: true},
		{name: "link dropped with the data", stored: linked, requested: &filer_pb.Entry{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scopeKeepsHardLink(tc.stored, tc.requested)
			if (err != nil) != tc.wantErr {
				t.Fatalf("scopeKeepsHardLink err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestScopeUpdateCannotFreeAnotherNamesHardLinkData(t *testing.T) {
	const storedFileID = "5,0c00000055"
	id := []byte("shared-hard-link")
	for _, tc := range []struct {
		name  string
		entry *filer_pb.Entry
	}{
		{name: "lowered link count", entry: &filer_pb.Entry{Name: "f", HardLinkId: id, HardLinkCounter: 1, Chunks: []*filer_pb.FileChunk{{FileId: storedFileID}}}},
		{name: "chunks detached from the link", entry: entryWith("f", storedFileID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRenameTestStore()
			store.kv = make(map[string][]byte)
			fs, scope := newScopeRetentionServer(t, store)
			for _, p := range []string{aliceWS + "/f", "/bob/g"} {
				e := &filer.Entry{FullPath: util.FullPath(p), HardLinkId: id, HardLinkCounter: 2, Attr: filer.Attr{Mode: 0644}, Chunks: []*filer_pb.FileChunk{{FileId: storedFileID}}}
				if err := fs.filer.Store.InsertEntry(context.Background(), e); err != nil {
					t.Fatalf("insert %s: %v", p, err)
				}
			}
			req := &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: tc.entry}
			_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + "UpdateEntry",
			}, func(handlerCtx context.Context, handlerReq any) (any, error) {
				return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
			})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("UpdateEntry err=%v, want PermissionDenied", err)
			}
			got, findErr := fs.filer.FindEntry(context.Background(), util.FullPath(aliceWS+"/f"))
			if findErr != nil {
				t.Fatalf("find %s/f: %v", aliceWS, findErr)
			}
			if !bytes.Equal(got.HardLinkId, id) || got.HardLinkCounter != 2 {
				t.Fatalf("%s/f link=%x count=%d, want %x and 2", aliceWS, got.HardLinkId, got.HardLinkCounter, id)
			}
		})
	}
}
