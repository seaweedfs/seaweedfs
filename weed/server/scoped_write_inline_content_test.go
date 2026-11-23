package weed_server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestScopedInlineContentWritesPersistThroughUnaryHandlers(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		t.Run(method, func(t *testing.T) {
			store := newRenameTestStore()
			store.entries[aliceWS] = newDirectoryEntry(aliceWS, 1)
			fs, scope := newScopeRetentionServer(t, store)
			name := "inline-" + method
			requested := &filer_pb.Entry{
				Name:       name,
				Content:    []byte("new inline content"),
				Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
			}
			if method == "UpdateEntry" {
				initial := &filer_pb.Entry{
					Name:       name,
					Content:    []byte("old inline content"),
					Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
				}
				if err := store.InsertEntry(context.Background(), filer.FromPbEntry(aliceWS, initial)); err != nil {
					t.Fatalf("seed %s: %v", method, err)
				}
			}

			var req any
			if method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{
					Directory:                aliceWS,
					Entry:                    requested,
					SkipCheckParentDirectory: true,
				}
			} else {
				req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: requested}
			}

			resp, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{
				FullMethod: scopeFilerMethodPrefix + method,
			}, func(handlerCtx context.Context, handlerReq any) (any, error) {
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
			if err != nil {
				t.Fatalf("scoped %s with inline content: %v", method, err)
			}
			if method == "CreateEntry" {
				createResp, ok := resp.(*filer_pb.CreateEntryResponse)
				if !ok || createResp == nil || createResp.GetError() != "" || createResp.GetErrorCode() != filer_pb.FilerError_OK {
					t.Fatalf("scoped CreateEntry response = %#v, want empty error and FilerError_OK", resp)
				}
			}

			stored, findErr := store.FindEntry(context.Background(), filer.FromPbEntry(aliceWS, requested).FullPath)
			if findErr != nil {
				t.Fatalf("find persisted %s: %v", method, findErr)
			}
			if !bytes.Equal(stored.Content, requested.Content) {
				t.Fatalf("persisted %s content = %q, want %q", method, stored.Content, requested.Content)
			}
		})
	}
}

func TestScopedInlineContentOutsidePrefixIsRefused(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	expectScopeDenied(t, h, "CreateEntry", "inline create outside prefix", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory:                bobWS,
			SkipCheckParentDirectory: true,
			Entry:                    &filer_pb.Entry{Name: "outside", Content: []byte("must refuse")},
		})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "inline update outside prefix", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: bobWS,
			Entry:     &filer_pb.Entry{Name: "outside", Content: []byte("must refuse")},
		})
		return err
	})
}

func TestScopedInlineAppendWithChunksIsRefusedBeforePersistence(t *testing.T) {
	fs, store := newAppendLockServer(t)
	close(store.release)

	const newFid = "5,0c00000056"
	entry := newFileEntry(appendLockTestPath, 1)
	entry.Content = []byte("stored inline bytes")
	entry.Chunks = nil
	store.entries[appendLockTestPath] = entry

	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, fs)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken("append", time.Hour, []string{"/data"}, nil),
	))
	_, err := scope.unary(ctx, &filer_pb.AppendToEntryRequest{
		Directory: "/data",
		EntryName: "app.log",
		Chunks:    []*filer_pb.FileChunk{{FileId: newFid, Size: 10}},
		ChunkAuth: map[string]string{newFid: scopedChunkProof(t, "append", newFid, time.Hour)},
	}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "AppendToEntry"}, func(handlerCtx context.Context, req any) (any, error) {
		return fs.AppendToEntry(handlerCtx, req.(*filer_pb.AppendToEntryRequest))
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped append to inline entry error = %v, want PermissionDenied", err)
	}

	stored, err := store.FindEntry(context.Background(), appendLockTestPath)
	if err != nil {
		t.Fatalf("find inline entry after refused append: %v", err)
	}
	if !bytes.Equal(stored.Content, entry.Content) {
		t.Fatalf("inline content after refused append = %q, want %q", stored.Content, entry.Content)
	}
	if len(stored.GetChunks()) != 0 {
		t.Fatalf("chunks after refused append = %v, want no chunks", stored.GetChunks())
	}
}
