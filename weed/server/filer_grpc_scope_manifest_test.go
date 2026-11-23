package weed_server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type scopeManifestSave struct {
	saves int
}

func (s *scopeManifestSave) save(reader io.Reader, _ string, offset int64, _ int64, _ uint64) (*filer_pb.FileChunk, error) {
	s.saves++
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return nil, err
	}
	return &filer_pb.FileChunk{FileId: fmt.Sprintf("2,%x", s.saves), Offset: offset}, nil
}

func scopeManifestChunks(n int) []*filer_pb.FileChunk {
	chunks := make([]*filer_pb.FileChunk, n)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{
			FileId: fmt.Sprintf("1,%x", i+1),
			Offset: int64(i),
			Size:   1,
		}
	}
	return chunks
}

func TestMaybeManifestizeChunksKeepsScopedListsFlat(t *testing.T) {
	store := &scopeManifestSave{}
	input := scopeManifestChunks(filer.SmallChunkManifestBatch)

	got, err := maybeManifestizeChunks(withScopeKeepOnly(context.Background()), store.save, nil, input)
	if err != nil {
		t.Fatalf("scoped manifestization: %v", err)
	}
	if len(got) != len(input) || filer.HasChunkManifest(got) {
		t.Fatalf("scoped chunks = %d entries, manifests=%v; want the original flat list", len(got), filer.HasChunkManifest(got))
	}
	if store.saves != 0 {
		t.Fatalf("scoped manifestization saved %d chunks, want 0", store.saves)
	}
}

func TestMaybeManifestizeChunksFoldsUnscopedLists(t *testing.T) {
	store := &scopeManifestSave{}
	input := scopeManifestChunks(filer.SmallChunkManifestBatch)

	got, err := maybeManifestizeChunks(context.Background(), store.save, nil, input)
	if err != nil {
		t.Fatalf("unscoped manifestization: %v", err)
	}
	if store.saves == 0 || !filer.HasChunkManifest(got) {
		t.Fatalf("unscoped manifestization saved %d chunks and returned manifests=%v; want a folded list", store.saves, filer.HasChunkManifest(got))
	}
}

func TestCleanupChunksKeepsScopedListFlat(t *testing.T) {
	store := newRenameTestStore()
	server := &FilerServer{
		filer:          newRenameTestFiler(t, store),
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	input := scopeManifestChunks(filer.SmallChunkManifestBatch)

	got, _, err := server.cleanupChunks(
		withScopeKeepOnly(context.Background()),
		"/test/obj",
		nil,
		&filer_pb.Entry{
			Name:       "obj",
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
			Chunks:     input,
		},
	)
	if err != nil {
		t.Fatalf("scoped cleanupChunks: %v", err)
	}
	if len(got) != len(input) || filer.HasChunkManifest(got) {
		t.Fatalf("scoped cleanup chunks = %d entries, manifests=%v; want the original flat list", len(got), filer.HasChunkManifest(got))
	}
}

func TestScopeKeepOnlyMarkerReachesUnaryAndStreamHandlers(t *testing.T) {
	h := newScopeHarness(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization",
		"Bearer "+scopedToken("alice", time.Hour, []string{aliceWS}, nil),
	))

	seenUnary := false
	_, err := h.scope.unary(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"}, &grpc.UnaryServerInfo{
		FullMethod: filer_pb.SeaweedFiler_LookupDirectoryEntry_FullMethodName,
	}, func(ctx context.Context, _ any) (any, error) {
		seenUnary = scopeKeepOnly(ctx)
		return &filer_pb.LookupDirectoryEntryResponse{}, nil
	})
	if err != nil || !seenUnary {
		t.Fatalf("unary marker err=%v seen=%v, want marker", err, seenUnary)
	}

	seenStream := false
	err = h.scope.stream(nil, &scopeExpiryServerStream{ctx: ctx}, &grpc.StreamServerInfo{
		FullMethod: filer_pb.SeaweedFiler_ListEntries_FullMethodName,
	}, func(_ any, stream grpc.ServerStream) error {
		seenStream = scopeKeepOnly(stream.Context())
		return nil
	})
	if err != nil || !seenStream {
		t.Fatalf("stream marker err=%v seen=%v, want marker", err, seenStream)
	}
}
