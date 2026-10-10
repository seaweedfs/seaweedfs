package filer

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// cancelledChunks returns a cancelled context and plain data chunks. Resolving
// chunks under a cancelled context fails even without manifests, so these
// exercise the error path with no lookup function.
func cancelledChunks() (context.Context, []*filer_pb.FileChunk) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, []*filer_pb.FileChunk{
		{FileId: "1,01", Offset: 0, Size: 100, ModifiedTsNs: 1},
		{FileId: "1,02", Offset: 0, Size: 100, ModifiedTsNs: 2},
	}
}

func TestCompactFileChunksCancelledContext(t *testing.T) {
	ctx, chunks := cancelledChunks()

	compacted, garbage, err := CompactFileChunks(ctx, nil, chunks)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if compacted != nil || garbage != nil {
		t.Fatalf("compacted = %v, garbage = %v, want both nil on error", compacted, garbage)
	}
}

func TestViewFromChunksCancelledContext(t *testing.T) {
	ctx, chunks := cancelledChunks()

	views, err := ViewFromChunks(ctx, nil, chunks, 0, 100)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if views != nil {
		t.Fatalf("views = %v, want nil on error", views)
	}
}

func TestChunkStreamReaderCancelledContext(t *testing.T) {
	ctx, chunks := cancelledChunks()

	reader := NewChunkStreamReaderFromLookup(ctx, nil, chunks)
	n, err := reader.Read(make([]byte, 10))
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Read = %d, %v, want 0, context.Canceled", n, err)
	}
	if !errors.Is(reader.SourceError(), context.Canceled) {
		t.Fatalf("SourceError = %v, want context.Canceled", reader.SourceError())
	}
}
