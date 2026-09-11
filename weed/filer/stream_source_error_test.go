package filer

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A chunk whose needle is gone answers 404, and that error is what tells a gone
// source from a sink failure. The AWS SDK drops it, so the reader keeps it.
func TestChunkStreamReaderSourceError(t *testing.T) {
	server := createTestServer(map[string][]byte{})
	defer server.Close()

	lookup := func(ctx context.Context, fileId string) ([]string, error) {
		return []string{server.URL + "/" + fileId}, nil
	}
	reader := NewChunkStreamReaderFromLookup(context.Background(), lookup,
		[]*filer_pb.FileChunk{{FileId: "7,01637037d6", Size: 8}})

	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("reading a gone chunk succeeded")
	}
	sourceErr := reader.SourceError()
	if sourceErr == nil {
		t.Fatal("reader kept no source error")
	}
	if !strings.Contains(sourceErr.Error(), "404") {
		t.Fatalf("source error does not name the volume answer: %v", sourceErr)
	}
	if ReaderSourceError(reader) != sourceErr {
		t.Fatal("ReaderSourceError does not return the kept error")
	}
}

// A lookup that cannot place the chunk fails before any volume is read.
func TestChunkStreamReaderSourceErrorOnLookup(t *testing.T) {
	master := &testMasterClient{urls: map[string][]string{}}
	reader := NewChunkStreamReaderFromLookup(context.Background(), master.GetLookupFileIdFunction(),
		[]*filer_pb.FileChunk{{FileId: "7,01637037d6", Size: 8}})

	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("reading a chunk with no location succeeded")
	}
	if reader.SourceError() == nil {
		t.Fatal("reader kept no source error")
	}
}

// Inlined content reaches no volume server and has no source error.
func TestReaderSourceErrorInlineEntry(t *testing.T) {
	reader := NewFileReader(nil, &filer_pb.Entry{Content: []byte("inline")})
	if err := ReaderSourceError(reader); err != nil {
		t.Fatalf("inline content reported a source error: %v", err)
	}
}
