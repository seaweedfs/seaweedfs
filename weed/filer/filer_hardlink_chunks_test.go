package filer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// chunkServer serves each manifest's content at /<fid> and answers 500 for
// any other fid; lookup points every fid at it.
func chunkServer(t *testing.T, manifests map[string][]*filer_pb.FileChunk) (lookup func(context.Context, string) ([]string, error)) {
	t.Helper()
	blobs := map[string][]byte{}
	for fid, chunks := range manifests {
		blob, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: chunks})
		require.NoError(t, err)
		blobs[fid] = blob
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blob, found := blobs[strings.TrimPrefix(r.URL.Path, "/")]
		if !found {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		w.Write(blob)
	}))
	t.Cleanup(srv.Close)
	return func(_ context.Context, fileId string) ([]string, error) {
		return []string{srv.URL + "/" + fileId}, nil
	}
}

func plainChunk(fid string, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fid, Offset: offset, Size: 8}
}

func manifestChunk(fid string, offset int64, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fid, Offset: offset, Size: size, IsChunkManifest: true}
}

func fileIds(chunks []*filer_pb.FileChunk) []string {
	ids := []string{}
	for _, c := range chunks {
		ids = append(ids, c.GetFileIdString())
	}
	sort.Strings(ids)
	return ids
}

// A file whose chunks sit behind two levels of manifests: every chunk it
// references, manifests included, stays; the rest is still deleted.
func TestWithoutChunksOfKeepsChunksBehindNestedManifests(t *testing.T) {
	lookup := chunkServer(t, map[string][]*filer_pb.FileChunk{
		"7,0b": {plainChunk("8,01", 8), manifestChunk("7,0c", 16, 16)},
		"7,0c": {plainChunk("8,02", 16), plainChunk("8,03", 24)},
	})
	held := []*filer_pb.FileChunk{plainChunk("7,0a", 0), manifestChunk("7,0b", 8, 24)}
	garbage := []*filer_pb.FileChunk{
		plainChunk("7,0a", 0), manifestChunk("7,0b", 8, 24), plainChunk("8,01", 8),
		manifestChunk("7,0c", 16, 16), plainChunk("8,02", 16), plainChunk("8,03", 24),
		plainChunk("9,0f", 0), plainChunk("9,10", 8),
	}

	kept, err := withoutChunksOf(context.Background(), lookup, nil, held, garbage)
	require.NoError(t, err)
	assert.Equal(t, []string{"9,0f", "9,10"}, fileIds(kept))
}

// When a manifest cannot be read, which chunks the file references is
// unknown: the error makes KeepHardLinkedChunks delete nothing, unrelated
// garbage included.
func TestWithoutChunksOfUnreadableManifest(t *testing.T) {
	lookup := chunkServer(t, map[string][]*filer_pb.FileChunk{
		"7,0b": {plainChunk("8,01", 8), manifestChunk("7,0c", 16, 16)},
		// 7,0c is not served
	})
	held := []*filer_pb.FileChunk{plainChunk("7,0a", 0), manifestChunk("7,0b", 8, 24)}
	garbage := []*filer_pb.FileChunk{plainChunk("8,01", 8), plainChunk("9,0f", 0)}

	kept, err := withoutChunksOf(context.Background(), lookup, nil, held, garbage)
	assert.Error(t, err)
	assert.Empty(t, kept)
}

func TestWithoutChunksOfPlainChunks(t *testing.T) {
	held := []*filer_pb.FileChunk{plainChunk("7,0a", 0)}
	garbage := []*filer_pb.FileChunk{plainChunk("7,0a", 0), plainChunk("9,0f", 0)}
	noLookup := func(context.Context, string) ([]string, error) {
		t.Fatal("no manifest to read")
		return nil, nil
	}

	kept, err := withoutChunksOf(context.Background(), noLookup, nil, held, garbage)
	require.NoError(t, err)
	assert.Equal(t, []string{"9,0f"}, fileIds(kept))
}
