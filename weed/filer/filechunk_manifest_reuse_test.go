package filer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// reuseFixture stores the manifests MaybeManifestize saves and serves them
// back, counting reads.
type reuseFixture struct {
	server *httptest.Server
	mu     sync.Mutex
	blobs  map[string][]byte
	saves  int
	reads  int
}

func newReuseFixture(t *testing.T) *reuseFixture {
	fixture := &reuseFixture{blobs: map[string][]byte{}}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.mu.Lock()
		data, found := fixture.blobs[strings.TrimPrefix(r.URL.Path, "/")]
		fixture.reads++
		fixture.mu.Unlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Write(data)
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *reuseFixture) save(reader io.Reader, _ string, offset int64, tsNs int64, _ uint64) (*filer_pb.FileChunk, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves++
	fid := fmt.Sprintf("9,%x", f.saves)
	f.blobs[fid] = data
	return &filer_pb.FileChunk{FileId: fid, Offset: offset, Size: uint64(len(data)), ModifiedTsNs: tsNs}, nil
}

func (f *reuseFixture) lookup(_ context.Context, fid string) ([]string, error) {
	return []string{f.server.URL + "/" + fid}, nil
}

func (f *reuseFixture) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// appendedChunks is what a client that appends one small record per flush
// holds: n chunks of 10 bytes, end to end.
func appendedChunks(n int) []*filer_pb.FileChunk {
	chunks := make([]*filer_pb.FileChunk, n)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x%08x", i+1, 0x5eed), Offset: int64(i) * 10, Size: 10, ModifiedTsNs: int64(i + 1)}
	}
	return chunks
}

// storedEntryChunks folds n appended chunks the way the filer does on a commit.
func (f *reuseFixture) storedEntryChunks(t *testing.T, n int) []*filer_pb.FileChunk {
	stored, err := MaybeManifestize(f.save, nil, appendedChunks(n))
	require.NoError(t, err)
	require.True(t, HasChunkManifest(stored))
	return stored
}

func manifestsOf(chunks []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	manifests, _ := SeparateManifestChunks(chunks)
	return manifests
}

// The client's next commit lists the chunks of the stored manifest flat, plus
// one new chunk: the manifest is kept and only the chunks it does not hold are
// left, in order.
func TestReuseStoredManifestsKeepsAManifestResentFlat(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch+10)
	sent := appendedChunks(SmallChunkManifestBatch + 11)

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
	require.Equal(t, manifestsOf(stored), reused)
	require.Equal(t, sent[SmallChunkManifestBatch:], rest)
}

func TestReuseStoredManifestsKeepsEveryManifestResentFlat(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, 3*SmallChunkManifestBatch+5)
	require.Len(t, manifestsOf(stored), 3)
	sent := appendedChunks(3*SmallChunkManifestBatch + 5)

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
	require.ElementsMatch(t, manifestsOf(stored), reused)
	require.Equal(t, sent[3*SmallChunkManifestBatch:], rest)
}

// A manifest stays only for the exact chunks it holds: a chunk that changed,
// or is gone, makes the commit's own list win.
func TestReuseStoredManifestsLeavesAManifestWhoseChunksChanged(t *testing.T) {
	for name, change := range map[string]func([]*filer_pb.FileChunk) []*filer_pb.FileChunk{
		"size":  func(c []*filer_pb.FileChunk) []*filer_pb.FileChunk { c[7].Size = 5; return c },
		"mtime": func(c []*filer_pb.FileChunk) []*filer_pb.FileChunk { c[7].ModifiedTsNs++; return c },
		"cipher": func(c []*filer_pb.FileChunk) []*filer_pb.FileChunk {
			c[7].CipherKey = []byte("k")
			return c
		},
		"gone": func(c []*filer_pb.FileChunk) []*filer_pb.FileChunk { return append(c[:7:7], c[8:]...) },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newReuseFixture(t)
			stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch+10)
			sent := change(appendedChunks(SmallChunkManifestBatch + 10))

			reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
			require.Empty(t, reused)
			require.Equal(t, sent, rest)
		})
	}
}

// A truncate before #13042 could shorten a manifest without cutting its
// chunks; such a manifest does not span what it holds and is not kept.
func TestReuseStoredManifestsLeavesAShortenedManifest(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch+10)
	manifest := manifestsOf(stored)[0]
	manifest.Size -= 5
	sent := appendedChunks(SmallChunkManifestBatch + 10)

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
	require.Empty(t, reused)
	require.Equal(t, sent, rest)
}

// A commit that lists the manifest itself, as a mount that folds does, needs
// nothing; a short list is never read against the stored manifests.
func TestReuseStoredManifestsReadsNothingItNeedsNot(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch+10)
	manifests, flat := SeparateManifestChunks(stored)
	sent := append(flat, appendedChunks(SmallChunkManifestBatch + 300)[SmallChunkManifestBatch+10:]...)

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, manifests, sent)
	require.Empty(t, reused)
	require.Equal(t, sent, rest)

	short := appendedChunks(SmallChunkManifestBatch - 1)
	reused, rest = ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, short)
	require.Empty(t, reused)
	require.Equal(t, short, rest)
	require.Zero(t, fixture.readCount())
}

// A manifest that cannot be read leaves the commit's list as it is.
func TestReuseStoredManifestsLeavesAnUnreadableManifest(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch+10)
	fixture.mu.Lock()
	clear(fixture.blobs)
	fixture.mu.Unlock()
	sent := appendedChunks(SmallChunkManifestBatch + 10)

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
	require.Empty(t, reused)
	require.Equal(t, sent, rest)
}

// The fold saves chunks with Fid set and FileId empty; a client spells the
// same chunks with FileId. Both are the same chunks.
func TestReuseStoredManifestsMatchesEitherFileIdSpelling(t *testing.T) {
	fixture := newReuseFixture(t)
	stored := fixture.storedEntryChunks(t, SmallChunkManifestBatch)
	sent := appendedChunks(SmallChunkManifestBatch)
	for _, chunk := range sent[:3] {
		chunk.FileId = strings.Replace(chunk.FileId, ",", ",000", 1)
	}
	spelledAsFid := proto.Clone(sent[3]).(*filer_pb.FileChunk)
	filer_pb.BeforeEntrySerialization([]*filer_pb.FileChunk{spelledAsFid})
	require.Empty(t, spelledAsFid.FileId)
	sent[3] = spelledAsFid

	reused, rest := ReuseStoredManifests(context.Background(), fixture.lookup, stored, nil, sent)
	require.Equal(t, manifestsOf(stored), reused)
	require.Empty(t, rest)
}
