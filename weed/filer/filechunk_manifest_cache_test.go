package filer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newManifestCacheTestChunk(fileID string) *filer_pb.FileChunk {
	return resolveTestManifest(fileID, 0)
}

func newTestManifestCache(t testing.TB) *ChunkManifestCache {
	t.Helper()
	cache := NewChunkManifestCache(MaxMountChunkManifestCacheEntries, MaxMountChunkManifestCacheBytes)
	t.Cleanup(cache.clear)
	return cache
}

func TestChunkGroupManifestResolutionCachesRepeatedOpens(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-repeated-open"
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		manifestID: {resolveTestData("cached-data", 0)},
	}, nil)
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		lookups.Add(1)
		return fixture.lookup(ctx, fileID)
	}

	for i := 0; i < 3; i++ {
		_, err := NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{
			newManifestCacheTestChunk(manifestID),
		}, 1, nil, cache)
		require.NoError(t, err)
	}

	require.Equal(t, int32(1), lookups.Load(), "repeated Mount opens should reuse the Manifest lookup")
	require.Equal(t, int32(1), fixture.loads.Load(), "repeated Mount opens should reuse the Manifest fetch")
}

func TestChunkGroupManifestResolutionDoesNotCacheFailedReads(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-failed-read"
	fixture := newManifestReadFixture(t, nil, nil)
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		lookups.Add(1)
		return fixture.lookup(ctx, fileID)
	}
	chunk := newManifestCacheTestChunk(manifestID)

	_, err := NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{chunk}, 1, nil, cache)
	require.Error(t, err)

	fixture.manifests[manifestID] = manifestBytes(t, resolveTestData("retried-data", 0))
	_, err = NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{chunk}, 1, nil, cache)
	require.NoError(t, err)

	require.Equal(t, int32(2), lookups.Load(), "a failed lookup must not poison later Mount opens")
	require.Equal(t, int32(2), fixture.loads.Load(), "a failed fetch must not poison later Mount opens")
}

func TestChunkGroupManifestResolutionConcurrentWarmOpensReuseFetch(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-concurrent-warm-open"
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		manifestID: {resolveTestData("concurrent-data", 0)},
	}, nil)
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		lookups.Add(1)
		return fixture.lookup(ctx, fileID)
	}

	_, err := NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{
		newManifestCacheTestChunk(manifestID),
	}, 1, nil, cache)
	require.NoError(t, err)

	const concurrentOpens = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrentOpens)
	for i := 0; i < concurrentOpens; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, openErr := NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{
				newManifestCacheTestChunk(manifestID),
			}, 1, nil, cache)
			errs <- openErr
		}()
	}
	wg.Wait()
	close(errs)
	for openErr := range errs {
		require.NoError(t, openErr)
	}

	require.Equal(t, int32(1), lookups.Load(), "warm concurrent Mount opens should not repeat the lookup")
	require.Equal(t, int32(1), fixture.loads.Load(), "warm concurrent Mount opens should not repeat the fetch")
}

func TestChunkGroupManifestResolutionCoalescesColdMisses(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-cold-miss"
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		manifestID: {resolveTestData("cold-data", 0)},
	}, nil)
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		lookups.Add(1)
		return fixture.lookup(ctx, fileID)
	}

	const concurrentOpens = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrentOpens)
	for i := 0; i < concurrentOpens; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, openErr := NewChunkGroup(lookup, nil, []*filer_pb.FileChunk{
				newManifestCacheTestChunk(manifestID),
			}, 1, nil, cache)
			errs <- openErr
		}()
	}
	wg.Wait()
	close(errs)
	for openErr := range errs {
		require.NoError(t, openErr)
	}

	require.Equal(t, int32(1), lookups.Load(), "concurrent cold opens should coalesce into a single lookup")
	require.Equal(t, int32(1), fixture.loads.Load(), "concurrent cold opens should coalesce into a single fetch")
}

func TestResolveOneChunkManifestDoesNotUseMountCache(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-public-resolver"
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		manifestID: {resolveTestData("public-resolver-data", 0)},
	}, nil)
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		lookups.Add(1)
		return fixture.lookup(ctx, fileID)
	}
	chunk := newManifestCacheTestChunk(manifestID)

	for i := 0; i < 2; i++ {
		_, err := ResolveOneChunkManifest(context.Background(), lookup, chunk, nil, nil)
		require.NoError(t, err)
	}

	require.Equal(t, int32(2), lookups.Load(), "the general resolver must retain its uncached behavior")
	require.Equal(t, int32(2), fixture.loads.Load(), "the general resolver must not use the Mount cache")

	// The shared cache must remain empty: a nil-cache caller must not pollute
	// a mount-owned cache, and vice versa.
	_, found := cache.get(chunkManifestCacheKey{fileID: manifestID})
	require.False(t, found, "a nil-cache resolve must not populate an unrelated cache")
}

func TestChunkGroupManifestResolutionDoesNotCacheMalformedManifest(t *testing.T) {
	cache := newTestManifestCache(t)
	const manifestID = "cache-malformed-manifest"
	fixture := newManifestReadFixture(t, map[string][]*filer_pb.FileChunk{
		manifestID: nil,
	}, nil)
	fixture.manifests[manifestID] = []byte("malformed manifest")
	chunk := newManifestCacheTestChunk(manifestID)

	_, err := NewChunkGroup(fixture.lookup, nil, []*filer_pb.FileChunk{chunk}, 1, nil, cache)
	require.Error(t, err)

	fixture.manifests[manifestID] = manifestBytes(t, resolveTestData("after-malformed-data", 0))
	_, err = NewChunkGroup(fixture.lookup, nil, []*filer_pb.FileChunk{chunk}, 1, nil, cache)
	require.NoError(t, err)
	require.Equal(t, int32(2), fixture.loads.Load(), "a malformed manifest must not poison later Mount opens")
}

func TestChunkManifestCacheSeparatesReadParameters(t *testing.T) {
	cache := NewChunkManifestCache(4, 1024)
	base := chunkManifestCacheKey{fileID: "same-file"}
	cache.put(base, []byte("manifest"))

	_, found := cache.get(chunkManifestCacheKey{fileID: "same-file", cipherKey: "key"})
	require.False(t, found, "cipher keys must be part of the cache key")
	_, found = cache.get(chunkManifestCacheKey{fileID: "same-file", isCompressed: true})
	require.False(t, found, "compression settings must be part of the cache key")
	_, found = cache.get(chunkManifestCacheKey{fileID: "different-file"})
	require.False(t, found, "FileIds must be part of the cache key")
}

func TestChunkManifestCacheCopiesData(t *testing.T) {
	cache := NewChunkManifestCache(1, 1024)
	key := chunkManifestCacheKey{fileID: "copy-data"}
	original := []byte("manifest")
	cache.put(key, original)
	original[0] = 'X'

	data, found := cache.get(key)
	require.True(t, found)
	require.Equal(t, []byte("manifest"), data)
	data[0] = 'Y'

	data, found = cache.get(key)
	require.True(t, found)
	require.Equal(t, []byte("manifest"), data)
}

func TestChunkManifestCacheEvictsLeastRecentlyUsedAndOversizedEntries(t *testing.T) {
	cache := NewChunkManifestCache(2, 6)
	first := chunkManifestCacheKey{fileID: "first"}
	second := chunkManifestCacheKey{fileID: "second"}
	third := chunkManifestCacheKey{fileID: "third"}
	oversized := chunkManifestCacheKey{fileID: "oversized"}

	cache.put(first, []byte("one"))
	cache.put(second, []byte("two"))
	_, found := cache.get(first)
	require.True(t, found, "the first entry should be present before eviction")
	cache.put(third, []byte("tri"))

	_, found = cache.get(first)
	require.True(t, found, "a recent entry should survive LRU eviction")
	_, found = cache.get(second)
	require.False(t, found, "the least recently used entry should be evicted")
	_, found = cache.get(third)
	require.True(t, found)

	cache.put(oversized, []byte("too large"))
	_, found = cache.get(oversized)
	require.False(t, found, "an entry over the byte limit must not be cached")
}

func TestResolveOneChunkManifestHonorsCanceledContextOnCacheHit(t *testing.T) {
	cache := NewChunkManifestCache(1, 1024)
	chunk := newManifestCacheTestChunk("cache-canceled-hit")
	cache.put(chunkManifestCacheKey{fileID: chunk.GetFileIdString()}, manifestBytes(t, resolveTestData("canceled-data", 0)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lookupCalled := false
	lookup := func(context.Context, string) ([]string, error) {
		lookupCalled = true
		return nil, errors.New("lookup should not be called")
	}

	_, err := ResolveOneChunkManifest(ctx, lookup, chunk, nil, cache)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, lookupCalled, "a canceled cache hit must not issue a lookup")
}

func manifestBytes(t testing.TB, chunks ...*filer_pb.FileChunk) []byte {
	t.Helper()
	data, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: chunks})
	require.NoError(t, err)
	return data
}
