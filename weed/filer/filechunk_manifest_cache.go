package filer

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"
)

const (
	MaxMountChunkManifestCacheEntries = 256
	MaxMountChunkManifestCacheBytes   = 64 << 20
)

type chunkManifestCacheKey struct {
	fileID       string
	cipherKey    string
	isCompressed bool
}

func (k chunkManifestCacheKey) flightKey() string {
	return fmt.Sprintf("%s\x00%s\x00%t", k.fileID, k.cipherKey, k.isCompressed)
}

type chunkManifestCacheEntry struct {
	key  chunkManifestCacheKey
	data []byte
}

// ChunkManifestCache is a bounded, thread-safe LRU cache for chunk manifest
// bytes. Each mount (WFS) owns its own instance so manifests fetched through
// one filer backend are never served to another. Concurrent cold misses for
// the same key are coalesced via singleflight so only one fetch runs.
type ChunkManifestCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	bytes      int64
	entries    map[chunkManifestCacheKey]*list.Element
	lru        *list.List
	flight     singleflight.Group
}

// NewChunkManifestCache creates a bounded LRU cache for chunk manifest bytes.
func NewChunkManifestCache(maxEntries int, maxBytes int64) *ChunkManifestCache {
	return &ChunkManifestCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		entries:    make(map[chunkManifestCacheKey]*list.Element),
		lru:        list.New(),
	}
}

func (c *ChunkManifestCache) get(key chunkManifestCacheKey) ([]byte, bool) {
	c.mu.Lock()
	element, found := c.entries[key]
	if !found {
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(element)
	// entry.data is immutable after insertion; copy outside the lock so a
	// large copy does not block concurrent hits, inserts, and evictions.
	data := element.Value.(*chunkManifestCacheEntry).data
	c.mu.Unlock()
	return append([]byte(nil), data...), true
}

func (c *ChunkManifestCache) put(key chunkManifestCacheKey, data []byte) {
	if c.maxEntries <= 0 || int64(len(data)) > c.maxBytes {
		return
	}

	entry := &chunkManifestCacheEntry{
		key:  key,
		data: append([]byte(nil), data...),
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old, found := c.entries[key]; found {
		c.removeElement(old)
	}

	element := c.lru.PushFront(entry)
	c.entries[key] = element
	c.bytes += int64(len(entry.data))

	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		c.removeElement(c.lru.Back())
	}
}

// fetchOrLoad returns cached manifest bytes for key, or invokes fetch and
// caches the result. Concurrent calls for the same key are coalesced via
// singleflight so only one fetch runs during a cold burst. A caller whose
// context is canceled while waiting for the in-flight fetch returns
// ctx.Err() immediately rather than blocking for the leader's result.
func (c *ChunkManifestCache) fetchOrLoad(ctx context.Context, key chunkManifestCacheKey, fetch func() ([]byte, error)) ([]byte, error) {
	if data, ok := c.get(key); ok {
		return data, nil
	}
	ch := c.flight.DoChan(key.flightKey(), func() (interface{}, error) {
		// Re-check under the flight: another flight may have just populated
		// the cache for this key.
		if data, ok := c.get(key); ok {
			return data, nil
		}
		data, err := fetch()
		if err != nil {
			return nil, err
		}
		c.put(key, data)
		return data, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]byte), nil
	}
}

func (c *ChunkManifestCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[chunkManifestCacheKey]*list.Element)
	c.lru.Init()
	c.bytes = 0
}

func (c *ChunkManifestCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	c.lru.Remove(element)
	entry := element.Value.(*chunkManifestCacheEntry)
	delete(c.entries, entry.key)
	c.bytes -= int64(len(entry.data))
}
