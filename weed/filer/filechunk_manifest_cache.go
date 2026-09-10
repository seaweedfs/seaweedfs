package filer

import (
	"container/list"
	"sync"
)

const (
	maxMountChunkManifestCacheEntries = 256
	maxMountChunkManifestCacheBytes   = 64 << 20
)

type chunkManifestCacheKey struct {
	fileID       string
	cipherKey    string
	isCompressed bool
}

type chunkManifestCacheEntry struct {
	key  chunkManifestCacheKey
	data []byte
}

type chunkManifestCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	bytes      int64
	entries    map[chunkManifestCacheKey]*list.Element
	lru        *list.List
}

func newChunkManifestCache(maxEntries int, maxBytes int64) *chunkManifestCache {
	return &chunkManifestCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		entries:    make(map[chunkManifestCacheKey]*list.Element),
		lru:        list.New(),
	}
}

func (c *chunkManifestCache) get(key chunkManifestCacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, found := c.entries[key]
	if !found {
		return nil, false
	}
	c.lru.MoveToFront(element)
	entry := element.Value.(*chunkManifestCacheEntry)
	return append([]byte(nil), entry.data...), true
}

func (c *chunkManifestCache) put(key chunkManifestCacheKey, data []byte) {
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

func (c *chunkManifestCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[chunkManifestCacheKey]*list.Element)
	c.lru.Init()
	c.bytes = 0
}

func (c *chunkManifestCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	c.lru.Remove(element)
	entry := element.Value.(*chunkManifestCacheEntry)
	delete(c.entries, entry.key)
	c.bytes -= int64(len(entry.data))
}

var mountChunkManifestCache = newChunkManifestCache(
	maxMountChunkManifestCacheEntries,
	maxMountChunkManifestCacheBytes,
)
