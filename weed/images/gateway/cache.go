package gateway

import (
	"container/list"
	"sync"
)

type result struct {
	data              []byte
	contentType, etag string
}

type cacheEntry struct {
	key   string
	value *result
	size  int64
}

// cache is a byte-bounded LRU with an entry limit to bound metadata for small objects.
type cache struct {
	mu             sync.Mutex
	entries        map[string]*list.Element
	order          *list.List
	capacity, used int64
}

// newCache creates a disposable in-process cache without creating storage objects.
func newCache(capacity int64) *cache {
	return &cache{entries: make(map[string]*list.Element), order: list.New(), capacity: capacity}
}

// get returns immutable results and updates recency.
func (c *cache) get(key string) *result {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.order.MoveToFront(e)
		return e.Value.(*cacheEntry).value
	}
	return nil
}

// put accounts for data and index overhead, skipping results larger than capacity.
func (c *cache) put(key string, value *result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	size := int64(len(key) + len(value.data) + len(value.contentType) + len(value.etag) + 128)
	if c.capacity == 0 || size > c.capacity {
		return
	}
	if old := c.entries[key]; old != nil {
		c.used -= old.Value.(*cacheEntry).size
		c.order.Remove(old)
		delete(c.entries, key)
	}
	for c.used+size > c.capacity || len(c.entries) >= 1024 {
		old := c.order.Back()
		entry := old.Value.(*cacheEntry)
		delete(c.entries, entry.key)
		c.used -= entry.size
		c.order.Remove(old)
	}
	c.entries[key] = c.order.PushFront(&cacheEntry{key: key, value: value, size: size})
	c.used += size
}
