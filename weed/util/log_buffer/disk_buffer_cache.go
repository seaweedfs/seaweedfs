package log_buffer

import (
	"container/list"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// DiskBufferCache is a small LRU cache for recently-read historical data buffers
// This reduces Filer load when multiple consumers are catching up on historical messages
type DiskBufferCache struct {
	maxSize   int
	ttl       time.Duration
	cache     map[string]*cacheEntry
	lruList   *list.List
	mu        sync.RWMutex
	hits      int64
	misses    int64
	evictions int64
}

type cacheEntry struct {
	key        string
	data       []byte
	offset     int64
	timestamp  time.Time
	lruElement *list.Element
	isNegative bool // true if this is a negative cache entry (data not found)
}

// NewDiskBufferCache creates a new cache with the specified size and TTL
// Recommended size: 3-5 buffers (each ~8MB)
// Recommended TTL: 30-60 seconds
func NewDiskBufferCache(maxSize int, ttl time.Duration) *DiskBufferCache {
	cache := &DiskBufferCache{
		maxSize: maxSize,
		ttl:     ttl,
		cache:   make(map[string]*cacheEntry),
		lruList: list.New(),
	}

	// Start background cleanup goroutine
	go cache.cleanupLoop()

	return cache
}

// Get retrieves a buffer from the cache
// Returns (data, offset, found)
// If found=true and data=nil, this is a negative cache entry (data doesn't exist)
func (c *DiskBufferCache) Get(key string) ([]byte, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[key]
	if !exists {
		c.misses++
		return nil, 0, false
	}

	// Check if entry has expired
	if time.Since(entry.timestamp) > c.ttl {
		c.evict(entry)
		c.misses++
		return nil, 0, false
	}

	// Move to front of LRU list (most recently used)
	c.lruList.MoveToFront(entry.lruElement)
	c.hits++

	if entry.isNegative {
		glog.V(4).Infof("📦 CACHE HIT (NEGATIVE): key=%s - data not found (hits=%d misses=%d)",
			key, c.hits, c.misses)
	} else {
		glog.V(4).Infof("📦 CACHE HIT: key=%s offset=%d size=%d (hits=%d misses=%d)",
			key, entry.offset, len(entry.data), c.hits, c.misses)
	}

	return entry.data, entry.offset, true
}

// Put adds a buffer to the cache
// If data is nil, this creates a negative cache entry (data doesn't exist)
func (c *DiskBufferCache) Put(key string, data []byte, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	isNegative := data == nil

	// Check if entry already exists
	if entry, exists := c.cache[key]; exists {
		// Update existing entry
		entry.data = data
		entry.offset = offset
		entry.timestamp = time.Now()
		entry.isNegative = isNegative
		c.lruList.MoveToFront(entry.lruElement)
		if isNegative {
			glog.V(4).Infof("📦 CACHE UPDATE (NEGATIVE): key=%s - data not found", key)
		} else {
			glog.V(4).Infof("📦 CACHE UPDATE: key=%s offset=%d size=%d", key, offset, len(data))
		}
		return
	}

	// Evict oldest entry if cache is full
	if c.lruList.Len() >= c.maxSize {
		oldest := c.lruList.Back()
		if oldest != nil {
			c.evict(oldest.Value.(*cacheEntry))
		}
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		data:       data,
		offset:     offset,
		timestamp:  time.Now(),
		isNegative: isNegative,
	}
	entry.lruElement = c.lruList.PushFront(entry)
	c.cache[key] = entry

	if isNegative {
		glog.V(4).Infof("📦 CACHE PUT (NEGATIVE): key=%s - data not found (cache_size=%d/%d)",
			key, c.lruList.Len(), c.maxSize)
	} else {
		glog.V(4).Infof("📦 CACHE PUT: key=%s offset=%d size=%d (cache_size=%d/%d)",
			key, offset, len(data), c.lruList.Len(), c.maxSize)
	}
}

// evict removes an entry from the cache (must be called with lock held)
func (c *DiskBufferCache) evict(entry *cacheEntry) {
	delete(c.cache, entry.key)
	c.lruList.Remove(entry.lruElement)
	c.evictions++
	glog.V(4).Infof("📦 CACHE EVICT: key=%s (evictions=%d)", entry.key, c.evictions)
}

// cleanupLoop periodically removes expired entries
func (c *DiskBufferCache) cleanupLoop() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup removes expired entries
func (c *DiskBufferCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var toEvict []*cacheEntry

	// Find expired entries
	for _, entry := range c.cache {
		if now.Sub(entry.timestamp) > c.ttl {
			toEvict = append(toEvict, entry)
		}
	}

	// Evict expired entries
	for _, entry := range toEvict {
		c.evict(entry)
	}

	if len(toEvict) > 0 {
		glog.V(3).Infof("📦 CACHE CLEANUP: evicted %d expired entries", len(toEvict))
	}
}

// Stats returns cache statistics
func (c *DiskBufferCache) Stats() (hits, misses, evictions int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses, c.evictions, c.lruList.Len()
}

// Clear removes all entries from the cache
func (c *DiskBufferCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cacheEntry)
	c.lruList = list.New()
	glog.V(2).Infof("📦 CACHE CLEARED")
}
