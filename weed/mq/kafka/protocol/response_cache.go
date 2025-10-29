package protocol

import (
	"sync"
	"time"
)

// ResponseCache caches API responses to reduce CPU usage for repeated requests
type ResponseCache struct {
	mu    sync.RWMutex
	cache map[string]*cacheEntry
	ttl   time.Duration
}

type cacheEntry struct {
	response  []byte
	timestamp time.Time
}

// NewResponseCache creates a new response cache with the specified TTL
func NewResponseCache(ttl time.Duration) *ResponseCache {
	return &ResponseCache{
		cache: make(map[string]*cacheEntry),
		ttl:   ttl,
	}
}

// Get retrieves a cached response if it exists and hasn't expired
func (c *ResponseCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cache[key]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Since(entry.timestamp) > c.ttl {
		return nil, false
	}

	return entry.response, true
}

// Put stores a response in the cache
func (c *ResponseCache) Put(key string, response []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = &cacheEntry{
		response:  response,
		timestamp: time.Now(),
	}
}

// Cleanup removes expired entries from the cache
func (c *ResponseCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.cache {
		if now.Sub(entry.timestamp) > c.ttl {
			delete(c.cache, key)
		}
	}
}

// StartCleanupLoop starts a background goroutine to periodically clean up expired entries
func (c *ResponseCache) StartCleanupLoop(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			c.Cleanup()
		}
	}()
}
