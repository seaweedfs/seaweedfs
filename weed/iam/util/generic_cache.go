package util

import (
	"context"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// CacheableStore defines the interface for stores that can be cached
type CacheableStore[T any] interface {
	Get(ctx context.Context, filerAddress string, key string) (T, error)
	Store(ctx context.Context, filerAddress string, key string, value T) error
	Delete(ctx context.Context, filerAddress string, key string) error
	List(ctx context.Context, filerAddress string) ([]string, error)
}

// CopyFunction defines how to deep copy cached values
type CopyFunction[T any] func(T) T

// CachedStore provides generic TTL caching for any store type
type CachedStore[T any] struct {
	baseStore CacheableStore[T]
	cache     *ccache.Cache
	listCache *ccache.Cache
	copyFunc  CopyFunction[T]
	ttl       time.Duration
	listTTL   time.Duration
}

// CachedStoreConfig holds configuration for the generic cached store
type CachedStoreConfig struct {
	TTL          time.Duration
	ListTTL      time.Duration
	MaxCacheSize int64
}

// NewCachedStore creates a new generic cached store
func NewCachedStore[T any](
	baseStore CacheableStore[T],
	copyFunc CopyFunction[T],
	config CachedStoreConfig,
) *CachedStore[T] {
	// Apply defaults
	if config.TTL == 0 {
		config.TTL = 5 * time.Minute
	}
	if config.ListTTL == 0 {
		config.ListTTL = 1 * time.Minute
	}
	if config.MaxCacheSize == 0 {
		config.MaxCacheSize = 1000
	}

	// Create ccache instances
	pruneCount := config.MaxCacheSize >> 3
	if pruneCount <= 0 {
		pruneCount = 100
	}

	return &CachedStore[T]{
		baseStore: baseStore,
		cache:     ccache.New(ccache.Configure().MaxSize(config.MaxCacheSize).ItemsToPrune(uint32(pruneCount))),
		listCache: ccache.New(ccache.Configure().MaxSize(100).ItemsToPrune(10)),
		copyFunc:  copyFunc,
		ttl:       config.TTL,
		listTTL:   config.ListTTL,
	}
}

// Get retrieves an item with caching
func (c *CachedStore[T]) Get(ctx context.Context, filerAddress string, key string) (T, error) {
	// Try cache first
	item := c.cache.Get(key)
	if item != nil {
		// Cache hit - return cached item (DO NOT extend TTL)
		value := item.Value().(T)
		glog.V(4).Infof("Cache hit for key %s", key)
		return c.copyFunc(value), nil
	}

	// Cache miss - fetch from base store
	glog.V(4).Infof("Cache miss for key %s, fetching from store", key)
	value, err := c.baseStore.Get(ctx, filerAddress, key)
	if err != nil {
		var zero T
		return zero, err
	}

	// Cache the result with TTL
	c.cache.Set(key, c.copyFunc(value), c.ttl)
	glog.V(3).Infof("Cached key %s with TTL %v", key, c.ttl)
	return value, nil
}

// Store stores an item and invalidates cache
func (c *CachedStore[T]) Store(ctx context.Context, filerAddress string, key string, value T) error {
	// Store in base store
	err := c.baseStore.Store(ctx, filerAddress, key, value)
	if err != nil {
		return err
	}

	// Invalidate cache entries
	c.cache.Delete(key)
	c.listCache.Clear() // Invalidate list cache

	glog.V(3).Infof("Stored and invalidated cache for key %s", key)
	return nil
}

// Delete deletes an item and invalidates cache
func (c *CachedStore[T]) Delete(ctx context.Context, filerAddress string, key string) error {
	// Delete from base store
	err := c.baseStore.Delete(ctx, filerAddress, key)
	if err != nil {
		return err
	}

	// Invalidate cache entries
	c.cache.Delete(key)
	c.listCache.Clear() // Invalidate list cache

	glog.V(3).Infof("Deleted and invalidated cache for key %s", key)
	return nil
}

// List lists all items with caching
func (c *CachedStore[T]) List(ctx context.Context, filerAddress string) ([]string, error) {
	const listCacheKey = "item_list"

	// Try list cache first
	item := c.listCache.Get(listCacheKey)
	if item != nil {
		// Cache hit - return cached list (DO NOT extend TTL)
		items := item.Value().([]string)
		glog.V(4).Infof("List cache hit, returning %d items", len(items))
		return append([]string(nil), items...), nil // Return a copy
	}

	// Cache miss - fetch from base store
	glog.V(4).Infof("List cache miss, fetching from store")
	items, err := c.baseStore.List(ctx, filerAddress)
	if err != nil {
		return nil, err
	}

	// Cache the result with TTL (store a copy)
	itemsCopy := append([]string(nil), items...)
	c.listCache.Set(listCacheKey, itemsCopy, c.listTTL)
	glog.V(3).Infof("Cached list with %d entries, TTL %v", len(items), c.listTTL)
	return items, nil
}

// ClearCache clears all cached entries
func (c *CachedStore[T]) ClearCache() {
	c.cache.Clear()
	c.listCache.Clear()
	glog.V(2).Infof("Cleared all cache entries")
}

// GetCacheStats returns cache statistics
func (c *CachedStore[T]) GetCacheStats() map[string]interface{} {
	return map[string]interface{}{
		"itemCache": map[string]interface{}{
			"size": c.cache.ItemCount(),
			"ttl":  c.ttl.String(),
		},
		"listCache": map[string]interface{}{
			"size": c.listCache.ItemCount(),
			"ttl":  c.listTTL.String(),
		},
	}
}
