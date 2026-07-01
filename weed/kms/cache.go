package kms

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const (
	// DefaultDataKeyCacheTTL is used when caching is enabled without an explicit TTL.
	DefaultDataKeyCacheTTL = time.Hour
	// DefaultDataKeyCacheSize bounds the number of cached decrypted data keys.
	DefaultDataKeyCacheSize = 1000
)

// CachedKMSProvider wraps a KMSProvider with an in-memory cache of decrypted
// data keys. A KMS Decrypt call is deterministic for a given (ciphertext,
// encryption context) pair, so every read of the same SSE-KMS object would
// otherwise repeat an identical, network-bound KMS round-trip. Caching the
// plaintext data key removes that round-trip on the read hot path — this is
// what the cache_enabled / cache_ttl provider settings control.
//
// Only Decrypt is cached. GenerateDataKey must return a unique key per call,
// and DescribeKey / GetKeyID are cheap metadata lookups, so all three pass
// straight through to the wrapped provider via the embedded interface.
type CachedKMSProvider struct {
	KMSProvider // wrapped provider; supplies the un-cached methods

	ttl        time.Duration
	maxEntries int

	mu      sync.Mutex
	entries map[string]*dataKeyCacheEntry
}

type dataKeyCacheEntry struct {
	keyID     string
	plaintext []byte
	expiresAt time.Time
}

// NewCachedKMSProvider wraps provider with a decrypted-data-key cache. A
// non-positive ttl or maxEntries falls back to the package defaults.
func NewCachedKMSProvider(provider KMSProvider, ttl time.Duration, maxEntries int) *CachedKMSProvider {
	if ttl <= 0 {
		ttl = DefaultDataKeyCacheTTL
	}
	if maxEntries <= 0 {
		maxEntries = DefaultDataKeyCacheSize
	}
	return &CachedKMSProvider{
		KMSProvider: provider,
		ttl:         ttl,
		maxEntries:  maxEntries,
		entries:     make(map[string]*dataKeyCacheEntry),
	}
}

// Decrypt returns the plaintext data key from cache when present, otherwise
// delegates to the wrapped provider and caches the result.
func (c *CachedKMSProvider) Decrypt(ctx context.Context, req *DecryptRequest) (*DecryptResponse, error) {
	if req == nil || len(req.CiphertextBlob) == 0 {
		// Let the wrapped provider produce its normal validation error.
		return c.KMSProvider.Decrypt(ctx, req)
	}

	cacheKey := dataKeyCacheKey(req)
	if resp := c.get(cacheKey); resp != nil {
		glog.V(4).Infof("KMS data key cache hit")
		return resp, nil
	}

	resp, err := c.KMSProvider.Decrypt(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store our own copy, then hand the original back to the caller. The
	// caller clears the plaintext it receives (ClearSensitiveData); keeping a
	// private copy means later cache hits still return valid key material.
	c.set(cacheKey, resp)
	return resp, nil
}

// get returns a fresh copy of the cached response, or nil on miss/expiry. The
// copy is essential: callers clear the returned plaintext after use, which
// would otherwise wipe the cached key material for every subsequent reader.
func (c *CachedKMSProvider) get(cacheKey string) *DecryptResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[cacheKey]
	if !ok {
		return nil
	}
	if time.Now().After(entry.expiresAt) {
		ClearSensitiveData(entry.plaintext)
		delete(c.entries, cacheKey)
		return nil
	}

	plaintext := make([]byte, len(entry.plaintext))
	copy(plaintext, entry.plaintext)
	return &DecryptResponse{KeyID: entry.keyID, Plaintext: plaintext}
}

// set stores a private copy of the plaintext data key.
func (c *CachedKMSProvider) set(cacheKey string, resp *DecryptResponse) {
	if resp == nil {
		return
	}

	plaintext := make([]byte, len(resp.Plaintext))
	copy(plaintext, resp.Plaintext)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[cacheKey]; !exists {
		c.evictIfFullLocked()
	}
	c.entries[cacheKey] = &dataKeyCacheEntry{
		keyID:     resp.KeyID,
		plaintext: plaintext,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// evictIfFullLocked drops expired entries and, if still at capacity, the entry
// closest to expiry. Callers must hold c.mu.
func (c *CachedKMSProvider) evictIfFullLocked() {
	if len(c.entries) < c.maxEntries {
		return
	}

	now := time.Now()
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			ClearSensitiveData(entry.plaintext)
			delete(c.entries, k)
		}
	}

	if len(c.entries) < c.maxEntries {
		return
	}

	// Still full: evict the soonest-to-expire entry.
	var oldestKey string
	var oldestAt time.Time
	for k, entry := range c.entries {
		if oldestKey == "" || entry.expiresAt.Before(oldestAt) {
			oldestKey = k
			oldestAt = entry.expiresAt
		}
	}
	if oldestKey != "" {
		ClearSensitiveData(c.entries[oldestKey].plaintext)
		delete(c.entries, oldestKey)
	}
}

// Close clears cached key material and closes the wrapped provider.
func (c *CachedKMSProvider) Close() error {
	c.mu.Lock()
	for k, entry := range c.entries {
		ClearSensitiveData(entry.plaintext)
		delete(c.entries, k)
	}
	c.mu.Unlock()
	return c.KMSProvider.Close()
}

// dataKeyCacheKey derives a stable cache key from the ciphertext blob and the
// encryption context. The context is part of the key because the same
// ciphertext decrypted under a different context is a different request, and
// caching by ciphertext alone would bypass the provider's context check.
func dataKeyCacheKey(req *DecryptRequest) string {
	h := sha256.New()

	// Length-prefix each field so distinct inputs cannot collide by concatenation.
	writeLengthPrefixed(h, req.CiphertextBlob)

	keys := make([]string, 0, len(req.EncryptionContext))
	for k := range req.EncryptionContext {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeLengthPrefixed(h, []byte(k))
		writeLengthPrefixed(h, []byte(req.EncryptionContext[k]))
	}

	return string(h.Sum(nil))
}

func writeLengthPrefixed(h hash.Hash, b []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
	h.Write(lenBuf[:])
	h.Write(b)
}

// wrapWithCacheIfEnabled returns provider wrapped in a data-key cache when the
// configuration enables caching, otherwise provider unchanged.
func wrapWithCacheIfEnabled(provider KMSProvider, config *KMSConfig) KMSProvider {
	if provider == nil || config == nil || !config.CacheEnabled {
		return provider
	}
	// Avoid double-wrapping if a cached provider is somehow reused.
	if _, ok := provider.(*CachedKMSProvider); ok {
		return provider
	}
	cached := NewCachedKMSProvider(provider, config.CacheTTL, config.MaxCacheSize)
	glog.V(1).Infof("KMS data key caching enabled (ttl=%s, max=%d)", cached.ttl, cached.maxEntries)
	return cached
}
