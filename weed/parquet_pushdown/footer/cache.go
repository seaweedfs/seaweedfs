package footer

import (
	"errors"
	"fmt"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"
)

// Identity uniquely names a Parquet file's content per the design's
// Index Consistency rules. The cache key is the same identity used
// to invalidate side indexes when a file's content changes.
//
// Iceberg manifest fields are preferred when available (Path,
// SizeBytes, RecordCount + ETag) because they pin the file by both
// its bytes and its declared row count — defending against the
// pathological case where a write produces a file with the same
// length but different content. ETag fills in for raw-S3 access
// without an Iceberg manifest. Mtime is intentionally not part of
// identity (see PARQUET_PUSHDOWN_DESIGN.md "Index Consistency").
type Identity struct {
	Path        string
	SizeBytes   int64
	RecordCount int64 // 0 when unknown (non-Iceberg-managed files)
	ETag        string
}

// Cache is a concurrent LRU of parsed footers keyed by Identity.
// Capacity is in entries, not bytes; Phase 1 footers are small
// enough that entry-count budgeting is easier to reason about.
type Cache struct {
	c      *lru.Cache
	hits   atomic.Int64
	misses atomic.Int64
}

// NewCache returns a Cache holding up to size entries. Returns an
// error when size is non-positive (a zero-size LRU is too easy to
// misconfigure to silently disable caching).
func NewCache(size int) (*Cache, error) {
	if size <= 0 {
		return nil, fmt.Errorf("cache size must be positive, got %d", size)
	}
	c, err := lru.New(size)
	if err != nil {
		return nil, fmt.Errorf("new lru: %w", err)
	}
	return &Cache{c: c}, nil
}

// Get returns the cached ParsedFooter for id, or nil + false on miss.
func (c *Cache) Get(id Identity) (*ParsedFooter, bool) {
	if v, ok := c.c.Get(id); ok {
		c.hits.Add(1)
		return v.(*ParsedFooter), true
	}
	c.misses.Add(1)
	return nil, false
}

// Add stores pf under id. Existing entries are replaced.
func (c *Cache) Add(id Identity, pf *ParsedFooter) {
	c.c.Add(id, pf)
}

// Stats returns cumulative hit and miss counters.
func (c *Cache) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// Len returns the current entry count.
func (c *Cache) Len() int {
	return c.c.Len()
}

// GetOrLoad atomically returns a cached entry or invokes load to
// produce one and inserts it. Two concurrent loads for the same id
// may both call load — the cache is not a singleflight; that
// optimization is deferred until profiling shows it matters. The
// returned pointer is the one stored in the cache so callers must
// treat it as read-only.
func (c *Cache) GetOrLoad(id Identity, load func() (*ParsedFooter, error)) (*ParsedFooter, error) {
	if pf, ok := c.Get(id); ok {
		return pf, nil
	}
	if load == nil {
		return nil, errors.New("nil load func")
	}
	pf, err := load()
	if err != nil {
		return nil, err
	}
	if pf == nil {
		return nil, errors.New("load returned nil ParsedFooter")
	}
	c.Add(id, pf)
	return pf, nil
}
