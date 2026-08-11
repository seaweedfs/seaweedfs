package winfsp

import (
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// pathCache stands in for the dentry and attribute caches the kernel provides
// on the unix mounts. WinFsp addresses every operation by path, so without it
// each operation walks the whole path through Lookup again, and in a directory
// the filer has not listed yet every one of those lookups is a filer round
// trip.
//
// The cache owns one lookup reference per entry, the way the kernel holds one
// until it sends FORGET. An evicted reference sits out one sweep in the
// graveyard before it is returned, so an operation that resolved just before
// the eviction is not left holding a reclaimed inode.
type pathCache struct {
	ttl    time.Duration
	forget func(inode uint64)

	mu        sync.Mutex
	entries   map[string]*pathCacheEntry
	graveyard []uint64
	lastSweep time.Time
}

type pathCacheEntry struct {
	inode   uint64
	attr    fuse.Attr
	expires time.Time
}

// maxCachedPaths bounds the references parked here. Overflow clears the whole
// cache rather than tracking recency: entries expire within ttl anyway, so
// exact eviction order buys nothing.
const maxCachedPaths = 64 << 10

func newPathCache(ttl time.Duration, forget func(inode uint64)) *pathCache {
	return &pathCache{
		ttl:       ttl,
		forget:    forget,
		entries:   map[string]*pathCacheEntry{},
		lastSweep: time.Now(),
	}
}

// cacheKey canonicalises a WinFsp path. The root maps to "", which is never
// cached: its inode is fixed and holds no reference.
func cacheKey(path string) string {
	return strings.Join(splitPath(path), "/")
}

// lookup reports the cached inode and attributes for key. The entry stays in
// the cache; the inode remains valid at least one sweep past its expiry.
func (c *pathCache) lookup(key string) (inode uint64, attr fuse.Attr, ok bool) {
	if key == "" {
		return 0, fuse.Attr{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if !found || time.Now().After(entry.expires) {
		return 0, fuse.Attr{}, false
	}
	return entry.inode, entry.attr, true
}

// insert takes ownership of one lookup reference on inode.
func (c *pathCache) insert(key string, inode uint64, attr fuse.Attr) {
	if key == "" {
		c.forget(inode)
		return
	}
	var pending []uint64
	c.mu.Lock()
	if existing, found := c.entries[key]; found {
		c.graveyard = append(c.graveyard, existing.inode)
	} else if len(c.entries) >= maxCachedPaths {
		for _, entry := range c.entries {
			c.graveyard = append(c.graveyard, entry.inode)
		}
		c.entries = map[string]*pathCacheEntry{}
	}
	c.entries[key] = &pathCacheEntry{inode: inode, attr: attr, expires: time.Now().Add(c.ttl)}
	pending = c.sweepLocked()
	c.mu.Unlock()
	c.forgetAll(pending)
}

// steal removes key and hands its reference to the caller.
func (c *pathCache) steal(key string) (inode uint64, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if !found {
		return 0, false
	}
	delete(c.entries, key)
	return entry.inode, true
}

// purge drops key, and everything under it when prefix is set, which a rename
// or removal of a directory needs: the children's cached paths name entries
// that are no longer there.
func (c *pathCache) purge(key string, prefix bool) {
	var pending []uint64
	c.mu.Lock()
	if entry, found := c.entries[key]; found {
		c.graveyard = append(c.graveyard, entry.inode)
		delete(c.entries, key)
	}
	if prefix {
		under := key + "/"
		for k, entry := range c.entries {
			if key == "" || strings.HasPrefix(k, under) {
				c.graveyard = append(c.graveyard, entry.inode)
				delete(c.entries, k)
			}
		}
	}
	pending = c.sweepLocked()
	c.mu.Unlock()
	c.forgetAll(pending)
}

// sweepLocked returns the previous graveyard for the caller to forget outside
// the lock, and moves expired entries into the next one. Sweeps run at most
// once per ttl, so a reference rests here for at least one full ttl.
func (c *pathCache) sweepLocked() []uint64 {
	now := time.Now()
	if now.Sub(c.lastSweep) < c.ttl {
		return nil
	}
	c.lastSweep = now
	pending := c.graveyard
	c.graveyard = nil
	for key, entry := range c.entries {
		if now.After(entry.expires) {
			c.graveyard = append(c.graveyard, entry.inode)
			delete(c.entries, key)
		}
	}
	return pending
}

func (c *pathCache) forgetAll(inodes []uint64) {
	for _, inode := range inodes {
		c.forget(inode)
	}
}
