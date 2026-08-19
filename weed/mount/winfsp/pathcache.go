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

	mu      sync.Mutex
	entries map[string]*pathCacheEntry
	// Two graveyard generations: an appended reference always survives the
	// sweep of the call that appended it, so a caller still using the inode -
	// a walk mid-resolution, an operation that looked it up just before - is
	// never holding a reference the same call just returned.
	graveyard     []uint64
	prevGraveyard []uint64
	lastSweep     time.Time
	// gen counts purges. A resolve holds no lock across its Lookup RPC, so a
	// purge can run between the RPC and the insert; the insert then carries
	// exactly the name the purge removed and has to be discarded. The recent
	// purges are kept by key so only a purge that covers the inserted name
	// discards it: unrelated churn must not starve resolveAndSteal into EIO.
	gen          uint64
	recentPurges []purgeRecord
}

type purgeRecord struct {
	key    string
	prefix bool
}

// maxRecentPurges bounds the purges remembered for the covers check. A resolve
// older than the window is discarded without one, which only costs a retry.
const maxRecentPurges = 128

func (r purgeRecord) covers(key string) bool {
	return r.key == key || (r.prefix && strings.HasPrefix(key, r.key+"/"))
}

// purgedSince reports whether any purge after gen covers key.
func (c *pathCache) purgedSince(gen uint64, key string) bool {
	dropped := c.gen - gen
	if dropped == 0 {
		return false
	}
	if dropped > uint64(len(c.recentPurges)) {
		return true
	}
	for _, r := range c.recentPurges[uint64(len(c.recentPurges))-dropped:] {
		if r.covers(key) {
			return true
		}
	}
	return false
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

// snapshot returns the purge generation. A caller about to resolve outside
// the lock passes it back to insert, which discards the entry if any purge ran
// in between.
func (c *pathCache) snapshot() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// insert takes ownership of one lookup reference on inode, in every outcome:
// an entry a covering purge outdated goes to the graveyard instead of the map,
// so the caller may keep using the inode for at least one sweep either way.
func (c *pathCache) insert(key string, inode uint64, attr fuse.Attr, gen uint64) {
	if key == "" {
		c.forget(inode)
		return
	}
	var pending []uint64
	c.mu.Lock()
	if c.purgedSince(gen, key) {
		c.graveyard = append(c.graveyard, inode)
		pending = c.sweepLocked()
		c.mu.Unlock()
		c.forgetAll(pending)
		return
	}
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
	// Recorded even when the map holds nothing: the point is as much the
	// in-flight resolve about to insert this very name.
	c.gen++
	c.recentPurges = append(c.recentPurges, purgeRecord{key: key, prefix: prefix})
	if len(c.recentPurges) > maxRecentPurges {
		c.recentPurges = append(c.recentPurges[:0], c.recentPurges[len(c.recentPurges)-maxRecentPurges:]...)
	}
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

// sweepLocked returns the generation before last for the caller to forget
// outside the lock, and retires the current one. Sweeps run at most once per
// ttl and an appended reference sits out the sweep of its own call, so every
// reference rests for at least one full ttl after its last possible use.
func (c *pathCache) sweepLocked() []uint64 {
	now := time.Now()
	if now.Sub(c.lastSweep) < c.ttl {
		return nil
	}
	c.lastSweep = now
	pending := c.prevGraveyard
	c.prevGraveyard = c.graveyard
	c.graveyard = nil
	for key, entry := range c.entries {
		if now.After(entry.expires) {
			// Returned by the next sweep, a full ttl away: served from the map
			// until a moment ago, so someone may still be holding it.
			c.prevGraveyard = append(c.prevGraveyard, entry.inode)
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
