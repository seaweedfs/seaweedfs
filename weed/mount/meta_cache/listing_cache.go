package meta_cache

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// DefaultListingCacheEntries is how many directory children the mount will hold
// decoded in memory across every cached listing.
//
// This is a ceiling, not a reservation: nothing is held for a directory that was
// never walked, so a mount over ordinary directories never approaches it. A
// chunkless child measured about 480 bytes, so reaching the default means
// holding something near 450MB, and a mount that wants a tighter bound should
// lower it rather than rely on never walking that far.
const DefaultListingCacheEntries = 1000000

// listingCache keeps directories that have been walked all the way through, so
// walking one again costs no store reads and no decoding. It holds only what the
// meta cache already holds, and is void the moment that is: every store write
// goes through one of a handful of methods, and each drops the directory it
// touched.
//
// The entries are shared with whoever listed them, and are never written to
// after publication.
type listingCache struct {
	mu       sync.Mutex
	maxTotal int
	total    int
	listings map[util.FullPath]*cachedListing
	// builds are the walks in progress. A walk publishes only if its build is
	// still the live one for that directory when it finishes, so a write landing
	// halfway through cannot leave a stale listing behind.
	builds map[util.FullPath]*listingBuild
}

type cachedListing struct {
	entries    []*filer.Entry
	lastAccess time.Time
}

type listingBuild struct {
	entries []*filer.Entry
	// nextStart is the name the next page has to begin at for this build to be
	// a continuation rather than a different walk.
	nextStart string
}

func newListingCache(maxTotal int) *listingCache {
	return &listingCache{
		maxTotal: maxTotal,
		listings: make(map[util.FullPath]*cachedListing),
		builds:   make(map[util.FullPath]*listingBuild),
	}
}

func (lc *listingCache) enabled() bool { return lc != nil && lc.maxTotal > 0 }

// lookup returns a directory's complete listing.
func (lc *listingCache) lookup(dirPath util.FullPath) ([]*filer.Entry, bool) {
	if !lc.enabled() {
		return nil, false
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	cached, found := lc.listings[dirPath]
	if !found {
		return nil, false
	}
	cached.lastAccess = time.Now()
	return cached.entries, true
}

// invalidate drops a directory's listing and kills any walk building one. Called
// for every write the meta cache makes, so it has to stay cheap on a miss.
func (lc *listingCache) invalidate(dirPath util.FullPath) {
	if !lc.enabled() {
		return
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.dropLocked(dirPath)
	delete(lc.builds, dirPath)
}

// invalidateChild drops the listing of the directory holding path.
func (lc *listingCache) invalidateChild(path util.FullPath) {
	if !lc.enabled() {
		return
	}
	dir, _ := path.DirAndName()
	lc.invalidate(util.FullPath(dir))
}

func (lc *listingCache) invalidateAll() {
	if !lc.enabled() {
		return
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.listings = make(map[util.FullPath]*cachedListing)
	lc.builds = make(map[util.FullPath]*listingBuild)
	lc.total = 0
}

func (lc *listingCache) dropLocked(dirPath util.FullPath) {
	if cached, found := lc.listings[dirPath]; found {
		lc.total -= len(cached.entries)
		delete(lc.listings, dirPath)
	}
}

// beginOrContinue reports the build a page belongs to, or nil when this page is
// not the next step of a walk from the directory's first child. Only a walk that
// covers every child in order can be published.
func (lc *listingCache) beginOrContinue(dirPath util.FullPath, startFileName string, includeStartFile bool) *listingBuild {
	if !lc.enabled() || includeStartFile {
		return nil
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if startFileName == "" {
		build := &listingBuild{}
		lc.builds[dirPath] = build
		return build
	}
	build, found := lc.builds[dirPath]
	if !found || build.nextStart != startFileName {
		// A seek, a second walk at a different position, or a page that follows
		// one this build never saw. Neither walk can be trusted to be complete.
		delete(lc.builds, dirPath)
		return nil
	}
	return build
}

// carry keeps a build alive for the next page.
func (lc *listingCache) carry(dirPath util.FullPath, build *listingBuild, nextStart string) {
	if build == nil {
		return
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.builds[dirPath] != build {
		return // invalidated mid-walk
	}
	build.nextStart = nextStart
}

// publish installs a completed walk, unless a write invalidated it on the way or
// it does not fit. Dropping is always safe: the next walk reads the store.
func (lc *listingCache) publish(dirPath util.FullPath, build *listingBuild) {
	if build == nil {
		return
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.builds[dirPath] != build {
		return
	}
	delete(lc.builds, dirPath)
	if len(build.entries) > lc.maxTotal {
		return
	}
	lc.dropLocked(dirPath)
	lc.evictLocked(len(build.entries))
	lc.listings[dirPath] = &cachedListing{entries: build.entries, lastAccess: time.Now()}
	lc.total += len(build.entries)
}

// evictLocked makes room for want entries, oldest use first.
func (lc *listingCache) evictLocked(want int) {
	for lc.total+want > lc.maxTotal && len(lc.listings) > 0 {
		var oldestPath util.FullPath
		var oldest time.Time
		for path, cached := range lc.listings {
			if oldest.IsZero() || cached.lastAccess.Before(oldest) {
				oldestPath, oldest = path, cached.lastAccess
			}
		}
		lc.dropLocked(oldestPath)
	}
}

func (lc *listingCache) size() (dirs, entries int) {
	if !lc.enabled() {
		return 0, 0
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.listings), lc.total
}
