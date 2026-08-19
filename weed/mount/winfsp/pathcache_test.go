package winfsp

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

type forgetRecorder struct {
	mu     sync.Mutex
	inodes []uint64
}

func (r *forgetRecorder) forget(inode uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inodes = append(r.inodes, inode)
}

func (r *forgetRecorder) forgotten() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.inodes...)
}

func TestPathCacheLookupAndExpiry(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(20*time.Millisecond, rec.forget)

	c.insert("a/b", 7, fuse.Attr{Ino: 7, Size: 42}, c.snapshot())
	inode, attr, ok := c.lookup("a/b")
	if !ok || inode != 7 || attr.Size != 42 {
		t.Fatalf("lookup = %d %v %v, want 7 size 42 true", inode, attr.Size, ok)
	}

	time.Sleep(30 * time.Millisecond)
	if _, _, ok := c.lookup("a/b"); ok {
		t.Fatal("expired entry still served")
	}
	// The reference survives at least one sweep past expiry before it is
	// forgotten: one insert moves it to the graveyard, the next returns it.
	c.insert("x", 8, fuse.Attr{}, c.snapshot())
	time.Sleep(30 * time.Millisecond)
	c.insert("y", 9, fuse.Attr{}, c.snapshot())
	found := false
	for _, inode := range rec.forgotten() {
		if inode == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expired reference never forgotten; forgot %v", rec.forgotten())
	}
}

func TestPathCacheStealTransfersOwnership(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(20*time.Millisecond, rec.forget)

	c.insert("a", 5, fuse.Attr{}, c.snapshot())
	inode, ok := c.steal("a")
	if !ok || inode != 5 {
		t.Fatalf("steal = %d %v, want 5 true", inode, ok)
	}
	if _, ok := c.steal("a"); ok {
		t.Fatal("second steal succeeded")
	}
	// Drive several sweeps; the stolen reference must never be forgotten.
	for i := 0; i < 4; i++ {
		time.Sleep(25 * time.Millisecond)
		c.insert("churn", uint64(100+i), fuse.Attr{}, c.snapshot())
	}
	for _, inode := range rec.forgotten() {
		if inode == 5 {
			t.Fatal("stolen reference was forgotten by the cache")
		}
	}
}

func TestPathCacheReplaceReturnsOldReference(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(20*time.Millisecond, rec.forget)

	c.insert("a", 5, fuse.Attr{}, c.snapshot())
	c.insert("a", 6, fuse.Attr{}, c.snapshot())
	if inode, _, ok := c.lookup("a"); !ok || inode != 6 {
		t.Fatalf("lookup after replace = %d %v, want 6 true", inode, ok)
	}
	for i := 0; i < 3; i++ {
		time.Sleep(25 * time.Millisecond)
		c.insert("churn", uint64(100+i), fuse.Attr{}, c.snapshot())
	}
	found := false
	for _, inode := range rec.forgotten() {
		if inode == 5 {
			found = true
		}
		if inode == 6 && !found {
			t.Fatal("live reference forgotten before the replaced one")
		}
	}
	if !found {
		t.Fatalf("replaced reference never forgotten; forgot %v", rec.forgotten())
	}
}

func TestPathCachePurgePrefix(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(time.Minute, rec.forget)

	c.insert("dir", 2, fuse.Attr{}, c.snapshot())
	c.insert("dir/a", 3, fuse.Attr{}, c.snapshot())
	c.insert("dir/a/b", 4, fuse.Attr{}, c.snapshot())
	c.insert("dirt", 5, fuse.Attr{}, c.snapshot())

	c.purge("dir", true)
	if _, _, ok := c.lookup("dir"); ok {
		t.Fatal("purged key still cached")
	}
	if _, _, ok := c.lookup("dir/a/b"); ok {
		t.Fatal("purged subtree still cached")
	}
	// A sibling that only shares the name as a string prefix stays.
	if _, _, ok := c.lookup("dirt"); !ok {
		t.Fatal("sibling was purged with the subtree")
	}
}

func TestPathCacheRootNeverCached(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(time.Minute, rec.forget)

	c.insert("", 9, fuse.Attr{}, c.snapshot())
	if _, _, ok := c.lookup(""); ok {
		t.Fatal("root was cached")
	}
	if got := rec.forgotten(); len(got) != 1 || got[0] != 9 {
		t.Fatalf("root reference not returned immediately: %v", got)
	}
}

func TestCacheKey(t *testing.T) {
	for path, want := range map[string]string{
		`/a/b`:   "a/b",
		`\a\b`:   "a/b",
		`a//b/`:  "a/b",
		`/`:      "",
		``:       "",
		`/a/./b`: "a/b",
	} {
		if got := cacheKey(path); got != want {
			t.Errorf("cacheKey(%q) = %q, want %q", path, got, want)
		}
	}
}

// A resolve runs its Lookup outside the cache lock, so a purge can land
// between the RPC and the insert. An insert a purge covered must then be
// discarded: it carries exactly the name the purge removed, and caching it
// would serve a deleted entry for a full ttl. This is how a rename's source
// briefly came back from the dead on the Windows mount.
func TestPathCacheInsertAfterCoveringPurgeIsDiscarded(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.purge("dir/src", false)
	c.insert("dir/src", 42, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/src"); ok {
		t.Fatal("purged name served from an insert that started before the purge")
	}
}

// A purge of a directory covers the resolves in flight under it.
func TestPathCacheInsertUnderPurgedPrefixIsDiscarded(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.purge("dir", true)
	c.insert("dir/child", 7, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/child"); ok {
		t.Fatal("purged prefix served a child from a stale insert")
	}
}

// A purge of something else entirely must not discard the insert: an open
// retries resolve-then-steal only a few times before giving up with EIO, so
// unrelated churn starving every insert would fail opens of untouched paths.
func TestPathCacheUnrelatedPurgeDoesNotDiscard(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.purge("elsewhere", true)
	c.purge("dir/srcling", false)
	c.insert("dir/src", 9, fuse.Attr{Size: 11}, gen)

	inode, attr, ok := c.lookup("dir/src")
	if !ok || inode != 9 || attr.Size != 11 {
		t.Fatalf("lookup = %d,%d,%v, want the inserted entry", inode, attr.Size, ok)
	}
}

// Purges beyond the remembered window cannot be checked by key, so the insert
// is discarded rather than trusted.
func TestPathCacheInsertOlderThanPurgeWindowIsDiscarded(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	for i := 0; i <= maxRecentPurges; i++ {
		c.purge(fmt.Sprintf("unrelated/%d", i), false)
	}
	c.insert("dir/src", 3, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/src"); ok {
		t.Fatal("insert older than the purge window was trusted")
	}
}

// The discarded insert still owns a lookup reference, which has to come back
// through the graveyard rather than leak - and not in the same call: the
// walker is still using the inode.
func TestPathCacheDiscardedInsertRestsBeforeForget(t *testing.T) {
	rec := &forgetRecorder{}
	c := newPathCache(10*time.Millisecond, rec.forget)

	gen := c.snapshot()
	c.purge("dir/src", false)
	// Make a sweep due, so the discarding insert itself sweeps: the reference
	// it just parked must not come back out of that same call.
	time.Sleep(15 * time.Millisecond)
	c.insert("dir/src", 42, fuse.Attr{}, gen)
	for _, inode := range rec.forgotten() {
		if inode == 42 {
			t.Fatal("discarded insert forgotten in the same call; the walker still holds it")
		}
	}

	for i := 0; i < 3; i++ {
		time.Sleep(15 * time.Millisecond)
		c.purge("churn", false)
	}
	found := false
	for _, inode := range rec.forgotten() {
		if inode == 42 {
			found = true
		}
	}
	if !found {
		t.Fatalf("discarded insert leaked its lookup reference; forgot %v", rec.forgotten())
	}
}

// A purge of the whole cache - the root, prefix set - covers every in-flight
// insert, exactly as it removed every entry.
func TestPathCacheRootPurgeCoversEveryInsert(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.purge("", true)
	c.insert("dir/src", 42, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/src"); ok {
		t.Fatal("root purge did not cover an in-flight insert")
	}
}
