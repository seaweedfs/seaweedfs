package winfsp

import (
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

	c.insert("a/b", 7, fuse.Attr{Ino: 7, Size: 42})
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
	c.insert("x", 8, fuse.Attr{})
	time.Sleep(30 * time.Millisecond)
	c.insert("y", 9, fuse.Attr{})
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

	c.insert("a", 5, fuse.Attr{})
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
		c.insert("churn", uint64(100+i), fuse.Attr{})
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

	c.insert("a", 5, fuse.Attr{})
	c.insert("a", 6, fuse.Attr{})
	if inode, _, ok := c.lookup("a"); !ok || inode != 6 {
		t.Fatalf("lookup after replace = %d %v, want 6 true", inode, ok)
	}
	for i := 0; i < 3; i++ {
		time.Sleep(25 * time.Millisecond)
		c.insert("churn", uint64(100+i), fuse.Attr{})
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

	c.insert("dir", 2, fuse.Attr{})
	c.insert("dir/a", 3, fuse.Attr{})
	c.insert("dir/a/b", 4, fuse.Attr{})
	c.insert("dirt", 5, fuse.Attr{})

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

	c.insert("", 9, fuse.Attr{})
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
