package winfsp

import (
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// A resolve runs its Lookup outside the cache lock, so a purge can land
// between the RPC and the insert. The insert must then be discarded: it
// carries exactly the name the purge removed, and caching it would serve a
// deleted entry for a full ttl. This is how a rename's source briefly came
// back from the dead on the Windows mount.
func TestInsertAfterPurgeIsDiscarded(t *testing.T) {
	var forgotten []uint64
	c := newPathCache(time.Minute, func(inode uint64) { forgotten = append(forgotten, inode) })

	gen := c.snapshot()
	c.purge("dir/src", false)
	c.insert("dir/src", 42, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/src"); ok {
		t.Fatal("purged name served from an insert that started before the purge")
	}
}

// The discarded insert still owns a lookup reference, which has to come back
// through the graveyard rather than leak.
func TestDiscardedInsertReturnsItsReference(t *testing.T) {
	forgotten := make(map[uint64]bool)
	c := newPathCache(time.Nanosecond, func(inode uint64) { forgotten[inode] = true })

	gen := c.snapshot()
	c.purge("dir/src", false)
	c.insert("dir/src", 42, fuse.Attr{}, gen)

	// Sweeps run at most once per ttl; two spaced mutations drain the
	// graveyard the discard parked the reference in.
	time.Sleep(time.Millisecond)
	c.purge("other", false)
	time.Sleep(time.Millisecond)
	c.purge("other", false)

	if !forgotten[42] {
		t.Fatal("discarded insert leaked its lookup reference")
	}
}

// A purge of a directory covers the resolves in flight under it.
func TestInsertUnderPurgedPrefixIsDiscarded(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.purge("dir", true)
	c.insert("dir/child", 7, fuse.Attr{}, gen)

	if _, _, ok := c.lookup("dir/child"); ok {
		t.Fatal("purged prefix served a child from a stale insert")
	}
}

// An undisturbed resolve caches normally.
func TestInsertWithCurrentGenerationServes(t *testing.T) {
	c := newPathCache(time.Minute, func(uint64) {})

	gen := c.snapshot()
	c.insert("dir/file", 9, fuse.Attr{Size: 11}, gen)

	inode, attr, ok := c.lookup("dir/file")
	if !ok || inode != 9 || attr.Size != 11 {
		t.Fatalf("lookup = %d,%d,%v, want the inserted entry", inode, attr.Size, ok)
	}
}
