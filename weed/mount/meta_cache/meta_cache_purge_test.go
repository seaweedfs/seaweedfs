package meta_cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func insertCacheEntry(t *testing.T, mc *MetaCache, path util.FullPath) {
	t.Helper()
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: path,
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 1,
		},
	}); err != nil {
		t.Fatalf("insert %s: %v", path, err)
	}
}

// barrier flushes the apply loop: enqueued before it, PurgeDirectoryChildren is
// asynchronous, so a synchronous apply-loop call afterward guarantees the purge
// has been processed (the loop is FIFO and single-threaded).
func barrier(t *testing.T, mc *MetaCache) {
	t.Helper()
	if err := mc.AbortDirectoryBuild(context.Background(), util.FullPath("/__barrier__")); err != nil {
		t.Fatalf("barrier: %v", err)
	}
}

// TestPurgeSkippedWhileDirectoryBuilding is the core regression guard: a purge
// (idle eviction / kernel Forget / copy-range fallback) that lands while a
// directory is being rebuilt must NOT wipe the entries the build just inserted.
// Otherwise CompleteDirectoryBuild marks the directory cached over an empty
// store, and every file in it vanishes from the mount though it is safe on the
// filer.
func TestPurgeSkippedWhileDirectoryBuilding(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()

	dir := util.FullPath("/dir")
	if err := mc.BeginDirectoryBuild(context.Background(), dir); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	insertCacheEntry(t, mc, "/dir/a.txt")
	insertCacheEntry(t, mc, "/dir/b.txt")

	// A concurrent eviction tries to purge the directory mid-build. It is
	// enqueued before CompleteDirectoryBuild, so the apply loop processes it
	// first — while /dir is still building.
	var resetCalls int32
	mc.PurgeDirectoryChildren(dir, func() { atomic.AddInt32(&resetCalls, 1) })

	if err := mc.CompleteDirectoryBuild(context.Background(), dir, 0); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	if got := atomic.LoadInt32(&resetCalls); got != 0 {
		t.Fatalf("resetFn ran %d times during build; purge must be skipped", got)
	}
	if !mc.IsDirectoryCached(dir) {
		t.Fatal("/dir should be cached after build completes")
	}
	for _, name := range []string{"/dir/a.txt", "/dir/b.txt"} {
		if _, err := mc.FindEntry(context.Background(), util.FullPath(name)); err != nil {
			t.Fatalf("%s missing after mid-build purge: %v", name, err)
		}
	}
}

// TestPurgeClearsWhenNotBuilding verifies the eviction path still works off the
// apply loop: with no build in flight, the purge resets the cached flag and
// wipes the store.
func TestPurgeClearsWhenNotBuilding(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/dir": true})
	defer mc.Shutdown()

	dir := util.FullPath("/dir")
	insertCacheEntry(t, mc, "/dir/a.txt")

	var resetCalls int32
	mc.PurgeDirectoryChildren(dir, func() { atomic.AddInt32(&resetCalls, 1) })
	barrier(t, mc)

	if got := atomic.LoadInt32(&resetCalls); got != 1 {
		t.Fatalf("resetFn ran %d times; want 1", got)
	}
	if _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/a.txt")); err == nil {
		t.Fatal("/dir/a.txt should have been purged")
	}
}
