package meta_cache

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newListingTestCache(t *testing.T, maxEntries int) *MetaCache {
	t.Helper()
	uidGidMapper, err := NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}
	cached := map[util.FullPath]bool{}
	mc := NewMetaCache(t.TempDir(), uidGidMapper, util.FullPath("/"), false,
		func(p util.FullPath) { cached[p] = true },
		func(p util.FullPath) bool { return cached[p] },
		func(EntryInvalidation) {}, nil, maxEntries)
	t.Cleanup(mc.Shutdown)
	return mc
}

func seedDir(t *testing.T, mc *MetaCache, dir util.FullPath, names ...string) {
	t.Helper()
	now := time.Now()
	ctx := context.Background()
	if err := mc.InsertEntry(ctx, &filer.Entry{
		FullPath: dir, Attr: filer.Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now},
	}, 0); err != nil {
		t.Fatalf("insert dir: %v", err)
	}
	for _, name := range names {
		if err := mc.InsertEntry(ctx, &filer.Entry{
			FullPath: dir.Child(name), Attr: filer.Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 1},
		}, 0); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}
	mc.markCachedFn(util.FullPath("/"))
	mc.markCachedFn(dir)
}

// walk lists a directory to exhaustion in pages, returning the names seen.
func walk(t *testing.T, mc *MetaCache, ctx context.Context, dir util.FullPath, pageSize int64) []string {
	t.Helper()
	var seen []string
	start := ""
	for round := 0; round < 100; round++ {
		count := 0
		last, err := mc.ListDirectoryEntries(ctx, dir, start, false, pageSize, func(entry *filer.Entry) (bool, error) {
			seen = append(seen, entry.Name())
			count++
			return true, nil
		})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if last == "" || last == start {
			break
		}
		start = last
		if int64(count) < pageSize && count == 0 {
			break
		}
	}
	return seen
}

func listingCtx() context.Context { return filer_pb.WithChunksOmitted(context.Background()) }

func TestListingCacheServesAfterFullWalk(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b", "c")

	if got := walk(t, mc, listingCtx(), dir, 100); len(got) != 3 {
		t.Fatalf("first walk listed %v, want 3 entries", got)
	}
	dirs, entries := mc.ListingCacheSize()
	if dirs != 1 || entries != 3 {
		t.Fatalf("cache holds %d dirs / %d entries, want 1/3", dirs, entries)
	}
	if got := walk(t, mc, listingCtx(), dir, 100); len(got) != 3 {
		t.Fatalf("second walk listed %v, want the same 3", got)
	}
}

func TestListingCacheNotUsedWithoutChunksOmitted(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b")

	// A listing that wants chunks must neither fill nor read the cache, since
	// the cached entries were decoded without them.
	if got := walk(t, mc, context.Background(), dir, 100); len(got) != 2 {
		t.Fatalf("listed %v, want 2", got)
	}
	if dirs, _ := mc.ListingCacheSize(); dirs != 0 {
		t.Fatalf("cache holds %d dirs, want none", dirs)
	}
}

func TestListingCachePartialWalkIsNotCached(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b", "c", "d")

	// Caller stops after the first entry: the rest of the directory was never
	// seen, so nothing may be published.
	if _, err := mc.ListDirectoryEntries(listingCtx(), dir, "", false, 100, func(entry *filer.Entry) (bool, error) {
		return false, nil
	}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if dirs, entries := mc.ListingCacheSize(); dirs != 0 {
		t.Fatalf("cache holds %d dirs / %d entries after a stopped walk, want none", dirs, entries)
	}
}

func TestListingCacheSeekDoesNotBuild(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b", "c")

	// Starting partway through is not a walk of the whole directory.
	if _, err := mc.ListDirectoryEntries(listingCtx(), dir, "a", false, 100, func(entry *filer.Entry) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if dirs, _ := mc.ListingCacheSize(); dirs != 0 {
		t.Fatalf("cache holds %d dirs after a mid-directory listing, want none", dirs)
	}
}

func TestListingCacheInvalidation(t *testing.T) {
	dir := util.FullPath("/d")
	now := time.Now()
	cases := []struct {
		name   string
		mutate func(t *testing.T, mc *MetaCache)
	}{
		{"insert a child", func(t *testing.T, mc *MetaCache) {
			if err := mc.InsertEntry(context.Background(), &filer.Entry{
				FullPath: dir.Child("new"), Attr: filer.Attr{Mode: 0o644, Mtime: now, Crtime: now},
			}, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"update a child", func(t *testing.T, mc *MetaCache) {
			if err := mc.UpdateEntry(context.Background(), &filer.Entry{
				FullPath: dir.Child("a"), Attr: filer.Attr{Mode: 0o644, Mtime: now, Crtime: now, FileSize: 99},
			}); err != nil {
				t.Fatal(err)
			}
		}},
		{"delete a child", func(t *testing.T, mc *MetaCache) {
			if err := mc.DeleteEntry(context.Background(), dir.Child("a")); err != nil {
				t.Fatal(err)
			}
		}},
		{"delete the children", func(t *testing.T, mc *MetaCache) {
			if err := mc.DeleteFolderChildren(context.Background(), dir); err != nil {
				t.Fatal(err)
			}
		}},
		{"rename into the directory", func(t *testing.T, mc *MetaCache) {
			if err := mc.AtomicUpdateEntryFromFiler(context.Background(), "", &filer.Entry{
				FullPath: dir.Child("moved"), Attr: filer.Attr{Mode: 0o644, Mtime: now, Crtime: now},
			}); err != nil {
				t.Fatal(err)
			}
		}},
		{"purge the children", func(t *testing.T, mc *MetaCache) {
			mc.PurgeDirectoryChildren(dir, func() {})
		}},
		{"distrust everything", func(t *testing.T, mc *MetaCache) {
			mc.InvalidateAllListings()
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mc := newListingTestCache(t, DefaultListingCacheEntries)
			seedDir(t, mc, dir, "a", "b", "c")
			walk(t, mc, listingCtx(), dir, 100)
			if dirs, _ := mc.ListingCacheSize(); dirs != 1 {
				t.Fatalf("expected the walk to cache the directory, got %d dirs", dirs)
			}
			tc.mutate(t, mc)
			if dirs, entries := mc.ListingCacheSize(); dirs != 0 {
				t.Errorf("cache still holds %d dirs / %d entries after %s", dirs, entries, tc.name)
			}
		})
	}
}

func TestListingCacheSeesAWriteThatLandsMidWalk(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b", "c", "d")
	ctx := listingCtx()

	// First page of a two-page walk.
	last, err := mc.ListDirectoryEntries(ctx, dir, "", false, 2, func(entry *filer.Entry) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// A write lands before the walk finishes.
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: dir.Child("e"), Attr: filer.Attr{Mode: 0o644, Mtime: time.Now(), Crtime: time.Now()},
	}, 0); err != nil {
		t.Fatal(err)
	}

	// Finishing the walk must not publish what it started collecting.
	if _, err := mc.ListDirectoryEntries(ctx, dir, last, false, 100, func(entry *filer.Entry) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if dirs, entries := mc.ListingCacheSize(); dirs != 0 {
		t.Fatalf("published %d dirs / %d entries from a walk a write interrupted", dirs, entries)
	}

	// A fresh walk sees all five.
	if got := walk(t, mc, ctx, dir, 100); len(got) != 5 {
		t.Fatalf("walk listed %v, want 5 entries", got)
	}
}

func TestListingCacheAppliesExpiryOnServe(t *testing.T) {
	mc := newListingTestCache(t, DefaultListingCacheEntries)
	dir := util.FullPath("/d")
	now := time.Now()
	ctx := context.Background()
	if err := mc.InsertEntry(ctx, &filer.Entry{
		FullPath: dir, Attr: filer.Attr{Mode: os.ModeDir | 0o755, Mtime: now, Crtime: now},
	}, 0); err != nil {
		t.Fatal(err)
	}
	// "b" expires two seconds from now; nothing will write when it does.
	for _, e := range []struct {
		name   string
		ttlSec int32
		crtime time.Time
	}{{"a", 0, now}, {"b", 2, now}, {"c", 0, now}} {
		if err := mc.InsertEntry(ctx, &filer.Entry{
			FullPath: dir.Child(e.name),
			Attr:     filer.Attr{Mode: 0o644, Mtime: now, Crtime: e.crtime, TtlSec: e.ttlSec},
		}, 0); err != nil {
			t.Fatal(err)
		}
	}
	mc.markCachedFn(util.FullPath("/"))
	mc.markCachedFn(dir)

	if got := walk(t, mc, listingCtx(), dir, 100); len(got) != 3 {
		t.Fatalf("first walk listed %v, want 3", got)
	}

	// Age "b" past its TTL in place, the way the clock would, and confirm the
	// cached listing stops reporting it without anything invalidating.
	entry, _, err := mc.FindEntry(ctx, dir.Child("b"))
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	entry.Crtime = now.Add(-time.Hour)
	cachedEntries, found := mc.listings.lookup(dir)
	if !found {
		t.Fatal("expected the directory to be cached")
	}
	for _, e := range cachedEntries {
		if e.Name() == "b" {
			e.Crtime = now.Add(-time.Hour)
		}
	}

	got := walk(t, mc, listingCtx(), dir, 100)
	for _, name := range got {
		if name == "b" {
			t.Fatalf("expired entry still listed: %v", got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("listed %v, want a and c", got)
	}
}

func TestListingCacheEvictsToStayWithinBudget(t *testing.T) {
	mc := newListingTestCache(t, 6)
	ctx := listingCtx()
	for d := 0; d < 4; d++ {
		dir := util.FullPath(fmt.Sprintf("/d%d", d))
		seedDir(t, mc, dir, "a", "b", "c")
		if got := walk(t, mc, ctx, dir, 100); len(got) != 3 {
			t.Fatalf("%s listed %v, want 3", dir, got)
		}
		_, entries := mc.ListingCacheSize()
		if entries > 6 {
			t.Fatalf("cache holds %d entries, over the 6 budget", entries)
		}
	}
	dirs, entries := mc.ListingCacheSize()
	if dirs != 2 || entries != 6 {
		t.Fatalf("cache holds %d dirs / %d entries, want 2/6 after eviction", dirs, entries)
	}
}

func TestListingCacheDisabled(t *testing.T) {
	mc := newListingTestCache(t, 0)
	dir := util.FullPath("/d")
	seedDir(t, mc, dir, "a", "b")
	if got := walk(t, mc, listingCtx(), dir, 100); len(got) != 2 {
		t.Fatalf("listed %v, want 2", got)
	}
	if dirs, entries := mc.ListingCacheSize(); dirs != 0 || entries != 0 {
		t.Fatalf("disabled cache holds %d dirs / %d entries", dirs, entries)
	}
}
