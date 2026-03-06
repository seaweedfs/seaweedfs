package meta_cache

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestMetaCache(t *testing.T) (*MetaCache, map[util.FullPath]bool) {
	t.Helper()
	uidGidMapper, err := NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}
	cached := make(map[util.FullPath]bool)
	mc := NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		util.FullPath("/"),
		func(path util.FullPath) { cached[path] = true },
		func(path util.FullPath) bool { return cached[path] },
		func(util.FullPath, *filer_pb.Entry) {},
		nil,
	)
	t.Cleanup(func() { mc.Shutdown() })
	return mc, cached
}

func makeEntry(dir, name string) *filer.Entry {
	return &filer.Entry{
		FullPath: util.NewFullPath(dir, name),
		Attr:     filer.Attr{Mode: 0644},
	}
}

func listEntries(t *testing.T, mc *MetaCache, dir string) []string {
	t.Helper()
	var names []string
	err := mc.ListDirectoryEntries(context.Background(), util.FullPath(dir), "", false, 10000, func(entry *filer.Entry) (bool, error) {
		names = append(names, entry.Name())
		return true, nil
	})
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	return names
}

// TestCommitRefreshDeleteDuringRefresh verifies that a delete event arriving
// during a directory refresh is not overwritten by the stale snapshot.
func TestCommitRefreshDeleteDuringRefresh(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	// Pre-populate the cache with 3 entries
	for _, name := range []string{"a", "b", "c"} {
		if err := mc.InsertEntry(ctx, makeEntry("/testdir", name)); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Start a refresh — simulate fetching a snapshot that includes all 3
	mc.BeginRefresh(dir)

	// While refresh is in progress, a subscription event deletes "b"
	if err := mc.AtomicUpdateEntryFromFiler(ctx, util.NewFullPath("/testdir", "b"), nil); err != nil {
		t.Fatalf("atomic delete b: %v", err)
	}

	// Commit the snapshot (which still includes "b") — the buffered delete
	// should be replayed, removing "b" from the final result
	snapshot := []*filer.Entry{
		makeEntry("/testdir", "a"),
		makeEntry("/testdir", "b"),
		makeEntry("/testdir", "c"),
	}
	if err := mc.CommitRefresh(ctx, dir, snapshot); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	names := listEntries(t, mc, "/testdir")
	expected := map[string]bool{"a": true, "c": true}
	if len(names) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, names)
	}
	for _, n := range names {
		if !expected[n] {
			t.Errorf("unexpected entry %q in listing", n)
		}
	}
}

// TestCommitRefreshCreateDuringRefresh verifies that a create event arriving
// during a directory refresh is preserved after the snapshot is applied.
func TestCommitRefreshCreateDuringRefresh(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	// Pre-populate with "a"
	if err := mc.InsertEntry(ctx, makeEntry("/testdir", "a")); err != nil {
		t.Fatalf("insert a: %v", err)
	}

	// Start refresh — snapshot will have "a" only
	mc.BeginRefresh(dir)

	// During refresh, a subscription event creates "d"
	newEntry := makeEntry("/testdir", "d")
	if err := mc.AtomicUpdateEntryFromFiler(ctx, "", newEntry); err != nil {
		t.Fatalf("atomic create d: %v", err)
	}

	// Commit snapshot (only "a") — buffered create of "d" should be replayed
	snapshot := []*filer.Entry{makeEntry("/testdir", "a")}
	if err := mc.CommitRefresh(ctx, dir, snapshot); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	names := listEntries(t, mc, "/testdir")
	expected := map[string]bool{"a": true, "d": true}
	if len(names) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, names)
	}
	for _, n := range names {
		if !expected[n] {
			t.Errorf("unexpected entry %q", n)
		}
	}
}

// TestCommitRefreshStaleEntriesRemoved verifies that entries deleted on the
// filer (absent from the snapshot) are removed from the local cache.
func TestCommitRefreshStaleEntriesRemoved(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	// Pre-populate with a, b, c
	for _, name := range []string{"a", "b", "c"} {
		if err := mc.InsertEntry(ctx, makeEntry("/testdir", name)); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Refresh with snapshot that only has "a" and "c" — "b" was deleted on filer
	mc.BeginRefresh(dir)
	snapshot := []*filer.Entry{
		makeEntry("/testdir", "a"),
		makeEntry("/testdir", "c"),
	}
	if err := mc.CommitRefresh(ctx, dir, snapshot); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	names := listEntries(t, mc, "/testdir")
	for _, n := range names {
		if n == "b" {
			t.Errorf("stale entry 'b' should have been removed by refresh")
		}
	}
	if len(names) != 2 {
		t.Fatalf("expected [a, c], got %v", names)
	}
}

// TestCommitRefreshLocalDeleteBuffered verifies that a local DeleteEntry
// during refresh is both applied immediately and replayed after commit.
func TestCommitRefreshLocalDeleteBuffered(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	// Pre-populate
	for _, name := range []string{"a", "b"} {
		if err := mc.InsertEntry(ctx, makeEntry("/testdir", name)); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	mc.BeginRefresh(dir)

	// Local delete of "a" — simulates Unlink calling DeleteEntry directly
	if err := mc.DeleteEntry(ctx, util.NewFullPath("/testdir", "a")); err != nil {
		t.Fatalf("delete a: %v", err)
	}

	// Verify "a" is gone immediately (before commit)
	_, err := mc.FindEntry(ctx, util.NewFullPath("/testdir", "a"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("expected ErrNotFound for deleted entry, got: %v", err)
	}

	// Commit with snapshot that includes "a" — the buffered delete replays
	snapshot := []*filer.Entry{
		makeEntry("/testdir", "a"),
		makeEntry("/testdir", "b"),
	}
	if err := mc.CommitRefresh(ctx, dir, snapshot); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	// "a" should still be gone after commit
	names := listEntries(t, mc, "/testdir")
	for _, n := range names {
		if n == "a" {
			t.Errorf("locally deleted entry 'a' reappeared after commit")
		}
	}
}

// TestCommitRefreshLocalCreateBuffered verifies that a local InsertEntry
// during refresh is preserved after the snapshot is applied.
func TestCommitRefreshLocalCreateBuffered(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	mc.BeginRefresh(dir)

	// Local create during refresh
	if err := mc.InsertEntry(ctx, makeEntry("/testdir", "new")); err != nil {
		t.Fatalf("insert new: %v", err)
	}

	// Commit with empty snapshot — the buffered create should still appear
	if err := mc.CommitRefresh(ctx, dir, nil); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	names := listEntries(t, mc, "/testdir")
	if len(names) != 1 || names[0] != "new" {
		t.Fatalf("expected [new], got %v", names)
	}
}

// TestCancelRefreshDiscardsBuffer verifies that CancelRefresh discards
// buffered events and resumes normal event processing.
func TestCancelRefreshDiscardsBuffer(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	if err := mc.InsertEntry(ctx, makeEntry("/testdir", "a")); err != nil {
		t.Fatalf("insert a: %v", err)
	}

	mc.BeginRefresh(dir)

	// Buffer a delete event
	if err := mc.AtomicUpdateEntryFromFiler(ctx, util.NewFullPath("/testdir", "a"), nil); err != nil {
		t.Fatalf("atomic delete: %v", err)
	}

	// Cancel — buffered events are discarded
	mc.CancelRefresh(dir)

	// "a" should still be in cache since the buffered delete was discarded
	entry, err := mc.FindEntry(ctx, util.NewFullPath("/testdir", "a"))
	if err != nil {
		t.Fatalf("find a: %v", err)
	}
	if entry == nil {
		t.Fatal("entry 'a' should still exist after cancel")
	}

	// After cancel, events should apply normally (not buffered)
	if err := mc.AtomicUpdateEntryFromFiler(ctx, util.NewFullPath("/testdir", "a"), nil); err != nil {
		t.Fatalf("atomic delete after cancel: %v", err)
	}
	_, err = mc.FindEntry(ctx, util.NewFullPath("/testdir", "a"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("expected ErrNotFound after direct delete, got: %v", err)
	}
}

// TestConcurrentDeletesDuringRefresh simulates the scenario from issue #8442:
// multiple concurrent deletes racing with a directory refresh.
func TestConcurrentDeletesDuringRefresh(t *testing.T) {
	mc, cached := newTestMetaCache(t)
	ctx := context.Background()
	dir := util.FullPath("/testdir")
	cached[dir] = true

	const numFiles = 1000

	// Pre-populate
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file_%04d", i)
		if err := mc.InsertEntry(ctx, makeEntry("/testdir", name)); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Build snapshot (taken before deletes happen)
	var snapshot []*filer.Entry
	for i := 0; i < numFiles; i++ {
		snapshot = append(snapshot, makeEntry("/testdir", fmt.Sprintf("file_%04d", i)))
	}

	mc.BeginRefresh(dir)

	// Concurrently delete all files via subscription events (simulates
	// deletes from two mount nodes)
	var wg sync.WaitGroup
	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("file_%04d", idx)
			fp := util.NewFullPath("/testdir", name)
			mc.AtomicUpdateEntryFromFiler(ctx, fp, nil)
		}(i)
	}
	wg.Wait()

	// Commit with the stale snapshot — all deletes should be replayed
	if err := mc.CommitRefresh(ctx, dir, snapshot); err != nil {
		t.Fatalf("commit refresh: %v", err)
	}

	names := listEntries(t, mc, "/testdir")
	if len(names) != 0 {
		t.Errorf("expected 0 entries after all deletes, got %d: first few: %v",
			len(names), names[:min(5, len(names))])
	}
}
