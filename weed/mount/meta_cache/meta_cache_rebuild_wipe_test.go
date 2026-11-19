package meta_cache

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func rebuildWipeChildName(i int) string {
	return fmt.Sprintf("f%06d", i)
}

// fillRebuildWipeDir caches n children of dir, named by rebuildWipeChildName.
func fillRebuildWipeDir(t *testing.T, mc *MetaCache, dir util.FullPath, n int) {
	t.Helper()
	entries := make([]*filer.Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, &filer.Entry{
			FullPath: dir.Child(rebuildWipeChildName(i)),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		})
	}
	if err := mc.doBatchInsertEntries(context.Background(), entries); err != nil {
		t.Fatalf("fill %s: %v", dir, err)
	}
}

// insertPinnedCacheEntry caches an entry the tests' pinnedChildFn keeps.
func insertPinnedCacheEntry(t *testing.T, mc *MetaCache, path util.FullPath) {
	t.Helper()
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: path,
		Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1, Inode: 42},
	}, 0); err != nil {
		t.Fatalf("insert pinned %s: %v", path, err)
	}
}

func pinInode42(e *filer.Entry) bool { return e.Attr.Inode == 42 }

func cachedChildNames(t *testing.T, mc *MetaCache, dir util.FullPath) []string {
	t.Helper()
	var names []string
	if _, err := mc.localStore.ListDirectoryEntries(context.Background(), dir, "", true, math.MaxInt64, func(entry *filer.Entry) (bool, error) {
		names = append(names, entry.Name())
		return true, nil
	}); err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	return names
}

// TestRebuildWipeSpansBatches: a wipe that takes several batches still drops
// every unpinned child, keeps a pinned one in whichever batch it falls, and
// leaves everything that is not a direct child alone.
func TestRebuildWipeSpansBatches(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)

	const n = 2*rebuildWipeBatchSize + 10
	fillRebuildWipeDir(t, mc, "/dir", n)
	pinnedFirst := rebuildWipeChildName(5) + "-pending"
	pinnedLast := rebuildWipeChildName(n-3) + "-pending"
	insertPinnedCacheEntry(t, mc, util.FullPath("/dir").Child(pinnedFirst))
	insertPinnedCacheEntry(t, mc, util.FullPath("/dir").Child(pinnedLast))
	insertCacheEntry(t, mc, "/dir/sub/grandchild")
	insertCacheEntry(t, mc, "/other/keep")

	if err := mc.deleteFolderChildrenForRebuild(context.Background(), "/dir"); err != nil {
		t.Fatalf("wipe: %v", err)
	}

	if got, want := cachedChildNames(t, mc, "/dir"), []string{pinnedFirst, pinnedLast}; !reflect.DeepEqual(got, want) {
		t.Errorf("children left by the wipe = %v, want only the pinned %v", got, want)
	}
	for _, p := range []util.FullPath{"/dir/sub/grandchild", "/other/keep"} {
		if _, _, err := mc.FindEntry(context.Background(), p); err != nil {
			t.Errorf("%s is not a child of the wiped directory but was lost: %v", p, err)
		}
	}
}

// TestRebuildWipeBatchIsBoundedAndJudgesPinnedLate drives the wipe one batch at
// a time. One batch removes at most rebuildWipeBatchSize children and leaves
// the lock free; what lands between batches is judged when its batch runs: a
// local-only create is kept, a stale entry is wiped.
func TestRebuildWipeBatchIsBoundedAndJudgesPinnedLate(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)

	const n = 2*rebuildWipeBatchSize + 10
	fillRebuildWipeDir(t, mc, "/dir", n)

	last, err := mc.deleteFolderChildrenBatch(context.Background(), "/dir", "")
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if !mc.TryRLock() {
		t.Fatal("a wipe batch returned with the meta cache still locked")
	}
	mc.RUnlock()
	if want := rebuildWipeChildName(rebuildWipeBatchSize - 1); last != want {
		t.Fatalf("first batch resumes after %q, want %q", last, want)
	}
	if got := len(cachedChildNames(t, mc, "/dir")); got != n-rebuildWipeBatchSize {
		t.Fatalf("children left after one batch = %d, want %d", got, n-rebuildWipeBatchSize)
	}

	// Both sort after every child the wipe has yet to visit.
	insertPinnedCacheEntry(t, mc, "/dir/zz-pending")
	insertCacheEntry(t, mc, "/dir/zz-stale")

	batches := 1
	for last != "" {
		if last, err = mc.deleteFolderChildrenBatch(context.Background(), "/dir", last); err != nil {
			t.Fatalf("batch %d: %v", batches+1, err)
		}
		batches++
	}
	if batches != 3 {
		t.Errorf("wipe of %d children took %d batches, want 3", n+2, batches)
	}
	if got, want := cachedChildNames(t, mc, "/dir"), []string{"zz-pending"}; !reflect.DeepEqual(got, want) {
		t.Errorf("children left by the wipe = %v, want only the create pinned between batches %v", got, want)
	}
}

// rebuildWipeReader reads another directory under mc's read lock and reports,
// while it still holds the lock, that it got in.
func rebuildWipeReader(mc *MetaCache, gotIn *atomic.Bool, done chan<- struct{}) {
	defer close(done)
	mc.ListDirectoryEntries(context.Background(), "/other", "", true, 1, func(*filer.Entry) (bool, error) {
		gotIn.Store(true)
		return true, nil
	})
}

// waitRebuildWipeReaderParked returns once rebuildWipeReader is parked on mc's
// read lock. The stack is a witness of where the reader is, not a timing
// assertion: no sleep decides who wins, and the deadline only refuses a broken
// arrangement.
func waitRebuildWipeReaderParked(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for n == len(buf) {
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			header, _, _ := strings.Cut(stack, "\n")
			if strings.Contains(header, "RWMutex.RLock") && strings.Contains(stack, ".rebuildWipeReader(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("the reader never parked on the meta cache read lock")
}

// TestRebuildWipeLetsReadersInBetweenBatches: a read of an unrelated directory
// that is parked behind the wipe gets the lock when the first batch ends,
// instead of waiting for the whole directory to be scanned.
func TestRebuildWipeLetsReadersInBetweenBatches(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/other": true})
	defer mc.Shutdown()

	fillRebuildWipeDir(t, mc, "/dir", 2*rebuildWipeBatchSize+1)
	insertCacheEntry(t, mc, "/other/x")

	var gotIn atomic.Bool
	readDone := make(chan struct{})
	calls := 0
	gotInBySecondBatch := false
	// pinnedChildFn runs once per child on this goroutine, under the wipe's
	// write lock.
	mc.SetPinnedChildFn(func(*filer.Entry) bool {
		calls++
		switch calls {
		case 1:
			go rebuildWipeReader(mc, &gotIn, readDone)
			waitRebuildWipeReaderParked(t)
		case rebuildWipeBatchSize + 1:
			// First child of the second batch. A reader parked when the first
			// batch unlocked holds the lock before the next writer gets it,
			// and the reader reports from under the lock.
			gotInBySecondBatch = gotIn.Load()
		}
		return false
	})

	if err := mc.deleteFolderChildrenForRebuild(context.Background(), "/dir"); err != nil {
		t.Fatalf("wipe: %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the reader never returned")
	}
	if calls != 2*rebuildWipeBatchSize+1 {
		t.Fatalf("pinnedChildFn calls = %d, want %d", calls, 2*rebuildWipeBatchSize+1)
	}
	if !gotInBySecondBatch {
		t.Fatal("a reader parked behind the wipe waited past the first batch; the wipe should release the lock between batches")
	}
}

// TestRebuildWipeKeepsInsertsWhileItRuns: a deferred create's placeholder can
// land between batches before its handle pins it. The wipe keeps it, as the
// single-lock wipe did by making the insert wait.
func TestRebuildWipeKeepsInsertsWhileItRuns(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)

	const n = 2*rebuildWipeBatchSize + 10
	fillRebuildWipeDir(t, mc, "/dir", n)

	stopKeeping := mc.keepInsertsDuringWipe("/dir")
	last, err := mc.deleteFolderChildrenBatch(context.Background(), "/dir", "")
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	insertCacheEntry(t, mc, "/dir/zz-placeholder")
	for batches := 1; last != ""; batches++ {
		if last, err = mc.deleteFolderChildrenBatch(context.Background(), "/dir", last); err != nil {
			t.Fatalf("batch %d: %v", batches+1, err)
		}
	}
	stopKeeping()

	if got, want := cachedChildNames(t, mc, "/dir"), []string{"zz-placeholder"}; !reflect.DeepEqual(got, want) {
		t.Errorf("children left by the wipe = %v, want only the one inserted while it ran %v", got, want)
	}
	if len(mc.rebuildWipeInserts) != 0 {
		t.Errorf("rebuildWipeInserts = %v after the wipe ended, want empty", mc.rebuildWipeInserts)
	}
}
