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
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

// midRebuildWipe begins a build of /dir over more children than one batch
// holds and runs the wipe's first batch. The returned func runs the rest of
// the wipe.
func midRebuildWipe(t *testing.T, mc *MetaCache) (finishWipe func()) {
	t.Helper()
	ctx := context.Background()
	fillRebuildWipeDir(t, mc, "/dir", 2*rebuildWipeBatchSize+10)
	if err := mc.BeginDirectoryBuild(ctx, "/dir"); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	stopKeeping := mc.keepInsertsDuringWipe("/dir")
	last, err := mc.deleteFolderChildrenBatch(ctx, "/dir", "")
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	return func() {
		t.Helper()
		for last != "" {
			if last, err = mc.deleteFolderChildrenBatch(ctx, "/dir", last); err != nil {
				t.Fatalf("batch: %v", err)
			}
		}
		stopKeeping()
	}
}

// applyDirEvent applies an event in /dir at tsNs that takes oldName away, puts
// newName there, or both. "" leaves that half out.
func applyDirEvent(t *testing.T, mc *MetaCache, options MetadataResponseApplyOptions, tsNs int64, oldName, newName string) {
	t.Helper()
	message := &filer_pb.EventNotification{}
	if oldName != "" {
		message.OldEntry = &filer_pb.Entry{Name: oldName}
	}
	if newName != "" {
		message.NewParentPath = "/dir"
		message.NewEntry = &filer_pb.Entry{Name: newName, Attributes: &filer_pb.FuseAttributes{FileMode: 0100644}}
	}
	event := &filer_pb.SubscribeMetadataResponse{Directory: "/dir", TsNs: tsNs, EventNotification: message}
	if err := mc.ApplyMetadataResponse(context.Background(), event, options); err != nil {
		t.Fatalf("apply event at %d (%q -> %q): %v", tsNs, oldName, newName, err)
	}
}

// insertListedEntries caches names as children of /dir the way a build's
// listing does.
func insertListedEntries(t *testing.T, mc *MetaCache, names ...string) {
	t.Helper()
	entries := make([]*filer.Entry, 0, len(names))
	for _, name := range names {
		entries = append(entries, &filer.Entry{
			FullPath: util.FullPath("/dir").Child(name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		})
	}
	if err := mc.doBatchInsertEntries(context.Background(), entries); err != nil {
		t.Fatalf("listing insert: %v", err)
	}
}

// TestRebuildWipeDropsAPlaceholderWhoseFileIsGone: a file created, flushed and
// then unlinked or renamed away, all while the wipe runs, is gone from the
// filer before the listing's snapshot. Neither event is replayed, so the
// removal has to take the placeholder out when it arrives, whichever side of
// the wipe's cursor the name is on.
func TestRebuildWipeDropsAPlaceholderWhoseFileIsGone(t *testing.T) {
	for _, tc := range []struct {
		desc, placeholder, renamedTo string
	}{
		{"unlinked ahead of the cursor", "zz.tmp", ""},
		{"unlinked behind the cursor", "a.tmp", ""},
		{"renamed ahead of the cursor", "zz.tmp", "final"},
		{"renamed behind the cursor", "a.tmp", "final"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
			defer mc.Shutdown()
			mc.SetPinnedChildFn(pinInode42)

			finishWipe := midRebuildWipe(t, mc)
			insertCacheEntry(t, mc, util.FullPath("/dir").Child(tc.placeholder))
			// the flush, then the unlink or the rename
			applyDirEvent(t, mc, LocalMetadataResponseApplyOptions, 100, "", tc.placeholder)
			applyDirEvent(t, mc, LocalMetadataResponseApplyOptions, 110, tc.placeholder, tc.renamedTo)
			finishWipe()

			// The listing starts after the wipe, so its snapshot, 200, is
			// past both events.
			var want []string
			if tc.renamedTo != "" {
				insertListedEntries(t, mc, tc.renamedTo)
				want = []string{tc.renamedTo}
			}
			if err := mc.CompleteDirectoryBuild(context.Background(), "/dir", 200, nil); err != nil {
				t.Fatalf("complete build: %v", err)
			}
			if got := cachedChildNames(t, mc, "/dir"); !reflect.DeepEqual(got, want) {
				t.Errorf("children of the rebuilt directory = %v, want %v: the filer has no %s", got, want, tc.placeholder)
			}

			// The placeholder's version record went with it, so the listing
			// fences the name like any other it did not have.
			applyDirEvent(t, mc, SubscriberMetadataResponseApplyOptions, 90, "", tc.placeholder)
			if got := cachedChildNames(t, mc, "/dir"); !reflect.DeepEqual(got, want) {
				t.Errorf("children after an event older than the listing = %v, want %v", got, want)
			}
		})
	}
}

// TestRebuildWipeKeepsAPlaceholderRewrittenInPlace: an event that names the
// same path as old and new, a flush or a setattr, vacates nothing.
func TestRebuildWipeKeepsAPlaceholderRewrittenInPlace(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)

	finishWipe := midRebuildWipe(t, mc)
	insertCacheEntry(t, mc, "/dir/zz-open")
	applyDirEvent(t, mc, LocalMetadataResponseApplyOptions, 100, "zz-open", "zz-open")
	finishWipe()

	if got, want := cachedChildNames(t, mc, "/dir"), []string{"zz-open"}; !reflect.DeepEqual(got, want) {
		t.Errorf("children left by the wipe = %v, want %v", got, want)
	}
}

// TestRebuildWipeKeepsAnEntryNewerThanTheRemoval: a removal that arrives during
// the wipe is fenced by versions like any other event, so one older than what
// the wipe kept leaves it alone.
func TestRebuildWipeKeepsAnEntryNewerThanTheRemoval(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)

	finishWipe := midRebuildWipe(t, mc)
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/zz-newer",
		Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1, Inode: 42},
	}, 500); err != nil {
		t.Fatalf("insert: %v", err)
	}
	applyDirEvent(t, mc, SubscriberMetadataResponseApplyOptions, 100, "zz-newer", "")
	finishWipe()

	if got, want := cachedChildNames(t, mc, "/dir"), []string{"zz-newer"}; !reflect.DeepEqual(got, want) {
		t.Errorf("children left by the wipe = %v, want %v", got, want)
	}
}

// TestRebuildWipeTellsAPlaceholderFromAnEarlierFile: a removal can arrive late,
// after a new file was created under its name. A placeholder is unversioned, so
// only the file the event took away tells whether it is the one cached there.
func TestRebuildWipeTellsAPlaceholderFromAnEarlierFile(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		removed   *filer_pb.FuseAttributes
		renamedTo string
		want      []string
		built     []string
	}{
		{"another inode", &filer_pb.FuseAttributes{Inode: 7, Crtime: 1}, "", []string{"zz-new"}, []string{"zz-new"}},
		{"another inode, renamed", &filer_pb.FuseAttributes{Inode: 7, Crtime: 1}, "zz-moved", []string{"zz-new"}, []string{"zz-moved", "zz-new"}},
		{"no inode and another creation time", &filer_pb.FuseAttributes{Crtime: 5}, "", []string{"zz-new"}, []string{"zz-new"}},
		{"the same inode", &filer_pb.FuseAttributes{Inode: 42, Crtime: 1}, "", nil, nil},
		{"the same inode, renamed", &filer_pb.FuseAttributes{Inode: 42, Crtime: 1}, "zz-moved", nil, []string{"zz-moved"}},
		{"no inode and the same creation time", &filer_pb.FuseAttributes{Crtime: 1}, "", nil, nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mc, _, _, invalidations := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
			defer mc.Shutdown()
			mc.SetPinnedChildFn(pinInode42)

			finishWipe := midRebuildWipe(t, mc)
			insertPinnedCacheEntry(t, mc, "/dir/zz-new") // inode 42, created at 1
			removal := &filer_pb.SubscribeMetadataResponse{Directory: "/dir", TsNs: 100, EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: "zz-new", Attributes: tc.removed},
			}}
			if tc.renamedTo != "" {
				removal.EventNotification.NewParentPath = "/dir"
				removal.EventNotification.NewEntry = &filer_pb.Entry{Name: tc.renamedTo, Attributes: &filer_pb.FuseAttributes{FileMode: 0100644, Inode: tc.removed.Inode, Crtime: tc.removed.Crtime}}
			}
			if err := mc.ApplyMetadataResponse(context.Background(), removal, SubscriberMetadataResponseApplyOptions); err != nil {
				t.Fatalf("apply removal: %v", err)
			}
			finishWipe()

			if got := cachedChildNames(t, mc, "/dir"); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("children left by the wipe = %v, want %v", got, tc.want)
			}

			// An unversioned listing replays every buffered event, so the
			// removal the wipe declined must not reach the placeholder there,
			// nor the handle that holds it.
			mc.WaitForEntryInvalidations()
			invalidatedBefore := len(invalidations.paths())
			if err := mc.CompleteDirectoryBuild(context.Background(), "/dir", 0, nil); err != nil {
				t.Fatalf("complete build: %v", err)
			}
			mc.WaitForEntryInvalidations()
			if got := cachedChildNames(t, mc, "/dir"); !reflect.DeepEqual(got, tc.built) {
				t.Errorf("children after an unversioned listing = %v, want %v", got, tc.built)
			}
			if tc.want != nil {
				for _, path := range invalidations.paths()[invalidatedBefore:] {
					if path == "/dir/zz-new" {
						t.Errorf("the build invalidated %s for the removal of another file", path)
					}
				}
			}
		})
	}
}

// TestRebuildWipeRechecksADeclinedRemovalOnReplay: a listing attempt that failed
// left file 7 cached and buffered its delete and the create of file 8 at the
// same name. The retry's wipe declines file 8's delete, since file 7 is still
// cached there, but the replay puts file 8 in place first and must then take
// it out again.
func TestRebuildWipeRechecksADeclinedRemovalOnReplay(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)
	ctx := context.Background()

	fillRebuildWipeDir(t, mc, "/dir", 2*rebuildWipeBatchSize+10)
	if err := mc.BeginDirectoryBuild(ctx, "/dir"); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.doBatchInsertEntries(ctx, []*filer.Entry{{
		FullPath: "/dir/zz-same",
		Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1, Inode: 7},
	}}); err != nil {
		t.Fatalf("first attempt's listing: %v", err)
	}
	file := func(inode uint64) *filer_pb.Entry {
		return &filer_pb.Entry{Name: "zz-same", Attributes: &filer_pb.FuseAttributes{FileMode: 0100644, Inode: inode, Crtime: 1}}
	}
	apply := func(tsNs int64, message *filer_pb.EventNotification) {
		t.Helper()
		if err := mc.ApplyMetadataResponse(ctx, &filer_pb.SubscribeMetadataResponse{Directory: "/dir", TsNs: tsNs, EventNotification: message}, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply event at %d: %v", tsNs, err)
		}
	}
	apply(100, &filer_pb.EventNotification{OldEntry: file(7)})
	apply(110, &filer_pb.EventNotification{NewParentPath: "/dir", NewEntry: file(8)})

	stopKeeping := mc.keepInsertsDuringWipe("/dir")
	last, err := mc.deleteFolderChildrenBatch(ctx, "/dir", "")
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	apply(120, &filer_pb.EventNotification{OldEntry: file(8)})
	for last != "" {
		if last, err = mc.deleteFolderChildrenBatch(ctx, "/dir", last); err != nil {
			t.Fatalf("batch: %v", err)
		}
	}
	stopKeeping()

	if err := mc.CompleteDirectoryBuild(ctx, "/dir", 0, nil); err != nil {
		t.Fatalf("complete build: %v", err)
	}
	if got := cachedChildNames(t, mc, "/dir"); len(got) != 0 {
		t.Errorf("children after an unversioned listing = %v, want none: file 8 was deleted", got)
	}
}

// TestRebuildWipeRenameKeepsTheHardLinkCount: taking a renamed name out of the
// cache removes no link, so the count the other names of the file share stays.
func TestRebuildWipeRenameKeepsTheHardLinkCount(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(pinInode42)
	ctx := context.Background()

	finishWipe := midRebuildWipe(t, mc)
	for _, path := range []util.FullPath{"/dir/zz-link", "/elsewhere/link"} {
		if err := mc.InsertEntry(ctx, &filer.Entry{
			FullPath:        path,
			Attr:            filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1, Inode: 42},
			HardLinkId:      filer.HardLinkId{1, 2, 3, 4},
			HardLinkCounter: 2,
		}, 0); err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}
	applyDirEvent(t, mc, LocalMetadataResponseApplyOptions, 100, "zz-link", "zz-moved")
	finishWipe()

	if got := cachedChildNames(t, mc, "/dir"); len(got) != 0 {
		t.Errorf("children left by the wipe = %v, want none: zz-link was renamed", got)
	}
	other, _, err := mc.FindEntry(ctx, "/elsewhere/link")
	if err != nil {
		t.Fatalf("find the other link: %v", err)
	}
	if other.HardLinkCounter != 2 {
		t.Errorf("hard link count of the other name = %d, want 2: a rename removes no link", other.HardLinkCounter)
	}
}

// TestRemovalAfterTheWipeIsLeftToTheReplay: once the wipe is over the store
// holds what the listing put there, and a removal from before the snapshot can
// still arrive. If its name was created again by then, the entry it would take
// out is the one the listing just inserted. A removal later than the snapshot
// is replayed.
func TestRemovalAfterTheWipeIsLeftToTheReplay(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()
	ctx := context.Background()

	if err := mc.BeginDirectoryBuild(ctx, "/dir"); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.deleteFolderChildrenForRebuild(ctx, "/dir"); err != nil {
		t.Fatalf("wipe: %v", err)
	}
	// The listing at 200: "again" was removed at 100 and created again by then.
	insertListedEntries(t, mc, "again", "gone-after", "stays")
	applyDirEvent(t, mc, SubscriberMetadataResponseApplyOptions, 100, "again", "")
	applyDirEvent(t, mc, SubscriberMetadataResponseApplyOptions, 300, "gone-after", "")
	if err := mc.CompleteDirectoryBuild(ctx, "/dir", 200, nil); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	if got, want := cachedChildNames(t, mc, "/dir"), []string{"again", "stays"}; !reflect.DeepEqual(got, want) {
		t.Errorf("children of the rebuilt directory = %v, want %v", got, want)
	}
}

// TestRebuildWipeIsRegisteredWhileItRuns: keeping inserts and applying
// removals both act only on a directory whose wipe is registered, so the wipe
// registers itself for as long as it runs, and no longer.
func TestRebuildWipeIsRegisteredWhileItRuns(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true})
	defer mc.Shutdown()

	fillRebuildWipeDir(t, mc, "/dir", rebuildWipeBatchSize+10)
	var visited, visitedWhileRegistered int
	// called for every child, under the wipe's lock
	mc.SetPinnedChildFn(func(*filer.Entry) bool {
		visited++
		if _, registered := mc.rebuildWipeInserts["/dir"]; registered {
			visitedWhileRegistered++
		}
		return false
	})
	if err := mc.deleteFolderChildrenForRebuild(context.Background(), "/dir"); err != nil {
		t.Fatalf("wipe: %v", err)
	}

	if visited == 0 || visitedWhileRegistered != visited {
		t.Errorf("the wipe was registered for %d of the %d children it visited, want all of them", visitedWhileRegistered, visited)
	}
	if len(mc.rebuildWipeInserts) != 0 {
		t.Errorf("rebuildWipeInserts = %v after the wipe ended, want empty", mc.rebuildWipeInserts)
	}
}
