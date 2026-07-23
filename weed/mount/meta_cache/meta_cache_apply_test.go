package meta_cache

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestApplyMetadataResponseAppliesEventsInOrder(t *testing.T) {
	mc, _, notifications, invalidations := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	createResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    1,
					FileMode: 0100644,
					FileSize: 11,
				},
			},
		},
	}
	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "file.txt",
			},
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    2,
					FileMode: 0100644,
					FileSize: 29,
				},
			},
			NewParentPath: "/dir",
		},
	}
	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "file.txt",
			},
		},
	}

	if err := mc.ApplyMetadataResponse(context.Background(), createResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply create: %v", err)
	}

	entry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/file.txt"))
	if err != nil {
		t.Fatalf("find created entry: %v", err)
	}
	if entry.FileSize != 11 {
		t.Fatalf("created file size = %d, want 11", entry.FileSize)
	}

	if err := mc.ApplyMetadataResponse(context.Background(), updateResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply update: %v", err)
	}

	entry, err = mc.FindEntry(context.Background(), util.FullPath("/dir/file.txt"))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if entry.FileSize != 29 {
		t.Fatalf("updated file size = %d, want 29", entry.FileSize)
	}

	if err := mc.ApplyMetadataResponse(context.Background(), deleteResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	entry, err = mc.FindEntry(context.Background(), util.FullPath("/dir/file.txt"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("find deleted entry error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	if entry != nil {
		t.Fatalf("deleted entry still cached: %+v", entry)
	}

	if got := countPath(notifications.paths(), util.FullPath("/dir")); got != 3 {
		t.Fatalf("directory notifications for /dir = %d, want 3", got)
	}
	mc.WaitForEntryInvalidations()
	if got := countPath(invalidations.paths(), util.FullPath("/dir/file.txt")); got != 3 {
		t.Fatalf("invalidations for /dir/file.txt = %d, want 3 (create + update + delete)", got)
	}
}

func TestApplyMetadataResponseRenamesAcrossCachedDirectories(t *testing.T) {
	mc, _, notifications, invalidations := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/src": true,
		"/dst": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/src/file.tmp",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 7,
		},
	}); err != nil {
		t.Fatalf("insert source entry: %v", err)
	}

	renameResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "file.tmp",
			},
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    2,
					FileMode: 0100644,
					FileSize: 41,
				},
			},
			NewParentPath: "/dst",
		},
	}

	if err := mc.ApplyMetadataResponse(context.Background(), renameResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	oldEntry, err := mc.FindEntry(context.Background(), util.FullPath("/src/file.tmp"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("find old path error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	if oldEntry != nil {
		t.Fatalf("old path still cached: %+v", oldEntry)
	}

	newEntry, err := mc.FindEntry(context.Background(), util.FullPath("/dst/file.txt"))
	if err != nil {
		t.Fatalf("find new path: %v", err)
	}
	if newEntry.FileSize != 41 {
		t.Fatalf("renamed file size = %d, want 41", newEntry.FileSize)
	}

	if got := countPath(notifications.paths(), util.FullPath("/src")); got != 1 {
		t.Fatalf("directory notifications for /src = %d, want 1", got)
	}
	if got := countPath(notifications.paths(), util.FullPath("/dst")); got != 1 {
		t.Fatalf("directory notifications for /dst = %d, want 1", got)
	}
	mc.WaitForEntryInvalidations()
	if got := countPath(invalidations.paths(), util.FullPath("/src/file.tmp")); got != 1 {
		t.Fatalf("invalidations for /src/file.tmp = %d, want 1", got)
	}
	if got := countPath(invalidations.paths(), util.FullPath("/dst/file.txt")); got != 1 {
		t.Fatalf("invalidations for /dst/file.txt = %d, want 1", got)
	}
}

func TestApplyMetadataResponseLocalOptionsSkipInvalidations(t *testing.T) {
	mc, _, notifications, invalidations := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 7,
		},
	}); err != nil {
		t.Fatalf("insert source entry: %v", err)
	}

	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "file.txt",
			},
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    2,
					FileMode: 0100644,
					FileSize: 17,
				},
			},
			NewParentPath: "/dir",
		},
	}

	if err := mc.ApplyMetadataResponse(context.Background(), updateResp, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply local update: %v", err)
	}

	entry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/file.txt"))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if entry.FileSize != 17 {
		t.Fatalf("updated file size = %d, want 17", entry.FileSize)
	}
	if got := countPath(notifications.paths(), util.FullPath("/dir")); got != 1 {
		t.Fatalf("directory notifications for /dir = %d, want 1", got)
	}
	mc.WaitForEntryInvalidations()
	if got := len(invalidations.paths()); got != 0 {
		t.Fatalf("invalidations = %d, want 0", got)
	}
}

func TestApplyMetadataResponseDeduplicatesRepeatedFilerEvent(t *testing.T) {
	mc, _, notifications, invalidations := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 5,
		},
	}); err != nil {
		t.Fatalf("insert source entry: %v", err)
	}

	updateResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "file.txt",
			},
			NewEntry: &filer_pb.Entry{
				Name: "file.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    2,
					FileMode: 0100644,
					FileSize: 15,
				},
			},
			NewParentPath: "/dir",
			Signatures:    []int32{7},
		},
		TsNs: 99,
	}

	if err := mc.ApplyMetadataResponse(context.Background(), updateResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := mc.ApplyMetadataResponse(context.Background(), updateResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	entry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/file.txt"))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if entry.FileSize != 15 {
		t.Fatalf("updated file size = %d, want 15", entry.FileSize)
	}
	if got := countPath(notifications.paths(), util.FullPath("/dir")); got != 1 {
		t.Fatalf("directory notifications for /dir = %d, want 1", got)
	}
	mc.WaitForEntryInvalidations()
	if got := countPath(invalidations.paths(), util.FullPath("/dir/file.txt")); got != 1 {
		t.Fatalf("invalidations for /dir/file.txt = %d, want 1", got)
	}
}

func TestApplyMetadataResponseSkipsHiddenSystemEntryWhenDisabled(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	createResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name: "topics",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    1,
					FileMode: uint32(os.ModeDir | 0o755),
				},
				IsDirectory: true,
			},
		},
	}

	if err := mc.ApplyMetadataResponse(context.Background(), createResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply create: %v", err)
	}

	entry, err := mc.FindEntry(context.Background(), util.FullPath("/topics"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("find hidden entry error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	if entry != nil {
		t.Fatalf("hidden entry still cached: %+v", entry)
	}
}

func TestApplyMetadataResponsePurgesHiddenDestinationPath(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/src": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/topics",
		Attr: filer.Attr{
			Crtime: time.Unix(1, 0),
			Mtime:  time.Unix(1, 0),
			Mode:   os.ModeDir | 0o755,
		},
	}); err != nil {
		t.Fatalf("insert stale hidden dir: %v", err)
	}
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/topics/leaked.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0o644,
			FileSize: 7,
		},
	}); err != nil {
		t.Fatalf("insert leaked hidden child: %v", err)
	}
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/src/visible",
		Attr: filer.Attr{
			Crtime: time.Unix(1, 0),
			Mtime:  time.Unix(1, 0),
			Mode:   os.ModeDir | 0o755,
		},
	}); err != nil {
		t.Fatalf("insert source dir: %v", err)
	}

	renameResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name:        "visible",
				IsDirectory: true,
			},
			NewEntry: &filer_pb.Entry{
				Name: "topics",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   2,
					Mtime:    2,
					FileMode: uint32(os.ModeDir | 0o755),
				},
				IsDirectory: true,
			},
			NewParentPath: "/",
		},
	}

	if err := mc.ApplyMetadataResponse(context.Background(), renameResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	if entry, err := mc.FindEntry(context.Background(), util.FullPath("/src/visible")); err != filer_pb.ErrNotFound || entry != nil {
		t.Fatalf("source dir after rename = %+v, %v; want nil, %v", entry, err, filer_pb.ErrNotFound)
	}
	if entry, err := mc.FindEntry(context.Background(), util.FullPath("/topics")); err != filer_pb.ErrNotFound || entry != nil {
		t.Fatalf("hidden destination after rename = %+v, %v; want nil, %v", entry, err, filer_pb.ErrNotFound)
	}
	if entry, err := mc.FindEntry(context.Background(), util.FullPath("/topics/leaked.txt")); err != filer_pb.ErrNotFound || entry != nil {
		t.Fatalf("hidden child after rename = %+v, %v; want nil, %v", entry, err, filer_pb.ErrNotFound)
	}
}

// The entry attached to each invalidation is what an open file handle gets
// refreshed with, so it must be the entry now at that path — or nil when the
// path was vacated and the handle should keep its last entry — versioned by
// the event's filer log timestamp.
func TestCollectEntryInvalidationsCarryAuthoritativeEntries(t *testing.T) {
	newEntry := &filer_pb.Entry{
		Name:       "file.txt",
		Attributes: &filer_pb.FuseAttributes{FileSize: 42},
	}

	update := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      77,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewEntry:      newEntry,
			NewParentPath: "/dir",
		},
	}
	got := collectEntryInvalidations(update)
	if len(got) != 1 || got[0].path != "/dir/file.txt" || got[0].entry != newEntry || got[0].tsNs != 77 {
		t.Fatalf("in-place update invalidations = %+v, want [{/dir/file.txt NewEntry ts 77}]", got)
	}

	rename := &filer_pb.SubscribeMetadataResponse{
		Directory: "/src",
		TsNs:      78,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.tmp"},
			NewEntry:      newEntry,
			NewParentPath: "/dst",
		},
	}
	got = collectEntryInvalidations(rename)
	if len(got) != 2 || got[0].path != "/src/file.tmp" || got[0].entry != nil || got[0].tsNs != 78 ||
		got[1].path != "/dst/file.txt" || got[1].entry != newEntry || got[1].tsNs != 78 {
		t.Fatalf("rename invalidations = %+v, want [{/src/file.tmp nil} {/dst/file.txt NewEntry}]", got)
	}

	create := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      79,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: newEntry,
		},
	}
	got = collectEntryInvalidations(create)
	if len(got) != 1 || got[0].path != "/dir/file.txt" || got[0].entry != newEntry || got[0].tsNs != 79 {
		t.Fatalf("create invalidations = %+v, want [{/dir/file.txt NewEntry ts 79}]", got)
	}

	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      80,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file.txt"},
		},
	}
	got = collectEntryInvalidations(deleteResp)
	if len(got) != 1 || got[0].path != "/dir/file.txt" || got[0].entry != nil || got[0].tsNs != 80 {
		t.Fatalf("delete invalidations = %+v, want [{/dir/file.txt nil ts 80}]", got)
	}
}

func newTestMetaCache(t *testing.T, cached map[util.FullPath]bool) (*MetaCache, map[util.FullPath]bool, *recordedPaths, *recordedPaths) {
	t.Helper()

	mapper, err := NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}

	var cachedMu sync.Mutex
	notifications := &recordedPaths{}
	invalidations := &recordedPaths{}

	mc := NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		mapper,
		util.FullPath("/"),
		false,
		func(path util.FullPath) {
			cachedMu.Lock()
			defer cachedMu.Unlock()
			cached[path] = true
		},
		func(path util.FullPath) bool {
			cachedMu.Lock()
			defer cachedMu.Unlock()
			return cached[path]
		},
		func(path util.FullPath, entry *filer_pb.Entry, eventTsNs int64, deleted bool) {
			invalidations.record(path)
		},
		func(dir util.FullPath) {
			notifications.record(dir)
		},
	)

	return mc, cached, notifications, invalidations
}

type recordedPaths struct {
	mu    sync.Mutex
	items []util.FullPath
}

func (r *recordedPaths) record(path util.FullPath) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, path)
}

func (r *recordedPaths) paths() []util.FullPath {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]util.FullPath(nil), r.items...)
}

func countPath(paths []util.FullPath, target util.FullPath) int {
	count := 0
	for _, path := range paths {
		if path == target {
			count++
		}
	}
	return count
}

// An unversioned local write replaces the content, so the previous version
// claim no longer describes it and must be cleared — keeping it would fence
// out events correcting the unversioned state.
func TestUnversionedWriteClearsEntryVersion(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	versioned := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "file.txt",
				Attributes: &filer_pb.FuseAttributes{FileSize: 11, FileMode: 0100644},
			},
		},
	}
	if err := mc.ApplyMetadataResponse(context.Background(), versioned, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply versioned event: %v", err)
	}
	if _, versionTsNs, err := mc.FindEntryWithVersion(context.Background(), util.FullPath("/dir/file.txt")); err != nil || versionTsNs != 1000 {
		t.Fatalf("versioned entry = ts %d, %v; want 1000", versionTsNs, err)
	}

	if err := mc.UpdateEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(2, 0),
			Mode:     0100644,
			FileSize: 22,
		},
	}); err != nil {
		t.Fatalf("local update: %v", err)
	}
	if _, versionTsNs, err := mc.FindEntryWithVersion(context.Background(), util.FullPath("/dir/file.txt")); err != nil || versionTsNs != 0 {
		t.Fatalf("after unversioned write = ts %d, %v; want 0", versionTsNs, err)
	}
	mc.WaitForEntryInvalidations()
}

// A rebuild against a pre-upgrade filer returns no snapshot; the reinserted
// entries must not inherit the version records their pre-rebuild
// incarnations left behind, or valid events below the stale claim are
// rejected.
func TestUnversionedRebuildClearsStaleVersions(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	versioned := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      3000,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "file.txt",
				Attributes: &filer_pb.FuseAttributes{FileSize: 11, FileMode: 0100644},
			},
		},
	}
	if err := mc.ApplyMetadataResponse(context.Background(), versioned, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply versioned event: %v", err)
	}

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.doBatchInsertEntries(context.Background(), []*filer.Entry{{
		FullPath: "/dir/file.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 22,
		},
	}}); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 0); err != nil {
		t.Fatalf("complete unversioned build: %v", err)
	}

	if _, versionTsNs, err := mc.FindEntryWithVersion(context.Background(), util.FullPath("/dir/file.txt")); err != nil || versionTsNs != 0 {
		t.Fatalf("rebuilt entry version = %d, %v; want 0 (stale claim must not survive an unversioned rebuild)", versionTsNs, err)
	}
	mc.WaitForEntryInvalidations()
}

// Tombstones are scoped to directories whose cached state the fence
// protects; an uncached parent never serves from the store nor applies the
// resurrecting insert, so a tombstone there would only accumulate.
func TestTombstonesScopedToCachedDirectories(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":       true,
		"/cached": true,
	})
	defer mc.Shutdown()

	deleteEventFor := func(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: dir,
			TsNs:      tsNs,
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: name},
			},
		}
	}

	if err := mc.ApplyMetadataResponse(context.Background(), deleteEventFor("/cached", "file", 2000), SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply cached delete: %v", err)
	}
	if tsNs, tombstone := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/cached/file")); tsNs != 2000 || !tombstone {
		t.Fatalf("cached-dir delete record = (%d, %v), want tombstone at 2000", tsNs, tombstone)
	}

	if err := mc.ApplyMetadataResponse(context.Background(), deleteEventFor("/uncached", "file", 2000), SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply uncached delete: %v", err)
	}
	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/uncached/file")); tsNs != 0 {
		t.Fatalf("uncached-dir delete record = %d, want none", tsNs)
	}
	mc.WaitForEntryInvalidations()
}

// A completed listing's absence floor supersedes the direct-child tombstones
// at or below its snapshot; newer tombstones survive.
func TestBuildCompletionPrunesSupersededTombstones(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	apply := func(resp *filer_pb.SubscribeMetadataResponse) {
		t.Helper()
		if err := mc.ApplyMetadataResponse(context.Background(), resp, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	createEvent := func(name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			TsNs:      tsNs,
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name:       name,
					Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644},
				},
			},
		}
	}
	deleteEvent := func(name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			TsNs:      tsNs,
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: name},
			},
		}
	}

	apply(createEvent("a", 900))
	apply(deleteEvent("a", 1000))
	apply(createEvent("b", 3900))
	apply(deleteEvent("b", 4000))

	// A deeper descendant's tombstone belongs to its own directory's floor.
	mc.Lock()
	mc.setEntryTombstoneLocked(context.Background(), util.FullPath("/dir/sub/x"), 1000)
	mc.Unlock()

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 3000); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/a")); tsNs != 0 {
		t.Fatalf("tombstone at 1000 should be pruned by the floor at 3000, got %d", tsNs)
	}
	if tsNs, tombstone := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/b")); tsNs != 4000 || !tombstone {
		t.Fatalf("tombstone at 4000 must survive the floor at 3000, got (%d, %v)", tsNs, tombstone)
	}
	if tsNs, tombstone := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/sub/x")); tsNs != 1000 || !tombstone {
		t.Fatalf("descendant tombstone must survive the parent's prune, got (%d, %v)", tsNs, tombstone)
	}
	mc.WaitForEntryInvalidations()
}

// Evicting a directory clears its children's version records (plain and
// tombstone) so they cannot accumulate for the lifetime of a long-running
// mount.
func TestDirectoryEvictionClearsVersionRecords(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	create := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1000,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "keep", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644}},
		},
	}
	if err := mc.ApplyMetadataResponse(context.Background(), create, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	del := &filer_pb.SubscribeMetadataResponse{
		Directory:         "/dir",
		TsNs:              1100,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "gone"}},
	}
	if err := mc.ApplyMetadataResponse(context.Background(), del, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/keep")); tsNs != 1000 {
		t.Fatalf("version record for /dir/keep = %d, want 1000 before eviction", tsNs)
	}
	if tsNs, tombstone := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/gone")); tsNs != 1100 || !tombstone {
		t.Fatalf("tombstone for /dir/gone = (%d,%v), want (1100,true) before eviction", tsNs, tombstone)
	}

	if err := mc.DeleteFolderChildren(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("evict directory: %v", err)
	}

	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/keep")); tsNs != 0 {
		t.Fatalf("version record for /dir/keep survived eviction: %d", tsNs)
	}
	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/gone")); tsNs != 0 {
		t.Fatalf("tombstone for /dir/gone survived eviction: %d", tsNs)
	}
	mc.WaitForEntryInvalidations()
}

// A completed build versions its children through one directory floor rather
// than a version record per child, and the floor fences exactly as per-child
// records did.
func TestBuildFloorVersionsChildrenWithoutPerChildRecords(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.doBatchInsertEntries(context.Background(), []*filer.Entry{{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 300,
		},
	}}); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 2000); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	// No per-child record was written...
	if tsNs, _ := mc.getEntryVersionRecordLocked(context.Background(), util.FullPath("/dir/file")); tsNs != 0 {
		t.Fatalf("per-child version record = %d, want none (the floor versions the child)", tsNs)
	}
	// ...yet the child reads back at the listing snapshot.
	if _, versionTsNs, err := mc.FindEntryWithVersion(context.Background(), util.FullPath("/dir/file")); err != nil || versionTsNs != 2000 {
		t.Fatalf("child version = %d, %v; want 2000 from the directory floor", versionTsNs, err)
	}

	// And an event the snapshot already covered is still fenced out.
	covered := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 100, FileMode: 0100644}},
			NewParentPath: "/dir",
		},
	}
	if err := mc.ApplyMetadataResponse(context.Background(), covered, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply covered event: %v", err)
	}
	entry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil || entry.FileSize != 300 {
		t.Fatalf("entry = %+v, %v; want size 300 (floor must fence the covered event)", entry, err)
	}
	mc.WaitForEntryInvalidations()
}

// The presence probe applies the same TTL expiry the read path does, so a
// logically-expired entry is judged by its directory's floor rather than by
// the stale version record it still carries.
func TestExpiredEntryIsJudgedByDirectoryFloor(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/file",
		Attr: filer.Attr{
			Crtime:   time.Now().Add(-2 * time.Hour),
			Mtime:    time.Now().Add(-2 * time.Hour),
			Mode:     0100644,
			FileSize: 7,
			TtlSec:   1,
		},
	}); err != nil {
		t.Fatalf("insert expiring entry: %v", err)
	}

	mc.Lock()
	// A record newer than the directory floor: it only governs while the entry
	// it describes still exists. Once the entry has logically expired the floor
	// is the accurate answer, and an event above the floor must apply.
	mc.setEntryVersionLocked(context.Background(), util.FullPath("/dir/file"), 5000)
	mc.dirVersionFloors[util.FullPath("/dir")] = 3000
	blocks := mc.entryVersionBlocksLocked(context.Background(), util.FullPath("/dir/file"), 4000)
	mc.Unlock()

	if blocks {
		t.Fatal("event at 4000 fenced by an expired entry's stale record at 5000; the floor at 3000 should govern")
	}
}
