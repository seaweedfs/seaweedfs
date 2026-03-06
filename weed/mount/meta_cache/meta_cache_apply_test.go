package meta_cache

import (
	"context"
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
	if got := countPath(invalidations.paths(), util.FullPath("/dir/file.txt")); got != 2 {
		t.Fatalf("invalidations for /dir/file.txt = %d, want 2", got)
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
	if got := countPath(invalidations.paths(), util.FullPath("/src/file.tmp")); got != 1 {
		t.Fatalf("invalidations for /src/file.tmp = %d, want 1", got)
	}
	if got := countPath(invalidations.paths(), util.FullPath("/dst/file.txt")); got != 1 {
		t.Fatalf("invalidations for /dst/file.txt = %d, want 1", got)
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
		func(path util.FullPath, entry *filer_pb.Entry) {
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
