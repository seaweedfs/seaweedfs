package meta_cache

import (
	"context"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// need to have logic similar to FilerStoreWrapper
// e.g. fill fileId field for chunks

// bufferedEvent represents a subscription event captured during a directory refresh.
type bufferedEvent struct {
	oldPath  util.FullPath
	newEntry *filer.Entry
}

// refreshState tracks events that arrive while a directory is being refreshed
// from the filer. After the refresh snapshot is applied, buffered events are
// replayed so that creates, deletes, and updates that raced with the snapshot
// are not lost.
type refreshState struct {
	events []bufferedEvent
}

type MetaCache struct {
	root         util.FullPath
	localStore   filer.VirtualFilerStore
	leveldbStore *leveldb.LevelDBStore // direct reference for batch operations
	sync.RWMutex
	uidGidMapper      *UidGidMapper
	markCachedFn      func(fullpath util.FullPath)
	isCachedFn        func(fullpath util.FullPath) bool
	invalidateFunc    func(fullpath util.FullPath, entry *filer_pb.Entry)
	onDirectoryUpdate func(dir util.FullPath)
	visitGroup        singleflight.Group // deduplicates concurrent EnsureVisited calls for the same path
	refreshing        map[util.FullPath]*refreshState
}

func NewMetaCache(dbFolder string, uidGidMapper *UidGidMapper, root util.FullPath,
	markCachedFn func(path util.FullPath), isCachedFn func(path util.FullPath) bool, invalidateFunc func(util.FullPath, *filer_pb.Entry), onDirectoryUpdate func(dir util.FullPath)) *MetaCache {
	leveldbStore, virtualStore := openMetaStore(dbFolder)
	return &MetaCache{
		root:              root,
		localStore:        virtualStore,
		leveldbStore:      leveldbStore,
		markCachedFn:      markCachedFn,
		isCachedFn:        isCachedFn,
		uidGidMapper:      uidGidMapper,
		onDirectoryUpdate: onDirectoryUpdate,
		invalidateFunc: func(fullpath util.FullPath, entry *filer_pb.Entry) {
			invalidateFunc(fullpath, entry)
		},
		refreshing: make(map[util.FullPath]*refreshState),
	}
}

func openMetaStore(dbFolder string) (*leveldb.LevelDBStore, filer.VirtualFilerStore) {

	os.RemoveAll(dbFolder)
	os.MkdirAll(dbFolder, 0755)

	store := &leveldb.LevelDBStore{}
	config := &cacheConfig{
		dir: dbFolder,
	}

	if err := store.Initialize(config, ""); err != nil {
		glog.Fatalf("Failed to initialize metadata cache store for %s: %+v", store.GetName(), err)
	}

	return store, filer.NewFilerStoreWrapper(store)

}

func (mc *MetaCache) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	// Buffer the insert if the parent directory is being refreshed
	dir, _ := entry.DirAndName()
	if state := mc.isRefreshingDir(util.FullPath(dir)); state != nil {
		state.events = append(state.events, bufferedEvent{newEntry: entry})
	}
	return mc.doInsertEntry(ctx, entry)
}

func (mc *MetaCache) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	return mc.localStore.InsertEntry(ctx, entry)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()

	// If the affected directory is being refreshed, buffer the event
	// instead of applying it. It will be replayed after the snapshot commits.
	if oldPath != "" {
		dir, _ := oldPath.DirAndName()
		if state := mc.isRefreshingDir(util.FullPath(dir)); state != nil {
			state.events = append(state.events, bufferedEvent{oldPath, newEntry})
			return nil
		}
	}
	if newEntry != nil {
		newDir, _ := newEntry.DirAndName()
		if state := mc.isRefreshingDir(util.FullPath(newDir)); state != nil {
			state.events = append(state.events, bufferedEvent{oldPath, newEntry})
			return nil
		}
	}

	return mc.doAtomicUpdateEntryFromFiler(ctx, oldPath, newEntry)
}

// doAtomicUpdateEntryFromFiler is the core logic for applying a filer event
// to the local cache. Caller must hold mc.Lock().
func (mc *MetaCache) doAtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	entry, err := mc.localStore.FindEntry(ctx, oldPath)
	if err != nil && err != filer_pb.ErrNotFound {
		glog.Errorf("Metacache: find entry error: %v", err)
		return err
	}
	if entry != nil {
		if oldPath != "" {
			if newEntry != nil && oldPath == newEntry.FullPath {
				// skip the unnecessary deletion
				// leave the update to the following InsertEntry operation
			} else {
				ctx = context.WithValue(ctx, "OP", "MV")
				glog.V(3).Infof("DeleteEntry %s", oldPath)
				if err := mc.localStore.DeleteEntry(ctx, oldPath); err != nil {
					return err
				}
			}
		}
	}

	if newEntry != nil {
		newDir, _ := newEntry.DirAndName()
		if mc.isCachedFn(util.FullPath(newDir)) {
			glog.V(3).Infof("InsertEntry %s/%s", newDir, newEntry.Name())
			if err := mc.localStore.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.UpdateEntry(ctx, entry)
}

func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer.Entry, err error) {
	mc.RLock()
	defer mc.RUnlock()
	entry, err = mc.localStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	if entry.TtlSec > 0 && entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
		return nil, filer_pb.ErrNotFound
	}
	mc.mapIdFromFilerToLocal(entry)
	return
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	// Buffer the delete if the parent directory is being refreshed
	dir, _ := fp.DirAndName()
	if state := mc.isRefreshingDir(util.FullPath(dir)); state != nil {
		state.events = append(state.events, bufferedEvent{oldPath: fp})
	}
	// Always apply the delete directly as well, so the entry is removed
	// immediately for the current node's view. CommitRefresh's snapshot
	// may re-insert it, but the buffered event replay will re-delete it.
	return mc.localStore.DeleteEntry(ctx, fp)
}
func (mc *MetaCache) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.DeleteFolderChildren(ctx, fp)
}

// BeginRefresh starts buffering subscription events for dirPath.
// While a refresh is active, AtomicUpdateEntryFromFiler will buffer events
// instead of applying them, so they can be replayed after the snapshot is committed.
func (mc *MetaCache) BeginRefresh(dirPath util.FullPath) {
	mc.Lock()
	defer mc.Unlock()
	mc.refreshing[dirPath] = &refreshState{}
}

// CommitRefresh atomically replaces a directory's cached entries with the
// filer snapshot, then replays any subscription events that were buffered
// during the refresh. This ensures no creates, deletes, or updates are lost.
func (mc *MetaCache) CommitRefresh(ctx context.Context, dirPath util.FullPath, entries []*filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()

	// Clear stale entries and insert the fresh snapshot
	if err := mc.localStore.DeleteFolderChildren(ctx, dirPath); err != nil {
		return err
	}
	if len(entries) > 0 {
		if err := mc.leveldbStore.BatchInsertEntries(ctx, entries); err != nil {
			return err
		}
	}

	// Replay buffered events so mutations that raced with the snapshot are applied
	state := mc.refreshing[dirPath]
	delete(mc.refreshing, dirPath)
	if state != nil {
		for _, ev := range state.events {
			if err := mc.doAtomicUpdateEntryFromFiler(ctx, ev.oldPath, ev.newEntry); err != nil {
				glog.Warningf("replay buffered event for %s: %v", dirPath, err)
			}
		}
	}
	return nil
}

// CancelRefresh discards the refresh state without replaying buffered events.
func (mc *MetaCache) CancelRefresh(dirPath util.FullPath) {
	mc.Lock()
	defer mc.Unlock()
	delete(mc.refreshing, dirPath)
}

// isRefreshing returns the refresh state for the directory containing fp,
// or nil if no refresh is active. Caller must hold mc.Lock().
func (mc *MetaCache) isRefreshingDir(dirPath util.FullPath) *refreshState {
	return mc.refreshing[dirPath]
}

func (mc *MetaCache) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) error {
	mc.RLock()
	defer mc.RUnlock()

	if !mc.isCachedFn(dirPath) {
		// if this request comes after renaming, it should be fine
		glog.Warningf("unsynchronized dir: %v", dirPath)
	}

	_, err := mc.localStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *filer.Entry) (bool, error) {
		if entry.TtlSec > 0 && entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
			return true, nil
		}
		mc.mapIdFromFilerToLocal(entry)
		return eachEntryFunc(entry)
	})
	if err != nil {
		return err
	}
	return err
}

func (mc *MetaCache) Shutdown() {
	mc.Lock()
	defer mc.Unlock()
	mc.localStore.Shutdown()
}

func (mc *MetaCache) mapIdFromFilerToLocal(entry *filer.Entry) {
	entry.Attr.Uid, entry.Attr.Gid = mc.uidGidMapper.FilerToLocal(entry.Attr.Uid, entry.Attr.Gid)
}

func (mc *MetaCache) Debug() {
	if debuggable, ok := mc.localStore.(filer.Debuggable); ok {
		println("start debugging")
		debuggable.Debug(os.Stderr)
	}
}

// IsDirectoryCached returns true if the directory has been fully cached
// (i.e., all entries have been loaded via EnsureVisited or ReadDir).
func (mc *MetaCache) IsDirectoryCached(dirPath util.FullPath) bool {
	return mc.isCachedFn(dirPath)
}

func (mc *MetaCache) noteDirectoryUpdate(dirPath util.FullPath) {
	if mc.onDirectoryUpdate != nil {
		mc.onDirectoryUpdate(dirPath)
	}
}
