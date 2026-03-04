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
	// pendingDeletes tracks deleted entry names while a directory refresh is in progress.
	pendingDeletes map[util.FullPath]map[string]struct{}
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
		pendingDeletes: make(map[util.FullPath]map[string]struct{}),
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
	return mc.doInsertEntry(ctx, entry)
}

func (mc *MetaCache) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	return mc.localStore.InsertEntry(ctx, entry)
}

// doBatchInsertEntries inserts multiple entries using LevelDB's batch write.
// This is more efficient than inserting entries one by one.
func (mc *MetaCache) doBatchInsertEntries(ctx context.Context, entries []*filer.Entry) error {
	return mc.leveldbStore.BatchInsertEntries(ctx, entries)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()

	if oldPath != "" && (newEntry == nil || oldPath != newEntry.FullPath) {
		mc.recordPendingDelete(oldPath)
	}

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
	} else {
		// println("unknown old directory:", oldDir)
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
	mc.recordPendingDelete(fp)
	return mc.localStore.DeleteEntry(ctx, fp)
}
func (mc *MetaCache) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.DeleteFolderChildren(ctx, fp)
}

// ReplaceDirectoryEntries atomically clears all children of dirPath and inserts
// the provided entries under a single lock. This prevents race conditions where
// concurrent DeleteEntry (from Unlink) or AtomicUpdateEntryFromFiler (from
// subscription) could interleave with streaming batch inserts, causing ghost entries.
func (mc *MetaCache) ReplaceDirectoryEntries(ctx context.Context, dirPath util.FullPath, entries []*filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	pendingDeletes := mc.pendingDeletes[dirPath]
	delete(mc.pendingDeletes, dirPath)

	if err := mc.localStore.DeleteFolderChildren(ctx, dirPath); err != nil {
		return err
	}

	n := len(entries)
	for i := 0; i < n; i += batchInsertSize {
		end := i + batchInsertSize
		if end > n {
			end = n
		}
		batch := entries[i:end]
		if len(pendingDeletes) > 0 {
			filtered := make([]*filer.Entry, 0, len(batch))
			for _, entry := range batch {
				if _, deleted := pendingDeletes[entry.Name()]; deleted {
					glog.V(2).Infof("[ghost_trace] replace skip reinsert dir=%s name=%s", dirPath, entry.Name())
					continue
				}
				filtered = append(filtered, entry)
			}
			batch = filtered
		}
		if len(batch) == 0 {
			continue
		}
		if err := mc.leveldbStore.BatchInsertEntries(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (mc *MetaCache) BeginRefresh(dirPath util.FullPath) {
	mc.Lock()
	defer mc.Unlock()
	if _, exists := mc.pendingDeletes[dirPath]; exists {
		return
	}
	mc.pendingDeletes[dirPath] = make(map[string]struct{})
}

func (mc *MetaCache) CancelRefresh(dirPath util.FullPath) {
	mc.Lock()
	defer mc.Unlock()
	if _, exists := mc.pendingDeletes[dirPath]; exists {
		delete(mc.pendingDeletes, dirPath)
	}
}

func (mc *MetaCache) recordPendingDelete(fp util.FullPath) {
	dir, name := fp.DirAndName()
	pendingDeletes := mc.pendingDeletes[util.FullPath(dir)]
	if pendingDeletes == nil {
		return
	}
	pendingDeletes[name] = struct{}{}
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
