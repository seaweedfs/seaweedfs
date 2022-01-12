package meta_cache

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/leveldb"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/bounded_tree"
	"os"
)

// need to have logic similar to FilerStoreWrapper
// e.g. fill fileId field for chunks

type MetaCache struct {
	localStore filer.VirtualFilerStore
	// sync.RWMutex
	visitedBoundary *bounded_tree.BoundedTree
	uidGidMapper    *UidGidMapper
	invalidateFunc  func(fullpath util.FullPath, entry *filer_pb.Entry)
}

func NewMetaCache(dbFolder string, baseDir util.FullPath, uidGidMapper *UidGidMapper, invalidateFunc func(util.FullPath, *filer_pb.Entry)) *MetaCache {
	return &MetaCache{
		localStore:      openMetaStore(dbFolder),
		visitedBoundary: bounded_tree.NewBoundedTree(baseDir),
		uidGidMapper:    uidGidMapper,
		invalidateFunc: func(fullpath util.FullPath, entry *filer_pb.Entry) {
			invalidateFunc(fullpath, entry)
		},
	}
}

func openMetaStore(dbFolder string) filer.VirtualFilerStore {

	os.RemoveAll(dbFolder)
	os.MkdirAll(dbFolder, 0755)

	store := &leveldb.LevelDBStore{}
	config := &cacheConfig{
		dir: dbFolder,
	}

	if err := store.Initialize(config, ""); err != nil {
		glog.Fatalf("Failed to initialize metadata cache store for %s: %+v", store.GetName(), err)
	}

	return filer.NewFilerStoreWrapper(store)

}

func (mc *MetaCache) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	//mc.Lock()
	//defer mc.Unlock()
	return mc.doInsertEntry(ctx, entry)
}

func (mc *MetaCache) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	return mc.localStore.InsertEntry(ctx, entry)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	//mc.Lock()
	//defer mc.Unlock()

	oldDir, _ := oldPath.DirAndName()
	if mc.visitedBoundary.HasVisited(util.FullPath(oldDir)) {
		if oldPath != "" {
			if newEntry != nil && oldPath == newEntry.FullPath {
				// skip the unnecessary deletion
				// leave the update to the following InsertEntry operation
			} else {
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
		if mc.visitedBoundary.HasVisited(util.FullPath(newDir)) {
			glog.V(3).Infof("InsertEntry %s/%s", newDir, newEntry.Name())
			if err := mc.localStore.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	//mc.Lock()
	//defer mc.Unlock()
	return mc.localStore.UpdateEntry(ctx, entry)
}

func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer.Entry, err error) {
	//mc.RLock()
	//defer mc.RUnlock()
	entry, err = mc.localStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	mc.mapIdFromFilerToLocal(entry)
	return
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	//mc.Lock()
	//defer mc.Unlock()
	return mc.localStore.DeleteEntry(ctx, fp)
}

func (mc *MetaCache) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) error {
	//mc.RLock()
	//defer mc.RUnlock()

	if !mc.visitedBoundary.HasVisited(dirPath) {
		// if this request comes after renaming, it should be fine
		glog.Warningf("unsynchronized dir: %v", dirPath)
	}

	_, err := mc.localStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *filer.Entry) bool {
		mc.mapIdFromFilerToLocal(entry)
		return eachEntryFunc(entry)
	})
	if err != nil {
		return err
	}
	return err
}

func (mc *MetaCache) Shutdown() {
	//mc.Lock()
	//defer mc.Unlock()
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
