package meta_cache

import (
	"context"
	"os"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/leveldb"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/bounded_tree"
)

// need to have logic similar to FilerStoreWrapper
// e.g. fill fileId field for chunks

type MetaCache struct {
	actualStore filer.FilerStore
	sync.RWMutex
	visitedBoundary *bounded_tree.BoundedTree
	uidGidMapper    *UidGidMapper
}

func NewMetaCache(dbFolder string, uidGidMapper *UidGidMapper) *MetaCache {
	return &MetaCache{
		actualStore:     openMetaStore(dbFolder),
		visitedBoundary: bounded_tree.NewBoundedTree(),
		uidGidMapper:    uidGidMapper,
	}
}

func openMetaStore(dbFolder string) filer.FilerStore {

	os.RemoveAll(dbFolder)
	os.MkdirAll(dbFolder, 0755)

	store := &leveldb.LevelDBStore{}
	config := &cacheConfig{
		dir: dbFolder,
	}

	if err := store.Initialize(config, ""); err != nil {
		glog.Fatalf("Failed to initialize metadata cache store for %s: %+v", store.GetName(), err)
	}

	return store

}

func (mc *MetaCache) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.doInsertEntry(ctx, entry)
}

func (mc *MetaCache) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	filer_pb.BeforeEntrySerialization(entry.Chunks)
	return mc.actualStore.InsertEntry(ctx, entry)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()

	oldDir, _ := oldPath.DirAndName()
	if mc.visitedBoundary.HasVisited(util.FullPath(oldDir)) {
		if oldPath != "" {
			if newEntry != nil && oldPath == newEntry.FullPath {
				// skip the unnecessary deletion
				// leave the update to the following InsertEntry operation
			} else {
				if err := mc.actualStore.DeleteEntry(ctx, oldPath); err != nil {
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
			if err := mc.actualStore.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	filer_pb.BeforeEntrySerialization(entry.Chunks)
	return mc.actualStore.UpdateEntry(ctx, entry)
}

func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer.Entry, err error) {
	mc.RLock()
	defer mc.RUnlock()
	entry, err = mc.actualStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	mc.mapIdFromFilerToLocal(entry)
	filer_pb.AfterEntryDeserialization(entry.Chunks)
	return
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.actualStore.DeleteEntry(ctx, fp)
}

func (mc *MetaCache) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*filer.Entry, error) {
	mc.RLock()
	defer mc.RUnlock()

	entries, err := mc.actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		mc.mapIdFromFilerToLocal(entry)
		filer_pb.AfterEntryDeserialization(entry.Chunks)
	}
	return entries, err
}

func (mc *MetaCache) Shutdown() {
	mc.Lock()
	defer mc.Unlock()
	mc.actualStore.Shutdown()
}

func (mc *MetaCache) mapIdFromFilerToLocal(entry *filer.Entry) {
	entry.Attr.Uid, entry.Attr.Gid = mc.uidGidMapper.FilerToLocal(entry.Attr.Uid, entry.Attr.Gid)
}
