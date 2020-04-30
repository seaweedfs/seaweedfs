package meta_cache

import (
	"context"
	"os"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MetaCache struct {
	actualStore filer2.FilerStore
	sync.RWMutex
}

func NewMetaCache(dbFolder string) *MetaCache {
	return &MetaCache{
		actualStore: openMetaStore(dbFolder),
	}
}

func openMetaStore(dbFolder string) filer2.FilerStore {

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

func (mc *MetaCache) InsertEntry(ctx context.Context, entry *filer2.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.actualStore.InsertEntry(ctx, entry)
}

func (mc *MetaCache) AtomicUpdateEntry(ctx context.Context, oldPath util.FullPath, newEntry *filer2.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	if oldPath != "" {
		if err := mc.actualStore.DeleteEntry(ctx, oldPath); err != nil {
			return err
		}
	}
	if newEntry != nil {
		if err := mc.actualStore.InsertEntry(ctx, newEntry); err != nil {
			return err
		}
	}
	return nil
}

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer2.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.actualStore.UpdateEntry(ctx, entry)
}

func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer2.Entry, err error) {
	mc.RLock()
	defer mc.RUnlock()
	return mc.actualStore.FindEntry(ctx, fp)
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.actualStore.DeleteEntry(ctx, fp)
}

func (mc *MetaCache) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*filer2.Entry, error) {
	mc.RLock()
	defer mc.RUnlock()
	return mc.actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
}

func (mc *MetaCache) Shutdown() {
	mc.Lock()
	defer mc.Unlock()
	mc.actualStore.Shutdown()
}
