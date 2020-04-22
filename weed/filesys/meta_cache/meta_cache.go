package meta_cache

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type MetaCache struct {
	filer2.FilerStore
}

func NewMetaCache(dbFolder string) *MetaCache {
	return &MetaCache{
		FilerStore: OpenMetaStore(dbFolder),
	}
}

func OpenMetaStore(dbFolder string) filer2.FilerStore {

	os.MkdirAll(dbFolder, 0755)

	store := &leveldb.LevelDBStore{}
	config := &cacheConfig{}

	if err := store.Initialize(config, ""); err != nil {
		glog.Fatalf("Failed to initialize metadata cache store for %s: %+v", store.GetName(), err)
	}

	return store

}
