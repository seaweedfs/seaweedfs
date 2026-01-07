package leveldb

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

var _ filer.BucketAware = (*LevelDB3Store)(nil)

func (store *LevelDB3Store) OnBucketCreation(bucket string) {
	store.createDB(bucket)
}

func (store *LevelDB3Store) OnBucketDeletion(bucket string) {
	store.closeDB(bucket)
	if bucket != "" { // just to make sure
		os.RemoveAll(store.dir + "/" + bucket)
	}
}

func (store *LevelDB3Store) CanDropWholeBucket() bool {
	return true
}
