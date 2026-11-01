package leveldb

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

var _ filer.BucketAware = (*LevelDB3Store)(nil)

func (store *LevelDB3Store) OnBucketCreation(bucket string) {
	if !isBucketFolder(bucket) {
		return
	}
	store.createDB(bucket)
}

func (store *LevelDB3Store) OnBucketDeletion(bucket string) {
	if !isBucketFolder(bucket) {
		return
	}
	store.closeDB(bucket)
	os.RemoveAll(store.dir + "/" + bucket)
}

func (store *LevelDB3Store) CanDropWholeBucket() bool {
	return true
}
