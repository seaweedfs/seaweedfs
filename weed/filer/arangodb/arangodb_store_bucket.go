package arangodb

import (
	"context"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/chrislusf/seaweedfs/weed/filer"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var _ filer.BucketAware = (*ArangodbStore)(nil)

func (store *ArangodbStore) OnBucketCreation(bucket string) {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// create the collection && add to cache
	_, err := store.ensureBucket(timeout, bucket)
	if err != nil {
		glog.V(0).Infof("bucket create %s : %w", bucket, err)
	}
}
func (store *ArangodbStore) OnBucketDeletion(bucket string) {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection, err := store.ensureBucket(timeout, bucket)
	if err != nil {
		glog.V(0).Infof("bucket delete %s : %w", bucket, err)
		return
	}
	err = collection.Remove(timeout)
	if err != nil && !driver.IsNotFound(err) {
		glog.V(0).Infof("bucket delete %s : %w", bucket, err)
		return
	}
}
func (store *ArangodbStore) CanDropWholeBucket() bool {
	return true
}
