package arangodb

import (
	"context"
	"github.com/arangodb/go-driver"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var _ filer.BucketAware = (*ArangodbStore)(nil)

func (store *ArangodbStore) OnBucketCreation(bucket string) {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// create the collection && add to cache
	_, err := store.ensureBucket(timeout, bucket)
	if err != nil {
		glog.Errorf("bucket create %s: %v", bucket, err)
	}
}
func (store *ArangodbStore) OnBucketDeletion(bucket string) {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection, err := store.ensureBucket(timeout, bucket)
	if err != nil {
		glog.Errorf("bucket delete %s: %v", bucket, err)
		return
	}
	err = collection.Remove(timeout)
	if err != nil && !driver.IsNotFound(err) {
		glog.Errorf("bucket delete %s: %v", bucket, err)
		return
	}
	store.mu.Lock()
	delete(store.buckets, bucket)
	store.mu.Unlock()
}
func (store *ArangodbStore) CanDropWholeBucket() bool {
	return true
}
