package arangodb

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var _ filer.BucketAware = (*ArangodbStore)(nil)

func (store *ArangodbStore) OnBucketCreation(bucket string) {
	//nothing needs to be done
}
func (store *ArangodbStore) OnBucketDeletion(bucket string) {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := store.database.Query(timeout, `
for d in files
filter d.bucket == @bucket
remove d in files`, map[string]interface{}{"bucket": bucket})
	if err != nil {
		glog.V(0).Infof("bucket delete %s : %v", bucket, err)
	}
	defer cur.Close()
}
func (store *ArangodbStore) CanDropWholeBucket() bool {
	return true
}
