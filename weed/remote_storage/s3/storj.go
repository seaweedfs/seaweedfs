package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["storj"] = new(StorjRemoteStorageMaker)
}

type StorjRemoteStorageMaker struct{}

func (s StorjRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s StorjRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
