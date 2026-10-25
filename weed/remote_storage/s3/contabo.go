package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["contabo"] = new(ContaboRemoteStorageMaker)
}

type ContaboRemoteStorageMaker struct{}

func (s ContaboRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s ContaboRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
