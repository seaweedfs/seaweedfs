package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["b2"] = new(BackBlazeRemoteStorageMaker)
}

type BackBlazeRemoteStorageMaker struct{}

func (s BackBlazeRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s BackBlazeRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
