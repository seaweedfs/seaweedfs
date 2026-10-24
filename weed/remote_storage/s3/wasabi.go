package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["wasabi"] = new(WasabiRemoteStorageMaker)
}

type WasabiRemoteStorageMaker struct{}

func (s WasabiRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s WasabiRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
