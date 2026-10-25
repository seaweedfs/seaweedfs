package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["filebase"] = new(FilebaseRemoteStorageMaker)
}

type FilebaseRemoteStorageMaker struct{}

func (s FilebaseRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s FilebaseRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
