package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["tencent"] = new(TencentRemoteStorageMaker)
}

type TencentRemoteStorageMaker struct{}

func (s TencentRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s TencentRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
