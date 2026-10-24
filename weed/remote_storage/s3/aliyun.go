package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["aliyun"] = new(AliyunRemoteStorageMaker)
}

type AliyunRemoteStorageMaker struct{}

func (s AliyunRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s AliyunRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
