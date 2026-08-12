package s3

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["baidu"] = new(BaiduRemoteStorageMaker)
}

type BaiduRemoteStorageMaker struct{}

func (s BaiduRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s BaiduRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}
