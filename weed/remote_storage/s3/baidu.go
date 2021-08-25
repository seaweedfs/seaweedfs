package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"os"
)

func init() {
	remote_storage.RemoteStorageClientMakers["baidu"] = new(BaiduRemoteStorageMaker)
}

type BaiduRemoteStorageMaker struct{}

func (s BaiduRemoteStorageMaker) Make(conf *filer_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		conf: conf,
	}
	accessKey := util.Nvl(conf.BaiduAccessKey, os.Getenv("BDCLOUD_ACCESS_KEY"))
	secretKey := util.Nvl(conf.BaiduSecretKey, os.Getenv("BDCLOUD_SECRET_KEY"))

	config := &aws.Config{
		Endpoint:         aws.String(conf.BaiduEndpoint),
		Region:           aws.String(conf.BaiduRegion),
		S3ForcePathStyle: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create baidu session: %v", err)
	}
	client.conn = s3.New(sess)
	return client, nil
}
