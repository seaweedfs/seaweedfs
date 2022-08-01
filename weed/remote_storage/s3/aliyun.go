package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
)

func init() {
	remote_storage.RemoteStorageClientMakers["aliyun"] = new(AliyunRemoteStorageMaker)
}

type AliyunRemoteStorageMaker struct{}

func (s AliyunRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s AliyunRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		supportTagging: true,
		conf:           conf,
	}
	accessKey := util.Nvl(conf.AliyunAccessKey, os.Getenv("ALICLOUD_ACCESS_KEY_ID"))
	secretKey := util.Nvl(conf.AliyunSecretKey, os.Getenv("ALICLOUD_ACCESS_KEY_SECRET"))

	config := &aws.Config{
		Endpoint:                      aws.String(conf.AliyunEndpoint),
		Region:                        aws.String(conf.AliyunRegion),
		S3ForcePathStyle:              aws.Bool(false),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aliyun session: %v", err)
	}
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)
	return client, nil
}
