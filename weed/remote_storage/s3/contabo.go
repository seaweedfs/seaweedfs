package s3

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	remote_storage.RemoteStorageClientMakers["contabo"] = new(ContaboRemoteStorageMaker)
}

type ContaboRemoteStorageMaker struct{}

func (s ContaboRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s ContaboRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		supportTagging: true,
		conf:           conf,
	}
	accessKey := util.Nvl(conf.ContaboAccessKey, os.Getenv("ACCESS_KEY"))
	secretKey := util.Nvl(conf.ContaboSecretKey, os.Getenv("SECRET_KEY"))

	config := &aws.Config{
		Endpoint:                      aws.String(conf.ContaboEndpoint),
		Region:                        aws.String(conf.ContaboRegion),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create contabo session: %v", err)
	}
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)
	return client, nil
}
