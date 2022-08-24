package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
)

func init() {
	remote_storage.RemoteStorageClientMakers["filebase"] = new(FilebaseRemoteStorageMaker)
}

type FilebaseRemoteStorageMaker struct{}

func (s FilebaseRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s FilebaseRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		supportTagging: true,
		conf:           conf,
	}
	accessKey := util.Nvl(conf.FilebaseAccessKey, os.Getenv("AWS_ACCESS_KEY_ID"))
	secretKey := util.Nvl(conf.FilebaseSecretKey, os.Getenv("AWS_SECRET_ACCESS_KEY"))

	config := &aws.Config{
		Endpoint:                      aws.String(conf.FilebaseEndpoint),
		Region:                        aws.String("us-east-1"),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create filebase session: %v", err)
	}
	sess.Handlers.Sign.PushBackNamed(v4.SignRequestHandler)
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)
	return client, nil
}
