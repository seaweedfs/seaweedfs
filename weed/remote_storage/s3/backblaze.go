package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
)

func init() {
	remote_storage.RemoteStorageClientMakers["b2"] = new(BackBlazeRemoteStorageMaker)
}

type BackBlazeRemoteStorageMaker struct{}

func (s BackBlazeRemoteStorageMaker) Make(conf *filer_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		conf: conf,
	}
	config := &aws.Config{
		Endpoint:         aws.String(conf.BackblazeEndpoint),
		Region:           aws.String("us-west-002"),
		S3ForcePathStyle: aws.Bool(true),
	}
	if conf.BackblazeKeyId != "" && conf.BackblazeApplicationKey != "" {
		config.Credentials = credentials.NewStaticCredentials(conf.BackblazeKeyId, conf.BackblazeApplicationKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session: %v", err)
	}
	client.conn = s3.New(sess)
	return client, nil
}
