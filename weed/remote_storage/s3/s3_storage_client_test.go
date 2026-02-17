package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/stretchr/testify/require"
)

func TestS3MakeUsesAnonymousCredentialsWhenKeysAreEmpty(t *testing.T) {
	maker := s3RemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Type:             "s3",
		S3Region:         "us-east-1",
		S3Endpoint:       "http://localhost:8333",
		S3ForcePathStyle: true,
	}

	remoteClient, err := maker.Make(conf)
	require.NoError(t, err)

	client, ok := remoteClient.(*s3RemoteStorageClient)
	require.True(t, ok)

	s3Client, ok := client.conn.(*awss3.S3)
	require.True(t, ok)
	require.Same(t, credentials.AnonymousCredentials, s3Client.Config.Credentials)
}

func TestS3MakeUsesStaticCredentialsWhenKeysAreProvided(t *testing.T) {
	maker := s3RemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Type:             "s3",
		S3Region:         "us-east-1",
		S3Endpoint:       "http://localhost:8333",
		S3ForcePathStyle: true,
		S3AccessKey:      "test-access",
		S3SecretKey:      "test-secret",
	}

	remoteClient, err := maker.Make(conf)
	require.NoError(t, err)

	client, ok := remoteClient.(*s3RemoteStorageClient)
	require.True(t, ok)

	s3Client, ok := client.conn.(*awss3.S3)
	require.True(t, ok)
	require.NotSame(t, credentials.AnonymousCredentials, s3Client.Config.Credentials)

	credValue, err := s3Client.Config.Credentials.Get()
	require.NoError(t, err)
	require.Equal(t, conf.S3AccessKey, credValue.AccessKeyID)
	require.Equal(t, conf.S3SecretKey, credValue.SecretAccessKey)
}
