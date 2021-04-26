package filer

import (
	"bytes"
	. "github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"

	"github.com/stretchr/testify/assert"
)

func TestS3Conf(t *testing.T) {
	s3Conf := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "some_name",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "some_access_key1",
						SecretKey: "some_secret_key1",
					},
				},
				Actions: []string{
					ACTION_ADMIN,
					ACTION_READ,
					ACTION_WRITE,
				},
			},
			{
				Name: "some_read_only_user",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "some_access_key2",
						SecretKey: "some_secret_key2",
					},
				},
				Actions: []string{
					ACTION_READ,
					ACTION_TAGGING,
					ACTION_LIST,
				},
			},
		},
	}
	var buf bytes.Buffer
	err := S3ConfigurationToText(&buf, s3Conf)
	assert.Equal(t, err, nil)
	s3ConfSaved := &iam_pb.S3ApiConfiguration{}
	err = ParseS3ConfigurationFromBytes(buf.Bytes(), s3ConfSaved)
	assert.Equal(t, err, nil)

	assert.Equal(t, "some_name", s3ConfSaved.Identities[0].Name)
	assert.Equal(t, "some_read_only_user", s3ConfSaved.Identities[1].Name)
	assert.Equal(t, "some_access_key1", s3ConfSaved.Identities[0].Credentials[0].AccessKey)
	assert.Equal(t, "some_secret_key2", s3ConfSaved.Identities[1].Credentials[0].SecretKey)
}
