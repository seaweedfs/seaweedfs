package s3iam

import (
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"

	"github.com/stretchr/testify/assert"
)

const (
	ACTION_READ    = "Read"
	ACTION_WRITE   = "Write"
	ACTION_ADMIN   = "Admin"
	ACTION_TAGGING = "Tagging"
	ACTION_LIST    = "List"
)

func TestS3Conf(t *testing.T) {
	ifs := &IAMFilerStore{}
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
	s3ConfSaved := &iam_pb.S3ApiConfiguration{}
	extended := make(map[string][]byte)
	_ = ifs.saveIAMConfigToEntryExtended(&extended, s3Conf)
	_ = ifs.loadIAMConfigFromEntryExtended(&extended, s3ConfSaved)

	assert.Equal(t, "some_name", s3ConfSaved.Identities[0].Name)
	assert.Equal(t, "some_read_only_user", s3ConfSaved.Identities[1].Name)
	assert.Equal(t, "some_access_key1", s3ConfSaved.Identities[0].Credentials[0].AccessKey)
	assert.Equal(t, "some_secret_key2", s3ConfSaved.Identities[1].Credentials[0].SecretKey)

}
