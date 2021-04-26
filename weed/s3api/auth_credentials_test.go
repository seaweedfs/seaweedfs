package s3api

import (
	. "github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"testing"

	"github.com/golang/protobuf/jsonpb"

	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
)

func TestIdentityListFileFormat(t *testing.T) {

	s3ApiConfiguration := &iam_pb.S3ApiConfiguration{}

	identity1 := &iam_pb.Identity{
		Name: "some_name",
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "some_access_key1",
				SecretKey: "some_secret_key2",
			},
		},
		Actions: []string{
			ACTION_ADMIN,
			ACTION_READ,
			ACTION_WRITE,
		},
	}
	identity2 := &iam_pb.Identity{
		Name: "some_read_only_user",
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "some_access_key1",
				SecretKey: "some_secret_key1",
			},
		},
		Actions: []string{
			ACTION_READ,
		},
	}
	identity3 := &iam_pb.Identity{
		Name: "some_normal_user",
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "some_access_key2",
				SecretKey: "some_secret_key2",
			},
		},
		Actions: []string{
			ACTION_READ,
			ACTION_WRITE,
		},
	}

	s3ApiConfiguration.Identities = append(s3ApiConfiguration.Identities, identity1)
	s3ApiConfiguration.Identities = append(s3ApiConfiguration.Identities, identity2)
	s3ApiConfiguration.Identities = append(s3ApiConfiguration.Identities, identity3)

	m := jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "  ",
	}

	text, _ := m.MarshalToString(s3ApiConfiguration)

	println(text)

}
