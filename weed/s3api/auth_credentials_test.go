package s3api

import (
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"

	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
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

	m := jsonpb.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, _ := m.Marshal(s3ApiConfiguration)

	println(string(text))

}

func TestCanDo(t *testing.T) {
	ident1 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Write:bucket1/a/b/c/*",
			"Write:bucket1/a/b/other",
		},
	}
	// object specific
	assert.Equal(t, true, ident1.canDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, false, ident1.canDo(ACTION_WRITE, "bucket1", "/a/b/other/some"), "action without *")

	// bucket specific
	ident2 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:bucket1",
			"Write:bucket1/*",
		},
	}
	assert.Equal(t, true, ident2.canDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident2.canDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, false, ident2.canDo(ACTION_LIST, "bucket1", "/a/b/c/d.txt"))

	// across buckets
	ident3 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read",
			"Write",
		},
	}
	assert.Equal(t, true, ident3.canDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident3.canDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, false, ident3.canDo(ACTION_LIST, "bucket1", "/a/b/other/some"))

	// partial buckets
	ident4 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:special_*",
		},
	}
	assert.Equal(t, true, ident4.canDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, false, ident4.canDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))

	// admin buckets
	ident5 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Admin:special_*",
		},
	}
	assert.Equal(t, true, ident5.canDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident5.canDo(ACTION_WRITE, "special_bucket", "/a/b/c/d.txt"))
}

type LoadS3ApiConfigurationTestCase struct {
	pbIdent     *iam_pb.Identity
	expectIdent *Identity
}

func TestLoadS3ApiConfiguration(t *testing.T) {
	testCases := map[string]*LoadS3ApiConfigurationTestCase{
		"notSpecifyAccountId": {
			pbIdent: &iam_pb.Identity{
				Name: "notSpecifyAccountId",
				Actions: []string{
					"Read",
					"Write",
				},
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "some_access_key1",
						SecretKey: "some_secret_key2",
					},
				},
			},
			expectIdent: &Identity{
				Name:      "notSpecifyAccountId",
				AccountId: s3account.AccountAdmin.Id,
				Actions: []Action{
					"Read",
					"Write",
				},
				Credentials: []*Credential{
					{
						AccessKey: "some_access_key1",
						SecretKey: "some_secret_key2",
					},
				},
			},
		},
		"specifiedAccountID": {
			pbIdent: &iam_pb.Identity{
				Name:      "specifiedAccountID",
				AccountId: "specifiedAccountID",
				Actions: []string{
					"Read",
					"Write",
				},
			},
			expectIdent: &Identity{
				Name:      "specifiedAccountID",
				AccountId: "specifiedAccountID",
				Actions: []Action{
					"Read",
					"Write",
				},
			},
		},
		"anonymous": {
			pbIdent: &iam_pb.Identity{
				Name: "anonymous",
				Actions: []string{
					"Read",
					"Write",
				},
			},
			expectIdent: &Identity{
				Name:      "anonymous",
				AccountId: "anonymous",
				Actions: []Action{
					"Read",
					"Write",
				},
			},
		},
	}

	config := &iam_pb.S3ApiConfiguration{
		Identities: make([]*iam_pb.Identity, 0),
	}
	for _, v := range testCases {
		config.Identities = append(config.Identities, v.pbIdent)
	}

	iam := IdentityAccessManagement{}
	err := iam.loadS3ApiConfiguration(config)
	if err != nil {
		return
	}

	for _, ident := range iam.identities {
		tc := testCases[ident.Name]
		if !reflect.DeepEqual(ident, tc.expectIdent) {
			t.Error("not expect")
		}
	}
}
