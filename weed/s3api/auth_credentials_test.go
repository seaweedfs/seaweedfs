package s3api

import (
	"reflect"
	"testing"

	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
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
	assert.Equal(t, true, ident1.canDo(ACTION_WRITE, "bucket1", "/a/b/c/d/e.txt"))
	assert.Equal(t, false, ident1.canDo(ACTION_DELETE_BUCKET, "bucket1", ""))
	assert.Equal(t, false, ident1.canDo(ACTION_WRITE, "bucket1", "/a/b/other/some"), "action without *")
	assert.Equal(t, false, ident1.canDo(ACTION_WRITE, "bucket1", "/a/b/*"), "action on parent directory")

	// bucket specific
	ident2 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:bucket1",
			"Write:bucket1/*",
			"WriteAcp:bucket1",
		},
	}
	assert.Equal(t, true, ident2.canDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident2.canDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident2.canDo(ACTION_WRITE_ACP, "bucket1", ""))
	assert.Equal(t, false, ident2.canDo(ACTION_READ_ACP, "bucket1", ""))
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
	assert.Equal(t, false, ident3.canDo(ACTION_WRITE_ACP, "bucket1", ""))

	// partial buckets
	ident4 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:special_*",
			"ReadAcp:special_*",
		},
	}
	assert.Equal(t, true, ident4.canDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident4.canDo(ACTION_READ_ACP, "special_bucket", ""))
	assert.Equal(t, false, ident4.canDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))

	// admin buckets
	ident5 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Admin:special_*",
		},
	}
	assert.Equal(t, true, ident5.canDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident5.canDo(ACTION_READ_ACP, "special_bucket", ""))
	assert.Equal(t, true, ident5.canDo(ACTION_WRITE, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident5.canDo(ACTION_WRITE_ACP, "special_bucket", ""))

	// anonymous buckets
	ident6 := &Identity{
		Name: "anonymous",
		Actions: []Action{
			"Read",
		},
	}
	assert.Equal(t, true, ident6.canDo(ACTION_READ, "anything_bucket", "/a/b/c/d.txt"))

	//test deleteBucket operation
	ident7 := &Identity{
		Name: "anything",
		Actions: []Action{
			"DeleteBucket:bucket1",
		},
	}
	assert.Equal(t, true, ident7.canDo(ACTION_DELETE_BUCKET, "bucket1", ""))
}

type LoadS3ApiConfigurationTestCase struct {
	pbAccount   *iam_pb.Account
	pbIdent     *iam_pb.Identity
	expectIdent *Identity
}

func TestLoadS3ApiConfiguration(t *testing.T) {
	specifiedAccount := Account{
		Id:           "specifiedAccountID",
		DisplayName:  "specifiedAccountName",
		EmailAddress: "specifiedAccounEmail@example.com",
	}
	pbSpecifiedAccount := iam_pb.Account{
		Id:           "specifiedAccountID",
		DisplayName:  "specifiedAccountName",
		EmailAddress: "specifiedAccounEmail@example.com",
	}
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
				Name:    "notSpecifyAccountId",
				Account: &AccountAdmin,
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
			pbAccount: &pbSpecifiedAccount,
			pbIdent: &iam_pb.Identity{
				Name:    "specifiedAccountID",
				Account: &pbSpecifiedAccount,
				Actions: []string{
					"Read",
					"Write",
				},
			},
			expectIdent: &Identity{
				Name:    "specifiedAccountID",
				Account: &specifiedAccount,
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
				Name:    "anonymous",
				Account: &AccountAnonymous,
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
		if v.pbAccount != nil {
			config.Accounts = append(config.Accounts, v.pbAccount)
		}
	}

	iam := IdentityAccessManagement{}
	err := iam.loadS3ApiConfiguration(config)
	if err != nil {
		return
	}

	for _, ident := range iam.identities {
		tc := testCases[ident.Name]
		if !reflect.DeepEqual(ident, tc.expectIdent) {
			t.Errorf("not expect for ident name %s", ident.Name)
		}
	}
}
