package s3api

import (
	"os"
	"reflect"
	"strings"
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

func TestLoadS3ApiConfigurationWithEnvVars(t *testing.T) {
	// Save original environment
	originalAccessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	// Clean up after test
	defer func() {
		if originalAccessKeyId != "" {
			os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKeyId)
		} else {
			os.Unsetenv("AWS_ACCESS_KEY_ID")
		}
		if originalSecretAccessKey != "" {
			os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretAccessKey)
		} else {
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		}
	}()

	tests := []struct {
		name               string
		accessKeyId        string
		secretAccessKey    string
		existingIdentities []*iam_pb.Identity
		expectEnvIdentity  bool
		expectTotal        int
	}{
		{
			name:               "Both env vars set with no existing identities",
			accessKeyId:        "AKIA1234567890ABCDEF",
			secretAccessKey:    "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  true,
			expectTotal:        1,
		},
		{
			name:               "Short access key (less than 8 characters)",
			accessKeyId:        "SHORT",
			secretAccessKey:    "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  true,
			expectTotal:        1,
		},
		{
			name:               "Empty access key",
			accessKeyId:        "",
			secretAccessKey:    "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  false,
			expectTotal:        0,
		},
		{
			name:            "Both env vars set with existing different identity",
			accessKeyId:     "AKIA1234567890ABCDEF",
			secretAccessKey: "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{
				{
					Name: "existing_user",
					Credentials: []*iam_pb.Credential{
						{AccessKey: "AKIA0000000000000000", SecretKey: "existing_secret"},
					},
					Actions: []string{ACTION_READ},
				},
			},
			expectEnvIdentity: true,
			expectTotal:       2,
		},
		{
			name:            "Both env vars set with existing same access key",
			accessKeyId:     "AKIA1234567890ABCDEF",
			secretAccessKey: "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{
				{
					Name: "existing_user",
					Credentials: []*iam_pb.Credential{
						{AccessKey: "AKIA1234567890ABCDEF", SecretKey: "existing_secret"}, // Same access key as env var
					},
					Actions: []string{ACTION_READ},
				},
			},
			expectEnvIdentity: false, // Should skip because access key exists
			expectTotal:       1,
		},
		{
			name:               "Only access key set",
			accessKeyId:        "AKIA1234567890ABCDEF",
			secretAccessKey:    "",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  false,
			expectTotal:        0,
		},
		{
			name:               "Only secret key set",
			accessKeyId:        "",
			secretAccessKey:    "secret123456789012345678901234567890abcdef12",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  false,
			expectTotal:        0,
		},
		{
			name:               "Neither env var set",
			accessKeyId:        "",
			secretAccessKey:    "",
			existingIdentities: []*iam_pb.Identity{},
			expectEnvIdentity:  false,
			expectTotal:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment variables
			if tt.accessKeyId != "" {
				os.Setenv("AWS_ACCESS_KEY_ID", tt.accessKeyId)
			} else {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
			}
			if tt.secretAccessKey != "" {
				os.Setenv("AWS_SECRET_ACCESS_KEY", tt.secretAccessKey)
			} else {
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			}

			// Create IAM instance and test the loadAdminCredentialsFromEnv function
			iam := &IdentityAccessManagement{}

			// Create S3ApiConfiguration with existing identities
			config := &iam_pb.S3ApiConfiguration{
				Identities: tt.existingIdentities,
			}

			// Test the loadAdminCredentialsFromEnv function
			iam.loadAdminCredentialsFromEnv(config)

			// Test the result
			assert.Len(t, config.Identities, tt.expectTotal, "Should have expected number of identities")

			if tt.expectEnvIdentity {
				// Find the identity created from environment variables
				found := false
				for _, identity := range config.Identities {
					for _, cred := range identity.Credentials {
						if cred.AccessKey == tt.accessKeyId {
							found = true
							assert.Equal(t, tt.accessKeyId, cred.AccessKey, "Access key should match environment variable")
							assert.Equal(t, tt.secretAccessKey, cred.SecretKey, "Secret key should match environment variable")
							assert.Contains(t, identity.Actions, ACTION_ADMIN, "Should have admin action")
							assert.True(t, strings.HasPrefix(identity.Name, "admin-"), "Identity name should have admin prefix")
							// Verify the suffix is correct (either full access key or first 8 chars)
							expectedSuffix := tt.accessKeyId
							if len(tt.accessKeyId) > 8 {
								expectedSuffix = tt.accessKeyId[:8]
							}
							assert.Equal(t, "admin-"+expectedSuffix, identity.Name, "Identity name should be admin- plus access key or first 8 chars")
							break
						}
					}
					if found {
						break
					}
				}
				assert.True(t, found, "Should find identity created from environment variables")
			}
		})
	}
}
