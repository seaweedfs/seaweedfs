package s3api

import (
	"os"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
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

func TestNewIdentityAccessManagementWithStoreEnvVars(t *testing.T) {
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
		name              string
		accessKeyId       string
		secretAccessKey   string
		expectEnvIdentity bool
		expectedName      string
		description       string
	}{
		{
			name:              "Environment variables used as fallback",
			accessKeyId:       "AKIA1234567890ABCDEF",
			secretAccessKey:   "secret123456789012345678901234567890abcdef12",
			expectEnvIdentity: true,
			expectedName:      "admin-AKIA1234",
			description:       "When no config file and no filer config, environment variables should be used",
		},
		{
			name:              "Short access key fallback",
			accessKeyId:       "SHORT",
			secretAccessKey:   "secret123456789012345678901234567890abcdef12",
			expectEnvIdentity: true,
			expectedName:      "admin-SHORT",
			description:       "Short access keys should work correctly as fallback",
		},
		{
			name:              "No env vars means no identities",
			accessKeyId:       "",
			secretAccessKey:   "",
			expectEnvIdentity: false,
			expectedName:      "",
			description:       "When no env vars and no config, should have no identities",
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

			// Create IAM instance with memory store for testing (no config file)
			option := &S3ApiServerOption{
				Config: "", // No config file - this should trigger environment variable fallback
			}
			iam := NewIdentityAccessManagementWithStore(option, string(credential.StoreTypeMemory))

			if tt.expectEnvIdentity {
				// Should have exactly one identity from environment variables
				assert.Len(t, iam.identities, 1, "Should have exactly one identity from environment variables")

				identity := iam.identities[0]
				assert.Equal(t, tt.expectedName, identity.Name, "Identity name should match expected")
				assert.Len(t, identity.Credentials, 1, "Should have one credential")
				assert.Equal(t, tt.accessKeyId, identity.Credentials[0].AccessKey, "Access key should match environment variable")
				assert.Equal(t, tt.secretAccessKey, identity.Credentials[0].SecretKey, "Secret key should match environment variable")
				assert.Contains(t, identity.Actions, Action(ACTION_ADMIN), "Should have admin action")
			} else {
				// When no env vars, should have no identities (since no config file)
				assert.Len(t, iam.identities, 0, "Should have no identities when no env vars and no config file")
			}
		})
	}
}

// TestBucketLevelListPermissions tests that bucket-level List permissions work correctly
// This test validates the fix for issue #7066
func TestBucketLevelListPermissions(t *testing.T) {
	// Test the functionality that was broken in issue #7066

	t.Run("Bucket Wildcard Permissions", func(t *testing.T) {
		// Create identity with bucket-level List permission using wildcards
		identity := &Identity{
			Name: "bucket-user",
			Actions: []Action{
				"List:mybucket*",
				"Read:mybucket*",
				"ReadAcp:mybucket*",
				"Write:mybucket*",
				"WriteAcp:mybucket*",
				"Tagging:mybucket*",
			},
		}

		// Test cases for bucket-level wildcard permissions
		testCases := []struct {
			name        string
			action      Action
			bucket      string
			object      string
			shouldAllow bool
			description string
		}{
			{
				name:        "exact bucket match",
				action:      "List",
				bucket:      "mybucket",
				object:      "",
				shouldAllow: true,
				description: "Should allow access to exact bucket name",
			},
			{
				name:        "bucket with suffix",
				action:      "List",
				bucket:      "mybucket-prod",
				object:      "",
				shouldAllow: true,
				description: "Should allow access to bucket with matching prefix",
			},
			{
				name:        "bucket with numbers",
				action:      "List",
				bucket:      "mybucket123",
				object:      "",
				shouldAllow: true,
				description: "Should allow access to bucket with numbers",
			},
			{
				name:        "different bucket",
				action:      "List",
				bucket:      "otherbucket",
				object:      "",
				shouldAllow: false,
				description: "Should deny access to bucket with different prefix",
			},
			{
				name:        "partial match",
				action:      "List",
				bucket:      "notmybucket",
				object:      "",
				shouldAllow: false,
				description: "Should deny access to bucket that contains but doesn't start with the prefix",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := identity.canDo(tc.action, tc.bucket, tc.object)
				assert.Equal(t, tc.shouldAllow, result, tc.description)
			})
		}
	})

	t.Run("Global List Permission", func(t *testing.T) {
		// Create identity with global List permission
		identity := &Identity{
			Name: "global-user",
			Actions: []Action{
				"List",
			},
		}

		// Should allow access to any bucket
		testCases := []string{"anybucket", "mybucket", "test-bucket", "prod-data"}

		for _, bucket := range testCases {
			result := identity.canDo("List", bucket, "")
			assert.True(t, result, "Global List permission should allow access to bucket %s", bucket)
		}
	})

	t.Run("No Wildcard Exact Match", func(t *testing.T) {
		// Create identity with exact bucket permission (no wildcard)
		identity := &Identity{
			Name: "exact-user",
			Actions: []Action{
				"List:specificbucket",
			},
		}

		// Should only allow access to the exact bucket
		assert.True(t, identity.canDo("List", "specificbucket", ""), "Should allow access to exact bucket")
		assert.False(t, identity.canDo("List", "specificbucket-test", ""), "Should deny access to bucket with suffix")
		assert.False(t, identity.canDo("List", "otherbucket", ""), "Should deny access to different bucket")
	})

	t.Log("This test validates the fix for issue #7066")
	t.Log("Bucket-level List permissions like 'List:bucket*' work correctly")
	t.Log("ListBucketsHandler now uses consistent authentication flow")
}
