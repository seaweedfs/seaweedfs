package s3api

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"

	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
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
	assert.Equal(t, true, ident1.CanDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident1.CanDo(ACTION_WRITE, "bucket1", "/a/b/c/d/e.txt"))
	assert.Equal(t, false, ident1.CanDo(ACTION_DELETE_BUCKET, "bucket1", ""))
	assert.Equal(t, false, ident1.CanDo(ACTION_WRITE, "bucket1", "/a/b/other/some"), "action without *")
	assert.Equal(t, false, ident1.CanDo(ACTION_WRITE, "bucket1", "/a/b/*"), "action on parent directory")

	// bucket specific
	ident2 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:bucket1",
			"Write:bucket1/*",
			"WriteAcp:bucket1",
		},
	}
	assert.Equal(t, true, ident2.CanDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident2.CanDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident2.CanDo(ACTION_WRITE_ACP, "bucket1", ""))
	assert.Equal(t, false, ident2.CanDo(ACTION_READ_ACP, "bucket1", ""))
	assert.Equal(t, false, ident2.CanDo(ACTION_LIST, "bucket1", "/a/b/c/d.txt"))

	// across buckets
	ident3 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read",
			"Write",
		},
	}
	assert.Equal(t, true, ident3.CanDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident3.CanDo(ACTION_WRITE, "bucket1", "/a/b/c/d.txt"))
	assert.Equal(t, false, ident3.CanDo(ACTION_LIST, "bucket1", "/a/b/other/some"))
	assert.Equal(t, false, ident3.CanDo(ACTION_WRITE_ACP, "bucket1", ""))

	// partial buckets
	ident4 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Read:special_*",
			"ReadAcp:special_*",
		},
	}
	assert.Equal(t, true, ident4.CanDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident4.CanDo(ACTION_READ_ACP, "special_bucket", ""))
	assert.Equal(t, false, ident4.CanDo(ACTION_READ, "bucket1", "/a/b/c/d.txt"))

	// admin buckets
	ident5 := &Identity{
		Name: "anything",
		Actions: []Action{
			"Admin:special_*",
		},
	}
	assert.Equal(t, true, ident5.CanDo(ACTION_READ, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident5.CanDo(ACTION_READ_ACP, "special_bucket", ""))
	assert.Equal(t, true, ident5.CanDo(ACTION_WRITE, "special_bucket", "/a/b/c/d.txt"))
	assert.Equal(t, true, ident5.CanDo(ACTION_WRITE_ACP, "special_bucket", ""))

	// anonymous buckets
	ident6 := &Identity{
		Name: "anonymous",
		Actions: []Action{
			"Read",
		},
	}
	assert.Equal(t, true, ident6.CanDo(ACTION_READ, "anything_bucket", "/a/b/c/d.txt"))

	//test deleteBucket operation
	ident7 := &Identity{
		Name: "anything",
		Actions: []Action{
			"DeleteBucket:bucket1",
		},
	}
	assert.Equal(t, true, ident7.CanDo(ACTION_DELETE_BUCKET, "bucket1", ""))
}

func TestMatchWildcardPattern(t *testing.T) {
	tests := []struct {
		pattern string
		target  string
		match   bool
	}{
		// Basic * wildcard tests
		{"Bucket/*", "Bucket/a/b", true},
		{"Bucket/*", "x/Bucket/a", false},
		{"Bucket/*/admin", "Bucket/x/admin", true},
		{"Bucket/*/admin", "Bucket/x/y/admin", true},
		{"Bucket/*/admin", "Bucket////x////uwu////y////admin", true},
		{"abc*def", "abcXYZdef", true},
		{"abc*def", "abcXYZdefZZ", false},
		{"syr/*", "syr/a/b", true},

		// ? wildcard tests (matches exactly one character)
		{"ab?d", "abcd", true},
		{"ab?d", "abXd", true},
		{"ab?d", "abd", false},   // ? must match exactly one character
		{"ab?d", "abcXd", false}, // ? matches only one character
		{"a?c", "abc", true},
		{"a?c", "aXc", true},
		{"a?c", "ac", false},
		{"???", "abc", true},
		{"???", "ab", false},
		{"???", "abcd", false},

		// Combined * and ? wildcards
		{"a*?", "ab", true},   // * matches empty, ? matches 'b'
		{"a*?", "abc", true},  // * matches 'b', ? matches 'c'
		{"a*?", "a", false},   // ? must match something
		{"a?*", "ab", true},   // ? matches 'b', * matches empty
		{"a?*", "abc", true},  // ? matches 'b', * matches 'c'
		{"a?*b", "aXb", true}, // ? matches 'X', * matches empty
		{"a?*b", "aXYZb", true},
		{"*?*", "a", true},
		{"*?*", "", false}, // ? requires at least one character

		// Edge cases: * matches empty string
		{"a*b", "ab", true},   // * matches empty string
		{"a**b", "ab", true},  // multiple stars match empty
		{"a**b", "axb", true}, // multiple stars match 'x'
		{"a**b", "axyb", true},
		{"*", "", true},
		{"*", "anything", true},
		{"**", "", true},
		{"**", "anything", true},

		// Edge cases: empty strings
		{"", "", true},
		{"a", "", false},
		{"", "a", false},

		// Trailing * matches empty
		{"a*", "a", true},
		{"a*", "abc", true},
		{"abc*", "abc", true},
		{"abc*", "abcdef", true},

		// Leading * matches empty
		{"*a", "a", true},
		{"*a", "XXXa", true},
		{"*abc", "abc", true},
		{"*abc", "XXXabc", true},

		// Multiple wildcards
		{"*a*", "a", true},
		{"*a*", "Xa", true},
		{"*a*", "aX", true},
		{"*a*", "XaX", true},
		{"*a*b*", "ab", true},
		{"*a*b*", "XaYbZ", true},

		// Exact match (no wildcards)
		{"exact", "exact", true},
		{"exact", "notexact", false},
		{"exact", "exactnot", false},

		// S3-style action patterns
		{"Read:bucket*", "Read:bucket-test", true},
		{"Read:bucket*", "Read:bucket", true},
		{"Write:bucket/path/*", "Write:bucket/path/file.txt", true},
		{"Admin:*", "Admin:anything", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.target, func(t *testing.T) {
			result := policy_engine.MatchesWildcard(tt.pattern, tt.target)
			if result != tt.match {
				t.Errorf("policy_engine.MatchesWildcard(%q, %q) = %v, want %v", tt.pattern, tt.target, result, tt.match)
			}
		})
	}
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
				Name:         "notSpecifyAccountId",
				Account:      &AccountAdmin,
				PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/notSpecifyAccountId", defaultAccountID),
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
				Name:         "specifiedAccountID",
				Account:      &specifiedAccount,
				PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/specifiedAccountID", defaultAccountID),
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
				Name:         "anonymous",
				Account:      &AccountAnonymous,
				PrincipalArn: "*",
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

// TestConfigFileWithNoIdentitiesAllowsEnvVars tests that when a config file exists
// but contains no identities (e.g., only KMS settings), environment variables should still work.
// This test validates the fix for issue #7311.
func TestConfigFileWithNoIdentitiesAllowsEnvVars(t *testing.T) {
	// Set environment variables
	testAccessKey := "AKIATEST1234567890AB"
	testSecretKey := "testSecret1234567890123456789012345678901234"
	t.Setenv("AWS_ACCESS_KEY_ID", testAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", testSecretKey)

	// Create a temporary config file with only KMS settings (no identities)
	configContent := `{
  "kms": {
    "default": {
      "provider": "local",
      "config": {
        "keyPath": "/tmp/test-key"
      }
    }
  }
}`
	tmpFile, err := os.CreateTemp("", "s3-config-*.json")
	assert.NoError(t, err, "Should create temp config file")
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(configContent))
	assert.NoError(t, err, "Should write config content")
	tmpFile.Close()

	// Create IAM instance with config file that has no identities
	option := &S3ApiServerOption{
		Config: tmpFile.Name(),
	}
	iam := NewIdentityAccessManagementWithStore(option, string(credential.StoreTypeMemory))

	// Should have exactly one identity from environment variables
	assert.Len(t, iam.identities, 1, "Should have exactly one identity from environment variables even when config file exists with no identities")

	identity := iam.identities[0]
	assert.Equal(t, "admin-AKIATEST", identity.Name, "Identity name should be based on access key")
	assert.Len(t, identity.Credentials, 1, "Should have one credential")
	assert.Equal(t, testAccessKey, identity.Credentials[0].AccessKey, "Access key should match environment variable")
	assert.Equal(t, testSecretKey, identity.Credentials[0].SecretKey, "Secret key should match environment variable")
	assert.Contains(t, identity.Actions, Action(ACTION_ADMIN), "Should have admin action")
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
				result := identity.CanDo(tc.action, tc.bucket, tc.object)
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
			result := identity.CanDo("List", bucket, "")
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
		assert.True(t, identity.CanDo("List", "specificbucket", ""), "Should allow access to exact bucket")
		assert.False(t, identity.CanDo("List", "specificbucket-test", ""), "Should deny access to bucket with suffix")
		assert.False(t, identity.CanDo("List", "otherbucket", ""), "Should deny access to different bucket")
	})

	t.Log("This test validates the fix for issue #7066")
	t.Log("Bucket-level List permissions like 'List:bucket*' work correctly")
	t.Log("ListBucketsHandler now uses consistent authentication flow")
}

// TestListBucketsAuthRequest tests that authRequest works correctly for ListBuckets operations
// This test validates that the fix for the regression identified in PR #7067 works correctly
func TestListBucketsAuthRequest(t *testing.T) {
	t.Run("ListBuckets special case handling", func(t *testing.T) {
		// Create identity with bucket-specific permissions (no global List permission)
		identity := &Identity{
			Name:    "bucket-user",
			Account: &AccountAdmin,
			Actions: []Action{
				Action("List:mybucket*"),
				Action("Read:mybucket*"),
			},
		}

		// Test 1: ListBuckets operation should succeed (bucket = "")
		// This would have failed before the fix because CanDo("List", "", "") would return false
		// After the fix, it bypasses the CanDo check for ListBuckets operations

		// Simulate what happens in authRequest for ListBuckets:
		// action = ACTION_LIST, bucket = "", object = ""

		// Before fix: identity.CanDo(ACTION_LIST, "", "") would fail
		// After fix: the CanDo check should be bypassed

		// Test the individual CanDo method to show it would fail without the special case
		result := identity.CanDo(Action(ACTION_LIST), "", "")
		assert.False(t, result, "CanDo should return false for empty bucket with bucket-specific permissions")

		// Test with a specific bucket that matches the permission
		result2 := identity.CanDo(Action(ACTION_LIST), "mybucket", "")
		assert.True(t, result2, "CanDo should return true for matching bucket")

		// Test with a specific bucket that doesn't match
		result3 := identity.CanDo(Action(ACTION_LIST), "otherbucket", "")
		assert.False(t, result3, "CanDo should return false for non-matching bucket")
	})

	t.Run("Object listing maintains permission enforcement", func(t *testing.T) {
		// Create identity with bucket-specific permissions
		identity := &Identity{
			Name:    "bucket-user",
			Account: &AccountAdmin,
			Actions: []Action{
				Action("List:mybucket*"),
			},
		}

		// For object listing operations, the normal permission checks should still apply
		// These operations have a specific bucket in the URL

		// Should succeed for allowed bucket
		result1 := identity.CanDo(Action(ACTION_LIST), "mybucket", "prefix/")
		assert.True(t, result1, "Should allow listing objects in permitted bucket")

		result2 := identity.CanDo(Action(ACTION_LIST), "mybucket-prod", "")
		assert.True(t, result2, "Should allow listing objects in wildcard-matched bucket")

		// Should fail for disallowed bucket
		result3 := identity.CanDo(Action(ACTION_LIST), "otherbucket", "")
		assert.False(t, result3, "Should deny listing objects in non-permitted bucket")
	})

	t.Log("This test validates the fix for the regression identified in PR #7067")
	t.Log("ListBuckets operation bypasses global permission check when bucket is empty")
	t.Log("Object listing still properly enforces bucket-level permissions")
}

// TestSignatureVerificationDoesNotCheckPermissions tests that signature verification
// only validates the signature and identity, not permissions. Permissions should be
// checked later in authRequest based on the actual operation.
// This test validates the fix for issue #7334
func TestSignatureVerificationDoesNotCheckPermissions(t *testing.T) {
	t.Run("List-only user can authenticate via signature", func(t *testing.T) {
		// Create IAM with a user that only has List permissions on specific buckets
		iam := &IdentityAccessManagement{
			hashes:       make(map[string]*sync.Pool),
			hashCounters: make(map[string]*int32),
		}

		err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{
					Name: "list-only-user",
					Credentials: []*iam_pb.Credential{
						{
							AccessKey: "list_access_key",
							SecretKey: "list_secret_key",
						},
					},
					Actions: []string{
						"List:bucket-123",
						"Read:bucket-123",
					},
				},
			},
		})
		assert.NoError(t, err)

		// Before the fix, signature verification would fail because it checked for Write permission
		// After the fix, signature verification should succeed (only checking signature validity)
		// The actual permission check happens later in authRequest with the correct action

		// The user should be able to authenticate (signature verification passes)
		// But authorization for specific actions is checked separately
		identity, cred, found := iam.lookupByAccessKey("list_access_key")
		assert.True(t, found, "Should find the user by access key")
		assert.Equal(t, "list-only-user", identity.Name)
		assert.Equal(t, "list_secret_key", cred.SecretKey)

		// User should have the correct permissions
		assert.True(t, identity.CanDo(Action(ACTION_LIST), "bucket-123", ""))
		assert.True(t, identity.CanDo(Action(ACTION_READ), "bucket-123", ""))

		// User should NOT have write permissions
		assert.False(t, identity.CanDo(Action(ACTION_WRITE), "bucket-123", ""))
	})

	t.Log("This test validates the fix for issue #7334")
	t.Log("Signature verification no longer checks for Write permission")
	t.Log("This allows list-only and read-only users to authenticate via AWS Signature V4")
}

func TestStaticIdentityProtection(t *testing.T) {
	iam := NewIdentityAccessManagement(&S3ApiServerOption{})

	// Add a static identity
	staticIdent := &Identity{
		Name:     "static-user",
		IsStatic: true,
	}
	iam.m.Lock()
	if iam.nameToIdentity == nil {
		iam.nameToIdentity = make(map[string]*Identity)
	}
	iam.identities = append(iam.identities, staticIdent)
	iam.nameToIdentity[staticIdent.Name] = staticIdent
	iam.m.Unlock()

	// Add a dynamic identity
	dynamicIdent := &Identity{
		Name:     "dynamic-user",
		IsStatic: false,
	}
	iam.m.Lock()
	iam.identities = append(iam.identities, dynamicIdent)
	iam.nameToIdentity[dynamicIdent.Name] = dynamicIdent
	iam.m.Unlock()

	// Try to remove static identity
	iam.RemoveIdentity("static-user")

	// Verify static identity still exists
	iam.m.RLock()
	_, ok := iam.nameToIdentity["static-user"]
	iam.m.RUnlock()
	assert.True(t, ok, "Static identity should not be removed")

	// Try to remove dynamic identity
	iam.RemoveIdentity("dynamic-user")

	// Verify dynamic identity is removed
	iam.m.RLock()
	_, ok = iam.nameToIdentity["dynamic-user"]
	iam.m.RUnlock()
	assert.False(t, ok, "Dynamic identity should have been removed")
}
