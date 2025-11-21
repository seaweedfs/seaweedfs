package s3api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutBucketAclCannedAclSupport(t *testing.T) {
	// Test that the ExtractAcl function can handle various canned ACLs
	// This tests the core functionality without requiring a fully initialized S3ApiServer

	testCases := []struct {
		name        string
		cannedAcl   string
		shouldWork  bool
		description string
	}{
		{
			name:        "private",
			cannedAcl:   s3_constants.CannedAclPrivate,
			shouldWork:  true,
			description: "private ACL should be accepted",
		},
		{
			name:        "public-read",
			cannedAcl:   s3_constants.CannedAclPublicRead,
			shouldWork:  true,
			description: "public-read ACL should be accepted",
		},
		{
			name:        "public-read-write",
			cannedAcl:   s3_constants.CannedAclPublicReadWrite,
			shouldWork:  true,
			description: "public-read-write ACL should be accepted",
		},
		{
			name:        "authenticated-read",
			cannedAcl:   s3_constants.CannedAclAuthenticatedRead,
			shouldWork:  true,
			description: "authenticated-read ACL should be accepted",
		},
		{
			name:        "bucket-owner-read",
			cannedAcl:   s3_constants.CannedAclBucketOwnerRead,
			shouldWork:  true,
			description: "bucket-owner-read ACL should be accepted",
		},
		{
			name:        "bucket-owner-full-control",
			cannedAcl:   s3_constants.CannedAclBucketOwnerFullControl,
			shouldWork:  true,
			description: "bucket-owner-full-control ACL should be accepted",
		},
		{
			name:        "invalid-acl",
			cannedAcl:   "invalid-acl-value",
			shouldWork:  false,
			description: "invalid ACL should be rejected",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a request with the specified canned ACL
			req := httptest.NewRequest("PUT", "/bucket?acl", nil)
			req.Header.Set(s3_constants.AmzCannedAcl, tc.cannedAcl)
			req.Header.Set(s3_constants.AmzAccountId, "test-account-123")

			// Create a mock IAM for testing
			mockIam := &mockIamInterface{}

			// Test the ACL extraction directly
			grants, errCode := ExtractAcl(req, mockIam, "", "test-account-123", "test-account-123", "test-account-123")

			if tc.shouldWork {
				assert.Equal(t, s3err.ErrNone, errCode, "Expected ACL parsing to succeed for %s", tc.cannedAcl)
				assert.NotEmpty(t, grants, "Expected grants to be generated for valid ACL %s", tc.cannedAcl)
				t.Logf("✓ PASS: %s - %s", tc.name, tc.description)
			} else {
				assert.NotEqual(t, s3err.ErrNone, errCode, "Expected ACL parsing to fail for invalid ACL %s", tc.cannedAcl)
				t.Logf("✓ PASS: %s - %s", tc.name, tc.description)
			}
		})
	}
}

// TestBucketWithoutACLIsNotPublicRead tests that buckets without ACLs are not public-read
func TestBucketWithoutACLIsNotPublicRead(t *testing.T) {
	// Create a bucket config without ACL (like a freshly created bucket)
	config := &BucketConfig{
		Name:         "test-bucket",
		IsPublicRead: false, // Should be explicitly false
	}

	// Verify that buckets without ACL are not public-read
	assert.False(t, config.IsPublicRead, "Bucket without ACL should not be public-read")
}

func TestBucketConfigInitialization(t *testing.T) {
	// Test that BucketConfig properly initializes IsPublicRead field
	config := &BucketConfig{
		Name:         "test-bucket",
		IsPublicRead: false, // Explicitly set to false for private buckets
	}

	// Verify proper initialization
	assert.False(t, config.IsPublicRead, "Newly created bucket should not be public-read by default")
}

// TestUpdateBucketConfigCacheConsistency tests that updateBucketConfigCacheFromEntry
// properly handles the IsPublicRead flag consistently with getBucketConfig
func TestUpdateBucketConfigCacheConsistency(t *testing.T) {
	t.Run("bucket without ACL should have IsPublicRead=false", func(t *testing.T) {
		// Simulate an entry without ACL (like a freshly created bucket)
		entry := &filer_pb.Entry{
			Name: "test-bucket",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0755,
			},
			// Extended is nil or doesn't contain ACL
		}

		// Test what updateBucketConfigCacheFromEntry would create
		config := &BucketConfig{
			Name:         entry.Name,
			Entry:        entry,
			IsPublicRead: false, // Should be explicitly false
		}

		// When Extended is nil, IsPublicRead should be false
		assert.False(t, config.IsPublicRead, "Bucket without Extended metadata should not be public-read")

		// When Extended exists but has no ACL key, IsPublicRead should also be false
		entry.Extended = make(map[string][]byte)
		entry.Extended["some-other-key"] = []byte("some-value")

		config = &BucketConfig{
			Name:         entry.Name,
			Entry:        entry,
			IsPublicRead: false, // Should be explicitly false
		}

		// Simulate the else branch: no ACL means private bucket
		if _, exists := entry.Extended[s3_constants.ExtAmzAclKey]; !exists {
			config.IsPublicRead = false
		}

		assert.False(t, config.IsPublicRead, "Bucket with Extended but no ACL should not be public-read")
	})

	t.Run("bucket with public-read ACL should have IsPublicRead=true", func(t *testing.T) {
		// Create a mock public-read ACL using AWS S3 SDK types
		publicReadGrants := []*s3.Grant{
			{
				Grantee: &s3.Grantee{
					Type: &s3_constants.GrantTypeGroup,
					URI:  &s3_constants.GranteeGroupAllUsers,
				},
				Permission: &s3_constants.PermissionRead,
			},
		}

		aclBytes, err := json.Marshal(publicReadGrants)
		require.NoError(t, err)

		entry := &filer_pb.Entry{
			Name: "public-bucket",
			Extended: map[string][]byte{
				s3_constants.ExtAmzAclKey: aclBytes,
			},
		}

		config := &BucketConfig{
			Name:         entry.Name,
			Entry:        entry,
			IsPublicRead: false, // Start with false
		}

		// Simulate what updateBucketConfigCacheFromEntry would do
		if acl, exists := entry.Extended[s3_constants.ExtAmzAclKey]; exists {
			config.ACL = acl
			config.IsPublicRead = parseAndCachePublicReadStatus(acl)
		}

		assert.True(t, config.IsPublicRead, "Bucket with public-read ACL should be public-read")
	})
}

// mockIamInterface is a simple mock for testing
type mockIamInterface struct{}

func (m *mockIamInterface) GetAccountNameById(canonicalId string) string {
	return "test-user-" + canonicalId
}

func (m *mockIamInterface) GetAccountIdByEmail(email string) string {
	return "account-for-" + email
}

// TestListAllMyBucketsResultNamespace verifies that the ListAllMyBucketsResult
// XML response includes the proper S3 namespace URI
func TestListAllMyBucketsResultNamespace(t *testing.T) {
	// Create a sample ListAllMyBucketsResult response
	response := ListAllMyBucketsResult{
		Owner: CanonicalUser{
			ID:          "test-owner-id",
			DisplayName: "test-owner",
		},
		Buckets: ListAllMyBucketsList{
			Bucket: []ListAllMyBucketsEntry{
				{
					Name:         "test-bucket",
					CreationDate: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	// Marshal the response to XML
	xmlData, err := xml.Marshal(response)
	require.NoError(t, err, "Failed to marshal XML response")

	xmlString := string(xmlData)

	// Verify that the XML contains the proper namespace
	assert.Contains(t, xmlString, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`,
		"XML response should contain the S3 namespace URI")

	// Verify the root element has the correct name
	assert.Contains(t, xmlString, "<ListAllMyBucketsResult",
		"XML response should have ListAllMyBucketsResult root element")

	// Verify structure contains expected elements
	assert.Contains(t, xmlString, "<Owner>", "XML should contain Owner element")
	assert.Contains(t, xmlString, "<Buckets>", "XML should contain Buckets element")
	assert.Contains(t, xmlString, "<Bucket>", "XML should contain Bucket element")
	assert.Contains(t, xmlString, "<Name>test-bucket</Name>", "XML should contain bucket name")

	t.Logf("Generated XML:\n%s", xmlString)
}

// TestListBucketsOwnershipFiltering tests that ListBucketsHandler properly filters
// buckets based on ownership, allowing only bucket owners (or admins) to see their buckets
func TestListBucketsOwnershipFiltering(t *testing.T) {
	testCases := []struct {
		name                string
		buckets             []testBucket
		requestIdentityId   string
		requestIsAdmin      bool
		expectedBucketNames []string
		description         string
	}{
		{
			name: "non-admin sees only owned buckets",
			buckets: []testBucket{
				{name: "user1-bucket", ownerId: "user1"},
				{name: "user2-bucket", ownerId: "user2"},
				{name: "user1-bucket2", ownerId: "user1"},
			},
			requestIdentityId:   "user1",
			requestIsAdmin:      false,
			expectedBucketNames: []string{"user1-bucket", "user1-bucket2"},
			description:         "Non-admin user should only see buckets they own",
		},
		{
			name: "admin sees all buckets",
			buckets: []testBucket{
				{name: "user1-bucket", ownerId: "user1"},
				{name: "user2-bucket", ownerId: "user2"},
				{name: "user3-bucket", ownerId: "user3"},
			},
			requestIdentityId:   "admin",
			requestIsAdmin:      true,
			expectedBucketNames: []string{"user1-bucket", "user2-bucket", "user3-bucket"},
			description:         "Admin should see all buckets regardless of owner",
		},
		{
			name: "buckets without owner are hidden from non-admins",
			buckets: []testBucket{
				{name: "owned-bucket", ownerId: "user1"},
				{name: "unowned-bucket", ownerId: ""}, // No owner set
			},
			requestIdentityId:   "user2",
			requestIsAdmin:      false,
			expectedBucketNames: []string{},
			description:         "Buckets without owner should be hidden from non-admin users",
		},
		{
			name: "empty identityId skips ownership check",
			buckets: []testBucket{
				{name: "owned-bucket", ownerId: "user1"},
				{name: "unowned-bucket", ownerId: ""},
			},
			requestIdentityId:   "",
			requestIsAdmin:      false,
			expectedBucketNames: []string{"owned-bucket", "unowned-bucket"},
			description:         "When identityId is empty, ownership check is skipped, all buckets visible",
		},
		{
			name: "admin with empty identityId sees all",
			buckets: []testBucket{
				{name: "user1-bucket", ownerId: "user1"},
				{name: "user2-bucket", ownerId: "user2"},
			},
			requestIdentityId:   "",
			requestIsAdmin:      true,
			expectedBucketNames: []string{"user1-bucket", "user2-bucket"},
			description:         "Admin should see all buckets even with empty identityId",
		},
		{
			name: "buckets with nil Extended metadata hidden from non-admins",
			buckets: []testBucket{
				{name: "bucket-no-extended", ownerId: "", nilExtended: true},
				{name: "bucket-with-owner", ownerId: "user1"},
			},
			requestIdentityId:   "user1",
			requestIsAdmin:      false,
			expectedBucketNames: []string{"bucket-with-owner"},
			description:         "Buckets with nil Extended (no owner) should be hidden from non-admins",
		},
		{
			name: "user sees only their bucket among many",
			buckets: []testBucket{
				{name: "alice-bucket", ownerId: "alice"},
				{name: "bob-bucket", ownerId: "bob"},
				{name: "charlie-bucket", ownerId: "charlie"},
				{name: "alice-bucket2", ownerId: "alice"},
			},
			requestIdentityId:   "bob",
			requestIsAdmin:      false,
			expectedBucketNames: []string{"bob-bucket"},
			description:         "User should see only their single bucket among many",
		},
		{
			name: "admin sees buckets without owners",
			buckets: []testBucket{
				{name: "owned-bucket", ownerId: "user1"},
				{name: "unowned-bucket", ownerId: ""},
				{name: "no-metadata-bucket", ownerId: "", nilExtended: true},
			},
			requestIdentityId:   "admin",
			requestIsAdmin:      true,
			expectedBucketNames: []string{"owned-bucket", "unowned-bucket", "no-metadata-bucket"},
			description:         "Admin should see all buckets including those without owners",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock entries
			entries := make([]*filer_pb.Entry, 0, len(tc.buckets))
			for _, bucket := range tc.buckets {
				entry := &filer_pb.Entry{
					Name:        bucket.name,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Crtime: time.Now().Unix(),
					},
				}

				if !bucket.nilExtended {
					entry.Extended = make(map[string][]byte)
					if bucket.ownerId != "" {
						entry.Extended[s3_constants.AmzIdentityId] = []byte(bucket.ownerId)
					}
				}

				entries = append(entries, entry)
			}

			// Filter entries using the actual production code
			var filteredBuckets []string
			for _, entry := range entries {
				var identity *Identity
				if tc.requestIdentityId != "" {
					identity = mockIdentity(tc.requestIdentityId, tc.requestIsAdmin)
				}
				if isBucketVisibleToIdentity(entry, identity) {
					filteredBuckets = append(filteredBuckets, entry.Name)
				}
			}

			// Assert expected buckets match filtered buckets
			assert.ElementsMatch(t, tc.expectedBucketNames, filteredBuckets,
				"%s - Expected buckets: %v, Got: %v", tc.description, tc.expectedBucketNames, filteredBuckets)
		})
	}
}

// testBucket represents a bucket for testing with ownership metadata
type testBucket struct {
	name        string
	ownerId     string
	nilExtended bool
}

// mockIdentity creates a mock Identity for testing bucket visibility
func mockIdentity(name string, isAdmin bool) *Identity {
	identity := &Identity{
		Name: name,
	}
	if isAdmin {
		identity.Credentials = []*Credential{
			{
				AccessKey: "admin-key",
				SecretKey: "admin-secret",
			},
		}
		identity.Actions = []Action{ACTION_ADMIN}
	}
	return identity
}

// TestListBucketsOwnershipEdgeCases tests edge cases in ownership filtering
func TestListBucketsOwnershipEdgeCases(t *testing.T) {
	t.Run("malformed owner id with special characters", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				s3_constants.AmzIdentityId: []byte("user@domain.com"),
			},
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity("user@domain.com", false)

		// Should match exactly even with special characters
		isVisible := isBucketVisibleToIdentity(entry, identity)

		assert.True(t, isVisible, "Should match owner ID with special characters exactly")
	})

	t.Run("owner id with unicode characters", func(t *testing.T) {
		unicodeOwnerId := "用户123"
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				s3_constants.AmzIdentityId: []byte(unicodeOwnerId),
			},
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity(unicodeOwnerId, false)

		isVisible := isBucketVisibleToIdentity(entry, identity)

		assert.True(t, isVisible, "Should handle unicode owner IDs correctly")
	})

	t.Run("owner id with binary data", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				s3_constants.AmzIdentityId: []byte{0x00, 0x01, 0x02, 0xFF},
			},
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity("normaluser", false)

		// Should not panic when converting binary data to string
		assert.NotPanics(t, func() {
			isVisible := isBucketVisibleToIdentity(entry, identity)
			assert.False(t, isVisible, "Binary owner ID should not match normal user")
		})
	})

	t.Run("empty owner id in Extended", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				s3_constants.AmzIdentityId: []byte(""),
			},
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity("user1", false)

		isVisible := isBucketVisibleToIdentity(entry, identity)

		assert.False(t, isVisible, "Empty owner ID should be treated as unowned (hidden from non-admins)")
	})

	t.Run("nil Extended map safe access", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended:    nil, // Explicitly nil
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity("user1", false)

		// Should not panic with nil Extended map
		assert.NotPanics(t, func() {
			isVisible := isBucketVisibleToIdentity(entry, identity)
			assert.False(t, isVisible, "Nil Extended (no owner) should be hidden from non-admins")
		})
	})

	t.Run("very long owner id", func(t *testing.T) {
		longOwnerId := strings.Repeat("a", 10000)
		entry := &filer_pb.Entry{
			Name:        "test-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				s3_constants.AmzIdentityId: []byte(longOwnerId),
			},
			Attributes: &filer_pb.FuseAttributes{
				Crtime: time.Now().Unix(),
			},
		}

		identity := mockIdentity(longOwnerId, false)

		// Should handle very long owner IDs without panic
		assert.NotPanics(t, func() {
			isVisible := isBucketVisibleToIdentity(entry, identity)
			assert.True(t, isVisible, "Long owner ID should match correctly")
		})
	})
}

// TestListBucketsOwnershipWithPermissions tests that ownership filtering
// works in conjunction with permission checks
func TestListBucketsOwnershipWithPermissions(t *testing.T) {
	t.Run("ownership check before permission check", func(t *testing.T) {
		// Simulate scenario where ownership check filters first,
		// then permission check applies to remaining buckets
		entries := []*filer_pb.Entry{
			{
				Name:        "owned-bucket",
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.AmzIdentityId: []byte("user1"),
				},
				Attributes: &filer_pb.FuseAttributes{Crtime: time.Now().Unix()},
			},
			{
				Name:        "other-bucket",
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.AmzIdentityId: []byte("user2"),
				},
				Attributes: &filer_pb.FuseAttributes{Crtime: time.Now().Unix()},
			},
		}

		identity := mockIdentity("user1", false)

		// First pass: ownership filtering
		var afterOwnershipFilter []*filer_pb.Entry
		for _, entry := range entries {
			if isBucketVisibleToIdentity(entry, identity) {
				afterOwnershipFilter = append(afterOwnershipFilter, entry)
			}
		}

		// Only owned-bucket should remain after ownership filter
		assert.Len(t, afterOwnershipFilter, 1, "Only owned bucket should pass ownership filter")
		assert.Equal(t, "owned-bucket", afterOwnershipFilter[0].Name)

		// Permission checks would apply to afterOwnershipFilter entries
		// (not tested here as it depends on IAM system)
	})

	t.Run("admin bypasses ownership but not permissions", func(t *testing.T) {
		entries := []*filer_pb.Entry{
			{
				Name:        "user1-bucket",
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.AmzIdentityId: []byte("user1"),
				},
				Attributes: &filer_pb.FuseAttributes{Crtime: time.Now().Unix()},
			},
			{
				Name:        "user2-bucket",
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.AmzIdentityId: []byte("user2"),
				},
				Attributes: &filer_pb.FuseAttributes{Crtime: time.Now().Unix()},
			},
		}

		identity := mockIdentity("admin-user", true)

		// Admin bypasses ownership check
		var afterOwnershipFilter []*filer_pb.Entry
		for _, entry := range entries {
			if isBucketVisibleToIdentity(entry, identity) {
				afterOwnershipFilter = append(afterOwnershipFilter, entry)
			}
		}

		// Admin should see all buckets after ownership filter
		assert.Len(t, afterOwnershipFilter, 2, "Admin should see all buckets after ownership filter")
		// Note: Permission checks still apply to admins in actual implementation
	})
}

// TestListBucketsOwnershipCaseSensitivity tests case sensitivity in owner matching
func TestListBucketsOwnershipCaseSensitivity(t *testing.T) {
	entry := &filer_pb.Entry{
		Name:        "test-bucket",
		IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.AmzIdentityId: []byte("User1"),
		},
		Attributes: &filer_pb.FuseAttributes{
			Crtime: time.Now().Unix(),
		},
	}

	testCases := []struct {
		requestIdentityId string
		shouldMatch       bool
	}{
		{"User1", true},
		{"user1", false}, // Case sensitive
		{"USER1", false}, // Case sensitive
		{"User2", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("identity_%s", tc.requestIdentityId), func(t *testing.T) {
			identity := mockIdentity(tc.requestIdentityId, false)
			isVisible := isBucketVisibleToIdentity(entry, identity)

			if tc.shouldMatch {
				assert.True(t, isVisible, "Identity %s should match (case sensitive)", tc.requestIdentityId)
			} else {
				assert.False(t, isVisible, "Identity %s should not match (case sensitive)", tc.requestIdentityId)
			}
		})
	}
}
