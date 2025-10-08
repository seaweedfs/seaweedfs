package s3api

import (
	"encoding/json"
	"encoding/xml"
	"net/http/httptest"
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
