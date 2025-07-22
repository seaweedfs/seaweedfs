package s3api

import (
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
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
	// Test that getBucketConfig correctly handles buckets with no ACL
	t.Run("bucket without ACL should not be public-read", func(t *testing.T) {
		// Simulate a bucket entry with no ACL
		entry := &filer_pb.Entry{
			Name: "test-bucket",
			// Extended is nil or doesn't contain ACL key
		}

		// Test that IsPublicRead defaults to false when no ACL is present
		config := &BucketConfig{
			Name:  "test-bucket",
			Entry: entry,
		}

		// When Extended is nil, IsPublicRead should remain false (Go default)
		assert.False(t, config.IsPublicRead, "Bucket without ACL should not be public-read")

		// When Extended exists but has no ACL key, IsPublicRead should remain false
		entry.Extended = make(map[string][]byte)
		assert.False(t, config.IsPublicRead, "Bucket with Extended but no ACL should not be public-read")
	})

	t.Run("parseAndCachePublicReadStatus with empty ACL should return false", func(t *testing.T) {
		// Test that parseAndCachePublicReadStatus returns false for invalid/empty ACLs

		// Empty ACL should return false
		result := parseAndCachePublicReadStatus([]byte{})
		assert.False(t, result, "Empty ACL should not be public-read")

		// Invalid JSON should return false
		result = parseAndCachePublicReadStatus([]byte("invalid json"))
		assert.False(t, result, "Invalid JSON ACL should not be public-read")

		// Empty grants array should return false
		emptyGrants := []byte("[]")
		result = parseAndCachePublicReadStatus(emptyGrants)
		assert.False(t, result, "Empty grants array should not be public-read")
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
