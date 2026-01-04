package s3api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

func TestReproIssue7912(t *testing.T) {
	// Create a temporary s3.json
	configContent := `{
  "identities": [
    {
      "name": "xx",
      "credentials": [
        {
          "accessKey": "xx_access_key",
          "secretKey": "xx_secret_key"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "read_only_user",
      "credentials": [
        {
          "accessKey": "readonly_access_key",
          "secretKey": "readonly_secret_key"
        }
      ],
      "actions": ["Read", "List"]
    }
  ]
}`
	tmpFile, err := os.CreateTemp("", "s3-config-*.json")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(configContent))
	assert.NoError(t, err)
	tmpFile.Close()

	// Initialize Identities Access Management
	option := &S3ApiServerOption{
		Config: tmpFile.Name(),
	}
	iam := NewIdentityAccessManagementWithStore(option, "memory")

	assert.True(t, iam.isEnabled(), "Auth should be enabled")

	// Test case 1: Correct credentials
	t.Run("Correct credentials", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		// We need to simulate a V4 signature. For simplicity, we can mock the signature verification
		// or use the real one if we can easily generate a signature.
		// Since we want to test the bypass, let's see if we can trigger any bypass.

		// Let's use a fake access key that is NOT in the config
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=unknown_key/20260103/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fake")

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.NotEqual(t, s3err.ErrNone, errCode, "Should NOT be allowed with unknown access key")
		assert.Nil(t, identity)
	})

	t.Run("Wrong secret key", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=readonly_access_key/20260103/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fake")
		r.Header.Set("x-amz-date", "20260103T000000Z")

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.NotEqual(t, s3err.ErrNone, errCode, "Should NOT be allowed with wrong signature")
		assert.Nil(t, identity)
	})

	t.Run("Anonymous request to protected bucket", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/somebucket/", nil)
		// No Authorization header

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.Equal(t, s3err.ErrAccessDenied, errCode, "Should be denied for anonymous")
		assert.Nil(t, identity)
	})

	t.Run("Non-S3 request should be denied", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		// No headers at all

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.NotEqual(t, s3err.ErrNone, errCode)
		assert.Nil(t, identity)
	})
	t.Run("Any other credentials", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=some_other_key/20260103/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=some_signature")
		r.Header.Set("x-amz-date", "20260103T000000Z")

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.NotEqual(t, s3err.ErrNone, errCode, "Should NOT be allowed with ANY other credentials")
		assert.Nil(t, identity)
	})
	t.Run("Streaming unsigned payload bypass attempt", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "http://localhost:8333/somebucket/someobject", nil)
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		// No Authorization header

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_WRITE)
		assert.NotEqual(t, s3err.ErrNone, errCode, "Should NOT be allowed with unsigned streaming if no auth header")
		assert.Nil(t, identity)
	})
}
