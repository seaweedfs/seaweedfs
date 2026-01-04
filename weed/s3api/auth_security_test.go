package s3api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func signRawHTTPRequest(ctx context.Context, req *http.Request, accessKey, secretKey, region string) error {
	creds := aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	signer := v4.NewSigner()
	payloadHash := fmt.Sprintf("%x", sha256.Sum256([]byte{}))
	return signer.SignHTTP(ctx, creds, req, payloadHash, "s3", region, time.Now())
}

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

	// Test case 1: Unknown access key should be rejected
	t.Run("Unknown access key should be rejected", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		// Let's use a fake access key that is NOT in the config
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=unknown_key/20260103/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fake")

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.NotEqual(t, s3err.ErrNone, errCode, "Should NOT be allowed with unknown access key")
		assert.Nil(t, identity)
	})

	t.Run("Positive test case: properly signed credentials", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Host = "localhost:8333"
		err := signRawHTTPRequest(context.Background(), r, "readonly_access_key", "readonly_secret_key", "us-east-1")
		require.NoError(t, err)

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.Equal(t, s3err.ErrNone, errCode)
		require.NotNil(t, identity)
		assert.Equal(t, "read_only_user", identity.Name)
	})

	t.Run("Nil identity tests for guards", func(t *testing.T) {
		var nilIdentity *Identity
		// Test isAdmin guard
		assert.False(t, nilIdentity.isAdmin())
		// Test canDo guard
		assert.False(t, nilIdentity.canDo(s3_constants.ACTION_LIST, "bucket", "object"))
	})

	t.Run("AuthSignatureOnly path", func(t *testing.T) {
		// Valid request
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Host = "localhost:8333"
		err := signRawHTTPRequest(context.Background(), r, "xx_access_key", "xx_secret_key", "us-east-1")
		require.NoError(t, err)

		identity, errCode := iam.AuthSignatureOnly(r)
		assert.Equal(t, s3err.ErrNone, errCode)
		require.NotNil(t, identity)
		assert.Equal(t, "xx", identity.Name)

		// Invalid request
		r2 := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r2.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=xx_access_key/20260103/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=wrong")
		_, errCode2 := iam.AuthSignatureOnly(r2)
		assert.NotEqual(t, s3err.ErrNone, errCode2)
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
