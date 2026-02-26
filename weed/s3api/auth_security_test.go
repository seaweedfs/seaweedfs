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
	iam := NewIdentityAccessManagementWithStore(option, nil, "memory")

	assert.True(t, iam.isEnabled(), "Auth should be enabled")

	// Test case 1: Unknown access key should be rejected
	t.Run("Unknown access key", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Host = "localhost:8333"
		err := signRawHTTPRequest(context.Background(), r, "unknown_key", "any_secret", "us-east-1")
		require.NoError(t, err)

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.Equal(t, s3err.ErrInvalidAccessKeyID, errCode, "Should be denied with unknown access key")
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
		// Test CanDo guard
		assert.False(t, nilIdentity.CanDo(s3_constants.ACTION_LIST, "bucket", "object"))
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

		// Invalid request (wrong signature)
		r2 := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r2.Host = "localhost:8333"
		err = signRawHTTPRequest(context.Background(), r2, "xx_access_key", "this_is_a_wrong_secret", "us-east-1")
		require.NoError(t, err)

		_, errCode2 := iam.AuthSignatureOnly(r2)
		assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode2)

		// Verify fix: Streaming unsigned payload should be denied without auth header in AuthSignatureOnly
		r3 := httptest.NewRequest(http.MethodPut, "http://localhost:8333/somebucket/someobject", nil)
		r3.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		// No Authorization header
		_, errCode3 := iam.AuthSignatureOnly(r3)
		assert.Equal(t, s3err.ErrAccessDenied, errCode3, "AuthSignatureOnly should be denied with unsigned streaming if no auth header")
	})

	t.Run("Wrong secret key", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Host = "localhost:8333"
		err := signRawHTTPRequest(context.Background(), r, "readonly_access_key", "this_is_a_wrong_secret", "us-east-1")
		require.NoError(t, err)

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode, "Should NOT be allowed with wrong signature")
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
		assert.Equal(t, s3err.ErrAccessDenied, errCode)
		assert.Nil(t, identity)
	})
	t.Run("Any other credentials", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/", nil)
		r.Host = "localhost:8333"
		err := signRawHTTPRequest(context.Background(), r, "some_other_key", "some_secret", "us-east-1")
		require.NoError(t, err)

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_LIST)
		assert.Equal(t, s3err.ErrInvalidAccessKeyID, errCode, "Should NOT be allowed with ANY other credentials")
		assert.Nil(t, identity)
	})
	t.Run("Streaming unsigned payload bypass attempt", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "http://localhost:8333/somebucket/someobject", nil)
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		// No Authorization header

		identity, errCode := iam.authRequest(r, s3_constants.ACTION_WRITE)
		assert.Equal(t, s3err.ErrAccessDenied, errCode, "Should be denied with unsigned streaming if no auth header")
		assert.Nil(t, identity)
	})
}

// TestExternalUrlSignatureVerification tests that S3 signature verification works
// correctly when s3.externalUrl is configured. It uses the real AWS SDK v2 signer
// to prove correctness against actual S3 clients behind a reverse proxy.
func TestExternalUrlSignatureVerification(t *testing.T) {
	configContent := `{
  "identities": [
    {
      "name": "test_user",
      "credentials": [
        {
          "accessKey": "AKIAIOSFODNN7EXAMPLE",
          "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}`
	tmpFile, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	_, err = tmpFile.Write([]byte(configContent))
	require.NoError(t, err)
	tmpFile.Close()

	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	tests := []struct {
		name           string
		clientUrl      string // URL the client signs against
		backendHost    string // Host header SeaweedFS sees (from proxy)
		externalUrl    string // s3.externalUrl config
		expectSuccess  bool
	}{
		{
			name:          "non-standard port with externalUrl",
			clientUrl:     "https://api.example.com:9000/test-bucket/object",
			backendHost:   "backend:8333",
			externalUrl:   "https://api.example.com:9000",
			expectSuccess: true,
		},
		{
			name:          "HTTPS default port 443 with externalUrl (port stripped by SDK and parseExternalUrlToHost)",
			clientUrl:     "https://api.example.com/test-bucket/object",
			backendHost:   "backend:8333",
			externalUrl:   "https://api.example.com:443",
			expectSuccess: true,
		},
		{
			name:          "HTTPS without explicit port with externalUrl",
			clientUrl:     "https://api.example.com/test-bucket/object",
			backendHost:   "backend:8333",
			externalUrl:   "https://api.example.com",
			expectSuccess: true,
		},
		{
			name:          "HTTP default port 80 with externalUrl",
			clientUrl:     "http://api.example.com/test-bucket/object",
			backendHost:   "backend:8333",
			externalUrl:   "http://api.example.com:80",
			expectSuccess: true,
		},
		{
			name:          "without externalUrl, internal host causes mismatch",
			clientUrl:     "https://api.example.com:9000/test-bucket/object",
			backendHost:   "backend:8333",
			externalUrl:   "",
			expectSuccess: false,
		},
		{
			name:          "without externalUrl, matching host works",
			clientUrl:     "http://localhost:8333/test-bucket/object",
			backendHost:   "localhost:8333",
			externalUrl:   "",
			expectSuccess: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create IAM with the externalUrl
			option := &S3ApiServerOption{
				Config:      tmpFile.Name(),
				ExternalUrl: tt.externalUrl,
			}
			iam := NewIdentityAccessManagementWithStore(option, nil, "memory")
			require.True(t, iam.isEnabled())

			// Step 1: Sign a request targeting the client-facing URL using real AWS SDK signer
			signReq, err := http.NewRequest(http.MethodGet, tt.clientUrl, nil)
			require.NoError(t, err)
			signReq.Host = signReq.URL.Host

			err = signRawHTTPRequest(context.Background(), signReq, accessKey, secretKey, "us-east-1")
			require.NoError(t, err)

			// Step 2: Create a separate request as the proxy would deliver it
			proxyReq := httptest.NewRequest(http.MethodGet, tt.clientUrl, nil)
			proxyReq.Host = tt.backendHost
			proxyReq.URL.Host = tt.backendHost

			// Copy the auth headers from the signed request
			proxyReq.Header.Set("Authorization", signReq.Header.Get("Authorization"))
			proxyReq.Header.Set("X-Amz-Date", signReq.Header.Get("X-Amz-Date"))
			proxyReq.Header.Set("X-Amz-Content-Sha256", signReq.Header.Get("X-Amz-Content-Sha256"))

			// Step 3: Verify
			identity, errCode := iam.authRequest(proxyReq, s3_constants.ACTION_LIST)
			if tt.expectSuccess {
				assert.Equal(t, s3err.ErrNone, errCode, "Expected successful signature verification")
				require.NotNil(t, identity)
				assert.Equal(t, "test_user", identity.Name)
			} else {
				assert.NotEqual(t, s3err.ErrNone, errCode, "Expected signature verification to fail")
			}
		})
	}
}

// TestRealSDKSignerWithForwardedHeaders proves that extractHostHeader produces
// values that match the real AWS SDK v2 signer's SanitizeHostForHeader behavior.
//
// Unlike the self-referential tests in auto_signature_v4_test.go (which sign and
// verify with the same extractHostHeader function), this test uses the real AWS
// SDK v2 signer to sign the request. The SDK has its own host sanitization:
//   - strips :80 for http
//   - strips :443 for https
//   - preserves all other ports
//
// If extractHostHeader disagrees with the SDK, the signature will not match.
func TestRealSDKSignerWithForwardedHeaders(t *testing.T) {
	configContent := `{
  "identities": [
    {
      "name": "test_user",
      "credentials": [
        {
          "accessKey": "AKIAIOSFODNN7EXAMPLE",
          "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}`
	tmpFile, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	_, err = tmpFile.Write([]byte(configContent))
	require.NoError(t, err)
	tmpFile.Close()

	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// Each test case simulates:
	//   1. A client connecting to clientUrl (SDK signs with the host from this URL)
	//   2. A proxy forwarding to SeaweedFS with X-Forwarded-* headers
	//   3. SeaweedFS extracting the host and verifying the signature
	tests := []struct {
		name           string
		clientUrl      string // URL the client/SDK signs against
		backendHost    string // Host header SeaweedFS receives (proxy rewrites this)
		forwardedHost  string // X-Forwarded-Host set by proxy
		forwardedPort  string // X-Forwarded-Port set by proxy
		forwardedProto string // X-Forwarded-Proto set by proxy
		expectedHost   string // what extractHostHeader should return (must match SDK)
	}{
		{
			name:           "HTTPS non-standard port",
			clientUrl:      "https://api.example.com:9000/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com",
			forwardedPort:  "9000",
			forwardedProto: "https",
			expectedHost:   "api.example.com:9000",
		},
		{
			name:           "HTTPS standard port 443 (SDK strips, we must too)",
			clientUrl:      "https://api.example.com/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com",
			forwardedPort:  "443",
			forwardedProto: "https",
			expectedHost:   "api.example.com",
		},
		{
			name:           "HTTP standard port 80 (SDK strips, we must too)",
			clientUrl:      "http://api.example.com/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com",
			forwardedPort:  "80",
			forwardedProto: "http",
			expectedHost:   "api.example.com",
		},
		{
			name:           "HTTP non-standard port 8080",
			clientUrl:      "http://api.example.com:8080/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "api.example.com:8080",
		},
		{
			name:           "X-Forwarded-Host with port (Traefik style), HTTPS 443",
			clientUrl:      "https://api.example.com/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com:443",
			forwardedPort:  "443",
			forwardedProto: "https",
			expectedHost:   "api.example.com",
		},
		{
			name:           "X-Forwarded-Host with port (Traefik style), HTTP 80",
			clientUrl:      "http://api.example.com/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com:80",
			forwardedPort:  "80",
			forwardedProto: "http",
			expectedHost:   "api.example.com",
		},
		{
			name:           "no forwarded headers, direct access",
			clientUrl:      "http://localhost:8333/bucket/key",
			backendHost:    "localhost:8333",
			forwardedHost:  "",
			forwardedPort:  "",
			forwardedProto: "",
			expectedHost:   "localhost:8333",
		},
		{
			name:           "empty proto with port 80 (defaults to http, strip)",
			clientUrl:      "http://api.example.com/bucket/key",
			backendHost:    "seaweedfs:8333",
			forwardedHost:  "api.example.com",
			forwardedPort:  "80",
			forwardedProto: "",
			expectedHost:   "api.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := &S3ApiServerOption{Config: tmpFile.Name()}
			iam := NewIdentityAccessManagementWithStore(option, nil, "memory")
			require.True(t, iam.isEnabled())

			// Step 1: Sign a request using the real AWS SDK v2 signer.
			// The SDK applies its own SanitizeHostForHeader logic.
			signReq, err := http.NewRequest(http.MethodGet, tt.clientUrl, nil)
			require.NoError(t, err)
			signReq.Host = signReq.URL.Host

			err = signRawHTTPRequest(context.Background(), signReq, accessKey, secretKey, "us-east-1")
			require.NoError(t, err)

			// Step 2: Build the request as the proxy would deliver it to SeaweedFS.
			proxyReq := httptest.NewRequest(http.MethodGet, tt.clientUrl, nil)
			proxyReq.Host = tt.backendHost

			// Copy signed auth headers
			proxyReq.Header.Set("Authorization", signReq.Header.Get("Authorization"))
			proxyReq.Header.Set("X-Amz-Date", signReq.Header.Get("X-Amz-Date"))
			proxyReq.Header.Set("X-Amz-Content-Sha256", signReq.Header.Get("X-Amz-Content-Sha256"))

			// Set forwarded headers
			if tt.forwardedHost != "" {
				proxyReq.Header.Set("X-Forwarded-Host", tt.forwardedHost)
			}
			if tt.forwardedPort != "" {
				proxyReq.Header.Set("X-Forwarded-Port", tt.forwardedPort)
			}
			if tt.forwardedProto != "" {
				proxyReq.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}

			// Step 3: Verify extractHostHeader returns the expected value.
			// This is the critical correctness check: our extracted host must match
			// what the SDK signed with, otherwise the signature will not match.
			extractedHost := extractHostHeader(proxyReq, iam.externalHost)
			assert.Equal(t, tt.expectedHost, extractedHost,
				"extractHostHeader must return a value matching what AWS SDK signed with")

			// Step 4: Verify the full signature matches.
			identity, errCode := iam.authRequest(proxyReq, s3_constants.ACTION_LIST)
			assert.Equal(t, s3err.ErrNone, errCode,
				"Signature from real AWS SDK signer must verify successfully")
			require.NotNil(t, identity)
			assert.Equal(t, "test_user", identity.Name)
		})
	}
}
