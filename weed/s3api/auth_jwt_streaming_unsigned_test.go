package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJWTStreamingUnsignedAuth covers the auth half of the JWT + unsigned-streaming
// gap. A bearer-token client whose SDK appends a CRC32 trailer sends
// x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER with no SigV4 signature,
// so the request is classified authTypeStreamingUnsigned. It must authenticate via
// the JWT integration rather than silently falling back to the anonymous identity.
func TestJWTStreamingUnsignedAuth(t *testing.T) {
	resetMemoryStore()
	defer resetMemoryStore()

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")
	iam.iamIntegration = &MockIAMIntegration{
		authenticateJWTFunc: func(ctx context.Context, r *http.Request) (*IAMIdentity, s3err.ErrorCode) {
			return &IAMIdentity{Name: "jwt-user", Account: &AccountAdmin}, s3err.ErrNone
		},
	}
	iam.isAuthEnabled = true

	r := httptest.NewRequest(http.MethodPut, "http://localhost:8333/somebucket/someobject", nil)
	r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	r.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	r.Header.Set("Authorization", "Bearer test.jwt.token")

	// Stays classified as unsigned-streaming so getRequestDataReader still decodes
	// the chunked body...
	assert.Equal(t, authTypeStreamingUnsigned, getRequestAuthType(r))

	// ...but authentication resolves to the JWT identity, not anonymous.
	identity, errCode := iam.AuthenticateRequest(r)
	assert.Equal(t, s3err.ErrNone, errCode, "JWT unsigned-streaming PUT must authenticate via the IAM integration")
	require.NotNil(t, identity)
	assert.Equal(t, "jwt-user", identity.Name)
}

// TestJWTStreamingUnsignedChunkedReader covers the body-decode half of the gap.
// newChunkedReader must not try to verify a bearer token as a SigV4 seed signature
// for an unsigned-streaming upload. It used to compute the seed signature whenever
// any Authorization header was present, so a JWT request failed to decode at all.
func TestJWTStreamingUnsignedChunkedReader(t *testing.T) {
	iam := setupIam()

	req, err := NewRequestStreamingUnsignedPayloadTrailer(true)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer test.jwt.token")

	runWithRequest(iam, req, t, strings.Repeat("a", 17408))
}
