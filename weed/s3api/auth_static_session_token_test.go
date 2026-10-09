package s3api

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Credential vendors like Unity Catalog emit a session token even for static
// credentials. A request signed by a configured access key must authenticate
// as that identity, tolerating the foreign token.
func TestStaticCredentialWithSessionToken(t *testing.T) {
	const token = "vended-static-session-token"

	newSigned := func(t *testing.T) *http.Request {
		req := mustNewRequest(http.MethodGet, "http://127.0.0.1:9000/bucket/object", 0, nil, t)
		req.Header.Set("X-Amz-Security-Token", token)
		require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
		return mux.SetURLVars(req, map[string]string{"bucket": "bucket", "object": "object"})
	}

	t.Run("authenticates as the static identity", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		r := newSigned(t)

		identity, errCode := iam.reqSignatureV4Verify(r)
		require.Equal(t, s3err.ErrNone, errCode)
		assert.Equal(t, "someone", identity.Name)
		assert.True(t, s3_constants.IsSessionTokenIgnored(r.Context()))
		assert.False(t, hasSessionToken(r), "tolerated token must be inert for authorization")
	})

	t.Run("authorized through the static action path", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		r := newSigned(t)

		_, errCode := iam.authRequest(r, s3_constants.ACTION_READ)
		assert.Equal(t, s3err.ErrNone, errCode)
	})

	t.Run("repeat verification stays idempotent", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		r := newSigned(t)

		_, errCode := iam.reqSignatureV4Verify(r)
		require.Equal(t, s3err.ErrNone, errCode)
		// Handlers re-authenticate the same request (e.g. PutObjectAcl).
		_, errCode = iam.reqSignatureV4Verify(r)
		assert.Equal(t, s3err.ErrNone, errCode)
	})

	t.Run("unsigned token is still rejected", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		r := mustNewSignedRequest(http.MethodGet, "http://127.0.0.1:9000/bucket/object", 0, nil, t)
		r.Header.Set("X-Amz-Security-Token", token)

		_, errCode := iam.reqSignatureV4Verify(r)
		assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	})

	t.Run("unknown access key with token still rejected", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		req := mustNewRequest(http.MethodGet, "http://127.0.0.1:9000/bucket/object", 0, nil, t)
		req.Header.Set("X-Amz-Security-Token", token)
		require.NoError(t, signRequestV4(req, "no_such_key", "secret_key_1"))

		_, errCode := iam.reqSignatureV4Verify(req)
		assert.Equal(t, s3err.ErrInvalidAccessKeyID, errCode)
	})

	t.Run("wrong secret with token still rejected", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		req := mustNewRequest(http.MethodGet, "http://127.0.0.1:9000/bucket/object", 0, nil, t)
		req.Header.Set("X-Amz-Security-Token", token)
		require.NoError(t, signRequestV4(req, "access_key_1", "wrong_secret"))

		_, errCode := iam.reqSignatureV4Verify(req)
		assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	})

	t.Run("presigned URL with token works", func(t *testing.T) {
		iam := newTestIAMWithCreds()
		req := mustNewRequest(http.MethodGet, "http://127.0.0.1:9000/bucket/object", 0, nil, t)
		q := req.URL.Query()
		q.Set("X-Amz-Security-Token", token)
		req.URL.RawQuery = q.Encode()
		require.NoError(t, preSignV4(iam, req, "access_key_1", "secret_key_1", int64((10*time.Minute).Seconds())))

		_, _, errCode := iam.doesPresignedSignatureMatch(req)
		require.Equal(t, s3err.ErrNone, errCode)
		assert.True(t, s3_constants.IsSessionTokenIgnored(req.Context()))
	})
}

func newTestIAMWithCreds() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "someone",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "access_key_1", SecretKey: "secret_key_1"},
				},
				Actions: []string{"Admin", "Read", "Write"},
			},
		},
	})
	return iam
}
