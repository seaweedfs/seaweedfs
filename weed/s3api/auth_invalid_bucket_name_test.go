package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	invalidNameTestAccessKey = "INVALIDNAMEKEY000001"
	invalidNameTestSecretKey = "invalid-name-secret-for-tests-only"
	invalidNameTestBucket    = "readable-bucket"
)

// newInvalidNameIAM has one identity that may read invalidNameTestBucket only
// and a configured anonymous identity with no actions.
func newInvalidNameIAM(t *testing.T) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{hashes: make(map[string]*sync.Pool), hashCounters: make(map[string]*int32)}
	require.NoError(t, iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "reader", Actions: []string{"Read:" + invalidNameTestBucket},
				Credentials: []*iam_pb.Credential{{AccessKey: invalidNameTestAccessKey, SecretKey: invalidNameTestSecretKey}}},
			{Name: s3_constants.AccountAnonymousId},
		},
	}))
	require.True(t, iam.isEnabled())
	return iam
}

func invalidNameRequest(t *testing.T, bucket string, sign bool) *http.Request {
	t.Helper()
	req := mustNewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:8333/%s/key", bucket), 0, nil, t)
	if sign {
		require.NoError(t, signRequestV4(req, invalidNameTestAccessKey, invalidNameTestSecretKey))
	}
	return mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": "key"})
}

func serveAuth(iam *IdentityAccessManagement, r *http.Request) (int, bool) {
	ran := false
	rec := httptest.NewRecorder()
	iam.Auth(func(w http.ResponseWriter, r *http.Request) { ran = true; w.WriteHeader(http.StatusOK) }, s3_constants.ACTION_READ).ServeHTTP(rec, r)
	return rec.Code, ran
}

// A denied request on a malformed bucket name answers 400 InvalidBucketName,
// as AWS S3 does; a denied request on a well-formed name stays 403.
func TestAuthDeniedInvalidBucketName(t *testing.T) {
	iam := newInvalidNameIAM(t)

	code, ran := serveAuth(iam, invalidNameRequest(t, invalidNameTestBucket, true))
	assert.True(t, ran, "the identity's own bucket is allowed")
	assert.Equal(t, http.StatusOK, code)

	code, ran = serveAuth(iam, invalidNameRequest(t, "other-bucket", true))
	assert.False(t, ran)
	assert.Equal(t, http.StatusForbidden, code, "a well-formed name the caller may not read stays AccessDenied")

	for _, bucket := range []string{"Bad_Bucket", "ab", "bucket..name", "bücket"} {
		for _, sign := range []bool{true, false} {
			code, ran = serveAuth(iam, invalidNameRequest(t, bucket, sign))
			assert.False(t, ran, bucket)
			assert.Equal(t, http.StatusBadRequest, code, "bucket %q signed=%v", bucket, sign)
		}
	}
}

func TestRefineDenialForBucketName(t *testing.T) {
	reader := &Identity{Name: "reader"}
	req := func(bucket string) *http.Request {
		return mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/", nil), map[string]string{"bucket": bucket})
	}
	cases := []struct {
		name     string
		bucket   string
		identity *Identity
		in, want s3err.ErrorCode
	}{
		{"space in the name", "bucket name", reader, s3err.ErrAccessDenied, s3err.ErrInvalidBucketName},
		{"non-ASCII letter", "bücket", reader, s3err.ErrAccessDenied, s3err.ErrInvalidBucketName},
		{"superscript digit", "b²cket", reader, s3err.ErrAccessDenied, s3err.ErrInvalidBucketName},
		{"too short", "ab", reader, s3err.ErrAccessDenied, s3err.ErrInvalidBucketName},
		{"well-formed name", "other-bucket", reader, s3err.ErrAccessDenied, s3err.ErrAccessDenied},
		{"no bucket in the request", "", reader, s3err.ErrAccessDenied, s3err.ErrAccessDenied},
		{"allowed", "Bad_Bucket", reader, s3err.ErrNone, s3err.ErrNone},
		{"authentication failed", "Bad_Bucket", nil, s3err.ErrAccessDenied, s3err.ErrAccessDenied},
		{"bad signature", "Bad_Bucket", nil, s3err.ErrSignatureDoesNotMatch, s3err.ErrSignatureDoesNotMatch},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, refineDenialForBucketName(req(c.bucket), c.identity, c.in), c.name)
	}
}
