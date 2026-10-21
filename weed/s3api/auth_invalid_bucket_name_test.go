package s3api

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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

// signedPostForm builds a multipart POST Object request for bucket, its policy
// signed with secret under postAuthzAccessKey.
func signedPostForm(t *testing.T, bucket, key, secret string) *http.Request {
	t.Helper()
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	day := now.Format(yyyymmdd)
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", postAuthzAccessKey, day, postAuthzRegion, postAuthzService)
	policyJSON := fmt.Sprintf(`{"expiration":"%s","conditions":[["eq","$bucket","%s"],["eq","$key","%s"],`+
		`["eq","$x-amz-credential","%s"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-date","%s"]]}`,
		now.Add(time.Hour).Format("2006-01-02T15:04:05.000Z"), bucket, key, credential, amzDate)
	encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policyJSON))
	signature := getSignature(getSigningKey(secret, day, postAuthzRegion, postAuthzService), encodedPolicy)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	for _, field := range [][2]string{{"bucket", bucket}, {"key", key}, {"x-amz-credential", credential},
		{"x-amz-algorithm", "AWS4-HMAC-SHA256"}, {"x-amz-date", amzDate}, {"policy", encodedPolicy}, {"x-amz-signature", signature}} {
		require.NoError(t, writer.WriteField(field[0], field[1]))
	}
	filePart, err := writer.CreateFormFile("file", "payload.txt")
	require.NoError(t, err)
	_, err = filePart.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/"+bucket, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return mux.SetURLVars(req, map[string]string{"bucket": bucket})
}

// A browser POST upload authenticates in the handler, after AuthPostPolicy has
// delegated it, so its denials are refined there: a verified form denied on a
// malformed bucket name answers 400, a bad signature or a well-formed name 403.
func TestPostPolicyDeniedInvalidBucketName(t *testing.T) {
	s3a := newPostAuthzServer(t)
	for _, bucket := range []string{"Bad_Bucket", "other-bucket"} {
		s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: bucket})
	}
	// A second identity may write Bad_Bucket, except under denied/.
	require.NoError(t, s3a.iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "tester", Actions: []string{"Write:" + postAuthzBucket},
				Credentials: []*iam_pb.Credential{{AccessKey: postAuthzAccessKey, SecretKey: postAuthzSecretKey}}},
		},
	}))
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("Bad_Bucket", `{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Deny","Principal":"*","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::Bad_Bucket/denied/*"]}]}`))

	serve := func(r *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s3a.iam.AuthPostPolicy(s3a.PostPolicyBucketHandler, s3_constants.ACTION_WRITE).ServeHTTP(rec, r)
		return rec
	}

	rec := serve(signedPostForm(t, "Bad_Bucket", "k", postAuthzSecretKey))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "InvalidBucketName")

	rec = serve(signedPostForm(t, "Bad_Bucket", "k", "not-the-secret"))
	assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "SignatureDoesNotMatch", "a form that fails verification is not refined")

	rec = serve(signedPostForm(t, "other-bucket", "k", postAuthzSecretKey))
	assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AccessDenied")

	// The per-key authorization after the coarse Write check is refined too.
	require.NoError(t, s3a.iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{Name: "tester", Actions: []string{"Write:Bad_Bucket"},
				Credentials: []*iam_pb.Credential{{AccessKey: postAuthzAccessKey, SecretKey: postAuthzSecretKey}}},
		},
	}))
	rec = serve(signedPostForm(t, "Bad_Bucket", "denied/k", postAuthzSecretKey))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "InvalidBucketName")
}

// Unsigned GET/HEAD object reads are authorized inside the object handlers, not
// by Auth; their denial on a malformed bucket name answers 400 as well.
func TestAnonymousReadDeniedInvalidBucketName(t *testing.T) {
	read := func(s3a *S3ApiServer, method, bucket string) int {
		r := httptest.NewRequest(method, "/"+bucket+"/key", nil)
		r = mux.SetURLVars(r, map[string]string{"bucket": bucket, "object": "key"})
		handler := s3a.GetObjectHandler
		if method == http.MethodHead {
			handler = s3a.HeadObjectHandler
		}
		rr := httptest.NewRecorder()
		s3a.AuthWithPublicRead(handler, s3_constants.ACTION_READ)(rr, r)
		return rr.Code
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		s3a, _ := newAnonymousReadTestServer(t)
		s3a.iam.identityAnonymous = &Identity{Name: s3_constants.AccountAnonymousId}
		assert.Equal(t, http.StatusBadRequest, read(s3a, method, "Bad_Bucket"), method)
		assert.Equal(t, http.StatusForbidden, read(s3a, method, "other-bucket"), "%s: a well-formed missing bucket stays masked", method)

		// No anonymous identity configured: no identity resolves, as in Auth.
		s3a, _ = newAnonymousReadTestServer(t)
		assert.Equal(t, http.StatusForbidden, read(s3a, method, "Bad_Bucket"), method)
	}
}

// An existing private key and a missing key in the same malformed-name bucket
// must answer alike, or the status code tells an anonymous caller which keys
// exist.
func TestAnonymousReadDeniedInvalidBucketNameExistingKey(t *testing.T) {
	const bucket = "bücket"
	read := func(s3a *S3ApiServer, method, object string) int {
		r := httptest.NewRequest(method, "/"+bucket+"/"+object, nil)
		r = mux.SetURLVars(r, map[string]string{"bucket": bucket, "object": object})
		handler := s3a.GetObjectHandler
		if method == http.MethodHead {
			handler = s3a.HeadObjectHandler
		}
		rr := httptest.NewRecorder()
		s3a.AuthWithPublicRead(handler, s3_constants.ACTION_READ)(rr, r)
		return rr.Code
	}
	setup := func(anonymous bool) *S3ApiServer {
		s3a, f := newAnonymousReadTestServer(t)
		if anonymous {
			s3a.iam.identityAnonymous = &Identity{Name: s3_constants.AccountAnonymousId}
		}
		s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Ownership: s3_constants.OwnershipObjectWriter})
		f.entries["/buckets/"+bucket+"/private"] = anonymousReadEntry([]byte(`[]`))
		f.entries["/buckets/"+bucket+"/public"] = anonymousReadEntry(anonymousReadGrant("READ"))
		return s3a
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		s3a := setup(true)
		assert.Equal(t, http.StatusOK, read(s3a, method, "public"), "%s: a public key is still served", method)
		assert.Equal(t, http.StatusBadRequest, read(s3a, method, "missing"), method)
		assert.Equal(t, http.StatusBadRequest, read(s3a, method, "private"), "%s: an existing private key answers like a missing one", method)

		// No anonymous identity configured: both stay 403, as in Auth.
		s3a = setup(false)
		assert.Equal(t, http.StatusForbidden, read(s3a, method, "missing"), method)
		assert.Equal(t, http.StatusForbidden, read(s3a, method, "private"), method)
	}
}
