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
	postAuthzAccessKey = "AKIATESTTESTTEST"
	postAuthzSecretKey = "secret-key-for-tests"
	postAuthzRegion    = "us-east-1"
	postAuthzService   = "s3"
	postAuthzBucket    = "testbucket"
	postAuthzPrincipal = "arn:aws:iam::000000000000:user/tester"
)

// newPostAuthzServer builds an S3ApiServer whose non-admin "tester" identity
// holds the coarse Write action on the bucket and whose bucket policy denies it
// s3:PutObject under denied/: a writer confined by a bucket policy to part of a
// shared bucket.
func newPostAuthzServer(t *testing.T) *S3ApiServer {
	t.Helper()

	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "tester",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "tester"},
			Credentials: []*iam_pb.Credential{{AccessKey: postAuthzAccessKey, SecretKey: postAuthzSecretKey}},
			Actions:     []string{"Read:" + postAuthzBucket, "Write:" + postAuthzBucket, "List:" + postAuthzBucket},
		}},
	})
	require.NoError(t, err)

	policyEngine := NewBucketPolicyEngine()
	err = policyEngine.engine.SetBucketPolicy(postAuthzBucket, fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "DenyTesterDeniedPrefix",
			"Effect": "Deny",
			"Principal": {"AWS": "%s"},
			"Action": ["s3:PutObject"],
			"Resource": ["arn:aws:s3:::%s/denied/*"]
		}]
	}`, postAuthzPrincipal, postAuthzBucket))
	require.NoError(t, err)
	iam.policyEngine = policyEngine

	s3a := &S3ApiServer{
		option:       &S3ApiServerOption{BucketsPath: "/buckets"},
		iam:          iam,
		policyEngine: policyEngine,
	}
	// Pre-populate the bucket registry so validateTableBucketObjectPath sees a
	// non-table bucket without needing a live filer connection.
	s3a.bucketRegistry = NewBucketRegistry(s3a)
	s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: postAuthzBucket, IsTableBucket: false})
	return s3a
}

// newSignedPostRequest builds a signed multipart POST Object request whose POST
// policy conditions are satisfied by the form, so the handler passes signature
// and CheckPostPolicy verification and reaches the write-authorization stage.
func newSignedPostRequest(t *testing.T, key string) *http.Request {
	t.Helper()

	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	yyyymmddStr := now.Format(yyyymmdd)
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", postAuthzAccessKey, yyyymmddStr, postAuthzRegion, postAuthzService)
	expiration := now.Add(1 * time.Hour).Format("2006-01-02T15:04:05.000Z")

	policyJSON := fmt.Sprintf(
		`{"expiration":"%s","conditions":[`+
			`["eq","$bucket","%s"],`+
			`["eq","$key","%s"],`+
			`["eq","$x-amz-credential","%s"],`+
			`["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],`+
			`["eq","$x-amz-date","%s"]`+
			`]}`,
		expiration, postAuthzBucket, key, credential, amzDate,
	)
	encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policyJSON))
	signature := getSignature(getSigningKey(postAuthzSecretKey, yyyymmddStr, postAuthzRegion, postAuthzService), encodedPolicy)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	require.NoError(t, writer.WriteField("bucket", postAuthzBucket))
	require.NoError(t, writer.WriteField("key", key))
	require.NoError(t, writer.WriteField("x-amz-credential", credential))
	require.NoError(t, writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256"))
	require.NoError(t, writer.WriteField("x-amz-date", amzDate))
	require.NoError(t, writer.WriteField("policy", encodedPolicy))
	require.NoError(t, writer.WriteField("x-amz-signature", signature))
	filePart, err := writer.CreateFormFile("file", "payload.txt")
	require.NoError(t, err)
	_, err = filePart.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return mux.SetURLVars(req, map[string]string{"bucket": postAuthzBucket})
}

// TestPostPolicyBucketHandlerHonorsBucketPolicyDeny drives a signed POST Object
// upload whose key a bucket policy denies the caller, and asserts the handler
// rejects it with 403 -- the same result the equivalent PUT gives, now that POST
// runs the shared per-key authorization instead of only the coarse Write check.
func TestPostPolicyBucketHandlerHonorsBucketPolicyDeny(t *testing.T) {
	s3a := newPostAuthzServer(t)

	// Precondition: the policy really denies this principal the denied/ key, so a
	// non-403 below is the POST path not consulting it, not a mis-configured policy.
	putReq := httptest.NewRequest(http.MethodPut, "/"+postAuthzBucket+"/denied/secret.txt", nil)
	allowed, evaluated, err := s3a.iam.policyEngine.EvaluatePolicy(
		postAuthzBucket, "denied/secret.txt", s3_constants.ACTION_WRITE, postAuthzPrincipal, putReq, nil, nil)
	require.NoError(t, err)
	require.True(t, evaluated, "policy must match the denied key for the tester principal")
	require.False(t, allowed, "policy must deny the tester principal on the denied key")

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequest(t, "denied/secret.txt"))

	assert.Equal(t, http.StatusForbidden, rec.Code,
		"POST Object into a bucket-policy-denied key must be rejected, body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AccessDenied",
		"response should identify AccessDenied, body: %s", rec.Body.String())
}

// TestAuthorizeObjectWrite covers the write-authorization decision the POST
// handler defers to: a bucket-policy Deny blocks the key, a key the policy does
// not deny clears (the coarse Write action still grants it), and admins are
// unaffected. Driving the full handler for the allowed case is not hermetic --
// past authorization it reaches the lifecycle/write path that needs a filer --
// so the counter-cases are checked at the authorization boundary directly.
func TestAuthorizeObjectWrite(t *testing.T) {
	s3a := newPostAuthzServer(t)
	tester, _, found := s3a.iam.lookupByAccessKey(postAuthzAccessKey)
	require.True(t, found)

	req := httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, nil)

	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.AuthorizeObjectWrite(req, tester, postAuthzBucket, "denied/secret.txt"),
		"a bucket-policy-denied key must be denied")
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.AuthorizeObjectWrite(req, tester, postAuthzBucket, "allowed/report.txt"),
		"a key the policy does not deny must clear on the coarse Write grant")

	admin := &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.AuthorizeObjectWrite(req, admin, postAuthzBucket, "denied/secret.txt"),
		"an admin is not confined by the bucket policy")
}
