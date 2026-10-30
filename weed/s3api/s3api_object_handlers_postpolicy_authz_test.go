package s3api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

	postAclAccessKey = "AKIAACLTESTTEST"
	postAclSecretKey = "secret-key-for-acl-tests"
	postAclPrincipal = "arn:aws:iam::000000000000:user/aclwriter"

	postPolicyAccessKey = "AKIAPOLICYONLY0000"
	postPolicySecretKey = "secret-key-for-policy-only"
	postReadAccessKey   = "AKIAPOLICYREAD0000"
	postReadSecretKey   = "secret-key-for-policy-read"

	postAdminAccessKey = "AKIAADMINTEST000"
	postAdminSecretKey = "secret-key-for-admin-tests"
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
		}, {
			Name:        "aclwriter",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "aclwriter"},
			Credentials: []*iam_pb.Credential{{AccessKey: postAclAccessKey, SecretKey: postAclSecretKey}},
			Actions:     []string{"Read:" + postAuthzBucket, "Write:" + postAuthzBucket, "WriteAcp:" + postAuthzBucket},
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
		}, {
			"Sid": "DenyAclWriterAclPrefix",
			"Effect": "Deny",
			"Principal": {"AWS": "%s"},
			"Action": ["s3:PutObjectAcl"],
			"Resource": ["arn:aws:s3:::%s/acl-denied/*"]
		}]
	}`, postAuthzPrincipal, postAuthzBucket, postAclPrincipal, postAuthzBucket))
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
	return newSignedPostRequestFor(t, key, postAuthzAccessKey, postAuthzSecretKey, nil)
}

// extraFields are added to both the form and the policy conditions so
// CheckPostPolicy accepts them.
func newSignedPostRequestFor(t *testing.T, key, accessKey, secretKey string, extraFields map[string]string) *http.Request {
	t.Helper()

	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	yyyymmddStr := now.Format(yyyymmdd)
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", accessKey, yyyymmddStr, postAuthzRegion, postAuthzService)
	expiration := now.Add(1 * time.Hour).Format("2006-01-02T15:04:05.000Z")

	extraKeys := make([]string, 0, len(extraFields))
	for k := range extraFields {
		extraKeys = append(extraKeys, k)
	}
	sort.Strings(extraKeys)
	conditions := []any{
		[]any{"eq", "$bucket", postAuthzBucket},
		[]any{"eq", "$key", key},
		[]any{"eq", "$x-amz-credential", credential},
		[]any{"eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"},
		[]any{"eq", "$x-amz-date", amzDate},
	}
	for _, k := range extraKeys {
		conditions = append(conditions, []any{"eq", "$" + k, extraFields[k]})
	}
	policyJSON, err := json.Marshal(map[string]any{
		"expiration": expiration,
		"conditions": conditions,
	})
	require.NoError(t, err)
	encodedPolicy := base64.StdEncoding.EncodeToString(policyJSON)
	signature := getSignature(getSigningKey(secretKey, yyyymmddStr, postAuthzRegion, postAuthzService), encodedPolicy)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	require.NoError(t, writer.WriteField("bucket", postAuthzBucket))
	require.NoError(t, writer.WriteField("key", key))
	require.NoError(t, writer.WriteField("x-amz-credential", credential))
	require.NoError(t, writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256"))
	require.NoError(t, writer.WriteField("x-amz-date", amzDate))
	for _, k := range extraKeys {
		require.NoError(t, writer.WriteField(k, extraFields[k]))
	}
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

// TestPostPolicyBucketHandlerGatesAclFields drives signed POST uploads carrying
// an acl form field and asserts the handler rejects them the way the PUT path
// does: a writer without the WriteAcp action is refused, and so is a writer the
// bucket policy denies s3:PutObjectAcl on the target key.
func TestPostPolicyBucketHandlerGatesAclFields(t *testing.T) {
	s3a := newPostAuthzServer(t)

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "allowed/report.txt",
		postAuthzAccessKey, postAuthzSecretKey, map[string]string{"acl": "public-read"}))
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"POST with an acl field needs WriteAcp like PUT, body: %s", rec.Body.String())

	rec = httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "acl-denied/secret.txt",
		postAclAccessKey, postAclSecretKey, map[string]string{"acl": "public-read"}))
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"POST with an acl field must honor the bucket-policy PutObjectAcl deny, body: %s", rec.Body.String())

	for _, field := range []string{"x-amz-grant-read", "x-amz-grant-write", "x-amz-grant-read-acp", "x-amz-grant-write-acp", "x-amz-grant-full-control"} {
		rec = httptest.NewRecorder()
		s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "allowed/report.txt",
			postAuthzAccessKey, postAuthzSecretKey,
			map[string]string{field: `id="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"`}))
		assert.Equal(t, http.StatusForbidden, rec.Code,
			"POST with an %s field needs WriteAcp like PUT, body: %s", field, rec.Body.String())
	}
}

func TestPostRequestsAcl(t *testing.T) {
	for _, header := range []string{
		s3_constants.AmzCannedAcl, s3_constants.AmzAclFullControl, s3_constants.AmzAclRead,
		s3_constants.AmzAclReadAcp, s3_constants.AmzAclWrite, s3_constants.AmzAclWriteAcp,
	} {
		req := httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, nil)
		req.Header.Set(header, "x")
		assert.True(t, postRequestsAcl(req), "%s must trip the ACL gate", header)
	}
	assert.False(t, postRequestsAcl(httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, nil)))
}

// TestAuthorizeObjectAclWrite covers the acl authorization decision at the
// boundary, for the same reason as TestAuthorizeObjectWrite.
func TestAuthorizeObjectAclWrite(t *testing.T) {
	s3a := newPostAuthzServer(t)
	tester, _, found := s3a.iam.lookupByAccessKey(postAuthzAccessKey)
	require.True(t, found)
	aclwriter, _, found := s3a.iam.lookupByAccessKey(postAclAccessKey)
	require.True(t, found)

	req := httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, nil)

	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.AuthorizeObjectAclWrite(req, tester, postAuthzBucket, "allowed/report.txt"),
		"a writer without the WriteAcp action cannot attach an ACL")
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.AuthorizeObjectAclWrite(req, aclwriter, postAuthzBucket, "allowed/report.txt"),
		"the WriteAcp holder clears keys the policy does not deny")
	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.AuthorizeObjectAclWrite(req, aclwriter, postAuthzBucket, "acl-denied/secret.txt"),
		"a bucket-policy PutObjectAcl deny must block the ACL-bearing upload")

	admin := &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}
	assert.Equal(t, s3err.ErrNone,
		s3a.iam.AuthorizeObjectAclWrite(req, admin, postAuthzBucket, "acl-denied/secret.txt"),
		"an admin is not confined by the bucket policy")
}

// newPostPolicyOnlyServer builds an S3ApiServer with identities that hold no
// legacy Write action: "poster" carries an attached IAM policy allowing
// s3:PutObject on the test bucket, and "reader" one allowing only s3:GetObject.
func newPostPolicyOnlyServer(t *testing.T) *S3ApiServer {
	t.Helper()

	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "poster",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "poster"},
			Credentials: []*iam_pb.Credential{{AccessKey: postPolicyAccessKey, SecretKey: postPolicySecretKey}},
			Actions:     []string{"List:" + postAuthzBucket},
			PolicyNames: []string{"post-write"},
		}, {
			Name:        "reader",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "reader"},
			Credentials: []*iam_pb.Credential{{AccessKey: postReadAccessKey, SecretKey: postReadSecretKey}},
			Actions:     []string{"Read:" + postAuthzBucket, "List:" + postAuthzBucket},
			PolicyNames: []string{"post-read"},
		}},
		Policies: []*iam_pb.Policy{{
			Name: "post-write",
			Content: fmt.Sprintf(`{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
					"Resource": ["arn:aws:s3:::%s/*"]
				}, {
					"Effect": "Allow",
					"Action": ["s3:ListBucket", "s3:GetBucketLocation"],
					"Resource": ["arn:aws:s3:::%s"]
				}]
			}`, postAuthzBucket, postAuthzBucket),
		}, {
			Name: "post-read",
			Content: fmt.Sprintf(`{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": ["s3:GetObject"],
					"Resource": ["arn:aws:s3:::%s/*"]
				}]
			}`, postAuthzBucket),
		}},
	})
	require.NoError(t, err)

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: "/buckets"},
		iam:    iam,
	}
	s3a.bucketRegistry = NewBucketRegistry(s3a)
	s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: postAuthzBucket, IsTableBucket: false})
	return s3a
}

// postFormValues canonicalizes the signed request's multipart fields the way
// extractPostPolicyFormValues does, for driving doesPolicySignatureMatch.
func postFormValues(t *testing.T, r *http.Request) http.Header {
	t.Helper()
	require.NoError(t, r.ParseMultipartForm(1<<20))
	formValues := make(http.Header)
	for k, v := range r.MultipartForm.Value {
		formValues[http.CanonicalHeaderKey(k)] = v
	}
	return formValues
}

// TestPostPolicySignatureAllowsAttachedPolicyIdentity signs a POST Object form
// with a key whose holder has no legacy Write action - only an attached IAM
// policy granting s3:PutObject - and asserts the signature check accepts it
// and the shared object-write authorization allows the key. Gating the
// signature check on the Actions projection used to fail this with
// AccessDenied before authorization ever ran.
func TestPostPolicySignatureAllowsAttachedPolicyIdentity(t *testing.T) {
	s3a := newPostPolicyOnlyServer(t)

	req := newSignedPostRequestFor(t, "uploads/pic.png", postPolicyAccessKey, postPolicySecretKey, nil)
	identity, errCode := s3a.iam.doesPolicySignatureMatch(req.Context(), postFormValues(t, req))
	require.Equal(t, s3err.ErrNone, errCode,
		"a verified POST signature must not require the legacy Write action")
	require.NotNil(t, identity)

	assert.Equal(t, s3err.ErrNone,
		s3a.iam.AuthorizeObjectWrite(req, identity, postAuthzBucket, "uploads/pic.png"),
		"an attached IAM policy granting s3:PutObject must authorize the key like the PUT path")
}

// TestPostPolicyAttachedPolicyDeny covers the deny leg: a verified identity
// whose attached policy does not grant s3:PutObject is still refused by
// AuthorizeObjectWrite and by the full handler.
func TestPostPolicyAttachedPolicyDeny(t *testing.T) {
	s3a := newPostPolicyOnlyServer(t)

	req := newSignedPostRequestFor(t, "uploads/pic.png", postReadAccessKey, postReadSecretKey, nil)
	identity, errCode := s3a.iam.doesPolicySignatureMatch(req.Context(), postFormValues(t, req))
	require.Equal(t, s3err.ErrNone, errCode)
	require.NotNil(t, identity)

	assert.Equal(t, s3err.ErrAccessDenied,
		s3a.iam.AuthorizeObjectWrite(req, identity, postAuthzBucket, "uploads/pic.png"),
		"an attached policy without s3:PutObject must still deny the upload")

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "uploads/pic.png", postReadAccessKey, postReadSecretKey, nil))
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"POST from a read-only identity must stay AccessDenied, body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AccessDenied")
}

// postAutoCreateFiler stores every created entry and serves directory lookups
// from that store, so the POST upload's lazy bucket creation can be observed
// without a running filer. Every other RPC stays Unimplemented and fails.
type postAutoCreateFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
	created []string
}

func (f *postAutoCreateFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *postAutoCreateFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, req.Directory+"/"+req.Entry.Name)
	f.entries[req.Directory+"/"+req.Entry.Name] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

// newPostAutoCreateServer builds an S3ApiServer on the fake filer with
// auto-create enabled, an "admin" identity allowed to create buckets, and the
// non-admin "tester" writer.
func newPostAutoCreateServer(t *testing.T, filer *postAutoCreateFiler) *S3ApiServer {
	t.Helper()

	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "admin",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "admin"},
			Credentials: []*iam_pb.Credential{{AccessKey: postAdminAccessKey, SecretKey: postAdminSecretKey}},
			Actions:     []string{"Admin"},
		}, {
			Name:        "tester",
			Account:     &iam_pb.Account{Id: "000000000000", DisplayName: "tester"},
			Credentials: []*iam_pb.Credential{{AccessKey: postAuthzAccessKey, SecretKey: postAuthzSecretKey}},
			Actions:     []string{"Read:" + postAuthzBucket, "Write:" + postAuthzBucket, "List:" + postAuthzBucket},
		}},
	})
	require.NoError(t, err)

	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.iam = iam
	s3a.option.BucketsPath = "/buckets"
	s3a.option.AutoCreateBucket = true
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	return s3a
}

// A signed POST Object upload to a bucket that does not exist must lazy-create
// it the way PutObject does: an admin signer is allowed to create the bucket
// and the upload proceeds against it instead of answering NoSuchBucket.
func TestPostPolicyBucketHandlerAutoCreatesMissingBucket(t *testing.T) {
	filer := &postAutoCreateFiler{entries: map[string]*filer_pb.Entry{}}
	s3a := newPostAutoCreateServer(t, filer)

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "uploads/pic.png", postAdminAccessKey, postAdminSecretKey, nil))

	filer.mu.Lock()
	defer filer.mu.Unlock()
	assert.Contains(t, filer.created, "/buckets/"+postAuthzBucket,
		"POST to a missing bucket must create it like PUT, got status %d body %s", rec.Code, rec.Body.String())
	assert.NotEqual(t, http.StatusNotFound, rec.Code,
		"missing-bucket POST must no longer be a 404, body: %s", rec.Body.String())
}

// A non-admin signer keeps the PUT semantics: the upload itself is authorized
// but the lazy create is refused, surfacing AccessDenied rather than creating
// the bucket or answering NoSuchBucket.
func TestPostPolicyBucketHandlerAutoCreateDeniedForNonAdmin(t *testing.T) {
	filer := &postAutoCreateFiler{entries: map[string]*filer_pb.Entry{}}
	s3a := newPostAutoCreateServer(t, filer)

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequestFor(t, "uploads/pic.png", postAuthzAccessKey, postAuthzSecretKey, nil))

	assert.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AccessDenied")

	filer.mu.Lock()
	defer filer.mu.Unlock()
	assert.Empty(t, filer.created, "a non-admin POST must not create the bucket")
}
