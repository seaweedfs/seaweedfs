package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
)

func existingTagDenyPutPolicy(bucket string) string {
	return fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:*",
				"Resource": ["arn:aws:s3:::%[1]s", "arn:aws:s3:::%[1]s/*"]
			},
			{
				"Effect": "Deny",
				"Principal": "*",
				"Action": "s3:PutObject",
				"Resource": "arn:aws:s3:::%[1]s/*",
				"Condition": {"StringEquals": {"s3:ExistingObjectTag/classification": "confidential"}}
			}
		]
	}`, bucket)
}

func taggedEntry(name, classification string) *filer_pb.Entry {
	entry := &filer_pb.Entry{
		Name:       name,
		Content:    []byte("original"),
		Attributes: &filer_pb.FuseAttributes{FileSize: 8, Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{},
	}
	if classification != "" {
		entry.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte(classification)
	}
	return entry
}

// Overwrites are writes to an existing object, so a Deny on s3:PutObject
// conditioned on the object's current tags applies to them the way it does to
// DeleteObject. New keys and untagged objects stay writable.
func TestOverwriteHonorsExistingObjectTagDeny(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{
		"/buckets/vault/secret.txt": taggedEntry("secret.txt", "confidential"),
		"/buckets/vault/plain.txt":  taggedEntry("plain.txt", ""),
		"/buckets/vault/public.txt": taggedEntry("public.txt", "public"),
		"/buckets/vault/source.txt": taggedEntry("source.txt", ""),
	}}
	addr := startFakeFiler(t, filer)
	s3a := newPutTestServer(t, addr)
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("vault", &BucketConfig{Name: "vault"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, addr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{addr}, 1)
	s3a.policyEngine = NewBucketPolicyEngine()
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("vault", existingTagDenyPutPolicy("vault")))
	identity := &Identity{Name: "writer", PrincipalArn: "arn:aws:iam::123456789012:user/writer"}

	withIdentity := func(r *http.Request, object string) *http.Request {
		r = mux.SetURLVars(r, map[string]string{"bucket": "vault", "object": object})
		return r.WithContext(s3_constants.SetIdentityInContext(r.Context(), identity))
	}
	put := func(object string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s3a.PutObjectHandler(rec, withIdentity(httptest.NewRequest(http.MethodPut, "/vault/"+object, strings.NewReader("replaced")), object))
		return rec
	}
	copyTo := func(object string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/vault/"+object, nil)
		req.Header.Set("X-Amz-Copy-Source", "/vault/source.txt")
		rec := httptest.NewRecorder()
		s3a.CopyObjectHandler(rec, withIdentity(req, object))
		return rec
	}
	complete := func(object string) *httptest.ResponseRecorder {
		uploadID := s3a.generateUploadID(object)
		body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"etag"</ETag></Part></CompleteMultipartUpload>`
		req := httptest.NewRequest(http.MethodPost, "/vault/"+object+"?uploadId="+uploadID, strings.NewReader(body))
		rec := httptest.NewRecorder()
		s3a.CompleteMultipartUploadHandler(rec, withIdentity(req, object))
		return rec
	}

	for name, write := range map[string]func(string) *httptest.ResponseRecorder{"PutObject": put, "CopyObject": copyTo, "CompleteMultipartUpload": complete} {
		rec := write("secret.txt")
		require.Equal(t, http.StatusForbidden, rec.Code, "%s over a confidential object: %s", name, rec.Body.String())
		require.Contains(t, rec.Body.String(), "AccessDenied", name)
		filer.mu.Lock()
		require.Equal(t, "original", string(filer.entries["/buckets/vault/secret.txt"].Content), name)
		filer.mu.Unlock()
	}

	for _, object := range []string{"plain.txt", "public.txt", "fresh.txt"} {
		require.Equal(t, http.StatusOK, put(object).Code, "PutObject %s", object)
		require.Equal(t, http.StatusOK, copyTo(object).Code, "CopyObject %s", object)
	}
	require.NotEqual(t, http.StatusForbidden, complete("fresh.txt").Code, "CompleteMultipartUpload of a new key")

	// A bucket default encryption is not caller input: a policy refusing an
	// explicit SSE header must not block a headerless copy over an object.
	s3a.bucketConfigCache.Set("vault", &BucketConfig{Name: "vault", Encryption: &s3_pb.EncryptionConfiguration{SseAlgorithm: EncryptionTypeAES256}})
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("vault", strings.Replace(existingTagDenyPutPolicy("vault"), `"Condition": {"StringEquals": {"s3:ExistingObjectTag/classification": "confidential"}}`,
		`"Condition": {"Null": {"s3:x-amz-server-side-encryption": "false"}}`, 1)))
	require.NotEqual(t, http.StatusForbidden, copyTo("plain.txt").Code, "headerless CopyObject over an object in a default-encrypted bucket")
}

func TestPostObjectOverwriteHonorsExistingObjectTagDeny(t *testing.T) {
	s3a, filer := newPostAclWriterServer(t)
	s3a.bucketConfigCache.Set(postAuthzBucket, &BucketConfig{Name: postAuthzBucket})
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy(postAuthzBucket, existingTagDenyPutPolicy(postAuthzBucket)))
	filer.mu.Lock()
	filer.entries["/buckets/"+postAuthzBucket+"/secret.txt"] = taggedEntry("secret.txt", "confidential")
	filer.mu.Unlock()

	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequest(t, "secret.txt"))
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "AccessDenied")
	filer.mu.Lock()
	require.Equal(t, "original", string(filer.entries["/buckets/"+postAuthzBucket+"/secret.txt"].Content))
	filer.mu.Unlock()

	rec = httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, newSignedPostRequest(t, "fresh.txt"))
	require.Less(t, rec.Code, 300, rec.Body.String())
}
