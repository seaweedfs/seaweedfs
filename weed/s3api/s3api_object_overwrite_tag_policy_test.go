package s3api

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
	completeUpload := func(object, uploadID, etag string) *httptest.ResponseRecorder {
		body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
		req := httptest.NewRequest(http.MethodPost, "/vault/"+object+"?uploadId="+uploadID, strings.NewReader(body))
		rec := httptest.NewRecorder()
		s3a.CompleteMultipartUploadHandler(rec, withIdentity(req, object))
		return rec
	}
	complete := func(object string) *httptest.ResponseRecorder {
		out, code := s3a.createMultipartUpload(withIdentity(httptest.NewRequest(http.MethodPost, "/vault/"+object+"?uploads", nil), object),
			&s3.CreateMultipartUploadInput{Bucket: aws.String("vault"), Key: aws.String(object)})
		require.Equal(t, s3err.ErrNone, code)
		req := httptest.NewRequest(http.MethodPut, "/vault/"+object+"?partNumber=1&uploadId="+*out.UploadId, strings.NewReader("replaced"))
		rec := httptest.NewRecorder()
		s3a.PutObjectPartHandler(rec, withIdentity(req, object))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Len(t, rec.Header()["ETag"], 1)
		return completeUpload(object, *out.UploadId, rec.Header()["ETag"][0])
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
	rec := complete("multipart.txt")
	require.Equal(t, http.StatusOK, rec.Code, "CompleteMultipartUpload of a new key: %s", rec.Body.String())
	filer.mu.Lock()
	require.NotNil(t, filer.entries["/buckets/vault/multipart.txt"])
	filer.mu.Unlock()

	// A key ending in "/" completes onto dir/dir, so that entry's tags decide.
	filer.mu.Lock()
	filer.entries["/buckets/vault/dir/dir"] = taggedEntry("dir", "confidential")
	filer.mu.Unlock()
	rec = complete("dir/")
	require.Equal(t, http.StatusForbidden, rec.Code, "CompleteMultipartUpload of dir/ over a confidential dir/dir: %s", rec.Body.String())
	filer.mu.Lock()
	require.Equal(t, "original", string(filer.entries["/buckets/vault/dir/dir"].Content))
	filer.mu.Unlock()

	// A replayed completion of the upload that wrote the tagged object returns
	// its committed result instead of being refused as an overwrite.
	filer.mu.Lock()
	filer.entries["/buckets/vault/secret.txt"].Extended[s3_constants.SeaweedFSUploadId] = []byte(s3a.generateUploadID("secret.txt"))
	filer.mu.Unlock()
	rec = completeUpload("secret.txt", s3a.generateUploadID("secret.txt"), `"etag"`)
	require.Equal(t, http.StatusOK, rec.Code, "replayed CompleteMultipartUpload: %s", rec.Body.String())

	// A bucket default encryption is not caller input: a policy refusing an
	// explicit SSE header must not block a headerless copy over an object.
	s3a.bucketConfigCache.Set("vault", &BucketConfig{Name: "vault", Encryption: &s3_pb.EncryptionConfiguration{SseAlgorithm: EncryptionTypeAES256}})
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("vault", strings.Replace(existingTagDenyPutPolicy("vault"), `"Condition": {"StringEquals": {"s3:ExistingObjectTag/classification": "confidential"}}`,
		`"Condition": {"Null": {"s3:x-amz-server-side-encryption": "false"}}`, 1)))
	require.NotEqual(t, http.StatusForbidden, copyTo("plain.txt").Code, "headerless CopyObject over an object in a default-encrypted bucket")
}

// A policy stored on the bucket reaches the engine when the bucket loads, so
// the first PutObject after a restart still honors it.
func TestPutObjectOverwriteHonorsExistingObjectTagDenyOnColdBucket(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{
		"/buckets/vault": {
			Name:        "vault",
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
			Extended:    map[string][]byte{BUCKET_POLICY_METADATA_KEY: []byte(existingTagDenyPutPolicy("vault"))},
		},
		"/buckets/vault/secret.txt": taggedEntry("secret.txt", "confidential"),
	}}
	addr := startFakeFiler(t, filer)
	s3a := newPutTestServer(t, addr)
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, addr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{addr}, 1)
	s3a.policyEngine = NewBucketPolicyEngine()
	identity := &Identity{Name: "writer", PrincipalArn: "arn:aws:iam::123456789012:user/writer"}

	r := httptest.NewRequest(http.MethodPut, "/vault/secret.txt", strings.NewReader("replaced"))
	r = mux.SetURLVars(r, map[string]string{"bucket": "vault", "object": "secret.txt"})
	r = r.WithContext(s3_constants.SetIdentityInContext(r.Context(), identity))
	rec := httptest.NewRecorder()
	s3a.PutObjectHandler(rec, r)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	filer.mu.Lock()
	require.Equal(t, "original", string(filer.entries["/buckets/vault/secret.txt"].Content))
	filer.mu.Unlock()
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

// Extraction writes the archive's members, not the request key: the members'
// existing tags decide, and a protected archive name does not block them.
func TestExtractZipHonorsMemberExistingObjectTagDeny(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)
	s3a.policyEngine = NewBucketPolicyEngine()
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", existingTagDenyPutPolicy("b")))
	filerImpl.mu.Lock()
	filerImpl.entries["/buckets/b/dir/archive.zip"] = taggedEntry("archive.zip", "confidential")
	filerImpl.entries["/buckets/b/dir/secret.txt"] = taggedEntry("secret.txt", "confidential")
	filerImpl.mu.Unlock()

	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip?extract=zip", bytes.NewReader(makeZipArchive(t, map[string]string{"a.txt": "hello", "secret.txt": "replaced"})))
	r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/archive.zip"})
	rec := httptest.NewRecorder()
	s3a.PutObjectHandler(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "AccessDenied")
	filerImpl.mu.Lock()
	defer filerImpl.mu.Unlock()
	require.NotNil(t, filerImpl.entries["/buckets/b/dir/a.txt"])
	require.Equal(t, "original", string(filerImpl.entries["/buckets/b/dir/secret.txt"].Content))
	require.Equal(t, "original", string(filerImpl.entries["/buckets/b/dir/archive.zip"].Content))
}

// A copy reads its source after the first destination check, so a tagged
// object stored at the destination meanwhile must still stop the commit.
func TestCopyObjectRechecksDestinationTagsAtCommit(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{
		"/buckets/vault/target.txt": taggedEntry("target.txt", ""),
		"/buckets/vault/source.txt": taggedEntry("source.txt", ""),
	}}
	destinationRead := false
	filer.beforeLookup = func(key string) {
		if key == "/buckets/vault/target.txt" && !destinationRead {
			destinationRead = true
			return
		}
		if destinationRead {
			filer.entries["/buckets/vault/target.txt"] = taggedEntry("target.txt", "confidential")
		}
	}
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

	req := httptest.NewRequest(http.MethodPut, "/vault/target.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/vault/source.txt")
	req = mux.SetURLVars(req, map[string]string{"bucket": "vault", "object": "target.txt"})
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), identity))
	rec := httptest.NewRecorder()
	s3a.CopyObjectHandler(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	filer.mu.Lock()
	defer filer.mu.Unlock()
	require.Equal(t, "confidential", string(filer.entries["/buckets/vault/target.txt"].Extended[s3_constants.AmzObjectTaggingPrefix+"classification"]))
	require.Equal(t, "original", string(filer.entries["/buckets/vault/target.txt"].Content))
}
