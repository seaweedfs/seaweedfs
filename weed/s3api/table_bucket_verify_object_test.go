package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

const verifyTestBucket = "tables"

// verifyTestNow is the server clock the tests pin; keys are dated around it.
const verifyTestNow = 1790000000000

func newVerifyTestServer(t *testing.T) *S3ApiServer {
	t.Helper()
	saved := volumeVerifyNow
	volumeVerifyNow = func() time.Time { return time.UnixMilli(verifyTestNow) }
	t.Cleanup(func() { volumeVerifyNow = saved })
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	s3a.bucketRegistry = NewBucketRegistry(s3a)
	s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: verifyTestBucket, IsTableBucket: true})
	s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: "plain", IsTableBucket: false})
	return s3a
}

func TestTableBucketAcceptsExternalVolumeVerifyKey(t *testing.T) {
	s3a := newVerifyTestServer(t)
	for _, key := range []string{
		"verify_1790000000000_1081846483",
		"/verify_1790000000000_1",
		"verify_1790003300000_42", // 55 minutes later: inside the window
		"verify_1789996700000_42", // 55 minutes earlier
	} {
		isVerify, err := s3a.validateTableBucketObjectPathOrVerify(verifyTestBucket, key)
		assert.True(t, isVerify, key)
		assert.NoError(t, err, key)
		// the handlers that keep the plain rule still refuse it
		assert.Error(t, s3a.validateTableBucketObjectPath(verifyTestBucket, key), key)
	}
	// the Iceberg layout is unchanged: the positive control
	isVerify, err := s3a.validateTableBucketObjectPathOrVerify(verifyTestBucket, "ns/t/data/part-00000.parquet")
	assert.False(t, isVerify)
	assert.NoError(t, err)
}

func TestTableBucketRefusesVerifyKeyLookalikes(t *testing.T) {
	s3a := newVerifyTestServer(t)
	for _, key := range []string{
		// the bucket root only
		"base/verify_1790000000000_1",
		"ns/verify_1790000000000_1",
		".uploads/verify_1790000000000_1",
		".uploads/abc/verify_1790000000000_1",
		// the exact shape only
		"verify_1790000000000_1.txt",
		"verify_1790000000000",
		"verify_179000000000_1",            // 12-digit timestamp
		"verify_1790000000000_12345678901", // 11-digit number
		"verify_abc_123",
		"verify_1790000000000_1/x",
		"xverify_1790000000000_1",
		// outside the window
		"verify_1789992000000_1",
		"verify_1790008000000_1",
		// ordinary non-Iceberg keys
		"notes.txt",
		"ns/t/data/notes.txt",
	} {
		isVerify, err := s3a.validateTableBucketObjectPathOrVerify(verifyTestBucket, key)
		assert.False(t, isVerify, key)
		assert.Error(t, err, key)
	}
	// a bucket that is not a table bucket is unaffected
	isVerify, err := s3a.validateTableBucketObjectPathOrVerify("plain", "verify_1790000000000_1")
	assert.False(t, isVerify)
	assert.NoError(t, err)
}

func verifyPutRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, "/"+verifyTestBucket+"/"+key, strings.NewReader("x"))
	return mux.SetURLVars(req, map[string]string{"bucket": verifyTestBucket, "object": key})
}

// A PUT to the verification key cannot store data: a small declared body and
// nothing stored beside it.
func TestTableBucketVerifyKeyPutLimits(t *testing.T) {
	s3a := newVerifyTestServer(t)
	const key = "verify_1790000000000_1081846483"
	for name, mutate := range map[string]func(*http.Request){
		"too large":           func(r *http.Request) { r.ContentLength = volumeVerifyMaxBytes + 1 },
		"undeclared length":   func(r *http.Request) { r.ContentLength = -1 },
		"chunked":             func(r *http.Request) { r.ContentLength = 0; r.TransferEncoding = []string{"chunked"} },
		"user metadata":       func(r *http.Request) { r.ContentLength = 0; r.Header.Set("X-Amz-Meta-Note", "x") },
		"tagging":             func(r *http.Request) { r.ContentLength = 0; r.Header.Set("X-Amz-Tagging", "a=b") },
		"content-disposition": func(r *http.Request) { r.ContentLength = 0; r.Header.Set("Content-Disposition", "attachment") },
		"website redirect":    func(r *http.Request) { r.ContentLength = 0; r.Header.Set("X-Amz-Website-Redirect-Location", "/x") },
	} {
		req := verifyPutRequest(key)
		mutate(req)
		assert.False(t, volumeVerifyPutAllowed(req), name)
		rec := httptest.NewRecorder()
		s3a.PutObjectHandler(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code, name)
	}
	ok := verifyPutRequest(key)
	ok.ContentLength = 21
	ok.Header.Set("Content-Type", "application/octet-stream")
	assert.True(t, volumeVerifyPutAllowed(ok), "the verification object itself")
}
