package s3api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketLifecycleConfigurationHandlerUsesStoredLifecycleConfig(t *testing.T) {
	const bucket = "cleanup-test-net"
	const lifecycleXML = `<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><Filter></Filter><ID>rotation</ID><Expiration><Days>1</Days></Expiration><Status>Enabled</Status></Rule></LifecycleConfiguration>`

	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.option = &S3ApiServerOption{BucketsPath: "/buckets"}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name: bucket,
		Entry: &filer_pb.Entry{
			Extended: map[string][]byte{
				bucketLifecycleConfigurationXMLKey:            []byte(lifecycleXML),
				bucketLifecycleTransitionMinimumObjectSizeKey: []byte("varies_by_storage_class"),
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	resp := httptest.NewRecorder()

	s3a.GetBucketLifecycleConfigurationHandler(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "varies_by_storage_class", resp.Header().Get(bucketLifecycleTransitionMinimumObjectSizeHeader))
	assert.Equal(t, lifecycleXML, resp.Body.String())
}

func TestGetBucketLifecycleConfigurationHandlerDefaultsTransitionMinimumObjectSize(t *testing.T) {
	const bucket = "cleanup-test-net"
	const lifecycleXML = `<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><Filter></Filter><ID>rotation</ID><Expiration><Days>1</Days></Expiration><Status>Enabled</Status></Rule></LifecycleConfiguration>`

	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.option = &S3ApiServerOption{BucketsPath: "/buckets"}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name: bucket,
		Entry: &filer_pb.Entry{
			Extended: map[string][]byte{
				bucketLifecycleConfigurationXMLKey: []byte(lifecycleXML),
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	resp := httptest.NewRecorder()

	s3a.GetBucketLifecycleConfigurationHandler(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, defaultLifecycleTransitionMinimumObjectSize, resp.Header().Get(bucketLifecycleTransitionMinimumObjectSizeHeader))
	assert.Equal(t, lifecycleXML, resp.Body.String())
}

func TestPutBucketLifecycleConfigurationHandlerRejectsOversizedBody(t *testing.T) {
	const bucket = "cleanup-test-net"

	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.option = &S3ApiServerOption{BucketsPath: "/buckets"}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name:  bucket,
		Entry: &filer_pb.Entry{},
	})

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?lifecycle", strings.NewReader(strings.Repeat("x", maxBucketLifecycleConfigurationSize+1)))
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	resp := httptest.NewRecorder()

	s3a.PutBucketLifecycleConfigurationHandler(resp, req)

	require.Equal(t, s3err.GetAPIError(s3err.ErrEntityTooLarge).HTTPStatusCode, resp.Code)
	assert.Contains(t, resp.Body.String(), "<Code>EntityTooLarge</Code>")
}

func TestPutBucketLifecycleConfigurationHandlerMapsReadErrorsToInvalidRequest(t *testing.T) {
	const bucket = "cleanup-test-net"

	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.option = &S3ApiServerOption{BucketsPath: "/buckets"}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name:  bucket,
		Entry: &filer_pb.Entry{},
	})

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?lifecycle", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	req.Body = failingReadCloser{err: errors.New("read failed")}
	resp := httptest.NewRecorder()

	s3a.PutBucketLifecycleConfigurationHandler(resp, req)

	require.Equal(t, s3err.GetAPIError(s3err.ErrInvalidRequest).HTTPStatusCode, resp.Code)
	assert.Contains(t, resp.Body.String(), "<Code>InvalidRequest</Code>")
}

type failingReadCloser struct {
	err error
}

func (f failingReadCloser) Read(_ []byte) (int, error) {
	return 0, f.err
}

func (f failingReadCloser) Close() error {
	return nil
}

// TestShouldSkipTTLFastPathForVersionedBuckets verifies that the TTL
// fast-path (filer.conf TTL entries + updateEntriesTTL) is skipped for
// versioned buckets. On AWS S3, Expiration.Days on a versioned bucket
// creates a delete marker — it does not delete data. TTL volumes would
// destroy all versions indiscriminately, so the lifecycle worker must
// handle versioned buckets at scan time instead. (issue #8757)
func TestShouldSkipTTLFastPathForVersionedBuckets(t *testing.T) {
	tests := []struct {
		name       string
		versioning string
		expectSkip bool
	}{
		{"versioning_enabled", s3_constants.VersioningEnabled, true},
		{"versioning_suspended", s3_constants.VersioningSuspended, true},
		{"no_versioning", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isVersioned := tt.versioning == s3_constants.VersioningEnabled ||
				tt.versioning == s3_constants.VersioningSuspended
			assert.Equal(t, tt.expectSkip, isVersioned)
		})
	}
}
