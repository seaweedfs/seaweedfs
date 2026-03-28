package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
