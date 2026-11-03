package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

const testPublicAccessBlockXML = `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
	`<BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls>` +
	`<BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets>` +
	`</PublicAccessBlockConfiguration>`

func newPublicAccessBlockTestServer(t *testing.T, entry *filer_pb.Entry) (*S3ApiServer, *fakeLookupFiler) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	filer := &fakeLookupFiler{entry: entry}
	s3a := newHeadBucketTestServer(t, filer)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
	t.Cleanup(s3a.iam.Shutdown)
	return s3a, filer
}

func publicAccessBlockRequest(method, bucket, body string) *http.Request {
	req := httptest.NewRequest(method, "http://s3/"+bucket+"?publicAccessBlock", strings.NewReader(body))
	return mux.SetURLVars(req, map[string]string{"bucket": bucket})
}

func TestPutPublicAccessBlockStoresConfiguration(t *testing.T) {
	const bucket = "pab-put"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	rr := httptest.NewRecorder()
	s3a.PutPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodPut, bucket, testPublicAccessBlockXML))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	require.Len(t, filer.txnSeen, 1)
	stored := filer.txnSeen[0].Mutations[0].SetExtended[s3_constants.ExtPublicAccessBlockKey]
	config, err := parsePublicAccessBlock(stored)
	require.NoError(t, err)
	require.Equal(t, PublicAccessBlockConfiguration{
		XMLName:               config.XMLName,
		BlockPublicAcls:       true,
		IgnorePublicAcls:      true,
		RestrictPublicBuckets: true,
	}, *config)
}

func TestPutPublicAccessBlockRejectsMalformedXML(t *testing.T) {
	const bucket = "pab-malformed"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	for _, body := range []string{"", "<PublicAccessBlockConfiguration><BlockPublicAcls>yes</BlockPublicAcls></PublicAccessBlockConfiguration>"} {
		rr := httptest.NewRecorder()
		s3a.PutPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodPut, bucket, body))
		require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
		require.Contains(t, rr.Body.String(), "MalformedXML")
	}
	require.Empty(t, filer.txnSeen)
}

func TestGetPublicAccessBlock(t *testing.T) {
	const bucket = "pab-get"
	s3a, _ := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	rr := httptest.NewRecorder()
	s3a.GetPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodGet, bucket, ""))
	require.Equal(t, http.StatusNotFound, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "NoSuchPublicAccessBlockConfiguration")

	s3a, _ = newPublicAccessBlockTestServer(t, &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: []byte(testPublicAccessBlockXML)},
	})
	rr = httptest.NewRecorder()
	s3a.GetPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodGet, bucket, ""))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	body := rr.Body.String()
	require.Contains(t, body, `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	require.Contains(t, body, "<BlockPublicAcls>true</BlockPublicAcls>")
	require.Contains(t, body, "<IgnorePublicAcls>true</IgnorePublicAcls>")
	require.Contains(t, body, "<BlockPublicPolicy>false</BlockPublicPolicy>")
	require.Contains(t, body, "<RestrictPublicBuckets>true</RestrictPublicBuckets>")
}

func TestDeletePublicAccessBlockRemovesConfiguration(t *testing.T) {
	const bucket = "pab-delete"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: []byte(testPublicAccessBlockXML)},
	})

	rr := httptest.NewRecorder()
	s3a.DeletePublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodDelete, bucket, ""))
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())
	require.Len(t, filer.txnSeen, 1)
	require.Contains(t, filer.txnSeen[0].Mutations[0].DeleteExtended, s3_constants.ExtPublicAccessBlockKey)
}

func TestPublicAccessBlockMissingBucket(t *testing.T) {
	s3a, _ := newPublicAccessBlockTestServer(t, nil)
	for _, tc := range []struct {
		method  string
		body    string
		handler http.HandlerFunc
	}{
		{http.MethodGet, "", s3a.GetPublicAccessBlockHandler},
		{http.MethodPut, testPublicAccessBlockXML, s3a.PutPublicAccessBlockHandler},
		{http.MethodDelete, "", s3a.DeletePublicAccessBlockHandler},
	} {
		rr := httptest.NewRecorder()
		tc.handler(rr, publicAccessBlockRequest(tc.method, "pab-missing", tc.body))
		require.Equal(t, http.StatusNotFound, rr.Code, tc.method)
		require.Contains(t, rr.Body.String(), "NoSuchBucket", tc.method)
	}
}
