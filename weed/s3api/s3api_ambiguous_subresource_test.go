package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAmbiguousSubresource pins the rule that a request may name only one
// operation. The router picks a handler by registration order and the IAM action
// resolver picks an action by its own order, so a request carrying two operation
// subresources gets authorized as one and served as the other.
func TestAmbiguousSubresource(t *testing.T) {
	for _, query := range []string{
		"",
		"policy=",
		"tagging=",
		"acl=&versionId=abc",
		"tagging=&versionId=abc",
		"retention=&versionId=abc",
		"uploadId=xyz&partNumber=3",
		"attributes=&partNumber=3&versionId=abc",
		"versions=&prefix=a&delimiter=/",
		"uploads=&prefix=a&x-id=CreateMultipartUpload",
		"list-type=2&prefix=a&continuation-token=x",
		"acl=&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef",
	} {
		req, _ := http.NewRequest("GET", "http://localhost/bucket/key?"+query, nil)
		assert.False(t, hasAmbiguousSubresource(req.URL.Query()), "%q names one operation", query)
	}

	for _, query := range []string{
		"policy=&tagging=",
		"tagging=&policy=",
		"cors=&tagging=",
		"lifecycle=&tagging=",
		"versioning=&tagging=",
		"object-lock=&tagging=",
		"requestPayment=&tagging=",
		"acl=&policy=",
		"policy=&cors=",
		"delete=&policy=",
		"uploads=&uploadId=xyz",
		"policy=&tagging=&cors=",
	} {
		req, _ := http.NewRequest("PUT", "http://localhost/bucket?"+query, nil)
		assert.True(t, hasAmbiguousSubresource(req.URL.Query()), "%q names two operations", query)
	}
}

// The bucket tagger's escalation: PUT /bucket?policy&tagging routes to the
// bucket-policy handler while resolving as s3:PutBucketTagging. The guard has to
// reject it before either the handler or the IAM check runs.
func TestAmbiguousSubresourceRejectedBeforeHandler(t *testing.T) {
	served := false
	handler := validateRequestPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = true
	}))

	req, _ := http.NewRequest("PUT", "http://localhost/bucket?policy=&tagging=", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.False(t, served, "an ambiguous request must not reach a handler")
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	served = false
	req, _ = http.NewRequest("PUT", "http://localhost/bucket?policy=", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.True(t, served, "an unambiguous request must still be served")
}
