package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRejectAmbiguousSubresource(t *testing.T) {
	passthrough := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })
	for _, query := range []string{
		"policy",
		"policy=",
		"tagging&versionId=v1",
		"list-type=2&prefix=a&continuation-token=x",
		"uploadId=abc&partNumber=3",
		"uploads",
		"acl&X-Amz-Signature=x&X-Amz-Expires=900",
		"analytics&id=weekly",
		"delete",
		"versions&versionId=",
		"versioning",
		"prefix=a&expected-bucket-owner=123",
		"tagging&tagging=x",
		"",
	} {
		r := httptest.NewRequest(http.MethodPut, "http://localhost/bucket?"+query, nil)
		w := httptest.NewRecorder()
		rejectAmbiguousSubresource(passthrough).ServeHTTP(w, r)
		if w.Code != http.StatusTeapot {
			t.Errorf("%q should pass through, got %d", query, w.Code)
		}
	}

	for _, query := range []string{
		"tagging&policy",
		"policy&tagging",
		"list-type=2&policy",
		"versions&policy",
		"versioning&policy",
		"delete&policy",
		"cors&policy",
		"acl&tagging",
		"tagging&seaweedfs-quota",
		"uploadId=x&acl",
		"retention&legal-hold",
		"ownershipControls&encryption",
	} {
		r := httptest.NewRequest(http.MethodPut, "http://localhost/bucket?"+query, nil)
		w := httptest.NewRecorder()
		rejectAmbiguousSubresource(passthrough).ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%q should be rejected, got %d", query, w.Code)
		}
	}
}
