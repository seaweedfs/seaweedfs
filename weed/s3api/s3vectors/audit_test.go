package s3vectors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestVectorAuditRecordsIdentityResourceAndErrors(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r := s3err.EnsureAuditTracking(httptest.NewRequest(http.MethodPost, "/QueryVectors", strings.NewReader(`{}`)))
			r = s3_constants.EnsureIdentityHolder(r)
			s := &Server{}
			var got *s3err.AccessLog
			s.instrument(func(w http.ResponseWriter, request *http.Request) {
				request = request.WithContext(s3_constants.SetIdentityNameInContext(request.Context(), "alice"))
				setRequestTarget(request, "embeddings", "documents")
				w.WriteHeader(status)
				if status >= http.StatusBadRequest {
					_, _ = w.Write([]byte(`{"code":"AccessDenied","message":"no secret data"}`))
				}
				info := request.Context().Value(requestInfoKey{}).(*requestInfo)
				got = accessLog(request, status, "s3vectors.QueryVectors", info)
			})(httptest.NewRecorder(), r)
			if !s3err.AuditAlreadyLogged(r) {
				t.Fatal("vector response did not emit an audit event")
			}
			if got.Requester != "alice" || got.Bucket != "embeddings" || got.Key != "documents" || got.Operation != "s3vectors.QueryVectors" || got.HTTPStatus != status {
				t.Fatalf("audit event: %+v", got)
			}
			if status >= http.StatusBadRequest && got.ErrorCode != "AccessDenied" {
				t.Fatalf("error code = %q", got.ErrorCode)
			}
		})
	}
}

func TestVectorBucketAuditHasNoObjectKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/CreateVectorBucket", nil).WithContext(context.Background())
	log := accessLog(r, http.StatusOK, "s3vectors.CreateVectorBucket", &requestInfo{bucket: "embeddings"})
	if log.Bucket != "embeddings" || log.Key != "" {
		t.Fatalf("bucket audit: %+v", log)
	}
}
