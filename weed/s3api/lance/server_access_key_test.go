package lance

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// keyedS3Authenticator accepts every request as one identity and records the
// key that verified into the request's holder, as S3ApiServer's verifiers do.
type keyedS3Authenticator struct {
	mockS3Authenticator
	accessKey string
}

func (k *keyedS3Authenticator) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	s3_constants.RecordAccessKeyId(r.Context(), k.accessKey)
	return "s3user", nil, s3err.ErrNone
}

func accessKeySeenByHandler(t *testing.T, s *Server, req *http.Request) string {
	t.Helper()
	gotKey := "unset"
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotKey = s3_constants.GetAccessKeyIdFromContext(r)
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	return gotKey
}

// Each way a Lance client authenticates leaves the access key it used in the
// request context, so the credentials it vends can name that key.
func TestAuthRecordsTheCallersAccessKey(t *testing.T) {
	now := time.Now()

	s := newTestServerWithOAuth()
	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	req.Header.Set("Authorization", "Bearer "+mintTestToken(t, "AKID123", "secret456", now, now.Add(time.Hour)))
	if got := accessKeySeenByHandler(t, s, req); got != "AKID123" {
		t.Errorf("Bearer: access key in context = %q, want AKID123", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	req.Header.Set("x-api-key", "AKID123:secret456")
	if got := accessKeySeenByHandler(t, s, req); got != "AKID123" {
		t.Errorf("x-api-key: access key in context = %q, want AKID123", got)
	}

	s.authenticator = &keyedS3Authenticator{accessKey: "AKIDSIGV4"}
	req = httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	if got := accessKeySeenByHandler(t, s, req); got != "AKIDSIGV4" {
		t.Errorf("SigV4: access key in context = %q, want AKIDSIGV4", got)
	}

	s.authenticator = &mockS3Authenticator{}
	req = httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	if got := accessKeySeenByHandler(t, s, req); got != "" {
		t.Errorf("authenticator without a key: access key in context = %q, want none", got)
	}
}
