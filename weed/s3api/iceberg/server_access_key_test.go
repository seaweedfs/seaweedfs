package iceberg

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// keyedS3Authenticator accepts every SigV4 request as one identity and records
// the key that verified into the request's holder, as S3ApiServer's verifiers do.
type keyedS3Authenticator struct {
	mockS3Authenticator
	accessKey string
}

func (k *keyedS3Authenticator) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	s3_constants.RecordAccessKeyId(r.Context(), k.accessKey)
	return "shared-identity", "identity-object", s3err.ErrNone
}

// A catalog token is minted from one access key; the request it authenticates
// carries that key, so the credentials it vends can name it.
func TestAuthBearerRecordsTheTokensAccessKey(t *testing.T) {
	s, _ := newAuthTestServer()
	now := time.Now()
	token := mintTestToken(t, "AKID123", "secret456", now, now.Add(time.Hour))

	var gotKey, gotName string
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotKey = s3_constants.GetAccessKeyIdFromContext(r)
		gotName = s3_constants.GetIdentityNameFromContext(r)
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("valid Bearer: status = %d, want 200", rec.Code)
	}
	if gotName != "testuser" {
		t.Errorf("identity = %q, want testuser", gotName)
	}
	if gotKey != "AKID123" {
		t.Errorf("access key in context = %q, want AKID123", gotKey)
	}
}

// A SigV4-signed catalog request carries the key its authenticator verified.
func TestAuthSigV4RecordsTheAuthenticatorsAccessKey(t *testing.T) {
	s := newTestServerWithOAuth()
	s.authenticator = &keyedS3Authenticator{accessKey: "AKIDSIGV4"}

	var gotKey string
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotKey = s3_constants.GetAccessKeyIdFromContext(r)
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDSIGV4/20261016/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotKey != "AKIDSIGV4" {
		t.Errorf("access key in context = %q, want AKIDSIGV4", gotKey)
	}
}

// An authenticator that records no key leaves the context without one.
func TestAuthSigV4WithoutKeyAuthenticatorRecordsNoKey(t *testing.T) {
	s := newTestServerWithOAuth()
	s.authenticator = &mockS3Authenticator{}

	gotKey := "unset"
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotKey = s3_constants.GetAccessKeyIdFromContext(r)
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotKey != "" {
		t.Errorf("access key in context = %q, want none", gotKey)
	}
}
