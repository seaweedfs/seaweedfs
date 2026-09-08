package iceberg

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type mockS3Authenticator struct {
	called  bool
	errCode s3err.ErrorCode
}

func (m *mockS3Authenticator) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	m.called = true
	if m.errCode != s3err.ErrNone {
		return "", nil, m.errCode
	}
	return "s3user", nil, s3err.ErrNone
}

func (m *mockS3Authenticator) DefaultAllow() bool { return false }

func mintTestToken(t *testing.T, accessKey, secret string, issuedAt, expiresAt time.Time) string {
	t.Helper()
	key := deriveSigningKey(accessKey, secret)
	claims := IcebergClaims{
		IdentityName: "testuser",
		AccessKey:    accessKey,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(issuedAt),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			Issuer:    "seaweedfs-iceberg",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func newAuthTestServer() (*Server, *mockS3Authenticator) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrNotImplemented}
	s.authenticator = auth
	return s, auth
}

// TestAuthExpiredBearerReturns401 is the BUG-0001 regression: an expired
// Bearer token must get 401 (the refresh signal for Iceberg clients), never
// fall through to the S3 authenticator and surface as 501 NotImplemented.
func TestAuthExpiredBearerReturns401(t *testing.T) {
	s, auth := newAuthTestServer()
	now := time.Now()
	expired := mintTestToken(t, "AKID123", "secret456", now.Add(-2*time.Hour), now.Add(-1*time.Hour))

	var handlerCalled bool
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer "+expired)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired Bearer: status = %d, want 401 (BUG-0001: was falling through to 501)", rec.Code)
	}
	if auth.called {
		t.Fatalf("expired Bearer must not fall through to the S3 authenticator")
	}
	if handlerCalled {
		t.Fatalf("handler must not run for an expired token")
	}
	if wa := rec.Header().Get("WWW-Authenticate"); wa != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q, want Bearer", wa)
	}
	if !strings.Contains(rec.Body.String(), "NotAuthorizedException") {
		t.Fatalf("error type must be NotAuthorizedException, got: %s", rec.Body.String())
	}
}

// TestAuthGarbageBearerReturns401 pins the same contract for malformed tokens.
func TestAuthGarbageBearerReturns401(t *testing.T) {
	s, auth := newAuthTestServer()

	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("garbage Bearer: status = %d, want 401", rec.Code)
	}
	if auth.called {
		t.Fatalf("garbage Bearer must not fall through to the S3 authenticator")
	}
}

// TestAuthFreshBearerRunsHandler verifies the happy path still authenticates
// and hands the identity to the handler.
func TestAuthFreshBearerRunsHandler(t *testing.T) {
	s, auth := newAuthTestServer()
	now := time.Now()
	fresh := mintTestToken(t, "AKID123", "secret456", now, now.Add(time.Hour))

	var gotIdentity string
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotIdentity = s3_constants.GetIdentityNameFromContext(r)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer "+fresh)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("fresh Bearer: status = %d, want 200", rec.Code)
	}
	if auth.called {
		t.Fatalf("fresh Bearer should not need the S3 authenticator")
	}
	if gotIdentity != "testuser" {
		t.Fatalf("identity in context = %q, want testuser", gotIdentity)
	}
}

// TestAuthNoBearerStillUsesS3Authenticator keeps the non-Bearer path intact:
// SigV4 requests keep flowing through the S3 authenticator as before.
func TestAuthNoBearerStillUsesS3Authenticator(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrNone}
	s.authenticator = auth

	var handlerCalled bool
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if !auth.called {
		t.Fatalf("request without Bearer header must use the S3 authenticator")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after successful S3 auth", rec.Code)
	}
	if !handlerCalled {
		t.Fatalf("handler must run after S3 auth succeeds")
	}
}

// TestAuthNonBearerAuthorizationHeaderUsesS3Authenticator pins that other
// Authorization schemes (e.g. SigV4) never take the Bearer fast path.
func TestAuthNonBearerAuthorizationHeaderUsesS3Authenticator(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrNone}
	s.authenticator = auth

	var handlerCalled bool
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID123/...")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if !auth.called {
		t.Fatalf("SigV4 Authorization header must use the S3 authenticator")
	}
	if !handlerCalled {
		t.Fatalf("handler must run after S3 auth succeeds")
	}
}
