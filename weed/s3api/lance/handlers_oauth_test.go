package lance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type mockCredentialValidator struct {
	credentials map[string]string // accessKey -> secretKey
	identities  map[string]string // accessKey -> identityName
}

func (m *mockCredentialValidator) ValidateS3Credential(accessKey, secretKey string) (string, interface{}, error) {
	expected, ok := m.credentials[accessKey]
	if !ok {
		return "", nil, fmt.Errorf("access key not found")
	}
	if expected != secretKey {
		return "", nil, fmt.Errorf("invalid secret key")
	}
	return m.identities[accessKey], nil, nil
}

func (m *mockCredentialValidator) GetCredentialByAccessKey(accessKey string) (string, interface{}, string, error) {
	secret, ok := m.credentials[accessKey]
	if !ok {
		return "", nil, "", fmt.Errorf("access key not found")
	}
	return m.identities[accessKey], nil, secret, nil
}

func newTestServerWithOAuth() *Server {
	return &Server{
		credentialValidator: &mockCredentialValidator{
			credentials: map[string]string{"AKID123": "secret456"},
			identities:  map[string]string{"AKID123": "testuser"},
		},
	}
}

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
	claims := LanceClaims{
		IdentityName: "testuser",
		AccessKey:    accessKey,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(issuedAt),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			Issuer:    "seaweedfs-lance",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func TestHandleOAuthTokens_Success(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=client_credentials&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	s.handleOAuthTokens(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TokenType != "bearer" {
		t.Errorf("expected token_type=bearer, got %s", resp.TokenType)
	}
	if resp.AccessToken == "" {
		t.Error("expected non-empty access_token")
	}
	if resp.ExpiresIn != oauthExpirySeconds() {
		t.Errorf("expected expires_in=%d, got %d", oauthExpirySeconds(), resp.ExpiresIn)
	}
}

func TestHandleOAuthTokens_InvalidCredentials(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=client_credentials&client_id=AKID123&client_secret=wrongsecret"
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	s.handleOAuthTokens(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	if wa := w.Header().Get("WWW-Authenticate"); wa != `Basic realm="lance"` {
		t.Fatalf("WWW-Authenticate = %q, want Basic challenge", wa)
	}
}

func TestHandleOAuthTokens_UnsupportedGrantType(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=authorization_code&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	s.handleOAuthTokens(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBearerTokenRoundTrip(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=client_credentials&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.handleOAuthTokens(w, req)

	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	authReq := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	authReq.Header.Set("Authorization", "Bearer "+resp.AccessToken)

	identityName, _, ok := s.authenticateBearer(authReq)
	if !ok {
		t.Fatal("expected Bearer auth to succeed")
	}
	if identityName != "testuser" {
		t.Errorf("expected identity 'testuser', got '%s'", identityName)
	}
}

func TestBearerTokenInvalid(t *testing.T) {
	s := newTestServerWithOAuth()

	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	_, _, ok := s.authenticateBearer(req)
	if ok {
		t.Error("expected Bearer auth to fail with invalid token")
	}
}

// The issue-11430 reproduction: behind an auth-enabled gateway, a catalog
// request with no signature is denied, but the same request carrying a Bearer
// token minted from S3 credentials must pass.
func TestAuthBearerRunsHandler(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrAccessDenied}
	s.authenticator = auth
	now := time.Now()
	fresh := mintTestToken(t, "AKID123", "secret456", now, now.Add(time.Hour))

	var gotIdentity string
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotIdentity = s3_constants.GetIdentityNameFromContext(r)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
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

// An expired or malformed Bearer token answers 401 rather than falling
// through to the S3 authenticator, which would misread the header as SigV4.
func TestAuthExpiredBearerReturns401(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrAccessDenied}
	s.authenticator = auth
	now := time.Now()
	expired := mintTestToken(t, "AKID123", "secret456", now.Add(-2*time.Hour), now.Add(-1*time.Hour))

	var handlerCalled bool
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
		req.Header.Set("Authorization", scheme+" "+expired)
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("scheme %q: status = %d, want 401", scheme, rec.Code)
		}
		if wa := rec.Header().Get("WWW-Authenticate"); wa != "Bearer" {
			t.Fatalf("scheme %q: WWW-Authenticate = %q, want Bearer", scheme, wa)
		}
	}
	if auth.called {
		t.Fatalf("expired Bearer must not fall through to the S3 authenticator")
	}
	if handlerCalled {
		t.Fatalf("handler must not run for an expired token")
	}
}

// x-api-key carries "access_key:secret_key" straight to the catalog, with no
// token to mint or expire - the header form LanceDB documents for API keys.
func TestAuthApiKeyRunsHandler(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrAccessDenied}
	s.authenticator = auth

	var gotIdentity string
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		gotIdentity = s3_constants.GetIdentityNameFromContext(r)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	req.Header.Set("x-api-key", "AKID123:secret456")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("x-api-key: status = %d, want 200", rec.Code)
	}
	if gotIdentity != "testuser" {
		t.Fatalf("identity in context = %q, want testuser", gotIdentity)
	}
}

func TestAuthApiKeyInvalid(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrAccessDenied}
	s.authenticator = auth

	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for _, key := range []string{"AKID123:wrongsecret", "AKID123", ":secret456", "unknown:key"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
		req.Header.Set("x-api-key", key)
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("x-api-key %q: status = %d, want 401", key, rec.Code)
		}
	}
	if auth.called {
		t.Fatalf("bad x-api-key must not fall through to the S3 authenticator")
	}
}

func TestAuthNoBearerStillUsesS3Authenticator(t *testing.T) {
	s := newTestServerWithOAuth()
	auth := &mockS3Authenticator{errCode: s3err.ErrNone}
	s.authenticator = auth

	var handlerCalled bool
	handler := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if !auth.called {
		t.Fatalf("request without Bearer header must use the S3 authenticator")
	}
	if rec.Code != http.StatusOK || !handlerCalled {
		t.Fatalf("status = %d, handler called = %v", rec.Code, handlerCalled)
	}
}
