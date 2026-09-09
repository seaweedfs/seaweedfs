package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	cv := &mockCredentialValidator{
		credentials: map[string]string{"AKID123": "secret456"},
		identities:  map[string]string{"AKID123": "testuser"},
	}
	s := &Server{
		credentialValidator: cv,
	}
	return s
}

func TestHandleOAuthTokens_Success(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=client_credentials&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
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
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	s.handleOAuthTokens(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleOAuthTokens_UnsupportedGrantType(t *testing.T) {
	s := newTestServerWithOAuth()

	body := "grant_type=authorization_code&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	s.handleOAuthTokens(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBearerTokenRoundTrip(t *testing.T) {
	s := newTestServerWithOAuth()

	// Get a token
	body := "grant_type=client_credentials&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.handleOAuthTokens(w, req)

	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	// Use the token for Bearer auth
	authReq := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	_, _, ok := s.authenticateBearer(req)
	if ok {
		t.Error("expected Bearer auth to fail with invalid token")
	}
}

func TestBearerTokenNone(t *testing.T) {
	s := newTestServerWithOAuth()

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)

	_, _, ok := s.authenticateBearer(req)
	if ok {
		t.Error("expected Bearer auth to fail with no token")
	}
}

// TestOauthExpirySecondsEnvOverride pins the BUG-0001 mitigation knob: the
// token TTL must be configurable so clients that cannot refresh on 401 can
// be given longer-lived tokens instead of dying every hour.
func TestOauthExpirySecondsEnvOverride(t *testing.T) {
	if got := oauthExpirySeconds(); got != defaultOauthTokenExpiry {
		t.Fatalf("default TTL = %d, want %d", got, defaultOauthTokenExpiry)
	}
	t.Setenv("ICEBERG_OAUTH_TOKEN_EXPIRY", "86400")
	if got := oauthExpirySeconds(); got != 86400 {
		t.Fatalf("env TTL = %d, want 86400", got)
	}
	t.Setenv("ICEBERG_OAUTH_TOKEN_EXPIRY", "-5")
	if got := oauthExpirySeconds(); got != defaultOauthTokenExpiry {
		t.Fatalf("negative TTL must fall back to default, got %d", got)
	}
	t.Setenv("ICEBERG_OAUTH_TOKEN_EXPIRY", "garbage")
	if got := oauthExpirySeconds(); got != defaultOauthTokenExpiry {
		t.Fatalf("invalid TTL must fall back to default, got %d", got)
	}

	// a token minted under an override carries the override's expiry
	t.Setenv("ICEBERG_OAUTH_TOKEN_EXPIRY", "7200")
	s := newTestServerWithOAuth()
	body := "grant_type=client_credentials&client_id=AKID123&client_secret=secret456"
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.handleOAuthTokens(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ExpiresIn != 7200 {
		t.Fatalf("minted token expires_in = %d, want 7200", resp.ExpiresIn)
	}
}
