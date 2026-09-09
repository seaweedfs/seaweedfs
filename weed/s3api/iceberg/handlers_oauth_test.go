package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
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
	// oversized values are clamped so Duration math cannot overflow
	t.Setenv("ICEBERG_OAUTH_TOKEN_EXPIRY", "99999999999999999")
	if got := oauthExpirySeconds(); got != maxOauthTokenExpiry {
		t.Fatalf("oversized TTL must clamp to %d, got %d", maxOauthTokenExpiry, got)
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

func exchangeRequest(t *testing.T, subjectToken string) *httptest.ResponseRecorder {
	return exchangeRequestWithBasic(t, subjectToken, "", "")
}

// exchangeRequestWithBasic mirrors Iceberg Java: refreshExpiredToken sends
// Basic client credentials with the token-exchange grant, the proactive
// scheduled refresh sends only the form body.
func exchangeRequestWithBasic(t *testing.T, subjectToken, basicUser, basicPass string) *httptest.ResponseRecorder {
	t.Helper()
	s := newTestServerWithOAuth()
	body := "grant_type=urn:ietf:params:oauth:grant-type:token-exchange&subject_token=" + url.QueryEscape(subjectToken)
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if basicUser != "" || basicPass != "" {
		req.SetBasicAuth(basicUser, basicPass)
	}
	w := httptest.NewRecorder()
	s.handleOAuthTokens(w, req)
	return w
}

// TestTokenExchangeLiveSubject: the proactive refresh path Iceberg Java
// 1.10.x uses (exchangeEnabled defaults true, Bearer-only headers) must
// mint a fresh working token.
func TestTokenExchangeLiveSubject(t *testing.T) {
	s := newTestServerWithOAuth()
	now := time.Now()
	live := mintTestToken(t, "AKID123", "secret456", now, now.Add(30*time.Minute))

	w := exchangeRequest(t, live)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TokenType != "bearer" || resp.AccessToken == "" {
		t.Fatalf("bad token response: %+v", resp)
	}
	if resp.IssuedTokenType != accessTokenTokenType {
		t.Fatalf("issued_token_type = %q, want %s", resp.IssuedTokenType, accessTokenTokenType)
	}
	// Unauthenticated exchange must not outlive the subject token.
	if resp.ExpiresIn > 30*60 {
		t.Fatalf("expires_in = %d, must be bounded by the subject's remaining lifetime (<= 1800)", resp.ExpiresIn)
	}
	// the exchanged token must authenticate like a normal Bearer
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	if _, _, ok := s.authenticateBearer(req); !ok {
		t.Fatalf("exchanged token must pass authenticateBearer")
	}
}

// TestTokenExchangeExpiredWithinGrace: a client whose token expired while
// exchange was unsupported must recover without a restart. This is the
// refreshExpiredToken path, which authenticates with Basic credentials.
func TestTokenExchangeExpiredWithinGrace(t *testing.T) {
	now := time.Now()
	expiredRecently := mintTestToken(t, "AKID123", "secret456", now.Add(-20*time.Minute), now.Add(-10*time.Minute))

	w := exchangeRequestWithBasic(t, expiredRecently, "AKID123", "secret456")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 within grace, got %d: %s", w.Code, w.Body.String())
	}
	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	// Authenticated refresh gets the full configured TTL.
	if resp.ExpiresIn != oauthExpirySeconds() {
		t.Fatalf("expires_in = %d, want %d", resp.ExpiresIn, oauthExpirySeconds())
	}
}

// TestTokenExchangeAuthenticatedLiveSubjectFullTTL: an authenticated client
// exchanging a live token renews the session — the TTL cap only applies to
// unauthenticated (Bearer-only) exchanges.
func TestTokenExchangeAuthenticatedLiveSubjectFullTTL(t *testing.T) {
	now := time.Now()
	live := mintTestToken(t, "AKID123", "secret456", now, now.Add(30*time.Minute))

	w := exchangeRequestWithBasic(t, live, "AKID123", "secret456")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp OAuthTokenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ExpiresIn != oauthExpirySeconds() {
		t.Fatalf("authenticated live exchange expires_in = %d, want full TTL %d", resp.ExpiresIn, oauthExpirySeconds())
	}
}

// TestTokenExchangeSubjectNearlyExpired: a Bearer-only exchange when the
// subject token has under a second left must be rejected, not minted into
// an already-expired token (expires_in: 0).
func TestTokenExchangeSubjectNearlyExpired(t *testing.T) {
	now := time.Now()
	nearlyDead := mintTestToken(t, "AKID123", "secret456", now.Add(-time.Hour), now.Add(300*time.Millisecond))

	w := exchangeRequest(t, nearlyDead)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatalf("expected invalid_grant, got: %s", w.Body.String())
	}
}

// TestTokenExchangeExpiredWithoutClientAuth: a leaked expired token must
// not be exchangeable without client credentials.
func TestTokenExchangeExpiredWithoutClientAuth(t *testing.T) {
	now := time.Now()
	expiredRecently := mintTestToken(t, "AKID123", "secret456", now.Add(-20*time.Minute), now.Add(-10*time.Minute))

	w := exchangeRequest(t, expiredRecently)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatalf("expected invalid_grant, got: %s", w.Body.String())
	}
}

// TestTokenExchangeExpiredBeyondGrace: stale tokens far past expiry must not
// act as eternal credentials, even with client auth.
func TestTokenExchangeExpiredBeyondGrace(t *testing.T) {
	now := time.Now()
	longDead := mintTestToken(t, "AKID123", "secret456", now.Add(-48*time.Hour), now.Add(-47*time.Hour))

	w := exchangeRequestWithBasic(t, longDead, "AKID123", "secret456")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 beyond grace, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatalf("expected invalid_grant, got: %s", w.Body.String())
	}
}

// TestTokenExchangeGarbageSubject: malformed subject tokens are rejected
// with 400 invalid_grant per RFC 6749 §5.2.
func TestTokenExchangeGarbageSubject(t *testing.T) {
	w := exchangeRequest(t, "not-a-jwt")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// TestTokenExchangeWrongClientCredentials: valid subject token, but Basic
// auth with wrong credentials or a different client is rejected.
func TestTokenExchangeWrongClientCredentials(t *testing.T) {
	now := time.Now()
	live := mintTestToken(t, "AKID123", "secret456", now, now.Add(30*time.Minute))

	// wrong secret → invalid_client 401
	w := exchangeRequestWithBasic(t, live, "AKID123", "wrongsecret")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong secret: expected 401, got %d", w.Code)
	}

	// a different valid client must not exchange someone else's token
	s := &Server{credentialValidator: &mockCredentialValidator{
		credentials: map[string]string{"AKID123": "secret456", "AKID999": "other-secret"},
		identities:  map[string]string{"AKID123": "testuser", "AKID999": "otheruser"},
	}}
	body := "grant_type=urn:ietf:params:oauth:grant-type:token-exchange&subject_token=" + url.QueryEscape(live)
	req := httptest.NewRequest(http.MethodPost, "/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("AKID999", "other-secret")
	rec := httptest.NewRecorder()
	s.handleOAuthTokens(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid_grant") {
		t.Fatalf("cross-client exchange: expected 400 invalid_grant, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestTokenExchangeSubjectWithoutExpiry: a correctly signed subject token
// without an exp claim must be rejected — it would otherwise refresh forever.
func TestTokenExchangeSubjectWithoutExpiry(t *testing.T) {
	claims := IcebergClaims{
		IdentityName: "testuser",
		AccessKey:    "AKID123",
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt: jwt.NewNumericDate(time.Now()),
			Issuer:   "seaweedfs-iceberg",
			// no ExpiresAt
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(deriveSigningKey("AKID123", "secret456"))
	if err != nil {
		t.Fatal(err)
	}

	w := exchangeRequestWithBasic(t, signed, "AKID123", "secret456")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatalf("expected 400 invalid_grant for nil-exp subject, got %d: %s", w.Code, w.Body.String())
	}
}
