package dash

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/sessions"
)

func TestBearerTokenFromRequest(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
		ok     bool
	}{
		{"no header", "", "", false},
		{"wrong scheme", "Basic abc123", "", false},
		{"empty token", "Bearer ", "", false},
		{"valid token", "Bearer my-secret-token", "my-secret-token", true},
		{"valid with spaces", "Bearer  token-with-spaces  ", "token-with-spaces", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			token, ok := bearerTokenFromRequest(req)
			if ok != tt.ok {
				t.Errorf("bearerTokenFromRequest() ok = %v, want %v", ok, tt.ok)
			}
			if token != tt.want {
				t.Errorf("bearerTokenFromRequest() token = %q, want %q", token, tt.want)
			}
		})
	}
}

func TestValidateBearerToken(t *testing.T) {
	apiKey := "test-api-key-12345"
	tests := []struct {
		name     string
		apiKey   string
		authHdr  string
		expected bool
	}{
		{"empty apiKey disables token auth", "", "Bearer test-api-key-12345", false},
		{"valid token matches", apiKey, "Bearer test-api-key-12345", true},
		{"wrong token does not match", apiKey, "Bearer wrong-token", false},
		{"no Authorization header", apiKey, "", false},
		{"non-bearer scheme", apiKey, "Basic test-api-key-12345", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
			if tt.authHdr != "" {
				req.Header.Set("Authorization", tt.authHdr)
			}
			result := validateBearerToken(tt.apiKey, req)
			if result != tt.expected {
				t.Errorf("validateBearerToken() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRequireAuthAPI_BearerTokenBypassesSession(t *testing.T) {
	store := sessions.NewCookieStore([]byte("test-secret"))
	apiKey := "my-api-key"
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		role := RoleFromContext(r.Context())
		if role != "admin" {
			t.Errorf("expected role 'admin', got %q", role)
		}
		w.WriteHeader(http.StatusOK)
	})
	mw := RequireAuthAPI(store, apiKey)
	wrapped := mw(handler)

	// Request with valid bearer token — should pass without session
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
	req.Header.Set("Authorization", "Bearer my-api-key")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestRequireAuthAPI_BearerTokenWrongReturns401(t *testing.T) {
	store := sessions.NewCookieStore([]byte("test-secret"))
	apiKey := "my-api-key"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not be called")
	})
	mw := RequireAuthAPI(store, apiKey)
	wrapped := mw(handler)

	// Request with wrong bearer token — should get 401
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRequireAuthAPI_NoTokenNoSessionReturns401(t *testing.T) {
	store := sessions.NewCookieStore([]byte("test-secret"))
	apiKey := "my-api-key"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not be called")
	})
	mw := RequireAuthAPI(store, apiKey)
	wrapped := mw(handler)

	// Request with no token and no session — should get 401
	req := httptest.NewRequest(http.MethodGet, "/api/s3/buckets", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
