package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// signFilerToken builds a signed filer JWT for tests.
func signFilerToken(t *testing.T, signingKey string, allowedPrefixes, allowedMethods []string) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes: allowedPrefixes,
		AllowedMethods:  allowedMethods,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		},
	}
	str, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return str
}

func TestFilerServer_checkTusJwtAuthorization(t *testing.T) {
	const writeKey = "write-secret"
	const readKey = "read-secret"
	fs := &FilerServer{
		filerGuard: security.NewGuard(nil, writeKey, 0, readKey, 0),
		option:     &FilerOption{TusBasePath: "/.tus"},
	}

	tests := []struct {
		name             string
		method           string
		path             string
		token            string
		expectAuthorized bool
	}{
		// The advisory: with a filer signing key configured, an unauthenticated
		// TUS request must be rejected the same as a normal filer write.
		{"create without token denied", http.MethodPost, "/.tus/buckets/secret/owned.txt", "", false},
		{"patch without token denied", http.MethodPatch, "/.tus/.uploads/abc", "", false},
		{"delete without token denied", http.MethodDelete, "/.tus/.uploads/abc", "", false},
		{"head without token denied", http.MethodHead, "/.tus/.uploads/abc", "", false},

		// A valid token for the right access level is accepted.
		{"create with write token allowed", http.MethodPost, "/.tus/buckets/data/ok.txt", signFilerToken(t, writeKey, nil, nil), true},
		{"patch with write token allowed", http.MethodPatch, "/.tus/.uploads/abc", signFilerToken(t, writeKey, nil, nil), true},
		{"head with read token allowed", http.MethodHead, "/.tus/.uploads/abc", signFilerToken(t, readKey, nil, nil), true},

		// HEAD is a read, so a write-only token must not authorize it and a read
		// token must not authorize a write.
		{"head with write token denied", http.MethodHead, "/.tus/.uploads/abc", signFilerToken(t, writeKey, nil, nil), false},
		{"create with read token denied", http.MethodPost, "/.tus/buckets/data/ok.txt", signFilerToken(t, readKey, nil, nil), false},

		// Prefix-restricted tokens are scoped against the resolved target path
		// (URL minus the /.tus prefix), not the /.tus route.
		{"create within allowed prefix", http.MethodPost, "/.tus/buckets/allowed/ok.txt", signFilerToken(t, writeKey, []string{"/buckets/allowed"}, nil), true},
		{"create outside allowed prefix denied", http.MethodPost, "/.tus/buckets/secret/owned.txt", signFilerToken(t, writeKey, []string{"/buckets/allowed"}, nil), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			if got := fs.checkTusJwtAuthorization(req); got != tt.expectAuthorized {
				t.Errorf("checkTusJwtAuthorization(%s %s) = %v, want %v", tt.method, tt.path, got, tt.expectAuthorized)
			}
		})
	}
}

// TestFilerServer_tusHandler_UnauthenticatedRejected exercises the full handler
// entry point: OPTIONS discovery stays open, but an unauthenticated write is
// rejected with 401 before any session is created.
func TestFilerServer_tusHandler_UnauthenticatedRejected(t *testing.T) {
	fs := &FilerServer{
		filerGuard: security.NewGuard(nil, "write-secret", 0, "read-secret", 0),
		option:     &FilerOption{TusBasePath: "/.tus"},
	}

	// OPTIONS is capability discovery and must not require a token.
	optionsReq := httptest.NewRequest(http.MethodOptions, "/.tus/buckets/secret/owned.txt", nil)
	optionsRec := httptest.NewRecorder()
	fs.tusHandler(optionsRec, optionsReq)
	if optionsRec.Code != http.StatusOK {
		t.Errorf("OPTIONS without token = %d, want %d", optionsRec.Code, http.StatusOK)
	}

	// POST without a token must be rejected before touching the filer store.
	postReq := httptest.NewRequest(http.MethodPost, "/.tus/buckets/secret/owned.txt", nil)
	postReq.Header.Set("Tus-Resumable", TusVersion)
	postReq.Header.Set("Upload-Length", "5")
	postRec := httptest.NewRecorder()
	fs.tusHandler(postRec, postReq)
	if postRec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated POST = %d, want %d", postRec.Code, http.StatusUnauthorized)
	}
}
