package weed_server

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestFilerServer_maybeCheckJwtAuthorization_Scoped(t *testing.T) {
	signingKey := "secret"
	filerGuard := security.NewGuard(nil, signingKey, 0, signingKey, 0)
	fs := &FilerServer{
		filerGuard: filerGuard,
	}

	// Helper to generate token
	genToken := func(allowedPrefixes []string, allowedMethods []string) string {
		claims := security.SeaweedFilerClaims{
			AllowedPrefixes: allowedPrefixes,
			AllowedMethods:  allowedMethods,
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			},
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		str, err := token.SignedString([]byte(signingKey))
		if err != nil {
			t.Fatalf("failed to sign token: %v", err)
		}
		return str
	}

	tests := []struct {
		name             string
		token            string
		method           string
		path             string
		isWrite          bool
		expectAuthorized bool
	}{
		{
			name:             "no restrictions",
			token:            genToken(nil, nil),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "allowed prefix match",
			token:            genToken([]string{"/data"}, nil),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "allowed prefix mismatch",
			token:            genToken([]string{"/private"}, nil),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: false,
		},
		{
			name:             "allowed method match",
			token:            genToken(nil, []string{"GET"}),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "allowed method mismatch",
			token:            genToken(nil, []string{"POST"}),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: false,
		},
		{
			name:             "both match",
			token:            genToken([]string{"/data"}, []string{"GET"}),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "prefix match, method mismatch",
			token:            genToken([]string{"/data"}, []string{"POST"}),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: false,
		},
		{
			name:             "multiple prefixes match",
			token:            genToken([]string{"/other", "/data"}, nil),
			method:           "GET",
			path:             "/data/test",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "write operation with method restriction",
			token:            genToken(nil, []string{"POST", "PUT"}),
			method:           "POST",
			path:             "/data/upload",
			isWrite:          true,
			expectAuthorized: true,
		},
		{
			name:             "root path with prefix restriction",
			token:            genToken([]string{"/data"}, nil),
			method:           "GET",
			path:             "/",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "root path without token",
			token:            "",
			method:           "GET",
			path:             "/",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "exact prefix match",
			token:            genToken([]string{"/data"}, nil),
			method:           "GET",
			path:             "/data",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "tenant prefix does not match sibling tenant",
			token:            genToken([]string{"/tenant1"}, nil),
			method:           "GET",
			path:             "/tenant1234/secret",
			isWrite:          false,
			expectAuthorized: false,
		},
		{
			name:             "tenant prefix does not match dashed sibling",
			token:            genToken([]string{"/tenant1"}, nil),
			method:           "GET",
			path:             "/tenant1-old/secret",
			isWrite:          false,
			expectAuthorized: false,
		},
		{
			name:             "tenant prefix matches own subtree",
			token:            genToken([]string{"/tenant1"}, nil),
			method:           "GET",
			path:             "/tenant1/ok.txt",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "tenant prefix with trailing slash matches own subtree",
			token:            genToken([]string{"/tenant1/"}, nil),
			method:           "GET",
			path:             "/tenant1/ok.txt",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "root prefix matches anywhere",
			token:            genToken([]string{"/"}, nil),
			method:           "GET",
			path:             "/tenant1234/secret",
			isWrite:          false,
			expectAuthorized: true,
		},
		{
			name:             "dot-dot cannot escape allowed subtree",
			token:            genToken([]string{"/tenant1"}, nil),
			method:           "GET",
			path:             "/tenant1/../tenant2/secret",
			isWrite:          false,
			expectAuthorized: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			if authorized := fs.maybeCheckJwtAuthorization(req, tt.isWrite); authorized != tt.expectAuthorized {
				t.Errorf("expected authorized=%v, got %v", tt.expectAuthorized, authorized)
			}
		})
	}
}

func TestPathHasComponentPrefix(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		prefix string
		want   bool
	}{
		{"exact match", "/tenant1", "/tenant1", true},
		{"subtree match", "/tenant1/a/b", "/tenant1", true},
		{"trailing slash prefix", "/tenant1/a", "/tenant1/", true},
		{"sibling numeric suffix", "/tenant1234/secret", "/tenant1", false},
		{"sibling dash suffix", "/tenant1-old/secret", "/tenant1", false},
		{"sibling dot suffix", "/tenant1.bak/x", "/tenant1", false},
		{"unrelated tree", "/other/x", "/tenant1", false},
		{"root prefix matches root", "/", "/", true},
		{"root prefix matches any", "/tenant1234/secret", "/", true},
		{"empty prefix denies", "/tenant1/x", "", false},
		{"dot-dot does not escape", "/tenant1/../tenant2/secret", "/tenant1", false},
		{"dot-dot stays inside", "/tenant1/a/../b", "/tenant1", true},
		{"double slashes normalised", "/tenant1//a", "/tenant1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pathHasComponentPrefix(tt.path, tt.prefix); got != tt.want {
				t.Errorf("pathHasComponentPrefix(%q, %q) = %v, want %v", tt.path, tt.prefix, got, tt.want)
			}
		})
	}
}
