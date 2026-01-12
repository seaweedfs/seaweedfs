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
			panic(err)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+tt.token)
			if authorized := fs.maybeCheckJwtAuthorization(req, tt.isWrite); authorized != tt.expectAuthorized {
				t.Errorf("expected authorized=%v, got %v", tt.expectAuthorized, authorized)
			}
		})
	}
}
