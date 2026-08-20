package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func signHlsTsTestJwt(t *testing.T, key string, prefixes, methods []string) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes: prefixes,
		AllowedMethods:  methods,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, err := token.SignedString([]byte(key))
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return encoded
}

func TestHlsTsJwtUsesUnderlyingSourcePath(t *testing.T) {
	const key = "hls-test-key"
	fs := &FilerServer{
		option:     &FilerOption{HlsTsEnabled: true},
		filerGuard: security.NewGuard(nil, key, 0, key, 0),
	}
	fs.hlsTsReadJwtRequired.Store(true)

	allowed := signHlsTsTestJwt(t, key, []string{"/media"}, []string{http.MethodGet})
	req := httptest.NewRequest(http.MethodGet, "/hls/media/movie/7.ts", nil)
	req.Header.Set("Authorization", "Bearer "+allowed)
	if !fs.maybeCheckJwtAuthorization(req, false) {
		t.Fatal("HLS segment was not authorized against its underlying /media path")
	}

	virtualOnly := signHlsTsTestJwt(t, key, []string{"/hls"}, []string{http.MethodGet})
	req = httptest.NewRequest(http.MethodGet, "/hls/media/movie/7.ts", nil)
	req.Header.Set("Authorization", "Bearer "+virtualOnly)
	if fs.maybeCheckJwtAuthorization(req, false) {
		t.Fatal("HLS segment was authorized against the virtual /hls path instead of the source path")
	}
}

func TestHlsTsJwtCanBeDisabledForPlaybackOnly(t *testing.T) {
	const key = "hls-test-key"
	fs := &FilerServer{
		option:     &FilerOption{HlsTsEnabled: true},
		filerGuard: security.NewGuard(nil, key, 0, key, 0),
	}
	fs.hlsTsReadJwtRequired.Store(false)

	readReq := httptest.NewRequest(http.MethodGet, "/hls/media/movie/index.m3u8", nil)
	if !fs.maybeCheckJwtAuthorization(readReq, false) {
		t.Fatal("HLS playback without JWT should be allowed when HLS JWT is disabled")
	}

	normalRead := httptest.NewRequest(http.MethodGet, "/media/movie", nil)
	if fs.maybeCheckJwtAuthorization(normalRead, false) {
		t.Fatal("disabling HLS JWT unexpectedly disabled JWT for normal filer reads")
	}

	writeReq := httptest.NewRequest(http.MethodPost, "/hls/media/movie", nil)
	if fs.maybeCheckJwtAuthorization(writeReq, true) {
		t.Fatal("disabling HLS playback JWT unexpectedly disabled write JWT for ingest")
	}
}
