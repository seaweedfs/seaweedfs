package lance

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func signAdminClaims(t *testing.T, key security.SigningKey, claims security.SeaweedFilerAdminClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return token
}

func trustedSeenByHandler(t *testing.T, s *Server, token string) bool {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/namespace/$/list", nil)
	if token != "" {
		req.Header.Set(clusterTokenHeader, token)
	}
	trusted := false
	rec := httptest.NewRecorder()
	s.Auth(func(w http.ResponseWriter, r *http.Request) {
		trusted = isTrustedCaller(r.Context())
		w.WriteHeader(http.StatusOK)
	})(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	return trusted
}

func TestClusterTokenMarksTheCallerTrusted(t *testing.T) {
	key := security.SigningKey("cluster-key")
	s := NewServer(nil, openAuthenticator{})
	s.SetSigningKey(key)

	expired := security.SeaweedFilerAdminClaims{}
	expired.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
	for _, tc := range []struct {
		name  string
		token string
		want  bool
	}{
		{"no token", "", false},
		{"signed with the cluster key", string(security.GenJwtForFilerAdmin(key, 60)), true},
		{"signed with another key", string(security.GenJwtForFilerAdmin(security.SigningKey("other-key"), 60)), false},
		{"expired", signAdminClaims(t, key, expired), false},
		{"session token", signAdminClaims(t, key, security.SeaweedFilerAdminClaims{SessionId: "session"}), false},
		{"garbage", "not-a-token", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := trustedSeenByHandler(t, s, tc.token); got != tc.want {
				t.Errorf("trusted = %v, want %v", got, tc.want)
			}
		})
	}

	s.SetSigningKey(nil)
	if trustedSeenByHandler(t, s, string(security.GenJwtForFilerAdmin(key, 60))) {
		t.Error("a server with no cluster key trusted a caller")
	}
}
