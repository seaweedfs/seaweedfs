package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestFilerBasicAuth(t *testing.T) {
	signingKey := "secret"
	token := func() string {
		claims := security.SeaweedFilerClaims{
			RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		}
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}()

	type creds struct {
		user, password string
		bearer         string
	}
	request := func(method string, c *creds) *http.Request {
		r := httptest.NewRequest(method, "/dir/file", nil)
		if c == nil {
			return r
		}
		if c.bearer != "" {
			r.Header.Set("Authorization", "Bearer "+c.bearer)
		} else {
			r.SetBasicAuth(c.user, c.password)
		}
		return r
	}

	tests := []struct {
		name      string
		password  string
		jwtKey    string
		creds     *creds
		wantRead  bool
		wantWrite bool
	}{
		{name: "off, no keys, anonymous", wantRead: true, wantWrite: true},
		{name: "on, no keys, anonymous", password: "pw"},
		{name: "on, no keys, valid", password: "pw", creds: &creds{user: "admin", password: "pw"}, wantRead: true, wantWrite: true},
		{name: "on, no keys, wrong password", password: "pw", creds: &creds{user: "admin", password: "pwx"}},
		{name: "on, no keys, wrong user", password: "pw", creds: &creds{user: "root", password: "pw"}},
		{name: "on, no keys, bearer ignored", password: "pw", creds: &creds{bearer: token}},
		{name: "on, keys, valid basic", password: "pw", jwtKey: signingKey, creds: &creds{user: "admin", password: "pw"}, wantRead: true, wantWrite: true},
		{name: "on, keys, valid jwt", password: "pw", jwtKey: signingKey, creds: &creds{bearer: token}, wantRead: true, wantWrite: true},
		{name: "on, keys, anonymous", password: "pw", jwtKey: signingKey},
		{name: "off, keys, basic is not a jwt", jwtKey: signingKey, creds: &creds{user: "admin", password: "pw"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FilerServer{filerGuard: security.NewGuard(nil, tt.jwtKey, 0, tt.jwtKey, 0)}
			fs.basicAuth.Store(security.NewBasicAuth("admin", tt.password))
			if got := fs.maybeCheckJwtAuthorization(request(http.MethodGet, tt.creds), false); got != tt.wantRead {
				t.Errorf("read: got %v, want %v", got, tt.wantRead)
			}
			if got := fs.maybeCheckJwtAuthorization(request(http.MethodPut, tt.creds), true); got != tt.wantWrite {
				t.Errorf("write: got %v, want %v", got, tt.wantWrite)
			}
		})
	}
}

func TestFilerBasicAuthChallenge(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, "", 0, "", 0)}
	fs.basicAuth.Store(security.NewBasicAuth("admin", "pw"))
	w := httptest.NewRecorder()
	fs.readonlyFilerHandler(w, httptest.NewRequest(http.MethodGet, "/dir/", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want %d", w.Code, http.StatusUnauthorized)
	}
	if got := w.Header().Get("WWW-Authenticate"); got == "" {
		t.Error("missing WWW-Authenticate challenge")
	}

	fs.basicAuth.Store(security.NewBasicAuth("", ""))
	w = httptest.NewRecorder()
	fs.writeUnauthorized(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := w.Header().Get("WWW-Authenticate"); got != "" {
		t.Errorf("challenge %q with Basic Auth off", got)
	}
}
