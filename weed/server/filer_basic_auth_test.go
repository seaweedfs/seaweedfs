package weed_server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
			fs := &FilerServer{filerGuard: security.NewGuard(nil, tt.jwtKey, 0, tt.jwtKey, 0, "", "")}
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

func TestFilerBasicAuthOrigin(t *testing.T) {
	fs := &FilerServer{
		filerGuard: security.NewGuard(nil, "", 0, "", 0, "", ""),
		option:     &FilerOption{AllowedOrigins: []string{"https://ui.example.com"}},
	}
	fs.basicAuth.Store(security.NewBasicAuth("admin", "pw"))
	tests := []struct {
		origin string
		want   bool
	}{
		{origin: "", want: true},
		{origin: "http://filer:8888", want: true},
		{origin: "http://FILER:8888", want: true},
		{origin: "https://ui.example.com", want: true},
		{origin: "https://evil.example.com"},
		{origin: "http://filer:9999"},
		{origin: "null"},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(http.MethodPost, "http://filer:8888/new?mv.from=/old", nil)
		r.SetBasicAuth("admin", "pw")
		if tt.origin != "" {
			r.Header.Set("Origin", tt.origin)
		}
		if got := fs.maybeCheckJwtAuthorization(r, true); got != tt.want {
			t.Errorf("origin %q: got %v, want %v", tt.origin, got, tt.want)
		}
	}

	fs.option.AllowedOrigins = []string{"*"}
	r := httptest.NewRequest(http.MethodPost, "http://filer:8888/new", nil)
	r.SetBasicAuth("admin", "pw")
	r.Header.Set("Origin", "https://evil.example.com")
	if fs.maybeCheckJwtAuthorization(r, true) {
		t.Error("the default allowed origin \"*\" let another website use Basic credentials")
	}
}

func TestFilerBasicAuthChallenge(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, "", 0, "", 0, "", "")}
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

func TestFilerBasicAuthReloadDropsRemovedPassword(t *testing.T) {
	dir := t.TempDir()
	saved := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(dir)
	t.Cleanup(func() { util.ConfigurationFileDirectory = saved })
	write := func(content string) {
		if err := os.WriteFile(filepath.Join(dir, "security.toml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	fs := &FilerServer{
		filerGuard:  security.NewGuard(nil, "", 0, "", 0, "", ""),
		volumeGuard: security.NewGuard(nil, "", 0, "", 0, "", ""),
	}
	write("[filer.basic_auth]\nuser = \"admin\"\npassword = \"pw\"\n")
	fs.Reload()
	if fs.basicAuth.Load() == nil {
		t.Fatal("Basic Auth off after reloading a file that sets it")
	}

	write("[filer.basic_auth]\nuser = \"admin\"\npassword = \"pw2\"\n")
	fs.Reload()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.SetBasicAuth("admin", "pw2")
	if !fs.basicAuth.Load().Allows(r) {
		t.Fatal("changed password not applied")
	}

	write("# Basic Auth removed\n")
	fs.Reload()
	if fs.basicAuth.Load() != nil {
		t.Error("Basic Auth still on after its section was removed")
	}

	write("[filer.basic_auth\n")
	fs.basicAuth.Store(security.NewBasicAuth("admin", "pw"))
	fs.Reload()
	if fs.basicAuth.Load() == nil {
		t.Error("an unparsable file turned Basic Auth off")
	}
	if err := os.Remove(filepath.Join(dir, "security.toml")); err != nil {
		t.Fatal(err)
	}
	fs.Reload()
	if fs.basicAuth.Load() == nil {
		t.Error("a missing file turned Basic Auth off")
	}

	write("[filer.basic_auth\n")
	t.Setenv("WEED_FILER_BASIC_AUTH_PASSWORD", "env")
	if auth, _ := loadBasicAuth(nil, ""); auth == nil {
		t.Error("an unparsable file dropped the environment password at startup")
	}
}

func TestFilerBasicAuthReloadStaysOnItsFile(t *testing.T) {
	primary, fallback := t.TempDir(), t.TempDir()
	saved := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(primary)
	t.Cleanup(func() { util.ConfigurationFileDirectory = saved })
	t.Chdir(fallback)
	for dir, content := range map[string]string{
		primary:  "[filer.basic_auth]\nuser = \"admin\"\npassword = \"pw\"\n",
		fallback: "# no Basic Auth\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, "security.toml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	fs := &FilerServer{
		filerGuard:  security.NewGuard(nil, "", 0, "", 0, "", ""),
		volumeGuard: security.NewGuard(nil, "", 0, "", 0, "", ""),
	}
	fs.Reload()
	if fs.basicAuth.Load() == nil {
		t.Fatal("Basic Auth off after reloading a file that sets it")
	}

	// The search no longer reaches the primary directory, as when it turns
	// unsearchable, so a fresh search would find the fallback file.
	util.ConfigurationFileDirectory = util.DirectoryValueType(t.TempDir())
	fs.Reload()
	if fs.basicAuth.Load() == nil {
		t.Error("reload switched to a lower-priority security.toml without Basic Auth")
	}
}
