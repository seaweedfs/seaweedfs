package lance

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
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
	s.SetSigningKey(func() security.SigningKey { return key })

	derived := clusterTokenKey(key)
	expired := security.SeaweedFilerAdminClaims{}
	expired.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
	for _, tc := range []struct {
		name  string
		token string
		want  bool
	}{
		{"no token", "", false},
		{"signed with the derived key", string(security.GenJwtForFilerAdmin(derived, 60)), true},
		{"signed with the cluster key itself", string(security.GenJwtForFilerAdmin(key, 60)), false},
		{"signed with another key", string(security.GenJwtForFilerAdmin(clusterTokenKey(security.SigningKey("other-key")), 60)), false},
		{"expired", signAdminClaims(t, derived, expired), false},
		{"session token", signAdminClaims(t, derived, security.SeaweedFilerAdminClaims{SessionId: "session"}), false},
		{"garbage", "not-a-token", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := trustedSeenByHandler(t, s, tc.token); got != tc.want {
				t.Errorf("trusted = %v, want %v", got, tc.want)
			}
		})
	}

	token := string(security.GenJwtForFilerAdmin(derived, 60))
	key = security.SigningKey("rotated-key")
	if trustedSeenByHandler(t, s, token) {
		t.Error("a token from before a key rotation is still trusted")
	}
	if !trustedSeenByHandler(t, s, string(security.GenJwtForFilerAdmin(clusterTokenKey(key), 60))) {
		t.Error("a token signed after a key rotation is not trusted")
	}

	key = nil
	if trustedSeenByHandler(t, s, token) {
		t.Error("a server with no cluster key trusted a caller")
	}
	s.SetSigningKey(nil)
	if trustedSeenByHandler(t, s, token) {
		t.Error("a server with no key source trusted a caller")
	}
}

// refusingAuthenticator turns every caller away, as the S3 authenticator does
// for a request that signs with nothing.
type refusingAuthenticator struct{}

func (refusingAuthenticator) AuthenticateRequest(*http.Request) (string, interface{}, s3err.ErrorCode) {
	return "", nil, s3err.ErrAccessDenied
}

func (refusingAuthenticator) DefaultAllow() bool { return false }

func (refusingAuthenticator) AuthEnabled() bool { return true }

// A request carrying the cluster token alone is a cluster process: it is
// authenticated by the token, runs through the trusted manager, and is vended
// no credentials. The same request without the token is turned away.
func TestClusterTokenAloneAuthenticatesAClusterProcess(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "lnc", s3tables.FormatLance)
	h.mustDo(t, http.MethodPost, "/v1/namespace/lnc$ns/create", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/lnc$ns$t/declare", `{}`, http.StatusOK)

	key := security.SigningKey("cluster-key")
	server := NewServer(s3tables.NewManagerClient(h.filer.Client), refusingAuthenticator{})
	server.SetSigningKey(func() security.SigningKey { return key })
	server.SetCredentialVendor(recordingVendor{})
	router := mux.NewRouter().SkipClean(true)
	server.RegisterRoutes(router)
	token := string(security.GenJwtForFilerAdmin(clusterTokenKey(key), 60))

	describe := func(withToken bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/table/lnc$ns$t/describe", strings.NewReader(`{"vend_credentials":true}`))
		if withToken {
			req.Header.Set(clusterTokenHeader, token)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	if rec := describe(false); rec.Code == http.StatusOK {
		t.Fatalf("a request without a credential described the table: %s", rec.Body.String())
	}
	rec := describe(true)
	if rec.Code != http.StatusOK {
		t.Fatalf("describe with the cluster token = %d (%s)", rec.Code, rec.Body.String())
	}
	described := decode[DescribeTableResponse](t, rec)
	if described.Location != "s3://lnc/ns/t" {
		t.Fatalf("location = %q", described.Location)
	}
	if _, vended := described.StorageOptions["aws_access_key_id"]; vended {
		t.Fatalf("a cluster process was vended credentials: %v", described.StorageOptions)
	}

	list := httptest.NewRequest(http.MethodGet, "/v1/namespace/lnc$ns/table/list", nil)
	list.Header.Set(clusterTokenHeader, token)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, list)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "lnc$ns$t") {
		t.Fatalf("list with the cluster token = %d (%s)", rec.Code, rec.Body.String())
	}

	// The vector namespace, read-only to every other Lance caller, takes a
	// cluster process's register and deregister.
	h.seedVectorIndex(t, "vec", s3tables.FormatLance)
	for _, tc := range []struct{ target, body string }{
		{"/v1/table/vec$vector$idx/register", `{"location":"s3://vec/vector/idx","mode":"overwrite"}`},
		{"/v1/table/vec$vector$idx/deregister", `{}`},
	} {
		req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
		req.Header.Set(clusterTokenHeader, token)
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s with the cluster token = %d (%s)", tc.target, rec.Code, rec.Body.String())
		}
	}
}

// The Rust catalog client derives the same key; its tests pin this value too.
func TestClusterTokenKeyDerivation(t *testing.T) {
	got := hex.EncodeToString(clusterTokenKey(security.SigningKey("cluster-key")))
	if want := "b38c0b66da41621f2a9b6de68e4239ddd0d2dd7b4ede01aa91bd34a17edc1021"; got != want {
		t.Errorf("clusterTokenKey = %s, want %s", got, want)
	}
}
