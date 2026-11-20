package weed_server

// Unit tests of the scope rules themselves: path canonicalisation, the grant
// match, the entry shape check, event filtering, and the HTTP side of
// read_prefixes.

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// securityGenScoped mints a scoped token with an arbitrary key (for the
// wrong-key case).
func securityGenScoped(key, id string, lifetime time.Duration, rw []string) security.EncodedJwt {
	return security.GenJwtForFilerScope(security.SigningKey(key), lifetime, id, rw, nil)
}

// expiredScopedToken is a scoped token whose expiry has passed.
func expiredScopedToken() string {
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes:  []string{aliceWS},
		RegisteredClaims: jwt.RegisteredClaims{ID: "alice", ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))},
	}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(scopeTestKey))
	if err != nil {
		panic(err)
	}
	return s
}

// stsToken is an S3 session credential: same key, carries "sid".
func stsToken() string {
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes:  []string{aliceWS},
		SessionId:        "session-1",
		RegisteredClaims: jwt.RegisteredClaims{ID: "alice", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(scopeTestKey))
	if err != nil {
		panic(err)
	}
	return s
}

func TestScopeFullPath(t *testing.T) {
	cases := []struct {
		dir, name, want string
		ok              bool
	}{
		{"/a/b", "c", "/a/b/c", true},
		{"/a/b", "", "/a/b", true},
		{"/", "a", "/a", true},
		{"/", "", "/", true},
		{"/a/b/", "c", "", false},
		{"/a//b", "c", "", false},
		{"/a/../b", "c", "", false},
		{"a/b", "c", "", false},
		{"", "c", "", false},
		{"/a", "..", "", false},
		{"/a", ".", "", false},
		{"/a", "b/c", "", false},
	}
	for _, tc := range cases {
		got, err := scopeFullPath(tc.dir, tc.name)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("scopeFullPath(%q, %q) = %q, %v; want %q, ok=%v", tc.dir, tc.name, got, err, tc.want, tc.ok)
		}
	}
}

func TestScopeGrantsFromClaims(t *testing.T) {
	grants, err := scopeGrantsFromClaims([]string{aliceWS}, []string{groupG1})
	if err != nil || len(grants) != 2 || !grants[0].readWrite || grants[1].readWrite {
		t.Fatalf("grants: %v %v", grants, err)
	}
	for _, bad := range []string{"", "relative", aliceWS + "/", "/a//b", "/a/./b", "/a/../b"} {
		if _, err := scopeGrantsFromClaims([]string{bad}, nil); err == nil {
			t.Errorf("prefix %q must be rejected", bad)
		}
	}
	if _, err := scopeGrantsFromClaims(nil, []string{"/"}); err != nil {
		t.Errorf("a root read grant is a clean path: %v", err)
	}
}

func TestScopePathAllowed(t *testing.T) {
	grants, _ := scopeGrantsFromClaims([]string{aliceWS}, []string{groupG1})
	cases := []struct {
		p           string
		read, write bool
	}{
		{aliceWS, true, true},
		{aliceWS + "/x", true, true},
		{aliceWS + "2", false, false},
		{"/buckets/t1/users/alice", false, false},
		{groupG1 + "/g", true, false},
		{bobWS, false, false},
		{"", false, false},
		{"relative", false, false},
	}
	for _, tc := range cases {
		if got := scopePathAllowed(grants, tc.p, false); got != tc.read {
			t.Errorf("read %q: got %v, want %v", tc.p, got, tc.read)
		}
		if got := scopePathAllowed(grants, tc.p, true); got != tc.write {
			t.Errorf("write %q: got %v, want %v", tc.p, got, tc.write)
		}
	}
	root, _ := scopeGrantsFromClaims([]string{"/"}, nil)
	if !scopePathAllowed(root, "/anything/at/all", true) {
		t.Error("a root grant covers every path")
	}
}

func TestScopePrefixAllowed(t *testing.T) {
	grants, _ := scopeGrantsFromClaims([]string{aliceWS}, nil)
	cases := map[string]bool{
		aliceWS + "/":   true,
		aliceWS:         true,
		aliceWS + "//":  false,
		aliceWS + "2/":  false,
		"/buckets/t1/":  false,
		"/":             false,
		"relative/":     false,
		aliceWS + "/x/": true,
	}
	for prefix, want := range cases {
		if got := scopePrefixAllowed(grants, prefix); got != want {
			t.Errorf("prefix %q: got %v, want %v", prefix, got, want)
		}
	}
}

func TestScopeCheckEntryShape(t *testing.T) {
	cases := map[string]*filer_pb.Entry{
		"remote entry":        {Name: "r", RemoteEntry: &filer_pb.RemoteEntry{StorageName: "s3"}},
		"source file id":      {Name: "s", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", SourceFileId: "1,a"}}},
		"source fid":          {Name: "s", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", SourceFid: &filer_pb.FileId{VolumeId: 1}}}},
		"chunk without an id": {Name: "s", Chunks: []*filer_pb.FileChunk{{Size: 1}}},
		"manifest chunk":      {Name: "m", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", IsChunkManifest: true}}},
	}
	for label, e := range cases {
		if err := scopeCheckEntryShape(e); err == nil {
			t.Errorf("%s: want a refusal", label)
		}
	}
	if err := scopeCheckEntryShape(&filer_pb.Entry{Name: "m", Chunks: []*filer_pb.FileChunk{{FileId: "1,a", IsChunkManifest: true}}}); err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Errorf("a manifest chunk must be refused with a clear reason, got %v", err)
	}
	if err := scopeCheckEntryShape(entryWith("ok", "1,a")); err != nil {
		t.Errorf("plain chunk: %v", err)
	}
	if err := scopeCheckEntryShape(nil); err != nil {
		t.Errorf("nil entry: %v", err)
	}
}

func TestScopeFilterEvent(t *testing.T) {
	grants, _ := scopeGrantsFromClaims([]string{aliceWS}, nil)
	sc := &scope{grants: grants}
	in := ev(aliceWS, &filer_pb.EventNotification{NewEntry: entryWith("x", "1,a")})
	if sc.filterEvent(in) == nil {
		t.Error("an event inside the scope is delivered")
	}
	out := ev(bobWS, &filer_pb.EventNotification{NewEntry: entryWith("y", "2,b")})
	if sc.filterEvent(out) != nil {
		t.Error("an event outside the scope is dropped")
	}
	if sc.filterEvent(&filer_pb.SubscribeMetadataResponse{TsNs: 1}) == nil {
		t.Error("a heartbeat without an event notification is delivered")
	}
	if sc.filterEvent(ev(bobWS, &filer_pb.EventNotification{})) != nil {
		t.Error("an entry-less event outside the scope is dropped")
	}
	if sc.filterEvent(ev(aliceWS, &filer_pb.EventNotification{})) == nil {
		t.Error("an entry-less event inside the scope is delivered")
	}
	unclean := ev(aliceWS+"/", &filer_pb.EventNotification{NewEntry: entryWith("z")})
	if sc.filterEvent(unclean) != nil {
		t.Error("an event with an unclean directory is dropped")
	}
}

func TestAuthorizeFilerJwtPathsHonoursReadPrefixes(t *testing.T) {
	claims := &security.SeaweedFilerClaims{AllowedPrefixes: []string{aliceWS}, ReadPrefixes: []string{groupG1}}
	get := httptest.NewRequest("GET", groupG1+"/g", nil)
	if !authorizeFilerJwtPaths(get, claims, []string{groupG1 + "/g"}) {
		t.Error("GET inside read_prefixes is allowed")
	}
	head := httptest.NewRequest("HEAD", groupG1+"/g", nil)
	if !authorizeFilerJwtPaths(head, claims, []string{groupG1 + "/g"}) {
		t.Error("HEAD inside read_prefixes is allowed")
	}
	put := httptest.NewRequest("PUT", groupG1+"/g", nil)
	if authorizeFilerJwtPaths(put, claims, []string{groupG1 + "/g"}) {
		t.Error("PUT inside read_prefixes only is refused")
	}
	if !authorizeFilerJwtPaths(put, claims, []string{aliceWS + "/a"}) {
		t.Error("PUT inside allowed_prefixes is allowed")
	}
	if authorizeFilerJwtPaths(get, claims, []string{bobWS + "/b"}) {
		t.Error("GET outside both lists is refused")
	}
	readOnly := &security.SeaweedFilerClaims{ReadPrefixes: []string{groupG1}}
	if !readOnly.IsScoped() || authorizeFilerJwtPaths(put, readOnly, []string{groupG1 + "/g"}) {
		t.Error("a read-only token is scoped and cannot write")
	}
	if !authorizeFilerJwtPaths(put, &security.SeaweedFilerClaims{}, nil) {
		t.Error("an unscoped token is not path-checked")
	}
}

// TestAuthorizeFilerJwtPathsRequestShapes pins the HTTP scope rule for each
// request shape the filer API takes: every path a request names (the target
// and a cp.from / mv.from source, see jwtScopedRequestPaths) must be inside the
// token's scope; read_prefixes widen that scope for GET and HEAD only. A
// copy's source is read, not written, but the rule stays the upstream one
// (every path of a write within allowed_prefixes): a scoped caller copies out
// of a read-only tree by reading and writing, never by a server-side copy.
func TestAuthorizeFilerJwtPathsRequestShapes(t *testing.T) {
	const (
		t1   = "/buckets/data/t1"
		t10  = "/buckets/data/t10"
		proj = "/buckets/data/t1/groups/g1/workspace"
		own  = "/buckets/data/t1/users/alice"
	)
	tenantWide := &security.SeaweedFilerClaims{AllowedPrefixes: []string{t1}}
	readOnly := &security.SeaweedFilerClaims{ReadPrefixes: []string{t1}}
	mixed := &security.SeaweedFilerClaims{AllowedPrefixes: []string{own}, ReadPrefixes: []string{proj}}
	cases := []struct {
		name   string
		claims *security.SeaweedFilerClaims
		method string
		target string
		want   bool
	}{
		{"unscoped token, any path", &security.SeaweedFilerClaims{}, "DELETE", t10 + "/x", true},
		{"tenant-wide rw: mkdir inside", tenantWide, "POST", t1 + "/users/bob/new/", true},
		{"tenant-wide rw: rename inside", tenantWide, "POST", t1 + "/b?mv.from=" + t1 + "/a", true},
		{"tenant-wide rw: rename from another tenant", tenantWide, "POST", t1 + "/b?mv.from=" + t10 + "/a", false},
		{"tenant-wide rw: rename into another tenant", tenantWide, "POST", t10 + "/b?mv.from=" + t1 + "/a", false},
		{"tenant-wide rw: delete in another tenant", tenantWide, "DELETE", t10 + "/a", false},
		{"component boundary: t1 does not cover t10", tenantWide, "GET", t10 + "/a", false},
		{"read-only: GET inside", readOnly, "GET", t1 + "/a", true},
		{"read-only: HEAD inside", readOnly, "HEAD", t1 + "/a", true},
		{"read-only: GET outside", readOnly, "GET", t10 + "/a", false},
		{"read-only: POST inside", readOnly, "POST", t1 + "/a", false},
		{"read-only: DELETE inside", readOnly, "DELETE", t1 + "/a", false},
		{"mixed: write into rw", mixed, "PUT", own + "/a", true},
		{"mixed: write into read-only", mixed, "PUT", proj + "/a", false},
		{"mixed: copy from read-only into rw", mixed, "POST", own + "/a?cp.from=" + proj + "/a", false},
		{"mixed: move from read-only into rw", mixed, "POST", own + "/a?mv.from=" + proj + "/a", false},
		{"mixed: copy within rw", mixed, "POST", own + "/b?cp.from=" + own + "/a", true},
		{"mixed: GET in read-only", mixed, "GET", proj + "/a", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.target, nil)
			if got := authorizeFilerJwtPaths(r, c.claims, jwtScopedRequestPaths(r)); got != c.want {
				t.Errorf("%s %s: authorized=%v, want %v", c.method, c.target, got, c.want)
			}
		})
	}
}

// expiredAssignToken is an assignment token of fid for holder whose expiry
// passed a minute ago, signed with the harness key.
func expiredAssignToken(fid, holder string) string {
	claims := security.SeaweedAssignClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ID: holder, ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))}}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(security.AssignSigningKey(security.SigningKey(scopeTestKey))))
	if err != nil {
		panic(err)
	}
	return s
}
