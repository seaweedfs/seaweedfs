package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// authTestEnv drives the vector API through Auth with a chosen identity and
// the table bucket API through the catalog with the same identity, so the
// two can be compared decision by decision.
type authTestEnv struct {
	t      *testing.T
	s      *Server
	auth   *fakeAuthenticator
	tables *s3tables.Manager
	admin  *s3tables.Manager
	client s3tables.FilerClient
}

func newAuthTestEnv(t *testing.T) *authTestEnv {
	fs := s3tablestest.Start(t)
	client := s3tables.NewManagerClient(fs.Client)
	auth := &fakeAuthenticator{}
	admin := s3tables.NewManager()
	admin.SetTrusted(true)
	return &authTestEnv{t: t, s: NewServer(client, auth, newFakeVectors(0)), auth: auth,
		tables: s3tables.NewManager(), admin: admin, client: client}
}

func (e *authTestEnv) vector(ident *s3api.Identity, handler http.HandlerFunc, payload interface{}) int {
	e.t.Helper()
	return e.vectorResp(ident, handler, payload).Code
}

func (e *authTestEnv) vectorResp(ident *s3api.Identity, handler http.HandlerFunc, payload interface{}) *httptest.ResponseRecorder {
	e.t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		e.t.Fatalf("marshal request: %v", err)
	}
	e.auth.identity = ident
	w := httptest.NewRecorder()
	e.s.Auth(handler)(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	return w
}

func (e *authTestEnv) table(ident *s3api.Identity, operation string, req interface{}) error {
	ctx := s3_constants.SetIdentityInContext(contextWithIdentity(context.Background(), ident.Name), ident)
	var resp map[string]interface{}
	return e.tables.Execute(ctx, e.client, operation, req, &resp, ident.Name)
}

func tableARN(bucket string) string {
	return "arn:aws:s3tables:us-east-1:000000000000:bucket/" + bucket
}

func denyPolicy(service, principal string) string {
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":%q},"Action":"%s:*","Resource":"*"}]}`,
		principal, service)
}

func (e *authTestEnv) createBuckets(ident *s3api.Identity, name string) {
	e.t.Helper()
	if err := e.table(ident, "CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: "t-" + name}); err != nil {
		e.t.Fatalf("%s: create table bucket: %v", ident.Name, err)
	}
	if code := e.vector(ident, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "v-" + name}); code != http.StatusOK {
		e.t.Fatalf("%s: create vector bucket: %d", ident.Name, code)
	}
}

func (e *authTestEnv) deny(name, principal string) {
	e.t.Helper()
	var resp map[string]interface{}
	if err := e.admin.Execute(context.Background(), e.client, "PutTableBucketPolicy", &s3tables.PutTableBucketPolicyRequest{
		TableBucketARN: tableARN("t-" + name), ResourcePolicy: denyPolicy("s3tables", principal),
	}, &resp, ""); err != nil {
		e.t.Fatalf("put table policy: %v", err)
	}
	if code := e.vector(adminIdentity(), e.s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{
		VectorBucketName: "v-" + name, Policy: denyPolicy("s3vectors", principal),
	}); code != http.StatusOK {
		e.t.Fatalf("put vector policy: %d", code)
	}
}

// canGet reports whether ident may read the table bucket and the vector
// bucket created under name.
func (e *authTestEnv) canGet(ident *s3api.Identity, name string) (table, vector bool) {
	e.t.Helper()
	table = e.table(ident, "GetTableBucket", &s3tables.GetTableBucketRequest{TableBucketARN: tableARN("t-" + name)}) == nil
	vector = e.vector(ident, e.s.handleGetVectorBucket, &GetVectorBucketRequest{VectorBucketName: "v-" + name}) == http.StatusOK
	return table, vector
}

func adminIdentity() *s3api.Identity {
	return &s3api.Identity{Name: "admin", Account: &s3api.AccountAdmin, Actions: []s3api.Action{s3_constants.ACTION_ADMIN}}
}

var createActions = []s3api.Action{"s3tables:CreateTableBucket", "s3vectors:CreateVectorBucket"}

// A vector bucket resolves its caller the way a table bucket does: an
// identity with an account owns what it created and is bound by a Deny
// naming its account, a federated identity is its issuer-scoped user id, and
// only the admin account's admin bypasses a Deny.
func TestVectorPrincipalMatchesTableBuckets(t *testing.T) {
	cases := []struct {
		name      string
		ident     *s3api.Identity
		principal string
		owns      bool
		denied    bool
	}{
		{name: "admin", ident: adminIdentity(), principal: "admin", owns: true, denied: false},
		{name: "admin-action", ident: &s3api.Identity{Name: "ops", Account: &s3api.Account{Id: "ops"},
			Actions: []s3api.Action{s3_constants.ACTION_ADMIN}}, principal: "ops", owns: true, denied: true},
		{name: "account", ident: &s3api.Identity{Name: "carol", Account: &s3api.Account{Id: "team-acct"},
			Actions: createActions}, principal: "team-acct", owns: true, denied: true},
		{name: "federated", ident: &s3api.Identity{Name: "alice-sub", Account: &s3api.Account{Id: "alice-sub"},
			Claims: map[string]interface{}{"aws:userid": "uid-alice", "sub": "alice-sub"}, Actions: createActions},
			principal: "uid-alice", owns: true, denied: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newAuthTestEnv(t)
			e.createBuckets(c.ident, c.name)
			table, vector := e.canGet(c.ident, c.name)
			if table != c.owns || vector != c.owns {
				t.Fatalf("creator access: table=%v vector=%v, want %v", table, vector, c.owns)
			}
			e.deny(c.name, c.principal)
			table, vector = e.canGet(c.ident, c.name)
			if table != !c.denied || vector != !c.denied {
				t.Fatalf("under a deny on %q: table=%v vector=%v, want %v", c.principal, table, vector, !c.denied)
			}
		})
	}
}

func (e *authTestEnv) createIndex(bucket string) {
	e.t.Helper()
	if code := e.vector(adminIdentity(), e.s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: bucket, IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
	}); code != http.StatusOK {
		e.t.Fatalf("create index in %s: %d", bucket, code)
	}
}

func (e *authTestEnv) listedVectorBuckets(ident *s3api.Identity) []string {
	e.t.Helper()
	w := e.vectorResp(ident, e.s.handleListVectorBuckets, &ListVectorBucketsRequest{})
	if w.Code != http.StatusOK {
		e.t.Fatalf("list vector buckets: %d %s", w.Code, w.Body.String())
	}
	var resp ListVectorBucketsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		e.t.Fatalf("decode list: %v", err)
	}
	names := []string{}
	for _, b := range resp.VectorBuckets {
		names = append(names, b.VectorBucketName)
	}
	return names
}

// An identity action scoped to a bucket, s3vectors:<Op>:<bucket>, grants that
// operation on that vector bucket and its indexes only, as
// s3tables:<Op>:<bucket> does on a table bucket. A Deny still wins, and the
// scoped form never creates a bucket.
func TestBucketScopedIdentityActions(t *testing.T) {
	e := newAuthTestEnv(t)
	for _, name := range []string{"team-a", "team-b"} {
		e.createBuckets(adminIdentity(), name)
		e.createIndex("v-" + name)
	}
	scoped := &s3api.Identity{Name: "scoped", Account: &s3api.Account{Id: "scoped"}, Actions: []s3api.Action{
		"s3tables:GetTableBucket:t-team-a", "s3tables:CreateTableBucket:t-team-c",
		"s3vectors:GetVectorBucket:v-team-a", "s3vectors:GetIndex:v-team-a", "s3vectors:CreateVectorBucket:v-team-c",
	}}

	if table, vector := e.canGet(scoped, "team-a"); !table || !vector {
		t.Fatalf("granted bucket: table=%v vector=%v, want both", table, vector)
	}
	if table, vector := e.canGet(scoped, "team-b"); table || vector {
		t.Fatalf("other bucket: table=%v vector=%v, want neither", table, vector)
	}
	getIndex := func(ident *s3api.Identity, bucket string) int {
		return e.vector(ident, e.s.handleGetIndex, &GetIndexRequest{VectorBucketName: bucket, IndexName: "idx"})
	}
	getVectors := func(ident *s3api.Identity, bucket string) int {
		return e.vector(ident, e.s.handleGetVectors, &GetVectorsRequest{VectorBucketName: bucket, IndexName: "idx", Keys: []string{"a"}})
	}
	if code := getIndex(scoped, "v-team-a"); code != http.StatusOK {
		t.Fatalf("GetIndex in granted bucket: %d", code)
	}
	if code := getIndex(scoped, "v-team-b"); code != http.StatusForbidden {
		t.Fatalf("GetIndex in other bucket: %d, want 403", code)
	}
	if code := getVectors(scoped, "v-team-a"); code != http.StatusForbidden {
		t.Fatalf("ungranted GetVectors in granted bucket: %d, want 403", code)
	}
	if got := e.listedVectorBuckets(scoped); len(got) != 1 || got[0] != "v-team-a" {
		t.Fatalf("listing: %v, want [v-team-a]", got)
	}
	if err := e.table(scoped, "CreateTableBucket", &s3tables.CreateTableBucketRequest{Name: "t-team-c"}); err == nil {
		t.Fatalf("scoped CreateTableBucket created a table bucket")
	}
	if code := e.vector(scoped, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "v-team-c"}); code != http.StatusForbidden {
		t.Fatalf("scoped CreateVectorBucket: %d, want 403", code)
	}

	wild := &s3api.Identity{Name: "wild", Account: &s3api.Account{Id: "wild"}, Actions: []s3api.Action{"s3vectors:*:v-team-a"}}
	if code := getVectors(wild, "v-team-a"); code != http.StatusOK {
		t.Fatalf("wildcard grant in its bucket: %d", code)
	}
	if code := getIndex(wild, "v-team-b"); code != http.StatusForbidden {
		t.Fatalf("wildcard grant in other bucket: %d, want 403", code)
	}
	if code := e.vector(wild, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "v-team-c"}); code != http.StatusForbidden {
		t.Fatalf("wildcard bucket grant created a bucket: %d, want 403", code)
	}

	e.deny("team-a", "scoped")
	if table, vector := e.canGet(scoped, "team-a"); table || vector {
		t.Fatalf("granted bucket under a deny: table=%v vector=%v, want neither", table, vector)
	}
	if code := getIndex(scoped, "v-team-a"); code != http.StatusForbidden {
		t.Fatalf("GetIndex under a deny: %d, want 403", code)
	}
}
