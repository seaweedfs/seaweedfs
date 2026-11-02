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
	body, err := json.Marshal(payload)
	if err != nil {
		e.t.Fatalf("marshal request: %v", err)
	}
	e.auth.identity = ident
	w := httptest.NewRecorder()
	e.s.Auth(handler)(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	return w.Code
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
