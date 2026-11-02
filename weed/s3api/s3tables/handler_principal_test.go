package s3tables

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestGetAccountIDRejectsHeaderAdmin(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(s3_constants.AmzAccountId, s3_constants.AccountAdminId)

	assert.NotEqual(t, s3_constants.AccountAdminId, h.getAccountID(req),
		"a client-supplied account header must not resolve to the admin principal")
}

func TestGetAccountIDHeaderBranchResolvesCaller(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(s3_constants.AmzAccountId, "alice")

	assert.Equal(t, "alice", h.getAccountID(req))
}

func TestPrincipalForMirrorsGetAccountID(t *testing.T) {
	m := NewManager()
	m.SetAccountID("000000000000")

	named := func(name string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		return req.WithContext(s3_constants.SetIdentityNameInContext(req.Context(), name))
	}
	withIdentity := func(id *testIdentity) *http.Request {
		req := named(id.Name)
		return req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))
	}

	assert.Equal(t, "000000000000", m.PrincipalFor(nil))
	assert.Equal(t, "000000000000", m.PrincipalFor(named("")))
	assert.Equal(t, "alice", m.PrincipalFor(named("alice")))
	assert.Equal(t, s3_constants.AccountAdminId, m.PrincipalFor(named(s3_constants.AccountAdminId)),
		"the name branch of getAccountID keeps the admin name")
	assert.Equal(t, "000000000000", m.PrincipalFor(named("   ")))
	assert.Equal(t, "bob", m.PrincipalFor(named("arn:aws:iam::123456789012:user/bob")),
		"ARN identities collapse to the trailing segment like getAccountID")
	assert.Equal(t, "team-acct", m.PrincipalFor(withIdentity(&testIdentity{Name: "carol", Account: &testIdentityAccount{Id: "team-acct"}})),
		"an identity with an account is its account, not its name")
	assert.Equal(t, "uid-alice", m.PrincipalFor(withIdentity(&testIdentity{Name: "alice-sub", Account: &testIdentityAccount{Id: "alice-sub"},
		Claims: map[string]interface{}{"aws:userid": "uid-alice"}})),
		"a federated identity is its issuer-scoped user id")
}

func TestCheckPermissionDenyPolicyBindsNonAdmin(t *testing.T) {
	denyAll := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:DeleteTableBucket"}]}`
	assert.False(t, CheckPermissionWithContext("s3tables:DeleteTableBucket", "mallory", "owner123", denyAll, "arn:aws:s3tables:us-east-1:000000000000:bucket/victim",
		&PolicyContext{DefaultAllow: true}))
}
