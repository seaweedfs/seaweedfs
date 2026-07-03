package s3api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountForUnscopedIdentity(t *testing.T) {
	assert.Equal(t, "alice", accountForUnscopedIdentity("alice").Id, "a named identity gets its own account id")
	assert.NotEqual(t, AccountAdmin.Id, accountForUnscopedIdentity("alice").Id, "a named identity must not inherit the admin account")
	assert.Same(t, &AccountAdmin, accountForUnscopedIdentity(AccountAdmin.Id), "the conventional admin keeps the admin account")
	assert.Same(t, &AccountAdmin, accountForUnscopedIdentity(""), "an empty name falls back to the admin account")
}

func TestUnscopedIdentitiesGetDistinctAccounts(t *testing.T) {
	resetMemoryStore()

	config := `{
  "identities": [
    {"name": "alice", "credentials": [{"accessKey": "alice_ak", "secretKey": "alice_sk"}], "actions": ["Read"]},
    {"name": "admin", "credentials": [{"accessKey": "admin_ak", "secretKey": "admin_sk"}], "actions": ["Admin"]}
  ]
}`
	tmp, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{Config: tmp.Name()}, nil, "memory")

	alice, _, found := iam.LookupByAccessKey("alice_ak")
	require.True(t, found)
	require.NotNil(t, alice.Account)
	assert.Equal(t, "alice", alice.Account.Id, "a non-admin account-less identity owns resources as itself, not as admin")

	admin, _, found := iam.LookupByAccessKey("admin_ak")
	require.True(t, found)
	require.NotNil(t, admin.Account)
	assert.Equal(t, AccountAdmin.Id, admin.Account.Id, "the admin identity keeps the admin account")
}

// A distinct non-owner is denied an admin-owned bucket (iam nil => isUserAdmin
// false, so only real ownership grants access).
func TestCheckAccessByOwnershipDeniesNonOwner(t *testing.T) {
	adminOwner := AccountAdmin.Id
	s3a := &S3ApiServer{
		bucketRegistry: NewBucketRegistry(nil),
	}
	s3a.bucketRegistry.setMetadataCache(&BucketMetaData{Name: "b", Owner: &s3.Owner{ID: &adminOwner}})

	nonOwner := httptest.NewRequest(http.MethodGet, "/b?ownershipControls=", nil)
	nonOwner.Header.Set(s3_constants.AmzAccountId, "alice")
	assert.Equal(t, s3err.ErrAccessDenied, s3a.checkAccessByOwnership(nonOwner, "b"), "a distinct non-owner is denied the admin-owned bucket")

	owner := httptest.NewRequest(http.MethodGet, "/b?ownershipControls=", nil)
	owner.Header.Set(s3_constants.AmzAccountId, AccountAdmin.Id)
	assert.Equal(t, s3err.ErrNone, s3a.checkAccessByOwnership(owner, "b"), "the actual owner is still allowed")
}

// An account-less identity's synthesized account must be registered in the
// account lookup so its id resolves to a display name. Otherwise ACL grantee
// validation and owner display report the id as "not exists" — the regression
// where a canned PutObjectAcl granting to the caller's own account returned
// 400 InvalidRequest.
func TestUnscopedIdentityAccountResolvesByName(t *testing.T) {
	resetMemoryStore()

	config := `{
  "identities": [
    {"name": "alice", "credentials": [{"accessKey": "alice_ak", "secretKey": "alice_sk"}], "actions": ["Read", "Write"]}
  ]
}`
	tmp, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{Config: tmp.Name()}, nil, "memory")

	assert.Equal(t, "alice", iam.GetAccountNameById("alice"),
		"account-less identity id must resolve to a display name for ACL/owner validation")
}

// When an account is explicitly configured with the same id an account-less
// identity would synthesize, the identity must reuse that configured account so
// its custom display name/email are preserved.
func TestUnscopedIdentityReusesConfiguredAccount(t *testing.T) {
	resetMemoryStore()

	config := `{
  "accounts": [
    {"id": "alice", "displayName": "Alice Smith", "emailAddress": "alice@example.com"}
  ],
  "identities": [
    {"name": "alice", "credentials": [{"accessKey": "alice_ak", "secretKey": "alice_sk"}], "actions": ["Read"]}
  ]
}`
	tmp, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{Config: tmp.Name()}, nil, "memory")

	assert.Equal(t, "Alice Smith", iam.GetAccountNameById("alice"),
		"explicitly configured account display name must be preserved")

	alice, _, found := iam.LookupByAccessKey("alice_ak")
	require.True(t, found)
	require.NotNil(t, alice.Account)
	assert.Equal(t, "Alice Smith", alice.Account.DisplayName,
		"identity must reuse the configured account, not the synthesized one")
}
