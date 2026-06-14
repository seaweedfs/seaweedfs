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
		bucketRegistry: &BucketRegistry{
			metadataCache: map[string]*BucketMetaData{
				"b": {Name: "b", Owner: &s3.Owner{ID: &adminOwner}},
			},
			notFound: map[string]struct{}{},
		},
	}

	nonOwner := httptest.NewRequest(http.MethodGet, "/b?ownershipControls=", nil)
	nonOwner.Header.Set(s3_constants.AmzAccountId, "alice")
	assert.Equal(t, s3err.ErrAccessDenied, s3a.checkAccessByOwnership(nonOwner, "b"), "a distinct non-owner is denied the admin-owned bucket")

	owner := httptest.NewRequest(http.MethodGet, "/b?ownershipControls=", nil)
	owner.Header.Set(s3_constants.AmzAccountId, AccountAdmin.Id)
	assert.Equal(t, s3err.ErrNone, s3a.checkAccessByOwnership(owner, "b"), "the actual owner is still allowed")
}
