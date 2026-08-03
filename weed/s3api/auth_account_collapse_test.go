package s3api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
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

// Users created through the IAM API carry their account inline on the identity,
// and no credential store emits a top-level accounts list. Such an account must
// be registered rather than collapsed into the admin account, which gave every
// one of those users the same owner id for ownership checks.
func TestInlineIdentityAccountIsRegistered(t *testing.T) {
	resetMemoryStore()

	config := `{
  "identities": [
    {"name": "alice", "account": {"id": "100000000001", "displayName": "Alice", "emailAddress": "alice@example.com"}, "credentials": [{"accessKey": "alice_ak", "secretKey": "alice_sk"}], "actions": ["Read", "Write"]},
    {"name": "bob", "account": {"id": "100000000002", "displayName": "Bob", "emailAddress": "bob@example.com"}, "credentials": [{"accessKey": "bob_ak", "secretKey": "bob_sk"}], "actions": ["Read", "Write"]}
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
	assert.Equal(t, "100000000001", alice.Account.Id, "an undeclared inline account must not collapse into admin")

	bob, _, found := iam.LookupByAccessKey("bob_ak")
	require.True(t, found)
	require.NotNil(t, bob.Account)
	assert.NotEqual(t, alice.Account.Id, bob.Account.Id, "distinct users must not share an owner id")

	assert.Equal(t, "Alice", iam.GetAccountNameById("100000000001"), "the registered account must resolve for ACL/owner display")
	assert.Equal(t, "100000000002", iam.GetAccountIdByEmail("bob@example.com"), "the registered account must resolve by email")
}

// A dynamic update carries a single identity with no accounts list, so the merge
// path must register the inline account the same way the full load does.
func TestInlineIdentityAccountIsRegisteredOnUpsert(t *testing.T) {
	resetMemoryStore()

	config := `{
  "identities": [
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

	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "alice",
		Account:     &iam_pb.Account{Id: "100000000001", DisplayName: "Alice", EmailAddress: "alice@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "alice_ak", SecretKey: "alice_sk"}},
		Actions:     []string{"Read", "Write"},
	}))

	alice, _, found := iam.LookupByAccessKey("alice_ak")
	require.True(t, found)
	require.NotNil(t, alice.Account)
	assert.Equal(t, "100000000001", alice.Account.Id, "a pushed identity must keep its own account id")
	assert.NotEqual(t, AccountAdmin.Id, alice.Account.Id, "a pushed identity must not inherit the admin account")
	assert.Equal(t, "Alice", iam.GetAccountNameById("100000000001"), "the registered account must resolve for ACL/owner display")

	// Changing the email keeps the account id, so the merge starts from a cached
	// account that still carries the old address.
	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "alice",
		Account:     &iam_pb.Account{Id: "100000000001", DisplayName: "Alice Smith", EmailAddress: "alice.smith@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "alice_ak", SecretKey: "alice_sk"}},
		Actions:     []string{"Read", "Write"},
	}))

	assert.Equal(t, "100000000001", iam.GetAccountIdByEmail("alice.smith@example.com"), "the new email must be indexed")
	assert.Empty(t, iam.GetAccountIdByEmail("alice@example.com"), "the replaced email must no longer resolve")
	assert.Equal(t, "Alice Smith", iam.GetAccountNameById("100000000001"), "the display name must follow the update")
}

// Two inline accounts can carry the same email. The first to register keeps the
// lookup, and the loser can claim it once the holder moves away — otherwise an
// email freed by an update would resolve to nobody.
func TestInlineAccountEmailClaimAndHandoff(t *testing.T) {
	resetMemoryStore()

	config := `{
  "identities": [
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

	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "alice",
		Account:     &iam_pb.Account{Id: "100000000001", DisplayName: "Alice", EmailAddress: "shared@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "alice_ak", SecretKey: "alice_sk"}},
		Actions:     []string{"Read"},
	}))
	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "bob",
		Account:     &iam_pb.Account{Id: "100000000002", DisplayName: "Bob", EmailAddress: "shared@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "bob_ak", SecretKey: "bob_sk"}},
		Actions:     []string{"Read"},
	}))

	assert.Equal(t, "100000000001", iam.GetAccountIdByEmail("shared@example.com"), "the first account to claim an email keeps it")

	// alice moves off the shared address, freeing it
	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "alice",
		Account:     &iam_pb.Account{Id: "100000000001", DisplayName: "Alice", EmailAddress: "alice@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "alice_ak", SecretKey: "alice_sk"}},
		Actions:     []string{"Read"},
	}))
	assert.Equal(t, "100000000001", iam.GetAccountIdByEmail("alice@example.com"), "the moved account indexes its new email")

	// bob re-syncs unchanged and picks up the address he never got to claim
	require.NoError(t, iam.UpsertIdentity(&iam_pb.Identity{
		Name:        "bob",
		Account:     &iam_pb.Account{Id: "100000000002", DisplayName: "Bob", EmailAddress: "shared@example.com"},
		Credentials: []*iam_pb.Credential{{AccessKey: "bob_ak", SecretKey: "bob_sk"}},
		Actions:     []string{"Read"},
	}))
	assert.Equal(t, "100000000002", iam.GetAccountIdByEmail("shared@example.com"), "a freed email resolves to the account still holding it")
}

// An account declared in a top-level accounts list outranks an identity's inline
// block, which must not rewrite its metadata or take over its email.
func TestDeclaredAccountOutranksInlineIdentityAccount(t *testing.T) {
	resetMemoryStore()

	config := `{
  "accounts": [
    {"id": "100000000001", "displayName": "Alice Smith", "emailAddress": "alice@example.com"}
  ],
  "identities": [
    {"name": "alice", "account": {"id": "100000000001", "displayName": "wrong", "emailAddress": "wrong@example.com"}, "credentials": [{"accessKey": "alice_ak", "secretKey": "alice_sk"}], "actions": ["Read"]}
  ]
}`
	tmp, err := os.CreateTemp("", "s3-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{Config: tmp.Name()}, nil, "memory")

	assert.Equal(t, "Alice Smith", iam.GetAccountNameById("100000000001"), "the declared display name must win")
	assert.Equal(t, "100000000001", iam.GetAccountIdByEmail("alice@example.com"), "the declared email must stay indexed")
	assert.Empty(t, iam.GetAccountIdByEmail("wrong@example.com"), "an inline block must not claim an email for a declared account")
}
