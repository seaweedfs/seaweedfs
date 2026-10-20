package s3api

// Targeted-write regression tests: single-identity IAM actions must write and
// propagate only the identity they changed, not every identity via
// SaveConfiguration. See https://github.com/seaweedfs/seaweedfs/issues/11865.

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingStore wraps a memory store and counts the write methods the
// dispatcher used, so a test can assert the single-identity path was taken and
// SaveConfiguration was not. It embeds the concrete store (rather than the
// CredentialStore interface) so optional interfaces like InlinePolicyStore and
// UserRenamer stay visible through the wrapper.
type recordingStore struct {
	*memory.MemoryStore
	saves            int
	createAccessKeys int
	deleteAccessKeys int
	updateUsers      int
	deleteUsers      int
	updateGroups     int
}

func (r *recordingStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	r.saves++
	return r.MemoryStore.SaveConfiguration(ctx, config)
}

func (r *recordingStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	r.createAccessKeys++
	return r.MemoryStore.CreateAccessKey(ctx, username, cred)
}

func (r *recordingStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	r.deleteAccessKeys++
	return r.MemoryStore.DeleteAccessKey(ctx, username, accessKey)
}

func (r *recordingStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	r.updateUsers++
	return r.MemoryStore.UpdateUser(ctx, username, identity)
}

func (r *recordingStore) DeleteUser(ctx context.Context, username string) error {
	r.deleteUsers++
	return r.MemoryStore.DeleteUser(ctx, username)
}

func (r *recordingStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	r.updateGroups++
	return r.MemoryStore.UpdateGroup(ctx, group)
}

// newTargetedWriteTestApi seeds cfg into the store and wraps the store in a
// recordingStore. The initial sync save happens before the recorder is
// installed so it does not count.
func newTargetedWriteTestApi(t *testing.T, cfg *iam_pb.S3ApiConfiguration) (*EmbeddedIamApiForTest, *recordingStore) {
	t.Helper()
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = cfg
	// Trigger the one-time mockConfig -> store seed through a read action.
	_, iamErr := api.ExecuteAction(context.Background(), url.Values{"Action": {"ListUsers"}}, false, "")
	require.Nil(t, iamErr)
	rec := &recordingStore{MemoryStore: api.credentialManager.Store.(*memory.MemoryStore)}
	api.credentialManager.Store = rec
	return api, rec
}

func executeAction(t *testing.T, api *EmbeddedIamApiForTest, vals url.Values) {
	t.Helper()
	_, iamErr := api.ExecuteAction(context.Background(), vals, false, "")
	require.Nil(t, iamErr)
}

func TestEmbeddedIamCreateAccessKeyTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
	})
	ctx := context.Background()

	resp, iamErr := api.ExecuteAction(ctx, url.Values{
		"Action":   {"CreateAccessKey"},
		"UserName": {"alice"},
	}, false, "")
	require.Nil(t, iamErr)
	require.Equal(t, 0, rec.saves, "CreateAccessKey must not SaveConfiguration")
	require.Equal(t, 1, rec.createAccessKeys)

	out := resp.(*iamCreateAccessKeyResponse)
	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, ident.Credentials, 1)
	assert.Equal(t, *out.CreateAccessKeyResult.AccessKey.AccessKeyId, ident.Credentials[0].AccessKey)

	// The new key resolves in the store.
	byKey, err := api.credentialManager.GetUserByAccessKey(ctx, *out.CreateAccessKeyResult.AccessKey.AccessKeyId)
	require.NoError(t, err)
	assert.Equal(t, "alice", byKey.Name)
}

func TestEmbeddedIamDeleteAccessKeyTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "alice",
			Credentials: []*iam_pb.Credential{{AccessKey: "AKIATESTFAKEKEY00001", SecretKey: "testsecretfake"}},
		}},
	})
	ctx := context.Background()

	executeAction(t, api, url.Values{
		"Action":      {"DeleteAccessKey"},
		"UserName":    {"alice"},
		"AccessKeyId": {"AKIATESTFAKEKEY00001"},
	})
	require.Equal(t, 0, rec.saves, "DeleteAccessKey must not SaveConfiguration")
	require.Equal(t, 1, rec.deleteAccessKeys)

	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, ident.Credentials)
	_, err = api.credentialManager.GetUserByAccessKey(ctx, "AKIATESTFAKEKEY00001")
	require.ErrorIs(t, err, credential.ErrAccessKeyNotFound)
}

func TestEmbeddedIamUpdateAccessKeyTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name: "alice",
			Credentials: []*iam_pb.Credential{
				{AccessKey: "AKIATESTFAKEKEY00001", SecretKey: "testsecretfake", Status: iamAccessKeyStatusActive},
			},
		}},
	})
	ctx := context.Background()

	executeAction(t, api, url.Values{
		"Action":      {"UpdateAccessKey"},
		"UserName":    {"alice"},
		"AccessKeyId": {"AKIATESTFAKEKEY00001"},
		"Status":      {iamAccessKeyStatusInactive},
	})
	require.Equal(t, 0, rec.saves, "UpdateAccessKey must not SaveConfiguration")
	require.Equal(t, 1, rec.updateUsers)

	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, ident.Credentials, 1)
	assert.Equal(t, iamAccessKeyStatusInactive, ident.Credentials[0].Status)
}

const iamTargetedTestPolicy = `{
	"Version": "2012-10-17",
	"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::bench/*"]}]
}`

func TestEmbeddedIamPutUserPolicyTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
	})
	ctx := context.Background()

	executeAction(t, api, url.Values{
		"Action":         {"PutUserPolicy"},
		"UserName":       {"alice"},
		"PolicyName":     {"read-bench"},
		"PolicyDocument": {iamTargetedTestPolicy},
	})
	require.Equal(t, 0, rec.saves, "PutUserPolicy must not SaveConfiguration")
	require.Equal(t, 1, rec.updateUsers)

	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.NotEmpty(t, ident.Actions, "identity Actions must be recomputed and persisted")

	doc, err := api.credentialManager.GetUserInlinePolicy(ctx, "alice", "read-bench")
	require.NoError(t, err)
	require.NotNil(t, doc)
}

func TestEmbeddedIamDeleteUserPolicyTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	})
	ctx := context.Background()

	executeAction(t, api, url.Values{
		"Action":         {"PutUserPolicy"},
		"UserName":       {"alice"},
		"PolicyName":     {"read-bench"},
		"PolicyDocument": {iamTargetedTestPolicy},
	})
	require.Equal(t, 1, rec.updateUsers)

	executeAction(t, api, url.Values{
		"Action":     {"DeleteUserPolicy"},
		"UserName":   {"alice"},
		"PolicyName": {"read-bench"},
	})
	require.Equal(t, 0, rec.saves, "DeleteUserPolicy must not SaveConfiguration")
	require.Equal(t, 2, rec.updateUsers)

	policies, err := api.credentialManager.ListUserInlinePolicies(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, policies)
	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, ident.Actions)
}

func TestEmbeddedIamSetUserStatusTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	})
	ctx := context.Background()

	executeAction(t, api, url.Values{
		"Action":   {"SetUserStatus"},
		"UserName": {"alice"},
		"Status":   {iamAccessKeyStatusInactive},
	})
	require.Equal(t, 0, rec.saves, "SetUserStatus must not SaveConfiguration")
	require.Equal(t, 1, rec.updateUsers)

	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, ident.Disabled)
}

func TestEmbeddedIamUpdateUserRenameTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
		Groups: []*iam_pb.Group{
			{Name: "devs", Members: []string{"alice", "bob"}},
			{Name: "ops", Members: []string{"bob"}},
		},
	})
	ctx := context.Background()
	// Groups in the seed config must also be created in the store directly:
	// the memory store only surfaces groups written via CreateGroup.
	require.NoError(t, api.credentialManager.CreateGroup(ctx, &iam_pb.Group{Name: "devs", Members: []string{"alice", "bob"}}))
	require.NoError(t, api.credentialManager.CreateGroup(ctx, &iam_pb.Group{Name: "ops", Members: []string{"bob"}}))
	require.NoError(t, api.credentialManager.PutUserInlinePolicy(ctx, "alice", "p1", mustParseIamTargetedTestPolicy(t)))

	executeAction(t, api, url.Values{
		"Action":      {"UpdateUser"},
		"UserName":    {"alice"},
		"NewUserName": {"alice-renamed"},
	})
	require.Equal(t, 0, rec.saves, "UpdateUser rename must not SaveConfiguration")

	_, err := api.credentialManager.GetUser(ctx, "alice")
	require.ErrorIs(t, err, credential.ErrUserNotFound)
	_, err = api.credentialManager.GetUser(ctx, "alice-renamed")
	require.NoError(t, err)

	// Group membership and inline policies follow the rename.
	g, err := api.credentialManager.GetGroup(ctx, "devs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice-renamed", "bob"}, g.Members)
	g, err = api.credentialManager.GetGroup(ctx, "ops")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob"}, g.Members)

	policies, err := api.credentialManager.ListUserInlinePolicies(ctx, "alice-renamed")
	require.NoError(t, err)
	assert.Equal(t, []string{"p1"}, policies)
	policies, err = api.credentialManager.ListUserInlinePolicies(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, policies)
}

func TestEmbeddedIamDeleteUserTargeted(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
		Groups:     []*iam_pb.Group{{Name: "devs", Members: []string{"alice", "bob"}}},
	})
	ctx := context.Background()
	require.NoError(t, api.credentialManager.CreateGroup(ctx, &iam_pb.Group{Name: "devs", Members: []string{"alice", "bob"}}))

	executeAction(t, api, url.Values{
		"Action":   {"DeleteUser"},
		"UserName": {"alice"},
	})
	require.Equal(t, 0, rec.saves, "DeleteUser must not SaveConfiguration")
	require.Equal(t, 1, rec.deleteUsers)
	require.Equal(t, 1, rec.updateGroups)

	_, err := api.credentialManager.GetUser(ctx, "alice")
	require.ErrorIs(t, err, credential.ErrUserNotFound)
	g, err := api.credentialManager.GetGroup(ctx, "devs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob"}, g.Members)
	_, err = api.credentialManager.GetUser(ctx, "bob")
	require.NoError(t, err, "other identities must be untouched")
}

// TestEmbeddedIamTargetedSkipPersist: skipPersist must suppress the targeted
// write exactly like it suppresses the full save.
func TestEmbeddedIamTargetedSkipPersist(t *testing.T) {
	api, rec := newTargetedWriteTestApi(t, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	})
	ctx := context.Background()

	for _, vals := range []url.Values{
		{"Action": {"CreateAccessKey"}, "UserName": {"alice"}},
		{"Action": {"SetUserStatus"}, "UserName": {"alice"}, "Status": {iamAccessKeyStatusInactive}},
		{"Action": {"PutUserPolicy"}, "UserName": {"alice"}, "PolicyName": {"p"}, "PolicyDocument": {iamTargetedTestPolicy}},
	} {
		_, iamErr := api.ExecuteAction(ctx, vals, true /*skipPersist*/, "")
		require.Nil(t, iamErr, "action %v", vals.Get("Action"))
	}
	assert.Equal(t, 0, rec.saves)
	assert.Equal(t, 0, rec.createAccessKeys)
	assert.Equal(t, 0, rec.updateUsers)

	ident, err := api.credentialManager.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Empty(t, ident.Credentials)
	assert.False(t, ident.Disabled)
}

// TestEmbeddedIamStaticIdentityKeepsFullSave: static identities live outside
// the store, so a change to one still takes the full-save path.
func TestEmbeddedIamStaticIdentityKeepsFullSave(t *testing.T) {
	api := NewEmbeddedIamApiForTest()
	api.mockConfig = &iam_pb.S3ApiConfiguration{}
	api.credentialManager.SetStaticIdentities([]*iam_pb.Identity{{Name: "static-admin"}})
	rec := &recordingStore{MemoryStore: api.credentialManager.Store.(*memory.MemoryStore)}
	api.credentialManager.Store = rec

	// Seed via a read action; static identities are merged into the loaded config.
	_, iamErr := api.ExecuteAction(context.Background(), url.Values{"Action": {"ListUsers"}}, false, "")
	require.Nil(t, iamErr)
	rec.saves = 0

	executeAction(t, api, url.Values{
		"Action":   {"SetUserStatus"},
		"UserName": {"static-admin"},
		"Status":   {iamAccessKeyStatusInactive},
	})
	assert.Equal(t, 1, rec.saves, "static identity changes keep the full-save path")
	assert.Equal(t, 0, rec.updateUsers)
}

func mustParseIamTargetedTestPolicy(t *testing.T) (doc policy_engine.PolicyDocument) {
	t.Helper()
	require.NoError(t, json.Unmarshal([]byte(iamTargetedTestPolicy), &doc))
	return doc
}
