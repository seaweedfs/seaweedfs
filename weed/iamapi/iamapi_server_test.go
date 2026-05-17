package iamapi

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIamS3ApiConfigureForTest builds an IamS3ApiConfigure backed by a memory
// credential store and no filer. The filer-less option is fine because the
// new code only touches the filer for inline/group policies, which these
// tests don't exercise.
func newIamS3ApiConfigureForTest(t *testing.T) (*IamS3ApiConfigure, *credential.CredentialManager) {
	t.Helper()
	store := &memory.MemoryStore{}
	require.NoError(t, store.Initialize(nil, ""))
	cm := &credential.CredentialManager{Store: store}
	cfg := &IamS3ApiConfigure{
		option:            &IamServerOption{},
		credentialManager: cm,
	}
	return cfg, cm
}

func samplePolicyDocument(resource string) policy_engine.PolicyDocument {
	return policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:Get*"),
				Resource: policy_engine.NewStringOrStringSlicePtr(resource),
			},
		},
	}
}

// TestPutPoliciesCreatesInCredentialStore is the regression test for
// https://github.com/seaweedfs/seaweedfs/issues/9518: the IAM API used to
// write managed policies straight to the filer, bypassing any non-filer
// credential store (e.g. postgres). After the fix, a new policy passed to
// PutPolicies must show up in the credential store.
func TestPutPoliciesCreatesInCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	doc := samplePolicyDocument("arn:aws:s3:::bucket-a")
	require.NoError(t, cfg.PutPolicies(&Policies{
		Policies: map[string]policy_engine.PolicyDocument{"policy-a": doc},
	}))

	stored, err := cm.GetPolicies(ctx)
	require.NoError(t, err)
	require.Contains(t, stored, "policy-a")
	assert.Equal(t, doc, stored["policy-a"])
}

// TestGetPoliciesReadsFromCredentialStore mirrors the read side of the bug:
// ListPolicies / GetPolicy via the IAM API used to read only the filer file
// and therefore missed policies that the Admin UI had written to the store.
func TestGetPoliciesReadsFromCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	doc := samplePolicyDocument("arn:aws:s3:::bucket-b")
	require.NoError(t, cm.CreatePolicy(ctx, "policy-b", doc))

	loaded := Policies{}
	require.NoError(t, cfg.GetPolicies(&loaded))
	require.Contains(t, loaded.Policies, "policy-b")
	assert.Equal(t, doc, loaded.Policies["policy-b"])
}

// TestPutPoliciesDeletesRemovedFromCredentialStore makes sure the delta path
// translates an in-memory deletion (the pattern the DeletePolicy handler
// uses) into a credential-store DeletePolicy call.
func TestPutPoliciesDeletesRemovedFromCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	keep := samplePolicyDocument("arn:aws:s3:::keep")
	drop := samplePolicyDocument("arn:aws:s3:::drop")
	require.NoError(t, cm.CreatePolicy(ctx, "keep", keep))
	require.NoError(t, cm.CreatePolicy(ctx, "drop", drop))

	// Caller fetched policies, removed "drop", and asked us to persist.
	require.NoError(t, cfg.PutPolicies(&Policies{
		Policies: map[string]policy_engine.PolicyDocument{"keep": keep},
	}))

	remaining, err := cm.GetPolicies(ctx)
	require.NoError(t, err)
	assert.Contains(t, remaining, "keep")
	assert.NotContains(t, remaining, "drop")
}

// TestPutPoliciesUpdatesChangedInCredentialStore confirms the delta also
// detects document changes for the same policy name.
func TestPutPoliciesUpdatesChangedInCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	original := samplePolicyDocument("arn:aws:s3:::v1")
	require.NoError(t, cm.CreatePolicy(ctx, "p", original))

	updated := samplePolicyDocument("arn:aws:s3:::v2")
	require.NoError(t, cfg.PutPolicies(&Policies{
		Policies: map[string]policy_engine.PolicyDocument{"p": updated},
	}))

	stored, err := cm.GetPolicy(ctx, "p")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, updated, *stored)
}

// TestPutPoliciesRoutesUserInlineThroughCredentialStore is the second half of
// the #9518 fix: per-user inline policies created via the IAM API must land
// in the credential store (so postgres users have a single source of truth)
// rather than in the filer bundle.
func TestPutPoliciesRoutesUserInlineThroughCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	doc := samplePolicyDocument("arn:aws:s3:::user-bucket")
	require.NoError(t, cfg.PutPolicies(&Policies{
		InlinePolicies: map[string]map[string]policy_engine.PolicyDocument{
			"alice": {"inline-1": doc},
		},
	}))

	stored, err := cm.GetUserInlinePolicy(ctx, "alice", "inline-1")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, doc, *stored)
}

// TestPutPoliciesDeletesRemovedUserInline mirrors the deletion side. The
// DeleteUserPolicy handler removes the entry from the in-memory map and calls
// PutPolicies; the delta must call DeleteUserInlinePolicy.
func TestPutPoliciesDeletesRemovedUserInline(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	keep := samplePolicyDocument("arn:aws:s3:::keep")
	drop := samplePolicyDocument("arn:aws:s3:::drop")
	require.NoError(t, cm.PutUserInlinePolicy(ctx, "bob", "keep", keep))
	require.NoError(t, cm.PutUserInlinePolicy(ctx, "bob", "drop", drop))

	require.NoError(t, cfg.PutPolicies(&Policies{
		InlinePolicies: map[string]map[string]policy_engine.PolicyDocument{
			"bob": {"keep": keep},
		},
	}))

	remaining, err := cm.ListUserInlinePolicies(ctx, "bob")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"keep"}, remaining)
}

// TestPutPoliciesRoutesGroupInlineThroughCredentialStore confirms group-attached
// inline policies also persist via the credential manager rather than the
// shared filer bundle.
func TestPutPoliciesRoutesGroupInlineThroughCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	doc := samplePolicyDocument("arn:aws:s3:::group-bucket")
	require.NoError(t, cfg.PutPolicies(&Policies{
		GroupInlinePolicies: map[string]map[string]policy_engine.PolicyDocument{
			"devs": {"inline-g": doc},
		},
	}))

	stored, err := cm.GetGroupInlinePolicy(ctx, "devs", "inline-g")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, doc, *stored)
}

// TestPutPoliciesDeletesRemovedGroupInline covers the deletion delta for
// group-attached inline policies.
func TestPutPoliciesDeletesRemovedGroupInline(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	keep := samplePolicyDocument("arn:aws:s3:::keep")
	drop := samplePolicyDocument("arn:aws:s3:::drop")
	require.NoError(t, cm.PutGroupInlinePolicy(ctx, "devs", "keep", keep))
	require.NoError(t, cm.PutGroupInlinePolicy(ctx, "devs", "drop", drop))

	require.NoError(t, cfg.PutPolicies(&Policies{
		GroupInlinePolicies: map[string]map[string]policy_engine.PolicyDocument{
			"devs": {"keep": keep},
		},
	}))

	remaining, err := cm.ListGroupInlinePolicies(ctx, "devs")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"keep"}, remaining)
}

// TestGetPoliciesReadsInlineAndGroupInlineFromCredentialStore confirms reads
// pull user-inline and group-inline policies from the store too, so handlers
// like ListUserPolicies / GetUserPolicy see Admin-UI writes.
func TestGetPoliciesReadsInlineAndGroupInlineFromCredentialStore(t *testing.T) {
	cfg, cm := newIamS3ApiConfigureForTest(t)
	ctx := context.Background()

	userDoc := samplePolicyDocument("arn:aws:s3:::user-data")
	groupDoc := samplePolicyDocument("arn:aws:s3:::group-data")
	require.NoError(t, cm.PutUserInlinePolicy(ctx, "carol", "u-policy", userDoc))
	require.NoError(t, cm.PutGroupInlinePolicy(ctx, "admins", "g-policy", groupDoc))

	loaded := Policies{}
	require.NoError(t, cfg.GetPolicies(&loaded))

	require.Contains(t, loaded.InlinePolicies, "carol")
	assert.Equal(t, userDoc, loaded.InlinePolicies["carol"]["u-policy"])
	require.Contains(t, loaded.GroupInlinePolicies, "admins")
	assert.Equal(t, groupDoc, loaded.GroupInlinePolicies["admins"]["g-policy"])
}
