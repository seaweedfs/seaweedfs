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
