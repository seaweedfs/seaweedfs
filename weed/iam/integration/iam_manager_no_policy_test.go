package integration

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeUserStore struct {
	users map[string]*iam_pb.Identity
}

func (f *fakeUserStore) GetUser(_ context.Context, name string) (*iam_pb.Identity, error) {
	if u, ok := f.users[name]; ok {
		return u, nil
	}
	return nil, nil
}

// TestIsActionAllowed_RegisteredUserWithoutPoliciesIsDenied is a regression test
// for a security issue where a user created via the S3 IAM API with no policies
// attached was granted access to every S3 action. The default-allow path was
// intended for unmanaged zero-config startup, not for explicitly registered IAM
// users with an empty policy set.
func TestIsActionAllowed_RegisteredUserWithoutPoliciesIsDenied(t *testing.T) {
	manager := NewIAMManager()
	require.NoError(t, manager.Initialize(&IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			// Reproduce the zero-config setup: with no IAM config file, the
			// loader picks DefaultEffect=Allow for in-memory mode.
			DefaultEffect: string(policy.EffectAllow),
			StoreType:     sts.StoreTypeMemory,
		},
		Roles: &RoleStoreConfig{StoreType: sts.StoreTypeMemory},
	}, func() string { return "localhost:8888" }))

	manager.SetUserStore(&fakeUserStore{users: map[string]*iam_pb.Identity{
		"newuser": {Name: "newuser"}, // no PolicyNames, no Actions
	}})

	allowed, err := manager.IsActionAllowed(context.Background(), &ActionRequest{
		Principal: "arn:aws:iam::seaweedfs:user/newuser",
		Action:    "s3:DeleteBucket",
		Resource:  "arn:aws:s3:::any-bucket",
	})
	require.NoError(t, err)
	assert.False(t, allowed, "registered user with no policies must not get default-allow access")
}

// TestIsActionAllowed_RegisteredRoleWithoutPoliciesIsDenied is the role-side
// counterpart of the registered-user regression: a managed role that exists
// but has zero AttachedPolicies must not inherit full access from the
// zero-config DefaultEffect=Allow fallback. Coderabbit flagged this as an
// uncovered branch of the same fix on PR #9317.
func TestIsActionAllowed_RegisteredRoleWithoutPoliciesIsDenied(t *testing.T) {
	manager := NewIAMManager()
	require.NoError(t, manager.Initialize(&IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: string(policy.EffectAllow),
			StoreType:     sts.StoreTypeMemory,
		},
		Roles: &RoleStoreConfig{StoreType: sts.StoreTypeMemory},
	}, func() string { return "localhost:8888" }))

	ctx := context.Background()
	require.NoError(t, manager.CreateRole(ctx, "", "EmptyRole", &RoleDefinition{
		RoleName:         "EmptyRole",
		RoleArn:          "arn:aws:iam::seaweedfs:role/EmptyRole",
		AttachedPolicies: nil, // explicit: managed role with no policies attached
	}))

	allowed, err := manager.IsActionAllowed(ctx, &ActionRequest{
		// Use the assumed-role principal form the STS path produces. The
		// IAM manager extracts role name via ExtractRoleNameFromPrincipal,
		// finds the role definition, and must deny because AttachedPolicies
		// is empty even though DefaultEffect is Allow.
		Principal: "arn:aws:sts::seaweedfs:assumed-role/EmptyRole/some-session",
		Action:    "s3:DeleteBucket",
		Resource:  "arn:aws:s3:::any-bucket",
	})
	require.NoError(t, err)
	assert.False(t, allowed, "registered role with no attached policies must not get default-allow access")
}

// TestIsActionAllowed_RegisteredUserWithNonMatchingPolicyIsDenied covers the
// adjacent case: the user has policies attached, but none of the policy
// statements match the requested action/resource. The DefaultEffect=Allow
// fallback must not silently grant access when an attached policy simply
// does not say anything about this action.
func TestIsActionAllowed_RegisteredUserWithNonMatchingPolicyIsDenied(t *testing.T) {
	manager := NewIAMManager()
	require.NoError(t, manager.Initialize(&IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: string(policy.EffectAllow),
			StoreType:     sts.StoreTypeMemory,
		},
		Roles: &RoleStoreConfig{StoreType: sts.StoreTypeMemory},
	}, func() string { return "localhost:8888" }))

	ctx := context.Background()
	require.NoError(t, manager.CreatePolicy(ctx, "", "ReadOnlyOneBucket", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::reports/*"},
		}},
	}))

	manager.SetUserStore(&fakeUserStore{users: map[string]*iam_pb.Identity{
		"alice": {Name: "alice", PolicyNames: []string{"ReadOnlyOneBucket"}},
	}})

	allowed, err := manager.IsActionAllowed(ctx, &ActionRequest{
		Principal: "arn:aws:iam::seaweedfs:user/alice",
		Action:    "s3:DeleteBucket",
		Resource:  "arn:aws:s3:::other-bucket",
	})
	require.NoError(t, err)
	assert.False(t, allowed, "action outside the user's attached policy must be denied")
}
