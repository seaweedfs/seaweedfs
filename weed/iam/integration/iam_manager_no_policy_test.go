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
