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

// The mirrored bucket policy decides a negated s3:ExistingObjectTag Deny on
// the tags the request context carries.
func TestMirroredBucketPolicyReadsExistingObjectTags(t *testing.T) {
	manager := NewIAMManager()
	require.NoError(t, manager.Initialize(&IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: string(policy.EffectDeny), StoreType: sts.StoreTypeMemory},
		Roles:  &RoleStoreConfig{StoreType: sts.StoreTypeMemory},
	}, func() string { return "localhost:8888" }))

	ctx := context.Background()
	require.NoError(t, manager.CreatePolicy(ctx, "", "ReadAll", &policy.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []policy.Statement{{Effect: "Allow", Action: []string{"s3:GetObject"}, Resource: []string{"arn:aws:s3:::*"}}},
	}))
	manager.SetUserStore(&fakeUserStore{users: map[string]*iam_pb.Identity{
		"app": {Name: "app", PolicyNames: []string{"ReadAll"}},
	}})
	require.NoError(t, manager.UpdateBucketPolicy(ctx, "test", []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::test/scanned/*",
		"Condition":{"StringNotEquals":{"s3:ExistingObjectTag/ScanStatus":"CLEAN"}}}]}`)))

	read := func(requestContext map[string]interface{}) bool {
		allowed, err := manager.IsActionAllowed(ctx, &ActionRequest{
			Principal:      "arn:aws:iam::seaweedfs:user/app",
			Action:         "s3:GetObject",
			Resource:       "arn:aws:s3:::test/scanned/a.txt",
			RequestContext: requestContext,
			PolicyNames:    []string{"ReadAll"},
		})
		require.NoError(t, err)
		return allowed
	}
	assert.False(t, read(map[string]interface{}{}), "an object without the tag is denied")
	assert.False(t, read(map[string]interface{}{"s3:ExistingObjectTag/ScanStatus": []string{"INFECTED"}}))
	assert.True(t, read(map[string]interface{}{"s3:ExistingObjectTag/ScanStatus": []string{"CLEAN"}}),
		"an object tagged CLEAN is readable")
}
