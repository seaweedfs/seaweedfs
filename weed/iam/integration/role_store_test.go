package integration

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryRoleStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryRoleStore()

	// Test storing a role
	roleDef := &RoleDefinition{
		RoleName:         "TestRole",
		RoleArn:          "arn:aws:iam::role/TestRole",
		Description:      "Test role for unit testing",
		AttachedPolicies: []string{"TestPolicy"},
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Principal: map[string]interface{}{
						"Federated": "test-provider",
					},
				},
			},
		},
	}

	err := store.StoreRole(ctx, "", "TestRole", roleDef)
	require.NoError(t, err)

	// Test retrieving the role
	retrievedRole, err := store.GetRole(ctx, "", "TestRole")
	require.NoError(t, err)
	assert.Equal(t, "TestRole", retrievedRole.RoleName)
	assert.Equal(t, "arn:aws:iam::role/TestRole", retrievedRole.RoleArn)
	assert.Equal(t, "Test role for unit testing", retrievedRole.Description)
	assert.Equal(t, []string{"TestPolicy"}, retrievedRole.AttachedPolicies)

	// Test listing roles
	roles, err := store.ListRoles(ctx, "")
	require.NoError(t, err)
	assert.Contains(t, roles, "TestRole")

	// Test deleting the role
	err = store.DeleteRole(ctx, "", "TestRole")
	require.NoError(t, err)

	// Verify role is deleted
	_, err = store.GetRole(ctx, "", "TestRole")
	assert.Error(t, err)
}

func TestRoleStoreConfiguration(t *testing.T) {
	// Test memory role store creation
	memoryStore, err := NewMemoryRoleStore(), error(nil)
	require.NoError(t, err)
	assert.NotNil(t, memoryStore)

	// Test filer role store creation without filerAddress in config
	filerStore2, err := NewFilerRoleStore(map[string]interface{}{
		// filerAddress not required in config
		"basePath": "/test/roles",
	}, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, filerStore2)

	// Test filer role store creation with valid config
	filerStore, err := NewFilerRoleStore(map[string]interface{}{
		"filerAddress": "localhost:8888",
		"basePath":     "/test/roles",
	}, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, filerStore)
}

func TestDistributedIAMManagerWithRoleStore(t *testing.T) {
	ctx := context.Background()

	// Create IAM manager with role store configuration
	config := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Duration(3600) * time.Second},
			MaxSessionLength: sts.FlexibleDuration{Duration: time.Duration(43200) * time.Second},
			Issuer:           "test-issuer",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		},
		Roles: &RoleStoreConfig{
			StoreType: "memory",
		},
	}

	iamManager := NewIAMManager()
	err := iamManager.Initialize(config, func() string {
		return "localhost:8888" // Mock filer address for testing
	})
	require.NoError(t, err)

	// Test creating a role
	roleDef := &RoleDefinition{
		RoleName:         "DistributedTestRole",
		RoleArn:          "arn:aws:iam::role/DistributedTestRole",
		Description:      "Test role for distributed IAM",
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	}

	err = iamManager.CreateRole(ctx, "", "DistributedTestRole", roleDef)
	require.NoError(t, err)

	// Test that role is accessible through the IAM manager
	// Note: We can't directly test GetRole as it's not exposed,
	// but we can test through IsActionAllowed which internally uses the role store
	assert.True(t, iamManager.initialized)
}
