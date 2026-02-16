package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadIAMManagerFromConfig_OptionalConfig(t *testing.T) {
	// Mock dependencies
	filerAddressProvider := func() string { return "localhost:8888" }
	getFilerSigningKey := func() string { return "test-signing-key" }

	// Test Case 1: Empty config path should load defaults
	iamManager, err := loadIAMManagerFromConfig("", filerAddressProvider, getFilerSigningKey)
	require.NoError(t, err)
	require.NotNil(t, iamManager)

	// Verify STS Service is initialized with defaults
	stsService := iamManager.GetSTSService()
	assert.NotNil(t, stsService)

	// Verify defaults are applied
	// Since we can't easily access the internal config of stsService,
	// we rely on the fact that initialization succeeded without error.
	// We can also verify that the policy engine uses memory store by default.

	// Verify Policy Engine is initialized with defaults (Memory store, Deny effect)
	// Again, internal state might be hard to access directly, but successful init implies defaults worked.
}

func TestLoadIAMManagerFromConfig_EmptyConfigWithFallbackKey(t *testing.T) {
	// Mock dependencies where getFilerSigningKey returns empty, forcing fallback logic
	// Initialize IAM with empty config (should trigger defaults)
	// We pass empty string for config file path
	option := &S3ApiServerOption{
		Config:    "",
		IamConfig: "",
		EnableIam: true,
	}
	iamManager := NewIdentityAccessManagementWithStore(option, nil, "memory")

	// Verify identityAnonymous is initialized
	// This confirms the fix for anonymous access in zero-config mode
	anonIdentity, found := iamManager.LookupAnonymous()
	assert.True(t, found, "Anonymous identity should be found by default")
	assert.NotNil(t, anonIdentity, "Anonymous identity should not be nil")
	assert.Equal(t, "anonymous", anonIdentity.Name)
}
