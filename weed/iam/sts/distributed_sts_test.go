package sts

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistributedSTSService verifies that multiple STS instances with identical configurations
// behave consistently across distributed environments
func TestDistributedSTSService(t *testing.T) {
	ctx := context.Background()

	// Common configuration for all instances
	commonConfig := &STSConfig{
		TokenDuration:    FlexibleDuration{time.Hour},
		MaxSessionLength: FlexibleDuration{12 * time.Hour},
		Issuer:           "distributed-sts-test",
		SigningKey:       []byte("test-signing-key-32-characters-long"),

		Providers: []*ProviderConfig{
			{
				Name:    "keycloak-oidc",
				Type:    "oidc",
				Enabled: true,
				Config: map[string]interface{}{
					"issuer":   "http://keycloak:8080/realms/seaweedfs-test",
					"clientId": "seaweedfs-s3",
					"jwksUri":  "http://keycloak:8080/realms/seaweedfs-test/protocol/openid-connect/certs",
				},
			},

			{
				Name:    "disabled-ldap",
				Type:    "oidc", // Use OIDC as placeholder since LDAP isn't implemented
				Enabled: false,
				Config: map[string]interface{}{
					"issuer":   "ldap://company.com",
					"clientId": "ldap-client",
				},
			},
		},
	}

	// Create multiple STS instances simulating distributed deployment
	instance1 := NewSTSService()
	instance2 := NewSTSService()
	instance3 := NewSTSService()

	// Initialize all instances with identical configuration
	err := instance1.Initialize(commonConfig)
	require.NoError(t, err, "Instance 1 should initialize successfully")

	err = instance2.Initialize(commonConfig)
	require.NoError(t, err, "Instance 2 should initialize successfully")

	err = instance3.Initialize(commonConfig)
	require.NoError(t, err, "Instance 3 should initialize successfully")

	// Manually register mock providers for testing (not available in production)
	mockProviderConfig := map[string]interface{}{
		"issuer":   "http://localhost:9999",
		"clientId": "test-client",
	}
	mockProvider1, err := createMockOIDCProvider("test-mock-provider", mockProviderConfig)
	require.NoError(t, err)
	mockProvider2, err := createMockOIDCProvider("test-mock-provider", mockProviderConfig)
	require.NoError(t, err)
	mockProvider3, err := createMockOIDCProvider("test-mock-provider", mockProviderConfig)
	require.NoError(t, err)

	instance1.RegisterProvider(mockProvider1)
	instance2.RegisterProvider(mockProvider2)
	instance3.RegisterProvider(mockProvider3)

	// Verify all instances have identical provider configurations
	t.Run("provider_consistency", func(t *testing.T) {
		// All instances should have same number of providers
		assert.Len(t, instance1.providers, 2, "Instance 1 should have 2 enabled providers")
		assert.Len(t, instance2.providers, 2, "Instance 2 should have 2 enabled providers")
		assert.Len(t, instance3.providers, 2, "Instance 3 should have 2 enabled providers")

		// All instances should have same provider names
		instance1Names := instance1.getProviderNames()
		instance2Names := instance2.getProviderNames()
		instance3Names := instance3.getProviderNames()

		assert.ElementsMatch(t, instance1Names, instance2Names, "Instance 1 and 2 should have same providers")
		assert.ElementsMatch(t, instance2Names, instance3Names, "Instance 2 and 3 should have same providers")

		// Verify specific providers exist on all instances
		expectedProviders := []string{"keycloak-oidc", "test-mock-provider"}
		assert.ElementsMatch(t, instance1Names, expectedProviders, "Instance 1 should have expected providers")
		assert.ElementsMatch(t, instance2Names, expectedProviders, "Instance 2 should have expected providers")
		assert.ElementsMatch(t, instance3Names, expectedProviders, "Instance 3 should have expected providers")

		// Verify disabled providers are not loaded
		assert.NotContains(t, instance1Names, "disabled-ldap", "Disabled providers should not be loaded")
		assert.NotContains(t, instance2Names, "disabled-ldap", "Disabled providers should not be loaded")
		assert.NotContains(t, instance3Names, "disabled-ldap", "Disabled providers should not be loaded")
	})

	// Test token generation consistency across instances
	t.Run("token_generation_consistency", func(t *testing.T) {
		sessionId := "test-session-123"
		expiresAt := time.Now().Add(time.Hour)

		// Generate tokens from different instances
		token1, err1 := instance1.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)
		token2, err2 := instance2.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)
		token3, err3 := instance3.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)

		require.NoError(t, err1, "Instance 1 token generation should succeed")
		require.NoError(t, err2, "Instance 2 token generation should succeed")
		require.NoError(t, err3, "Instance 3 token generation should succeed")

		// All tokens should be different (due to timestamp variations)
		// But they should all be valid JWTs with same signing key
		assert.NotEmpty(t, token1)
		assert.NotEmpty(t, token2)
		assert.NotEmpty(t, token3)
	})

	// Test token validation consistency - any instance should validate tokens from any other instance
	t.Run("cross_instance_token_validation", func(t *testing.T) {
		sessionId := "cross-validation-session"
		expiresAt := time.Now().Add(time.Hour)

		// Generate token on instance 1
		token, err := instance1.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)
		require.NoError(t, err)

		// Validate on all instances
		claims1, err1 := instance1.TokenGenerator.ValidateSessionToken(token)
		claims2, err2 := instance2.TokenGenerator.ValidateSessionToken(token)
		claims3, err3 := instance3.TokenGenerator.ValidateSessionToken(token)

		require.NoError(t, err1, "Instance 1 should validate token from instance 1")
		require.NoError(t, err2, "Instance 2 should validate token from instance 1")
		require.NoError(t, err3, "Instance 3 should validate token from instance 1")

		// All instances should extract same session ID
		assert.Equal(t, sessionId, claims1.SessionId)
		assert.Equal(t, sessionId, claims2.SessionId)
		assert.Equal(t, sessionId, claims3.SessionId)

		assert.Equal(t, claims1.SessionId, claims2.SessionId)
		assert.Equal(t, claims2.SessionId, claims3.SessionId)
	})

	// Test provider access consistency
	t.Run("provider_access_consistency", func(t *testing.T) {
		// All instances should be able to access the same providers
		provider1, exists1 := instance1.providers["test-mock-provider"]
		provider2, exists2 := instance2.providers["test-mock-provider"]
		provider3, exists3 := instance3.providers["test-mock-provider"]

		assert.True(t, exists1, "Instance 1 should have test-mock-provider")
		assert.True(t, exists2, "Instance 2 should have test-mock-provider")
		assert.True(t, exists3, "Instance 3 should have test-mock-provider")

		assert.Equal(t, provider1.Name(), provider2.Name())
		assert.Equal(t, provider2.Name(), provider3.Name())

		// Test authentication with the mock provider on all instances
		testToken := "valid_test_token"

		identity1, err1 := provider1.Authenticate(ctx, testToken)
		identity2, err2 := provider2.Authenticate(ctx, testToken)
		identity3, err3 := provider3.Authenticate(ctx, testToken)

		require.NoError(t, err1, "Instance 1 provider should authenticate successfully")
		require.NoError(t, err2, "Instance 2 provider should authenticate successfully")
		require.NoError(t, err3, "Instance 3 provider should authenticate successfully")

		// All instances should return identical identity information
		assert.Equal(t, identity1.UserID, identity2.UserID)
		assert.Equal(t, identity2.UserID, identity3.UserID)
		assert.Equal(t, identity1.Email, identity2.Email)
		assert.Equal(t, identity2.Email, identity3.Email)
		assert.Equal(t, identity1.Provider, identity2.Provider)
		assert.Equal(t, identity2.Provider, identity3.Provider)
	})
}

// TestSTSConfigurationValidation tests configuration validation for distributed deployments
func TestSTSConfigurationValidation(t *testing.T) {
	t.Run("consistent_signing_keys_required", func(t *testing.T) {
		// Different signing keys should result in incompatible token validation
		config1 := &STSConfig{
			TokenDuration:    FlexibleDuration{time.Hour},
			MaxSessionLength: FlexibleDuration{12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("signing-key-1-32-characters-long"),
		}

		config2 := &STSConfig{
			TokenDuration:    FlexibleDuration{time.Hour},
			MaxSessionLength: FlexibleDuration{12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("signing-key-2-32-characters-long"), // Different key!
		}

		instance1 := NewSTSService()
		instance2 := NewSTSService()

		err1 := instance1.Initialize(config1)
		err2 := instance2.Initialize(config2)

		require.NoError(t, err1)
		require.NoError(t, err2)

		// Generate token on instance 1
		sessionId := "test-session"
		expiresAt := time.Now().Add(time.Hour)
		token, err := instance1.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)
		require.NoError(t, err)

		// Instance 1 should validate its own token
		_, err = instance1.TokenGenerator.ValidateSessionToken(token)
		assert.NoError(t, err, "Instance 1 should validate its own token")

		// Instance 2 should reject token from instance 1 (different signing key)
		_, err = instance2.TokenGenerator.ValidateSessionToken(token)
		assert.Error(t, err, "Instance 2 should reject token with different signing key")
	})

	t.Run("consistent_issuer_required", func(t *testing.T) {
		// Different issuers should result in incompatible tokens
		commonSigningKey := []byte("shared-signing-key-32-characters-lo")

		config1 := &STSConfig{
			TokenDuration:    FlexibleDuration{time.Hour},
			MaxSessionLength: FlexibleDuration{12 * time.Hour},
			Issuer:           "sts-instance-1",
			SigningKey:       commonSigningKey,
		}

		config2 := &STSConfig{
			TokenDuration:    FlexibleDuration{time.Hour},
			MaxSessionLength: FlexibleDuration{12 * time.Hour},
			Issuer:           "sts-instance-2", // Different issuer!
			SigningKey:       commonSigningKey,
		}

		instance1 := NewSTSService()
		instance2 := NewSTSService()

		err1 := instance1.Initialize(config1)
		err2 := instance2.Initialize(config2)

		require.NoError(t, err1)
		require.NoError(t, err2)

		// Generate token on instance 1
		sessionId := "test-session"
		expiresAt := time.Now().Add(time.Hour)
		token, err := instance1.TokenGenerator.GenerateSessionToken(sessionId, expiresAt)
		require.NoError(t, err)

		// Instance 2 should reject token due to issuer mismatch
		// (Even though signing key is the same, issuer validation will fail)
		_, err = instance2.TokenGenerator.ValidateSessionToken(token)
		assert.Error(t, err, "Instance 2 should reject token with different issuer")
	})
}

// TestProviderFactoryDistributed tests the provider factory in distributed scenarios
func TestProviderFactoryDistributed(t *testing.T) {
	factory := NewProviderFactory()

	// Simulate configuration that would be identical across all instances
	configs := []*ProviderConfig{
		{
			Name:    "production-keycloak",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":       "https://keycloak.company.com/realms/seaweedfs",
				"clientId":     "seaweedfs-prod",
				"clientSecret": "super-secret-key",
				"jwksUri":      "https://keycloak.company.com/realms/seaweedfs/protocol/openid-connect/certs",
				"scopes":       []string{"openid", "profile", "email", "roles"},
			},
		},
		{
			Name:    "backup-oidc",
			Type:    "oidc",
			Enabled: false, // Disabled by default
			Config: map[string]interface{}{
				"issuer":   "https://backup-oidc.company.com",
				"clientId": "seaweedfs-backup",
			},
		},
	}

	// Create providers multiple times (simulating multiple instances)
	providers1, err1 := factory.LoadProvidersFromConfig(configs)
	providers2, err2 := factory.LoadProvidersFromConfig(configs)
	providers3, err3 := factory.LoadProvidersFromConfig(configs)

	require.NoError(t, err1, "First load should succeed")
	require.NoError(t, err2, "Second load should succeed")
	require.NoError(t, err3, "Third load should succeed")

	// All instances should have same provider counts
	assert.Len(t, providers1, 1, "First instance should have 1 enabled provider")
	assert.Len(t, providers2, 1, "Second instance should have 1 enabled provider")
	assert.Len(t, providers3, 1, "Third instance should have 1 enabled provider")

	// All instances should have same provider names
	names1 := make([]string, 0, len(providers1))
	names2 := make([]string, 0, len(providers2))
	names3 := make([]string, 0, len(providers3))

	for name := range providers1 {
		names1 = append(names1, name)
	}
	for name := range providers2 {
		names2 = append(names2, name)
	}
	for name := range providers3 {
		names3 = append(names3, name)
	}

	assert.ElementsMatch(t, names1, names2, "Instance 1 and 2 should have same provider names")
	assert.ElementsMatch(t, names2, names3, "Instance 2 and 3 should have same provider names")

	// Verify specific providers
	expectedProviders := []string{"production-keycloak"}
	assert.ElementsMatch(t, names1, expectedProviders, "Should have expected enabled providers")

	// Verify disabled providers are not included
	assert.NotContains(t, names1, "backup-oidc", "Disabled providers should not be loaded")
	assert.NotContains(t, names2, "backup-oidc", "Disabled providers should not be loaded")
	assert.NotContains(t, names3, "backup-oidc", "Disabled providers should not be loaded")
}
