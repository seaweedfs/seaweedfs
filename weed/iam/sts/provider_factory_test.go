package sts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderFactory_CreateOIDCProvider(t *testing.T) {
	factory := NewProviderFactory()

	config := &ProviderConfig{
		Name:    "test-oidc",
		Type:    "oidc",
		Enabled: true,
		Config: map[string]interface{}{
			"issuer":       "https://test-issuer.com",
			"clientId":     "test-client",
			"clientSecret": "test-secret",
			"jwksUri":      "https://test-issuer.com/.well-known/jwks.json",
			"scopes":       []string{"openid", "profile", "email"},
		},
	}

	provider, err := factory.CreateProvider(config)
	require.NoError(t, err)
	assert.NotNil(t, provider)
	assert.Equal(t, "test-oidc", provider.Name())
}

// Note: Mock provider tests removed - mock providers are now test-only
// and not available through the production ProviderFactory

func TestProviderFactory_DisabledProvider(t *testing.T) {
	factory := NewProviderFactory()

	config := &ProviderConfig{
		Name:    "disabled-provider",
		Type:    "oidc",
		Enabled: false,
		Config: map[string]interface{}{
			"issuer":   "https://test-issuer.com",
			"clientId": "test-client",
		},
	}

	provider, err := factory.CreateProvider(config)
	require.NoError(t, err)
	assert.Nil(t, provider) // Should return nil for disabled providers
}

func TestProviderFactory_InvalidProviderType(t *testing.T) {
	factory := NewProviderFactory()

	config := &ProviderConfig{
		Name:    "invalid-provider",
		Type:    "unsupported-type",
		Enabled: true,
		Config:  map[string]interface{}{},
	}

	provider, err := factory.CreateProvider(config)
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "unsupported provider type")
}

func TestProviderFactory_LoadMultipleProviders(t *testing.T) {
	factory := NewProviderFactory()

	configs := []*ProviderConfig{
		{
			Name:    "oidc-provider",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":   "https://oidc-issuer.com",
				"clientId": "oidc-client",
			},
		},

		{
			Name:    "disabled-provider",
			Type:    "oidc",
			Enabled: false,
			Config: map[string]interface{}{
				"issuer":   "https://disabled-issuer.com",
				"clientId": "disabled-client",
			},
		},
	}

	providers, err := factory.LoadProvidersFromConfig(configs)
	require.NoError(t, err)
	assert.Len(t, providers, 1) // Only enabled providers should be loaded

	assert.Contains(t, providers, "oidc-provider")
	assert.NotContains(t, providers, "disabled-provider")
}

func TestProviderFactory_ValidateOIDCConfig(t *testing.T) {
	factory := NewProviderFactory()

	t.Run("valid config", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "valid-oidc",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":   "https://valid-issuer.com",
				"clientId": "valid-client",
			},
		}

		err := factory.ValidateProviderConfig(config)
		assert.NoError(t, err)
	})

	t.Run("missing issuer", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "invalid-oidc",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"clientId": "valid-client",
			},
		}

		err := factory.ValidateProviderConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("missing clientId", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "invalid-oidc",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer": "https://valid-issuer.com",
			},
		}

		err := factory.ValidateProviderConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "clientId")
	})
}

func TestProviderFactory_ConvertToStringSlice(t *testing.T) {
	factory := NewProviderFactory()

	t.Run("string slice", func(t *testing.T) {
		input := []string{"a", "b", "c"}
		result, err := factory.convertToStringSlice(input)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, result)
	})

	t.Run("interface slice", func(t *testing.T) {
		input := []interface{}{"a", "b", "c"}
		result, err := factory.convertToStringSlice(input)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, result)
	})

	t.Run("invalid type", func(t *testing.T) {
		input := "not-a-slice"
		result, err := factory.convertToStringSlice(input)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestProviderFactory_ConfigConversionErrors(t *testing.T) {
	factory := NewProviderFactory()

	t.Run("invalid scopes type", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "invalid-scopes",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":   "https://test-issuer.com",
				"clientId": "test-client",
				"scopes":   "invalid-not-array", // Should be array
			},
		}

		provider, err := factory.CreateProvider(config)
		assert.Error(t, err)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "failed to convert scopes")
	})

	t.Run("invalid claimsMapping type", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "invalid-claims",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":        "https://test-issuer.com",
				"clientId":      "test-client",
				"claimsMapping": "invalid-not-map", // Should be map
			},
		}

		provider, err := factory.CreateProvider(config)
		assert.Error(t, err)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "failed to convert claimsMapping")
	})

	t.Run("invalid roleMapping type", func(t *testing.T) {
		config := &ProviderConfig{
			Name:    "invalid-roles",
			Type:    "oidc",
			Enabled: true,
			Config: map[string]interface{}{
				"issuer":      "https://test-issuer.com",
				"clientId":    "test-client",
				"roleMapping": "invalid-not-map", // Should be map
			},
		}

		provider, err := factory.CreateProvider(config)
		assert.Error(t, err)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "failed to convert roleMapping")
	})
}

func TestProviderFactory_ConvertToStringMap(t *testing.T) {
	factory := NewProviderFactory()

	t.Run("string map", func(t *testing.T) {
		input := map[string]string{"key1": "value1", "key2": "value2"}
		result, err := factory.convertToStringMap(input)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"key1": "value1", "key2": "value2"}, result)
	})

	t.Run("interface map", func(t *testing.T) {
		input := map[string]interface{}{"key1": "value1", "key2": "value2"}
		result, err := factory.convertToStringMap(input)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"key1": "value1", "key2": "value2"}, result)
	})

	t.Run("invalid type", func(t *testing.T) {
		input := "not-a-map"
		result, err := factory.convertToStringMap(input)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestProviderFactory_GetSupportedProviderTypes(t *testing.T) {
	factory := NewProviderFactory()

	supportedTypes := factory.GetSupportedProviderTypes()
	assert.Contains(t, supportedTypes, "oidc")
	assert.Len(t, supportedTypes, 1) // Currently only OIDC is supported in production
}

func TestSTSService_LoadProvidersFromConfig(t *testing.T) {
	stsConfig := &STSConfig{
		TokenDuration:    FlexibleDuration{3600 * time.Second},
		MaxSessionLength: FlexibleDuration{43200 * time.Second},
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-32-characters-long"),
		Providers: []*ProviderConfig{
			{
				Name:    "test-provider",
				Type:    "oidc",
				Enabled: true,
				Config: map[string]interface{}{
					"issuer":   "https://test-issuer.com",
					"clientId": "test-client",
				},
			},
		},
	}

	stsService := NewSTSService()
	err := stsService.Initialize(stsConfig)
	require.NoError(t, err)

	// Check that provider was loaded
	assert.Len(t, stsService.providers, 1)
	assert.Contains(t, stsService.providers, "test-provider")
	assert.Equal(t, "test-provider", stsService.providers["test-provider"].Name())
}

func TestSTSService_NoProvidersConfig(t *testing.T) {
	stsConfig := &STSConfig{
		TokenDuration:    FlexibleDuration{3600 * time.Second},
		MaxSessionLength: FlexibleDuration{43200 * time.Second},
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-32-characters-long"),
		// No providers configured
	}

	stsService := NewSTSService()
	err := stsService.Initialize(stsConfig)
	require.NoError(t, err)

	// Should initialize successfully with no providers
	assert.Len(t, stsService.providers, 0)
}
