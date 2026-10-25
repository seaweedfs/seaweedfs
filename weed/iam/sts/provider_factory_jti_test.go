package sts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertToOIDCConfigCarriesJTISettings(t *testing.T) {
	factory := NewProviderFactory()

	config, err := factory.convertToOIDCConfig(map[string]interface{}{
		"issuer":                     "https://issuer.example",
		"clientId":                   "client",
		"jtiReplayProtectionEnabled": false,
		"jtiCacheMaxSize":            float64(42),
	})
	require.NoError(t, err)
	require.NotNil(t, config.JTIReplayProtectionEnabled)
	assert.False(t, *config.JTIReplayProtectionEnabled)
	assert.Equal(t, 42, config.JTICacheMaxSize)

	config, err = factory.convertToOIDCConfig(map[string]interface{}{
		"issuer":   "https://issuer.example",
		"clientId": "client",
	})
	require.NoError(t, err)
	assert.Nil(t, config.JTIReplayProtectionEnabled)
	assert.Zero(t, config.JTICacheMaxSize)
}
