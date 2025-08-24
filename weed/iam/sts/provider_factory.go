package sts

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// ProviderFactory creates identity providers from configuration
type ProviderFactory struct{}

// NewProviderFactory creates a new provider factory
func NewProviderFactory() *ProviderFactory {
	return &ProviderFactory{}
}

// CreateProvider creates an identity provider from configuration
func (f *ProviderFactory) CreateProvider(config *ProviderConfig) (providers.IdentityProvider, error) {
	if config == nil {
		return nil, fmt.Errorf("provider config cannot be nil")
	}

	if config.Name == "" {
		return nil, fmt.Errorf("provider name cannot be empty")
	}

	if config.Type == "" {
		return nil, fmt.Errorf("provider type cannot be empty")
	}

	if !config.Enabled {
		glog.V(2).Infof("Provider %s is disabled, skipping", config.Name)
		return nil, nil
	}

	glog.V(2).Infof("Creating provider: name=%s, type=%s", config.Name, config.Type)

	switch config.Type {
	case "oidc":
		return f.createOIDCProvider(config)
	case "ldap":
		return f.createLDAPProvider(config)
	case "saml":
		return f.createSAMLProvider(config)
	case "mock":
		return f.createMockProvider(config)
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", config.Type)
	}
}

// createOIDCProvider creates an OIDC provider from configuration
func (f *ProviderFactory) createOIDCProvider(config *ProviderConfig) (providers.IdentityProvider, error) {
	oidcConfig, err := f.convertToOIDCConfig(config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert OIDC config: %w", err)
	}

	provider := oidc.NewOIDCProvider(config.Name)
	if err := provider.Initialize(oidcConfig); err != nil {
		return nil, fmt.Errorf("failed to initialize OIDC provider: %w", err)
	}

	return provider, nil
}

// createLDAPProvider creates an LDAP provider from configuration
func (f *ProviderFactory) createLDAPProvider(config *ProviderConfig) (providers.IdentityProvider, error) {
	// TODO: Implement LDAP provider when available
	return nil, fmt.Errorf("LDAP provider not implemented yet")
}

// createSAMLProvider creates a SAML provider from configuration
func (f *ProviderFactory) createSAMLProvider(config *ProviderConfig) (providers.IdentityProvider, error) {
	// TODO: Implement SAML provider when available
	return nil, fmt.Errorf("SAML provider not implemented yet")
}

// createMockProvider creates a mock provider for testing
func (f *ProviderFactory) createMockProvider(config *ProviderConfig) (providers.IdentityProvider, error) {
	oidcConfig, err := f.convertToOIDCConfig(config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert mock config: %w", err)
	}

	// Set default values for mock provider if not provided
	if oidcConfig.Issuer == "" {
		oidcConfig.Issuer = "http://localhost:9999"
	}

	provider := oidc.NewMockOIDCProvider(config.Name)
	if err := provider.Initialize(oidcConfig); err != nil {
		return nil, fmt.Errorf("failed to initialize mock provider: %w", err)
	}

	// Set up default test data for the mock provider
	provider.SetupDefaultTestData()

	return provider, nil
}

// convertToOIDCConfig converts generic config map to OIDC config struct
func (f *ProviderFactory) convertToOIDCConfig(configMap map[string]interface{}) (*oidc.OIDCConfig, error) {
	config := &oidc.OIDCConfig{}

	// Required fields
	if issuer, ok := configMap["issuer"].(string); ok {
		config.Issuer = issuer
	} else {
		return nil, fmt.Errorf("issuer is required for OIDC provider")
	}

	if clientID, ok := configMap["clientId"].(string); ok {
		config.ClientID = clientID
	} else {
		return nil, fmt.Errorf("clientId is required for OIDC provider")
	}

	// Optional fields
	if clientSecret, ok := configMap["clientSecret"].(string); ok {
		config.ClientSecret = clientSecret
	}

	if jwksUri, ok := configMap["jwksUri"].(string); ok {
		config.JWKSUri = jwksUri
	}

	if userInfoUri, ok := configMap["userInfoUri"].(string); ok {
		config.UserInfoUri = userInfoUri
	}

	// Convert scopes array
	if scopesInterface, ok := configMap["scopes"]; ok {
		if scopes, err := f.convertToStringSlice(scopesInterface); err == nil {
			config.Scopes = scopes
		}
	}

	// Convert claims mapping
	if claimsMapInterface, ok := configMap["claimsMapping"]; ok {
		if claimsMap, err := f.convertToStringMap(claimsMapInterface); err == nil {
			config.ClaimsMapping = claimsMap
		}
	}

	glog.V(3).Infof("Converted OIDC config: issuer=%s, clientId=%s, jwksUri=%s",
		config.Issuer, config.ClientID, config.JWKSUri)

	return config, nil
}

// convertToStringSlice converts interface{} to []string
func (f *ProviderFactory) convertToStringSlice(value interface{}) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			if str, ok := item.(string); ok {
				result[i] = str
			} else {
				return nil, fmt.Errorf("non-string item in slice: %v", item)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to []string", value)
	}
}

// convertToStringMap converts interface{} to map[string]string
func (f *ProviderFactory) convertToStringMap(value interface{}) (map[string]string, error) {
	switch v := value.(type) {
	case map[string]string:
		return v, nil
	case map[string]interface{}:
		result := make(map[string]string)
		for key, val := range v {
			if str, ok := val.(string); ok {
				result[key] = str
			} else {
				return nil, fmt.Errorf("non-string value for key %s: %v", key, val)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to map[string]string", value)
	}
}

// LoadProvidersFromConfig creates providers from configuration
func (f *ProviderFactory) LoadProvidersFromConfig(configs []*ProviderConfig) (map[string]providers.IdentityProvider, error) {
	providersMap := make(map[string]providers.IdentityProvider)

	for _, config := range configs {
		if config == nil {
			glog.V(1).Infof("Skipping nil provider config")
			continue
		}

		glog.V(2).Infof("Loading provider: %s (type: %s, enabled: %t)",
			config.Name, config.Type, config.Enabled)

		if !config.Enabled {
			glog.V(2).Infof("Provider %s is disabled, skipping", config.Name)
			continue
		}

		provider, err := f.CreateProvider(config)
		if err != nil {
			glog.Errorf("Failed to create provider %s: %v", config.Name, err)
			return nil, fmt.Errorf("failed to create provider %s: %w", config.Name, err)
		}

		if provider != nil {
			providersMap[config.Name] = provider
			glog.V(1).Infof("Successfully loaded provider: %s", config.Name)
		}
	}

	glog.V(1).Infof("Loaded %d identity providers from configuration", len(providersMap))
	return providersMap, nil
}

// ValidateProviderConfig validates a provider configuration
func (f *ProviderFactory) ValidateProviderConfig(config *ProviderConfig) error {
	if config == nil {
		return fmt.Errorf("provider config cannot be nil")
	}

	if config.Name == "" {
		return fmt.Errorf("provider name cannot be empty")
	}

	if config.Type == "" {
		return fmt.Errorf("provider type cannot be empty")
	}

	if config.Config == nil {
		return fmt.Errorf("provider config cannot be nil")
	}

	// Type-specific validation
	switch config.Type {
	case "oidc":
		return f.validateOIDCConfig(config.Config)
	case "ldap":
		return f.validateLDAPConfig(config.Config)
	case "saml":
		return f.validateSAMLConfig(config.Config)
	case "mock":
		return f.validateMockConfig(config.Config)
	default:
		return fmt.Errorf("unsupported provider type: %s", config.Type)
	}
}

// validateOIDCConfig validates OIDC provider configuration
func (f *ProviderFactory) validateOIDCConfig(config map[string]interface{}) error {
	if _, ok := config["issuer"]; !ok {
		return fmt.Errorf("OIDC provider requires 'issuer' field")
	}

	if _, ok := config["clientId"]; !ok {
		return fmt.Errorf("OIDC provider requires 'clientId' field")
	}

	return nil
}

// validateLDAPConfig validates LDAP provider configuration
func (f *ProviderFactory) validateLDAPConfig(config map[string]interface{}) error {
	// TODO: Implement when LDAP provider is available
	return nil
}

// validateSAMLConfig validates SAML provider configuration
func (f *ProviderFactory) validateSAMLConfig(config map[string]interface{}) error {
	// TODO: Implement when SAML provider is available
	return nil
}

// validateMockConfig validates mock provider configuration
func (f *ProviderFactory) validateMockConfig(config map[string]interface{}) error {
	// Mock provider is lenient for testing
	return nil
}

// GetSupportedProviderTypes returns list of supported provider types
func (f *ProviderFactory) GetSupportedProviderTypes() []string {
	return []string{"oidc", "mock"}
}
