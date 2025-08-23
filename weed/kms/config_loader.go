package kms

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ViperConfig interface extends Configuration with additional methods needed for KMS configuration
type ViperConfig interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetStringSlice(key string) []string
	SetDefault(key string, value interface{})
	GetStringMap(key string) map[string]interface{}
	IsSet(key string) bool
}

// ConfigLoader handles loading KMS configurations from filer.toml
type ConfigLoader struct {
	viper   ViperConfig
	manager *KMSManager
}

// NewConfigLoader creates a new KMS configuration loader
func NewConfigLoader(v ViperConfig) *ConfigLoader {
	return &ConfigLoader{
		viper:   v,
		manager: GetKMSManager(),
	}
}

// LoadConfigurations loads all KMS provider configurations from filer.toml
func (loader *ConfigLoader) LoadConfigurations() error {
	// Check if KMS section exists
	if !loader.viper.IsSet("kms") {
		glog.V(1).Infof("No KMS configuration found in filer.toml")
		return nil
	}

	// Get the KMS configuration section
	kmsConfig := loader.viper.GetStringMap("kms")

	// Load global KMS settings
	if err := loader.loadGlobalKMSSettings(kmsConfig); err != nil {
		return fmt.Errorf("failed to load global KMS settings: %w", err)
	}

	// Load KMS providers
	if providersConfig, exists := kmsConfig["providers"]; exists {
		if providers, ok := providersConfig.(map[string]interface{}); ok {
			if err := loader.loadKMSProviders(providers); err != nil {
				return fmt.Errorf("failed to load KMS providers: %w", err)
			}
		}
	}

	// Set default provider after all providers are loaded
	if err := loader.setDefaultProvider(); err != nil {
		return fmt.Errorf("failed to set default KMS provider: %w", err)
	}

	// Initialize global KMS provider for backwards compatibility
	if err := loader.initializeGlobalKMSProvider(); err != nil {
		glog.Warningf("Failed to initialize global KMS provider: %v", err)
	}

	// Load bucket-specific KMS configurations
	if bucketsConfig, exists := kmsConfig["buckets"]; exists {
		if buckets, ok := bucketsConfig.(map[string]interface{}); ok {
			if err := loader.loadBucketKMSConfigurations(buckets); err != nil {
				return fmt.Errorf("failed to load bucket KMS configurations: %w", err)
			}
		}
	}

	glog.V(1).Infof("KMS configuration loaded successfully")
	return nil
}

// loadGlobalKMSSettings loads global KMS settings
func (loader *ConfigLoader) loadGlobalKMSSettings(kmsConfig map[string]interface{}) error {
	// Set default KMS provider if specified
	if defaultProvider, exists := kmsConfig["default_provider"]; exists {
		if providerName, ok := defaultProvider.(string); ok {
			// We'll set this after providers are loaded
			glog.V(2).Infof("Default KMS provider will be set to: %s", providerName)
		}
	}

	return nil
}

// loadKMSProviders loads individual KMS provider configurations
func (loader *ConfigLoader) loadKMSProviders(providers map[string]interface{}) error {
	for providerName, providerConfigInterface := range providers {
		providerConfig, ok := providerConfigInterface.(map[string]interface{})
		if !ok {
			glog.Warningf("Invalid configuration for KMS provider %s", providerName)
			continue
		}

		if err := loader.loadSingleKMSProvider(providerName, providerConfig); err != nil {
			glog.Errorf("Failed to load KMS provider %s: %v", providerName, err)
			continue
		}

		glog.V(1).Infof("Loaded KMS provider: %s", providerName)
	}

	return nil
}

// loadSingleKMSProvider loads a single KMS provider configuration
func (loader *ConfigLoader) loadSingleKMSProvider(providerName string, config map[string]interface{}) error {
	// Get provider type
	providerType, exists := config["type"]
	if !exists {
		return fmt.Errorf("provider type not specified for %s", providerName)
	}

	providerTypeStr, ok := providerType.(string)
	if !ok {
		return fmt.Errorf("invalid provider type for %s", providerName)
	}

	// Get provider-specific configuration
	providerConfig := make(map[string]interface{})
	for key, value := range config {
		if key != "type" {
			providerConfig[key] = value
		}
	}

	// Set default cache settings if not specified
	if _, exists := providerConfig["cache_enabled"]; !exists {
		providerConfig["cache_enabled"] = true
	}

	if _, exists := providerConfig["cache_ttl"]; !exists {
		providerConfig["cache_ttl"] = "1h"
	}

	if _, exists := providerConfig["max_cache_size"]; !exists {
		providerConfig["max_cache_size"] = 1000
	}

	// Parse cache TTL
	cacheTTL := time.Hour // default
	if ttlStr, exists := providerConfig["cache_ttl"]; exists {
		if ttlStrValue, ok := ttlStr.(string); ok {
			if parsed, err := time.ParseDuration(ttlStrValue); err == nil {
				cacheTTL = parsed
			}
		}
	}

	// Create KMS configuration
	kmsConfig := &KMSConfig{
		Provider:     providerTypeStr,
		Config:       providerConfig,
		CacheEnabled: getBoolFromConfig(providerConfig, "cache_enabled", true),
		CacheTTL:     cacheTTL,
		MaxCacheSize: getIntFromConfig(providerConfig, "max_cache_size", 1000),
	}

	// Add the provider to the KMS manager
	if err := loader.manager.AddKMSProvider(providerName, kmsConfig); err != nil {
		return err
	}

	// Test the provider with a health check
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	health := loader.manager.GetKMSHealth(ctx)
	if providerHealth, exists := health[providerName]; exists && providerHealth != nil {
		glog.Warningf("KMS provider %s health check failed: %v", providerName, providerHealth)
	}

	return nil
}

// loadBucketKMSConfigurations loads bucket-specific KMS configurations
func (loader *ConfigLoader) loadBucketKMSConfigurations(buckets map[string]interface{}) error {
	for bucketName, bucketConfigInterface := range buckets {
		bucketConfig, ok := bucketConfigInterface.(map[string]interface{})
		if !ok {
			glog.Warningf("Invalid KMS configuration for bucket %s", bucketName)
			continue
		}

		// Get provider for this bucket
		if provider, exists := bucketConfig["provider"]; exists {
			if providerName, ok := provider.(string); ok {
				if err := loader.manager.SetBucketKMSProvider(bucketName, providerName); err != nil {
					glog.Errorf("Failed to set KMS provider for bucket %s: %v", bucketName, err)
					continue
				}
				glog.V(2).Infof("Set KMS provider for bucket %s to %s", bucketName, providerName)
			}
		}
	}

	return nil
}

// setDefaultProvider sets the default KMS provider after all providers are loaded
func (loader *ConfigLoader) setDefaultProvider() error {
	kmsConfig := loader.viper.GetStringMap("kms")
	if defaultProvider, exists := kmsConfig["default_provider"]; exists {
		if providerName, ok := defaultProvider.(string); ok {
			if err := loader.manager.SetDefaultKMSProvider(providerName); err != nil {
				return fmt.Errorf("failed to set default KMS provider: %w", err)
			}
			glog.V(1).Infof("Set default KMS provider to: %s", providerName)
		}
	}
	return nil
}

// initializeGlobalKMSProvider initializes the global KMS provider for backwards compatibility
func (loader *ConfigLoader) initializeGlobalKMSProvider() error {
	// Get the default provider from the manager
	defaultProviderName := ""
	kmsConfig := loader.viper.GetStringMap("kms")
	if defaultProvider, exists := kmsConfig["default_provider"]; exists {
		if providerName, ok := defaultProvider.(string); ok {
			defaultProviderName = providerName
		}
	}

	if defaultProviderName == "" {
		// If no default provider, try to use the first available provider
		providers := loader.manager.ListKMSProviders()
		if len(providers) > 0 {
			defaultProviderName = providers[0]
		}
	}

	if defaultProviderName == "" {
		glog.V(2).Infof("No KMS providers configured, skipping global KMS initialization")
		return nil
	}

	// Get the provider from the manager
	provider, err := loader.manager.GetKMSProviderByName(defaultProviderName)
	if err != nil {
		return fmt.Errorf("failed to get KMS provider %s: %w", defaultProviderName, err)
	}

	// Set as global KMS provider
	SetGlobalKMSProvider(provider)
	glog.V(1).Infof("Initialized global KMS provider: %s", defaultProviderName)

	return nil
}

// ValidateConfiguration validates the KMS configuration
func (loader *ConfigLoader) ValidateConfiguration() error {
	providers := loader.manager.ListKMSProviders()
	if len(providers) == 0 {
		glog.V(1).Infof("No KMS providers configured")
		return nil
	}

	// Test connectivity to all providers
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	health := loader.manager.GetKMSHealth(ctx)
	hasHealthyProvider := false

	for providerName, err := range health {
		if err != nil {
			glog.Warningf("KMS provider %s is unhealthy: %v", providerName, err)
		} else {
			hasHealthyProvider = true
			glog.V(2).Infof("KMS provider %s is healthy", providerName)
		}
	}

	if !hasHealthyProvider {
		glog.Warningf("No healthy KMS providers found")
	}

	return nil
}

// LoadKMSFromFilerToml is a convenience function to load KMS configuration from filer.toml
func LoadKMSFromFilerToml(v ViperConfig) error {
	loader := NewConfigLoader(v)
	if err := loader.LoadConfigurations(); err != nil {
		return err
	}
	return loader.ValidateConfiguration()
}

// LoadKMSFromConfig loads KMS configuration directly from parsed JSON data
func LoadKMSFromConfig(kmsConfig interface{}) error {
	kmsMap, ok := kmsConfig.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid KMS configuration format")
	}

	// Create a direct config adapter that doesn't use Viper
	// Wrap the KMS config under a "kms" key as expected by LoadConfigurations
	wrappedConfig := map[string]interface{}{
		"kms": kmsMap,
	}
	adapter := &directConfigAdapter{config: wrappedConfig}
	loader := NewConfigLoader(adapter)

	if err := loader.LoadConfigurations(); err != nil {
		return err
	}

	return loader.ValidateConfiguration()
}

// directConfigAdapter implements ViperConfig interface for direct map access
type directConfigAdapter struct {
	config map[string]interface{}
}

func (d *directConfigAdapter) GetStringMap(key string) map[string]interface{} {
	if val, exists := d.config[key]; exists {
		if mapVal, ok := val.(map[string]interface{}); ok {
			return mapVal
		}
	}
	return make(map[string]interface{})
}

func (d *directConfigAdapter) GetString(key string) string {
	if val, exists := d.config[key]; exists {
		if strVal, ok := val.(string); ok {
			return strVal
		}
	}
	return ""
}

func (d *directConfigAdapter) GetBool(key string) bool {
	if val, exists := d.config[key]; exists {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
	}
	return false
}

func (d *directConfigAdapter) GetInt(key string) int {
	if val, exists := d.config[key]; exists {
		switch v := val.(type) {
		case int:
			return v
		case float64:
			return int(v)
		}
	}
	return 0
}

func (d *directConfigAdapter) GetStringSlice(key string) []string {
	if val, exists := d.config[key]; exists {
		if sliceVal, ok := val.([]interface{}); ok {
			result := make([]string, len(sliceVal))
			for i, item := range sliceVal {
				if strItem, ok := item.(string); ok {
					result[i] = strItem
				}
			}
			return result
		}
		if strSlice, ok := val.([]string); ok {
			return strSlice
		}
	}
	return []string{}
}

func (d *directConfigAdapter) SetDefault(key string, value interface{}) {
	// For direct config adapter, we don't need to set defaults
	// as the configuration is already parsed
}

func (d *directConfigAdapter) IsSet(key string) bool {
	_, exists := d.config[key]
	return exists
}

// Helper functions

func getBoolFromConfig(config map[string]interface{}, key string, defaultValue bool) bool {
	if value, exists := config[key]; exists {
		if boolValue, ok := value.(bool); ok {
			return boolValue
		}
	}
	return defaultValue
}

func getIntFromConfig(config map[string]interface{}, key string, defaultValue int) int {
	if value, exists := config[key]; exists {
		if intValue, ok := value.(int); ok {
			return intValue
		}
		if floatValue, ok := value.(float64); ok {
			return int(floatValue)
		}
	}
	return defaultValue
}

func getStringFromConfig(config map[string]interface{}, key string, defaultValue string) string {
	if value, exists := config[key]; exists {
		if stringValue, ok := value.(string); ok {
			return stringValue
		}
	}
	return defaultValue
}
