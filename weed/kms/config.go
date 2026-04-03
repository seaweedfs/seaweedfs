package kms

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// KMSManager manages KMS provider instances and configurations
type KMSManager struct {
	mu         sync.RWMutex
	providers  map[string]KMSProvider // provider name -> provider instance
	configs    map[string]*KMSConfig  // provider name -> configuration
	bucketKMS  map[string]string      // bucket name -> provider name
	defaultKMS string                 // default KMS provider name
}

// KMSConfig represents a complete KMS provider configuration
type KMSConfig struct {
	Provider     string                 `json:"provider"`       // Provider type (aws, azure, gcp, local)
	Config       map[string]interface{} `json:"config"`         // Provider-specific configuration
	CacheEnabled bool                   `json:"cache_enabled"`  // Enable data key caching
	CacheTTL     time.Duration          `json:"cache_ttl"`      // Cache TTL (default: 1 hour)
	MaxCacheSize int                    `json:"max_cache_size"` // Maximum cached keys (default: 1000)
}

// BucketKMSConfig represents KMS configuration for a specific bucket
type BucketKMSConfig struct {
	Provider  string            `json:"provider"`   // KMS provider to use
	KeyID     string            `json:"key_id"`     // Default KMS key ID for this bucket
	BucketKey bool              `json:"bucket_key"` // Enable S3 Bucket Keys optimization
	Context   map[string]string `json:"context"`    // Additional encryption context
	Enabled   bool              `json:"enabled"`    // Whether KMS encryption is enabled
}

// configAdapter adapts KMSConfig.Config to util.Configuration interface
type configAdapter struct {
	config map[string]interface{}
}

// GetConfigMap returns the underlying configuration map for direct access
func (c *configAdapter) GetConfigMap() map[string]interface{} {
	return c.config
}

func (c *configAdapter) GetString(key string) string {
	if val, ok := c.config[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (c *configAdapter) GetBool(key string) bool {
	if val, ok := c.config[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

func (c *configAdapter) GetInt(key string) int {
	if val, ok := c.config[key]; ok {
		if i, ok := val.(int); ok {
			return i
		}
		if f, ok := val.(float64); ok {
			return int(f)
		}
	}
	return 0
}

func (c *configAdapter) GetStringSlice(key string) []string {
	if val, ok := c.config[key]; ok {
		if slice, ok := val.([]string); ok {
			return slice
		}
		if interfaceSlice, ok := val.([]interface{}); ok {
			result := make([]string, len(interfaceSlice))
			for i, v := range interfaceSlice {
				if str, ok := v.(string); ok {
					result[i] = str
				}
			}
			return result
		}
	}
	return nil
}

func (c *configAdapter) SetDefault(key string, value interface{}) {
	if c.config == nil {
		c.config = make(map[string]interface{})
	}
	if _, exists := c.config[key]; !exists {
		c.config[key] = value
	}
}

var (
	globalKMSManager *KMSManager
	globalKMSMutex   sync.RWMutex

	// Global KMS provider for legacy compatibility
	globalKMSProvider KMSProvider
)

// GetGlobalKMS returns the global KMS provider
func GetGlobalKMS() KMSProvider {
	globalKMSMutex.RLock()
	defer globalKMSMutex.RUnlock()
	return globalKMSProvider
}

// SetGlobalKMSProvider sets the global KMS provider.
// This is mainly for backward compatibility.
func SetGlobalKMSProvider(provider KMSProvider) {
	globalKMSMutex.Lock()
	defer globalKMSMutex.Unlock()

	// Close existing provider if any
	if globalKMSProvider != nil {
		globalKMSProvider.Close()
	}

	globalKMSProvider = provider
}

// InitializeKMSManager initializes the global KMS manager
func InitializeKMSManager() *KMSManager {
	globalKMSMutex.Lock()
	defer globalKMSMutex.Unlock()

	if globalKMSManager == nil {
		globalKMSManager = &KMSManager{
			providers: make(map[string]KMSProvider),
			configs:   make(map[string]*KMSConfig),
			bucketKMS: make(map[string]string),
		}
		glog.V(1).Infof("KMS Manager initialized")
	}

	return globalKMSManager
}

// GetKMSManager returns the global KMS manager
func GetKMSManager() *KMSManager {
	globalKMSMutex.RLock()
	manager := globalKMSManager
	globalKMSMutex.RUnlock()

	if manager == nil {
		return InitializeKMSManager()
	}

	return manager
}

// AddKMSProvider adds a KMS provider configuration
func (km *KMSManager) AddKMSProvider(name string, config *KMSConfig) error {
	if name == "" {
		return fmt.Errorf("provider name cannot be empty")
	}

	if config == nil {
		return fmt.Errorf("KMS configuration cannot be nil")
	}

	km.mu.Lock()
	defer km.mu.Unlock()

	// Close existing provider if it exists
	if existingProvider, exists := km.providers[name]; exists {
		if err := existingProvider.Close(); err != nil {
			glog.Errorf("Failed to close existing KMS provider %s: %v", name, err)
		}
	}

	// Create new provider instance
	configAdapter := &configAdapter{config: config.Config}
	provider, err := GetProvider(config.Provider, configAdapter)
	if err != nil {
		return fmt.Errorf("failed to create KMS provider %s: %w", name, err)
	}

	// Store provider and configuration
	km.providers[name] = provider
	km.configs[name] = config

	glog.V(1).Infof("Added KMS provider %s (type: %s)", name, config.Provider)
	return nil
}

// SetDefaultKMSProvider sets the default KMS provider
func (km *KMSManager) SetDefaultKMSProvider(name string) error {
	km.mu.RLock()
	_, exists := km.providers[name]
	km.mu.RUnlock()

	if !exists {
		return fmt.Errorf("KMS provider %s does not exist", name)
	}

	km.mu.Lock()
	km.defaultKMS = name
	km.mu.Unlock()

	glog.V(1).Infof("Set default KMS provider to %s", name)
	return nil
}

// SetBucketKMSProvider sets the KMS provider for a specific bucket
func (km *KMSManager) SetBucketKMSProvider(bucket, providerName string) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	km.mu.RLock()
	_, exists := km.providers[providerName]
	km.mu.RUnlock()

	if !exists {
		return fmt.Errorf("KMS provider %s does not exist", providerName)
	}

	km.mu.Lock()
	km.bucketKMS[bucket] = providerName
	km.mu.Unlock()

	glog.V(2).Infof("Set KMS provider for bucket %s to %s", bucket, providerName)
	return nil
}

// GetKMSProviderByName returns a specific KMS provider by name
func (km *KMSManager) GetKMSProviderByName(name string) (KMSProvider, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	provider, exists := km.providers[name]
	if !exists {
		return nil, fmt.Errorf("KMS provider %s not found", name)
	}

	return provider, nil
}

// ListKMSProviders returns all configured KMS provider names
func (km *KMSManager) ListKMSProviders() []string {
	km.mu.RLock()
	defer km.mu.RUnlock()

	names := make([]string, 0, len(km.providers))
	for name := range km.providers {
		names = append(names, name)
	}

	return names
}

// GetKMSHealth returns health status of all KMS providers
func (km *KMSManager) GetKMSHealth(ctx context.Context) map[string]error {
	km.mu.RLock()
	defer km.mu.RUnlock()

	health := make(map[string]error)

	for name, provider := range km.providers {
		// Try to perform a basic operation to check health
		// We'll use DescribeKey with a dummy key - the error will tell us if KMS is reachable
		req := &DescribeKeyRequest{KeyID: "health-check-dummy-key"}
		_, err := provider.DescribeKey(ctx, req)

		// If it's a "not found" error, KMS is healthy but key doesn't exist (expected)
		if kmsErr, ok := err.(*KMSError); ok && kmsErr.Code == ErrCodeNotFoundException {
			health[name] = nil // Healthy
		} else if err != nil {
			health[name] = err // Unhealthy
		} else {
			health[name] = nil // Healthy (shouldn't happen with dummy key, but just in case)
		}
	}

	return health
}
