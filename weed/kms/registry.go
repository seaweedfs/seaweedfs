package kms

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ProviderRegistry manages KMS provider implementations
type ProviderRegistry struct {
	mu        sync.RWMutex
	providers map[string]ProviderFactory
	instances map[string]KMSProvider
}

// ProviderFactory creates a new KMS provider instance
type ProviderFactory func(config util.Configuration) (KMSProvider, error)

var defaultRegistry = NewProviderRegistry()

// NewProviderRegistry creates a new provider registry
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers: make(map[string]ProviderFactory),
		instances: make(map[string]KMSProvider),
	}
}

// RegisterProvider registers a new KMS provider factory
func RegisterProvider(name string, factory ProviderFactory) {
	defaultRegistry.RegisterProvider(name, factory)
}

// RegisterProvider registers a new KMS provider factory in this registry
func (r *ProviderRegistry) RegisterProvider(name string, factory ProviderFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[name] = factory
}

// GetProvider returns a KMS provider instance, creating it if necessary
func GetProvider(name string, config util.Configuration) (KMSProvider, error) {
	return defaultRegistry.GetProvider(name, config)
}

// GetProvider returns a KMS provider instance, creating it if necessary
func (r *ProviderRegistry) GetProvider(name string, config util.Configuration) (KMSProvider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return existing instance if available
	if instance, exists := r.instances[name]; exists {
		return instance, nil
	}

	// Find the factory
	factory, exists := r.providers[name]
	if !exists {
		return nil, fmt.Errorf("KMS provider '%s' not registered", name)
	}

	// Create new instance
	instance, err := factory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create KMS provider '%s': %v", name, err)
	}

	// Cache the instance
	r.instances[name] = instance
	return instance, nil
}
