package kms

import (
	"context"
	"errors"
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

// ListProviders returns the names of all registered providers
func ListProviders() []string {
	return defaultRegistry.ListProviders()
}

// ListProviders returns the names of all registered providers
func (r *ProviderRegistry) ListProviders() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// CloseAll closes all provider instances
func CloseAll() error {
	return defaultRegistry.CloseAll()
}

// CloseAll closes all provider instances in this registry
func (r *ProviderRegistry) CloseAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var allErrors []error
	for name, instance := range r.instances {
		if err := instance.Close(); err != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to close KMS provider '%s': %w", name, err))
		}
	}

	// Clear the instances map
	r.instances = make(map[string]KMSProvider)

	return errors.Join(allErrors...)
}

// WithKMSProvider is a helper function to execute code with a KMS provider
func WithKMSProvider(name string, config util.Configuration, fn func(KMSProvider) error) error {
	provider, err := GetProvider(name, config)
	if err != nil {
		return err
	}
	return fn(provider)
}

// TestKMSConnection tests the connection to a KMS provider
func TestKMSConnection(ctx context.Context, provider KMSProvider, testKeyID string) error {
	if provider == nil {
		return fmt.Errorf("KMS provider is nil")
	}

	// Try to describe a test key to verify connectivity
	_, err := provider.DescribeKey(ctx, &DescribeKeyRequest{
		KeyID: testKeyID,
	})

	if err != nil {
		// If the key doesn't exist, that's still a successful connection test
		if kmsErr, ok := err.(*KMSError); ok && kmsErr.Code == ErrCodeNotFoundException {
			return nil
		}
		return fmt.Errorf("KMS connection test failed: %v", err)
	}

	return nil
}
