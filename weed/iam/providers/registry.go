package providers

import (
	"fmt"
	"sync"
)

// ProviderRegistry manages registered identity providers
type ProviderRegistry struct {
	mu        sync.RWMutex
	providers map[string]IdentityProvider
}

// NewProviderRegistry creates a new provider registry
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers: make(map[string]IdentityProvider),
	}
}

// RegisterProvider registers a new identity provider
func (r *ProviderRegistry) RegisterProvider(provider IdentityProvider) error {
	if provider == nil {
		return fmt.Errorf("provider cannot be nil")
	}

	name := provider.Name()
	if name == "" {
		return fmt.Errorf("provider name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.providers[name]; exists {
		return fmt.Errorf("provider %s is already registered", name)
	}

	r.providers[name] = provider
	return nil
}

// GetProvider retrieves a provider by name
func (r *ProviderRegistry) GetProvider(name string) (IdentityProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	provider, exists := r.providers[name]
	return provider, exists
}

// ListProviders returns all registered provider names
func (r *ProviderRegistry) ListProviders() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var names []string
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// UnregisterProvider removes a provider from the registry
func (r *ProviderRegistry) UnregisterProvider(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.providers[name]; !exists {
		return fmt.Errorf("provider %s is not registered", name)
	}

	delete(r.providers, name)
	return nil
}

// Clear removes all providers from the registry
func (r *ProviderRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.providers = make(map[string]IdentityProvider)
}

// GetProviderCount returns the number of registered providers
func (r *ProviderRegistry) GetProviderCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.providers)
}

// Default global registry
var defaultRegistry = NewProviderRegistry()

// RegisterProvider registers a provider in the default registry
func RegisterProvider(provider IdentityProvider) error {
	return defaultRegistry.RegisterProvider(provider)
}

// GetProvider retrieves a provider from the default registry
func GetProvider(name string) (IdentityProvider, bool) {
	return defaultRegistry.GetProvider(name)
}

// ListProviders returns all provider names from the default registry
func ListProviders() []string {
	return defaultRegistry.ListProviders()
}
