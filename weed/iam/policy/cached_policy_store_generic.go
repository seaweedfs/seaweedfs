package policy

import (
	"context"
	"encoding/json"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// PolicyStoreAdapter adapts PolicyStore interface to CacheableStore[*PolicyDocument]
type PolicyStoreAdapter struct {
	store PolicyStore
}

// NewPolicyStoreAdapter creates a new adapter for PolicyStore
func NewPolicyStoreAdapter(store PolicyStore) *PolicyStoreAdapter {
	return &PolicyStoreAdapter{store: store}
}

// Get implements CacheableStore interface
func (a *PolicyStoreAdapter) Get(ctx context.Context, filerAddress string, key string) (*PolicyDocument, error) {
	return a.store.GetPolicy(ctx, filerAddress, key)
}

// Store implements CacheableStore interface
func (a *PolicyStoreAdapter) Store(ctx context.Context, filerAddress string, key string, value *PolicyDocument) error {
	return a.store.StorePolicy(ctx, filerAddress, key, value)
}

// Delete implements CacheableStore interface
func (a *PolicyStoreAdapter) Delete(ctx context.Context, filerAddress string, key string) error {
	return a.store.DeletePolicy(ctx, filerAddress, key)
}

// List implements CacheableStore interface
func (a *PolicyStoreAdapter) List(ctx context.Context, filerAddress string) ([]string, error) {
	metas, err := a.store.ListPolicies(ctx, filerAddress)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(metas))
	for i, meta := range metas {
		names[i] = meta.Name
	}
	return names, nil
}

// GenericCachedPolicyStore implements PolicyStore using the generic cache
type GenericCachedPolicyStore struct {
	*util.CachedStore[*PolicyDocument]
	adapter *PolicyStoreAdapter
}

// NewGenericCachedPolicyStore creates a new cached policy store using generics
func NewGenericCachedPolicyStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*GenericCachedPolicyStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerPolicyStore(config, filerAddressProvider, masterClient)
	if err != nil {
		return nil, err
	}

	// Parse cache configuration with defaults
	cacheTTL := 5 * time.Minute
	listTTL := 1 * time.Minute
	maxCacheSize := int64(1000)

	if config != nil {
		if ttlStr, ok := config["ttl"].(string); ok && ttlStr != "" {
			if parsed, err := time.ParseDuration(ttlStr); err == nil {
				cacheTTL = parsed
			}
		}
		if listTTLStr, ok := config["listTtl"].(string); ok && listTTLStr != "" {
			if parsed, err := time.ParseDuration(listTTLStr); err == nil {
				listTTL = parsed
			}
		}
		if maxSize, ok := config["maxCacheSize"].(int); ok && maxSize > 0 {
			maxCacheSize = int64(maxSize)
		}
	}

	// Create adapter and generic cached store
	adapter := NewPolicyStoreAdapter(filerStore)
	cachedStore := util.NewCachedStore(
		"policy",
		adapter,
		genericCopyPolicyDocument, // Copy function
		util.CachedStoreConfig{
			TTL:          cacheTTL,
			ListTTL:      listTTL,
			MaxCacheSize: maxCacheSize,
		},
	)

	glog.V(2).Infof("Initialized GenericCachedPolicyStore with TTL %v, List TTL %v, Max Cache Size %d",
		cacheTTL, listTTL, maxCacheSize)

	return &GenericCachedPolicyStore{
		CachedStore: cachedStore,
		adapter:     adapter,
	}, nil
}

// StorePolicy implements PolicyStore interface
func (c *GenericCachedPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	return c.Store(ctx, filerAddress, name, policy)
}

// GetPolicy implements PolicyStore interface
func (c *GenericCachedPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	return c.Get(ctx, filerAddress, name)
}

// ListPolicies implements PolicyStore interface
func (c *GenericCachedPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]PolicyMetadata, error) {
	// Bypass list cache to ensure metadata (timestamps) is fresh
	return c.adapter.store.ListPolicies(ctx, filerAddress)
}

// DeletePolicy implements PolicyStore interface
func (c *GenericCachedPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	return c.Delete(ctx, filerAddress, name)
}

// genericCopyPolicyDocument creates a deep copy of a PolicyDocument for the generic cache
func genericCopyPolicyDocument(policy *PolicyDocument) *PolicyDocument {
	if policy == nil {
		return nil
	}

	// Perform a deep copy to ensure cache isolation
	// Using JSON marshaling is a safe way to achieve this
	policyData, err := json.Marshal(policy)
	if err != nil {
		glog.Errorf("Failed to marshal policy document for deep copy: %v", err)
		return nil
	}

	var copied PolicyDocument
	if err := json.Unmarshal(policyData, &copied); err != nil {
		glog.Errorf("Failed to unmarshal policy document for deep copy: %v", err)
		return nil
	}

	return &copied
}
