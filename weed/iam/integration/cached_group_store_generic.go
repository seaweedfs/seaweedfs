package integration

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// GroupStoreAdapter adapts GroupStore interface to CacheableStore[*GroupDefinition]
type GroupStoreAdapter struct {
	store GroupStore
}

// NewGroupStoreAdapter creates a new adapter for GroupStore
func NewGroupStoreAdapter(store GroupStore) *GroupStoreAdapter {
	return &GroupStoreAdapter{store: store}
}

// Get implements CacheableStore interface
func (a *GroupStoreAdapter) Get(ctx context.Context, filerAddress string, key string) (*GroupDefinition, error) {
	return a.store.GetGroup(ctx, filerAddress, key)
}

// Store implements CacheableStore interface
func (a *GroupStoreAdapter) Store(ctx context.Context, filerAddress string, key string, value *GroupDefinition) error {
	return a.store.StoreGroup(ctx, filerAddress, key, value)
}

// Delete implements CacheableStore interface
func (a *GroupStoreAdapter) Delete(ctx context.Context, filerAddress string, key string) error {
	return a.store.DeleteGroup(ctx, filerAddress, key)
}

// List implements CacheableStore interface
func (a *GroupStoreAdapter) List(ctx context.Context, filerAddress string) ([]string, error) {
	return a.store.ListGroups(ctx, filerAddress)
}

// GenericCachedGroupStore implements GroupStore using the generic cache
type GenericCachedGroupStore struct {
	*util.CachedStore[*GroupDefinition]
	adapter *GroupStoreAdapter
}

// NewGenericCachedGroupStore creates a new cached group store using generics
func NewGenericCachedGroupStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*GenericCachedGroupStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerGroupStore(config, filerAddressProvider, masterClient)
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
	adapter := NewGroupStoreAdapter(filerStore)
	cachedStore := util.NewCachedStore(
		"group",
		adapter,
		copyGroupDefinition, // Copy function from group_store.go
		util.CachedStoreConfig{
			TTL:          cacheTTL,
			ListTTL:      listTTL,
			MaxCacheSize: maxCacheSize,
		},
	)

	glog.V(2).Infof("Initialized GenericCachedGroupStore with TTL %v, List TTL %v, Max Cache Size %d",
		cacheTTL, listTTL, maxCacheSize)

	return &GenericCachedGroupStore{
		CachedStore: cachedStore,
		adapter:     adapter,
	}, nil
}

// StoreGroup implements GroupStore interface
func (c *GenericCachedGroupStore) StoreGroup(ctx context.Context, filerAddress string, groupName string, group *GroupDefinition) error {
	return c.Store(ctx, filerAddress, groupName, group)
}

// GetGroup implements GroupStore interface
func (c *GenericCachedGroupStore) GetGroup(ctx context.Context, filerAddress string, groupName string) (*GroupDefinition, error) {
	return c.Get(ctx, filerAddress, groupName)
}

// ListGroups implements GroupStore interface
func (c *GenericCachedGroupStore) ListGroups(ctx context.Context, filerAddress string) ([]string, error) {
	return c.List(ctx, filerAddress)
}

// DeleteGroup implements GroupStore interface
func (c *GenericCachedGroupStore) DeleteGroup(ctx context.Context, filerAddress string, groupName string) error {
	return c.Delete(ctx, filerAddress, groupName)
}

// InvalidateCache invalidates the cache for a specific group
func (c *GenericCachedGroupStore) InvalidateCache(groupName string) {
	c.CachedStore.Invalidate(groupName)
}
