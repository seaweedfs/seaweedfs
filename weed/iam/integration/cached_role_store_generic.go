package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// RoleStoreAdapter adapts RoleStore interface to CacheableStore[*RoleDefinition]
type RoleStoreAdapter struct {
	store RoleStore
}

// NewRoleStoreAdapter creates a new adapter for RoleStore
func NewRoleStoreAdapter(store RoleStore) *RoleStoreAdapter {
	return &RoleStoreAdapter{store: store}
}

// Get implements CacheableStore interface
func (a *RoleStoreAdapter) Get(ctx context.Context, filerAddress string, key string) (*RoleDefinition, error) {
	return a.store.GetRole(ctx, filerAddress, key)
}

// Store implements CacheableStore interface
func (a *RoleStoreAdapter) Store(ctx context.Context, filerAddress string, key string, value *RoleDefinition) error {
	return a.store.StoreRole(ctx, filerAddress, key, value)
}

// Delete implements CacheableStore interface
func (a *RoleStoreAdapter) Delete(ctx context.Context, filerAddress string, key string) error {
	return a.store.DeleteRole(ctx, filerAddress, key)
}

// List implements CacheableStore interface
func (a *RoleStoreAdapter) List(ctx context.Context, filerAddress string) ([]string, error) {
	return a.store.ListRoles(ctx, filerAddress)
}

// GenericCachedRoleStore implements RoleStore using the generic cache
type GenericCachedRoleStore struct {
	*util.CachedStore[*RoleDefinition]
	adapter *RoleStoreAdapter
}

// NewGenericCachedRoleStore creates a new cached role store using generics
func NewGenericCachedRoleStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*GenericCachedRoleStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerRoleStore(config, filerAddressProvider, masterClient)
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
	adapter := NewRoleStoreAdapter(filerStore)
	cachedStore := util.NewCachedStore(
		"role",
		adapter,
		genericCopyRoleDefinition, // Copy function
		util.CachedStoreConfig{
			TTL:          cacheTTL,
			ListTTL:      listTTL,
			MaxCacheSize: maxCacheSize,
		},
	)

	glog.V(2).Infof("Initialized GenericCachedRoleStore with TTL %v, List TTL %v, Max Cache Size %d",
		cacheTTL, listTTL, maxCacheSize)

	return &GenericCachedRoleStore{
		CachedStore: cachedStore,
		adapter:     adapter,
	}, nil
}

// StoreRole implements RoleStore interface
func (c *GenericCachedRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	return c.Store(ctx, filerAddress, roleName, role)
}

// GetRole implements RoleStore interface
func (c *GenericCachedRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	return c.Get(ctx, filerAddress, roleName)
}

// ListRoles implements RoleStore interface
func (c *GenericCachedRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	return c.List(ctx, filerAddress)
}

// DeleteRole implements RoleStore interface
func (c *GenericCachedRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	return c.Delete(ctx, filerAddress, roleName)
}

// InvalidateCache invalidates the cache for a specific role
func (c *GenericCachedRoleStore) InvalidateCache(roleName string) {
	c.CachedStore.Invalidate(roleName)
}

// ListRolesDefinitions lists all role definitions (delegates to adapter/store)
func (c *GenericCachedRoleStore) ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error) {
	// The generic CachedStore doesn't natively support ListWithDetails caching yet.
	// We delegate to the underlying store (FilerRoleStore) which has the parallel fetch implementation.
	// Note: We need to cast the store in the adapter to call the specific method.
	if filerStore, ok := c.adapter.store.(interface {
		ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error)
	}); ok {
		return filerStore.ListRolesDefinitions(ctx, filerAddress)
	}
	
	// Fallback if underlying store doesn't support it (e.g. mock or old store)
	// perform manual N+1 fetch (this shouldn't happen with our FilerRoleStore)
	return nil, fmt.Errorf("underlying store does not support ListRolesDefinitions")
}

// ListAttachedRolePolicies lists all policies attached to a specific role (delegates to adapter/store)
func (c *GenericCachedRoleStore) ListAttachedRolePolicies(ctx context.Context, filerAddress string, roleName string) ([]string, error) {
	// The generic CachedStore doesn't natively support this specific query.
	// We delegate to the underlying store.
	if filerStore, ok := c.adapter.store.(interface {
		ListAttachedRolePolicies(ctx context.Context, filerAddress string, roleName string) ([]string, error)
	}); ok {
		return filerStore.ListAttachedRolePolicies(ctx, filerAddress, roleName)
	}

	return nil, fmt.Errorf("underlying store does not support ListAttachedRolePolicies")
}

// genericCopyRoleDefinition creates a deep copy of a RoleDefinition for the generic cache
func genericCopyRoleDefinition(role *RoleDefinition) *RoleDefinition {
	if role == nil {
		return nil
	}

	result := &RoleDefinition{
		RoleName:           role.RoleName,
		RoleArn:            role.RoleArn,
		Description:        role.Description,
		MaxSessionDuration: role.MaxSessionDuration,
	}

	// Deep copy trust policy if it exists
	if role.TrustPolicy != nil {
		trustPolicyData, err := json.Marshal(role.TrustPolicy)
		if err != nil {
			glog.Errorf("Failed to marshal trust policy for deep copy: %v", err)
			return nil
		}
		var trustPolicyCopy policy.PolicyDocument
		if err := json.Unmarshal(trustPolicyData, &trustPolicyCopy); err != nil {
			glog.Errorf("Failed to unmarshal trust policy for deep copy: %v", err)
			return nil
		}
		result.TrustPolicy = &trustPolicyCopy
	}

	// Deep copy attached policies slice
	if role.AttachedPolicies != nil {
		result.AttachedPolicies = make([]string, len(role.AttachedPolicies))
		copy(result.AttachedPolicies, role.AttachedPolicies)
	}

	// Deep copy inline policies map
	if role.InlinePolicies != nil {
		result.InlinePolicies = make(map[string]string, len(role.InlinePolicies))
		for k, v := range role.InlinePolicies {
			result.InlinePolicies[k] = v
		}
	}

	return result
}

