package integration

import (
	"context"
	"encoding/json"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/util"
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
func NewGenericCachedRoleStore(config map[string]interface{}, filerAddressProvider func() string) (*GenericCachedRoleStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerRoleStore(config, filerAddressProvider)
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

// genericCopyRoleDefinition creates a deep copy of a RoleDefinition for the generic cache
func genericCopyRoleDefinition(role *RoleDefinition) *RoleDefinition {
	if role == nil {
		return nil
	}

	result := &RoleDefinition{
		RoleName:    role.RoleName,
		RoleArn:     role.RoleArn,
		Description: role.Description,
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

	return result
}
