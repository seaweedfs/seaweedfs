package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// RoleStore defines the interface for storing IAM role definitions
type RoleStore interface {
	// StoreRole stores a role definition (filerAddress ignored for memory stores)
	StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error

	// GetRole retrieves a role definition (filerAddress ignored for memory stores)
	GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error)

	// ListRoles lists all role names (filerAddress ignored for memory stores)
	ListRoles(ctx context.Context, filerAddress string) ([]string, error)

	// DeleteRole deletes a role definition (filerAddress ignored for memory stores)
	DeleteRole(ctx context.Context, filerAddress string, roleName string) error
}

// MemoryRoleStore implements RoleStore using in-memory storage
type MemoryRoleStore struct {
	roles map[string]*RoleDefinition
	mutex sync.RWMutex
}

// NewMemoryRoleStore creates a new memory-based role store
func NewMemoryRoleStore() *MemoryRoleStore {
	return &MemoryRoleStore{
		roles: make(map[string]*RoleDefinition),
	}
}

// StoreRole stores a role definition in memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	if role == nil {
		return fmt.Errorf("role cannot be nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Deep copy the role to prevent external modifications
	m.roles[roleName] = copyRoleDefinition(role)
	return nil
}

// GetRole retrieves a role definition from memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	role, exists := m.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}

	// Return a copy to prevent external modifications
	return copyRoleDefinition(role), nil
}

// ListRoles lists all role names in memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	names := make([]string, 0, len(m.roles))
	for name := range m.roles {
		names = append(names, name)
	}

	return names, nil
}

// DeleteRole deletes a role definition from memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.roles, roleName)
	return nil
}

// copyRoleDefinition creates a deep copy of a role definition
func copyRoleDefinition(original *RoleDefinition) *RoleDefinition {
	if original == nil {
		return nil
	}

	copied := &RoleDefinition{
		RoleName:    original.RoleName,
		RoleArn:     original.RoleArn,
		Description: original.Description,
	}

	// Deep copy trust policy if it exists
	if original.TrustPolicy != nil {
		// Use JSON marshaling for deep copy of the complex policy structure
		trustPolicyData, _ := json.Marshal(original.TrustPolicy)
		var trustPolicyCopy policy.PolicyDocument
		json.Unmarshal(trustPolicyData, &trustPolicyCopy)
		copied.TrustPolicy = &trustPolicyCopy
	}

	// Copy attached policies slice
	if original.AttachedPolicies != nil {
		copied.AttachedPolicies = make([]string, len(original.AttachedPolicies))
		copy(copied.AttachedPolicies, original.AttachedPolicies)
	}

	return copied
}

// FilerRoleStore implements RoleStore using SeaweedFS filer
type FilerRoleStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
}

// NewFilerRoleStore creates a new filer-based role store
func NewFilerRoleStore(config map[string]interface{}, filerAddressProvider func() string) (*FilerRoleStore, error) {
	store := &FilerRoleStore{
		basePath:             "/etc/iam/roles", // Default path for role storage - aligned with /etc/ convention
		filerAddressProvider: filerAddressProvider,
	}

	// Parse configuration - only basePath and other settings, NOT filerAddress
	if config != nil {
		if basePath, ok := config["basePath"].(string); ok && basePath != "" {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	glog.V(2).Infof("Initialized FilerRoleStore with basePath %s", store.basePath)

	return store, nil
}

// StoreRole stores a role definition in filer
func (f *FilerRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	if role == nil {
		return fmt.Errorf("role cannot be nil")
	}

	// Serialize role to JSON
	roleData, err := json.MarshalIndent(role, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize role: %v", err)
	}

	rolePath := f.getRolePath(roleName)

	// Store in filer
	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        f.getRoleFileName(roleName),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0600), // Read/write for owner only
					Uid:      uint32(0),
					Gid:      uint32(0),
				},
				Content: roleData,
			},
		}

		glog.V(3).Infof("Storing role %s at %s", roleName, rolePath)
		_, err := client.CreateEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to store role %s: %v", roleName, err)
		}

		return nil
	})
}

// GetRole retrieves a role definition from filer
func (f *FilerRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	var roleData []byte
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.getRoleFileName(roleName),
		}

		glog.V(3).Infof("Looking up role %s", roleName)
		response, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("role not found: %v", err)
		}

		if response.Entry == nil {
			return fmt.Errorf("role not found")
		}

		roleData = response.Entry.Content
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Deserialize role from JSON
	var role RoleDefinition
	if err := json.Unmarshal(roleData, &role); err != nil {
		return nil, fmt.Errorf("failed to deserialize role: %v", err)
	}

	return &role, nil
}

// ListRoles lists all role names in filer
func (f *FilerRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerRoleStore")
	}

	var roleNames []string

	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory:          f.basePath,
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000, // Process in batches of 1000
		}

		glog.V(3).Infof("Listing roles in %s", f.basePath)
		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list roles: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			// Extract role name from filename
			filename := resp.Entry.Name
			if strings.HasSuffix(filename, ".json") {
				roleName := strings.TrimSuffix(filename, ".json")
				roleNames = append(roleNames, roleName)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return roleNames, nil
}

// DeleteRole deletes a role definition from filer
func (f *FilerRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:    f.basePath,
			Name:         f.getRoleFileName(roleName),
			IsDeleteData: true,
		}

		glog.V(3).Infof("Deleting role %s", roleName)
		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return nil // Idempotent: deletion of non-existent role is successful
			}
			return fmt.Errorf("failed to delete role %s: %v", roleName, err)
		}

		if resp.Error != "" {
			if strings.Contains(resp.Error, "not found") {
				return nil // Idempotent: deletion of non-existent role is successful
			}
			return fmt.Errorf("failed to delete role %s: %s", roleName, resp.Error)
		}

		return nil
	})
}

// Helper methods for FilerRoleStore

func (f *FilerRoleStore) getRoleFileName(roleName string) string {
	return roleName + ".json"
}

func (f *FilerRoleStore) getRolePath(roleName string) string {
	return f.basePath + "/" + f.getRoleFileName(roleName)
}

func (f *FilerRoleStore) withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), f.grpcDialOption, fn)
}

// CachedFilerRoleStore implements RoleStore with TTL caching on top of FilerRoleStore
type CachedFilerRoleStore struct {
	filerStore *FilerRoleStore
	cache      *ccache.Cache
	listCache  *ccache.Cache
	ttl        time.Duration
	listTTL    time.Duration
}

// CachedFilerRoleStoreConfig holds configuration for the cached role store
type CachedFilerRoleStoreConfig struct {
	BasePath     string `json:"basePath,omitempty"`
	TTL          string `json:"ttl,omitempty"`          // e.g., "5m", "1h"
	ListTTL      string `json:"listTtl,omitempty"`      // e.g., "1m", "30s"
	MaxCacheSize int    `json:"maxCacheSize,omitempty"` // Maximum number of cached roles
}

// NewCachedFilerRoleStore creates a new cached filer-based role store
func NewCachedFilerRoleStore(config map[string]interface{}) (*CachedFilerRoleStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerRoleStore(config, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create filer role store: %w", err)
	}

	// Parse cache configuration with defaults
	cacheTTL := 5 * time.Minute // Default 5 minutes for role cache
	listTTL := 1 * time.Minute  // Default 1 minute for list cache
	maxCacheSize := 1000        // Default max 1000 cached roles

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
			maxCacheSize = maxSize
		}
	}

	// Create ccache instances with appropriate configurations
	pruneCount := int64(maxCacheSize) >> 3
	if pruneCount <= 0 {
		pruneCount = 100
	}

	store := &CachedFilerRoleStore{
		filerStore: filerStore,
		cache:      ccache.New(ccache.Configure().MaxSize(int64(maxCacheSize)).ItemsToPrune(uint32(pruneCount))),
		listCache:  ccache.New(ccache.Configure().MaxSize(100).ItemsToPrune(10)), // Smaller cache for lists
		ttl:        cacheTTL,
		listTTL:    listTTL,
	}

	glog.V(2).Infof("Initialized CachedFilerRoleStore with TTL %v, List TTL %v, Max Cache Size %d",
		cacheTTL, listTTL, maxCacheSize)

	return store, nil
}

// StoreRole stores a role definition and invalidates the cache
func (c *CachedFilerRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	// Store in filer
	err := c.filerStore.StoreRole(ctx, filerAddress, roleName, role)
	if err != nil {
		return err
	}

	// Invalidate cache entries
	c.cache.Delete(roleName)
	c.listCache.Clear() // Invalidate list cache

	glog.V(3).Infof("Stored and invalidated cache for role %s", roleName)
	return nil
}

// GetRole retrieves a role definition with caching
func (c *CachedFilerRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	// Try to get from cache first
	item := c.cache.Get(roleName)
	if item != nil {
		// Cache hit - return cached role (DO NOT extend TTL)
		role := item.Value().(*RoleDefinition)
		glog.V(4).Infof("Cache hit for role %s", roleName)
		return copyRoleDefinition(role), nil
	}

	// Cache miss - fetch from filer
	glog.V(4).Infof("Cache miss for role %s, fetching from filer", roleName)
	role, err := c.filerStore.GetRole(ctx, filerAddress, roleName)
	if err != nil {
		return nil, err
	}

	// Cache the result with TTL
	c.cache.Set(roleName, copyRoleDefinition(role), c.ttl)
	glog.V(3).Infof("Cached role %s with TTL %v", roleName, c.ttl)
	return role, nil
}

// ListRoles lists all role names with caching
func (c *CachedFilerRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	// Use a constant key for the role list cache
	const listCacheKey = "role_list"

	// Try to get from list cache first
	item := c.listCache.Get(listCacheKey)
	if item != nil {
		// Cache hit - return cached list (DO NOT extend TTL)
		roles := item.Value().([]string)
		glog.V(4).Infof("List cache hit, returning %d roles", len(roles))
		return append([]string(nil), roles...), nil // Return a copy
	}

	// Cache miss - fetch from filer
	glog.V(4).Infof("List cache miss, fetching from filer")
	roles, err := c.filerStore.ListRoles(ctx, filerAddress)
	if err != nil {
		return nil, err
	}

	// Cache the result with TTL (store a copy)
	rolesCopy := append([]string(nil), roles...)
	c.listCache.Set(listCacheKey, rolesCopy, c.listTTL)
	glog.V(3).Infof("Cached role list with %d entries, TTL %v", len(roles), c.listTTL)
	return roles, nil
}

// DeleteRole deletes a role definition and invalidates the cache
func (c *CachedFilerRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	// Delete from filer
	err := c.filerStore.DeleteRole(ctx, filerAddress, roleName)
	if err != nil {
		return err
	}

	// Invalidate cache entries
	c.cache.Delete(roleName)
	c.listCache.Clear() // Invalidate list cache

	glog.V(3).Infof("Deleted and invalidated cache for role %s", roleName)
	return nil
}

// ClearCache clears all cached entries (for testing or manual cache invalidation)
func (c *CachedFilerRoleStore) ClearCache() {
	c.cache.Clear()
	c.listCache.Clear()
	glog.V(2).Infof("Cleared all role cache entries")
}

// GetCacheStats returns cache statistics
func (c *CachedFilerRoleStore) GetCacheStats() map[string]interface{} {
	return map[string]interface{}{
		"roleCache": map[string]interface{}{
			"size": c.cache.ItemCount(),
			"ttl":  c.ttl.String(),
		},
		"listCache": map[string]interface{}{
			"size": c.listCache.ItemCount(),
			"ttl":  c.listTTL.String(),
		},
	}
}
