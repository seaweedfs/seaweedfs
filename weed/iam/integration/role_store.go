package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/karlseguin/ccache/v2"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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

	// ListRolesDefinitions lists all roles with their full definitions (mailerAddress ignored for memory stores)
	// This allows for optimized batch retrieval (e.g. parallel fetching)
	ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error)

	// ListAttachedRolePolicies lists all policies attached to a specific role
	ListAttachedRolePolicies(ctx context.Context, filerAddress string, roleName string) ([]string, error)

	// InvalidateCache invalidates the cache for a specific role (no-op for non-cached stores)
	InvalidateCache(roleName string)
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

// ListAttachedRolePolicies lists all policies attached to a specific role
func (m *MemoryRoleStore) ListAttachedRolePolicies(ctx context.Context, filerAddress string, roleName string) ([]string, error) {
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
	policies := make([]string, len(role.AttachedPolicies))
	copy(policies, role.AttachedPolicies)
	return policies, nil
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

// ListRolesDefinitions lists all role definitions from memory
func (m *MemoryRoleStore) ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	definitions := make([]*RoleDefinition, 0, len(m.roles))
	for _, role := range m.roles {
		definitions = append(definitions, copyRoleDefinition(role))
	}

	return definitions, nil
}

// InvalidateCache invalidates the cache for a specific role (no-op for memory store)
func (m *MemoryRoleStore) InvalidateCache(roleName string) {
	// No-op for memory store as it's the source of truth
}

// copyRoleDefinition creates a deep copy of a role definition
func copyRoleDefinition(original *RoleDefinition) *RoleDefinition {
	if original == nil {
		return nil
	}

	copied := &RoleDefinition{
		RoleName:           original.RoleName,
		RoleArn:            original.RoleArn,
		Description:        original.Description,
		MaxSessionDuration: original.MaxSessionDuration,
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

	// Copy inline policies map
	if original.InlinePolicies != nil {
		copied.InlinePolicies = make(map[string]string, len(original.InlinePolicies))
		for k, v := range original.InlinePolicies {
			copied.InlinePolicies[k] = v
		}
	}

	return copied
}

// FilerRoleStore implements RoleStore using SeaweedFS filer
type FilerRoleStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
	masterClient         *wdclient.MasterClient
}

// NewFilerRoleStore creates a new filer-based role store
func NewFilerRoleStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*FilerRoleStore, error) {
	store := &FilerRoleStore{
		basePath:             "/etc/iam/roles", // Default path for role storage - aligned with /etc/ convention
		filerAddressProvider: filerAddressProvider,
		masterClient:         masterClient,
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
		glog.V(0).Infof("StoreRole: Storing role %s at %s. Data Size: %d", roleName, rolePath, len(roleData))
		glog.V(4).Infof("StoreRole: Role Data: %s", string(roleData))

		return filer.SaveInsideFiler(client, f.basePath, f.getRoleFileName(roleName), roleData)
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

		// If content is empty but chunks exist, we need to read the full entry (requires masterClient)
		if len(roleData) == 0 && len(response.Entry.Chunks) > 0 {
			if f.masterClient != nil {
				glog.V(3).Infof("Reading chunked role data for %s", roleName)
				var buf bytes.Buffer
				if err := filer.ReadEntry(f.masterClient, client, f.basePath, f.getRoleFileName(roleName), &buf); err != nil {
					return fmt.Errorf("failed to read chunked role: %v", err)
				}
				roleData = buf.Bytes()
			} else {
				// Fallback or warning? Without masterClient we can't read chunks reliable via standard helpers
				// But we can try to return error to prompt upstream to handle it differently
				glog.Warningf("Role %s has chunks but no masterClient available to read them", roleName)
			}
		}

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

// ListRolesDefinitions lists all role definitions from filer with parallel fetching
func (f *FilerRoleStore) ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error) {
	// 1. List all role names first
	roleNames, err := f.ListRoles(ctx, filerAddress)
	if err != nil {
		return nil, err
	}

	if len(roleNames) == 0 {
		return []*RoleDefinition{}, nil
	}

	// 2. Fetch all roles in parallel
	// Use a worker pool pattern via errgroup to limit concurrency
	g, ctx := errgroup.WithContext(ctx)
	
	// Create a channel to send tasks
	roleNamesChan := make(chan string, len(roleNames))
	for _, name := range roleNames {
		roleNamesChan <- name
	}
	close(roleNamesChan)

	// Create a buffered channel for results
	resultsChan := make(chan *RoleDefinition, len(roleNames))

	// Start workers (limit to 10 concurrent fetches to avoid overwhelming the filer)
	numWorkers := 10
	if len(roleNames) < numWorkers {
		numWorkers = len(roleNames)
	}

	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			for name := range roleNamesChan {
				// Use the context from errgroup to cancel early if one fails
				role, err := f.GetRole(ctx, filerAddress, name)
				if err != nil {
					// Log error but continue? Or fail fast?
					// For Admin UI, partial results might be misleading. Let's fail fast or skip invalid files.
					// However, if one file is corrupt, we shouldn't break the whole list?
					// Let's log and skip for robustness.
					glog.Warningf("Failed to fetch role %s in batch: %v", name, err)
					continue
				}
				resultsChan <- role
			}
			return nil
		})
	}

	// Wait for all workers to finish
	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(resultsChan)

	// Collect results
	definitions := make([]*RoleDefinition, 0, len(roleNames))
	for role := range resultsChan {
		definitions = append(definitions, role)
	}

	return definitions, nil
}

// ListAttachedRolePolicies lists all policies attached to a specific role
func (f *FilerRoleStore) ListAttachedRolePolicies(ctx context.Context, filerAddress string, roleName string) ([]string, error) {
	// 1. Fetch the role
	role, err := f.GetRole(ctx, filerAddress, roleName)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}

	// 2. Return attached policies (safe copy not needed as GetRole already deserializes a new object)
	return role.AttachedPolicies, nil
}

// InvalidateCache invalidates the cache for a specific role (no-op for filer store)
func (f *FilerRoleStore) InvalidateCache(roleName string) {
	// No-op for filer store as it reads directly from source
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
func NewCachedFilerRoleStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*CachedFilerRoleStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerRoleStore(config, filerAddressProvider, masterClient)
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

// ListRolesDefinitions lists all role definitions (delegates to filer store, no caching for full list content yet)
func (c *CachedFilerRoleStore) ListRolesDefinitions(ctx context.Context, filerAddress string) ([]*RoleDefinition, error) {
	// We could cache the full list of definitions, but that might be large.
	// For now, let's just delegate to the filer store's parallel fetcher.
	// Optimization: We COULD check the cache for individual roles if we already have them!
	
	// 1. Get List of names
	roleNames, err := c.ListRoles(ctx, filerAddress)
	if err != nil {
		return nil, err
	}

	definitions := make([]*RoleDefinition, 0, len(roleNames))
	missingNames := make([]string, 0)

	// 2. Check cache for each
	for _, name := range roleNames {
		item := c.cache.Get(name)
		if item != nil {
			role := item.Value().(*RoleDefinition)
			definitions = append(definitions, copyRoleDefinition(role))
		} else {
			missingNames = append(missingNames, name)
		}
	}

	// 3. Fetch missing only
	if len(missingNames) > 0 {
		glog.V(3).Infof("ListRolesDefinitions: %d cached, fetching %d missing", len(definitions), len(missingNames))
		
		// Create a temporary "partial" fetcher logic here, or just rely on manual parallel fetch for missing
		// Since FilerRoleStore.ListRolesDefinitions fetches ALL, we can't easily reuse it for a subset without refactoring.
		// Let's just implement a parallel fetch for missing items here manually, similar to FilerRoleStore.
		
		g, ctx := errgroup.WithContext(ctx)
		
		nameChan := make(chan string, len(missingNames))
		for _, name := range missingNames {
			nameChan <- name
		}
		close(nameChan)
		
		resultChan := make(chan *RoleDefinition, len(missingNames))
		
		numWorkers := 10
		if len(missingNames) < numWorkers {
			numWorkers = len(missingNames)
		}

		for i := 0; i < numWorkers; i++ {
			g.Go(func() error {
				for name := range nameChan {
					// Fetch from underlying store
					role, err := c.filerStore.GetRole(ctx, filerAddress, name)
					if err != nil {
						glog.Warningf("Failed to fetch role %s: %v", name, err)
						continue
					}
					// Update cache
					c.cache.Set(name, copyRoleDefinition(role), c.ttl)
					resultChan <- role
				}
				return nil
			})
		}
		
		if err := g.Wait(); err != nil {
			return nil, err
		}
		close(resultChan)
		
		for role := range resultChan {
			definitions = append(definitions, role)
		}
	}

	return definitions, nil
}

// InvalidateCache invalidates the cache for a specific role
func (c *CachedFilerRoleStore) InvalidateCache(roleName string) {
	c.cache.Delete(roleName)
	// We might want to clear list cache too if needed, but for role update, just role cache is sufficient
	// unless we want to be safe about list consistency
	glog.V(3).Infof("Invalidated cache for role %s", roleName)
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
