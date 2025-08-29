package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// MemoryPolicyStore implements PolicyStore using in-memory storage
type MemoryPolicyStore struct {
	policies map[string]*PolicyDocument
	mutex    sync.RWMutex
}

// NewMemoryPolicyStore creates a new memory-based policy store
func NewMemoryPolicyStore() *MemoryPolicyStore {
	return &MemoryPolicyStore{
		policies: make(map[string]*PolicyDocument),
	}
}

// StorePolicy stores a policy document in memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Deep copy the policy to prevent external modifications
	s.policies[name] = copyPolicyDocument(policy)
	return nil
}

// GetPolicy retrieves a policy document from memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	if name == "" {
		return nil, fmt.Errorf("policy name cannot be empty")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	policy, exists := s.policies[name]
	if !exists {
		return nil, fmt.Errorf("policy not found: %s", name)
	}

	// Return a copy to prevent external modifications
	return copyPolicyDocument(policy), nil
}

// DeletePolicy deletes a policy document from memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.policies, name)
	return nil
}

// ListPolicies lists all policy names in memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	names := make([]string, 0, len(s.policies))
	for name := range s.policies {
		names = append(names, name)
	}

	return names, nil
}

// copyPolicyDocument creates a deep copy of a policy document
func copyPolicyDocument(original *PolicyDocument) *PolicyDocument {
	if original == nil {
		return nil
	}

	copied := &PolicyDocument{
		Version: original.Version,
		Id:      original.Id,
	}

	// Copy statements
	copied.Statement = make([]Statement, len(original.Statement))
	for i, stmt := range original.Statement {
		copied.Statement[i] = Statement{
			Sid:          stmt.Sid,
			Effect:       stmt.Effect,
			Principal:    stmt.Principal,
			NotPrincipal: stmt.NotPrincipal,
		}

		// Copy action slice
		if stmt.Action != nil {
			copied.Statement[i].Action = make([]string, len(stmt.Action))
			copy(copied.Statement[i].Action, stmt.Action)
		}

		// Copy NotAction slice
		if stmt.NotAction != nil {
			copied.Statement[i].NotAction = make([]string, len(stmt.NotAction))
			copy(copied.Statement[i].NotAction, stmt.NotAction)
		}

		// Copy resource slice
		if stmt.Resource != nil {
			copied.Statement[i].Resource = make([]string, len(stmt.Resource))
			copy(copied.Statement[i].Resource, stmt.Resource)
		}

		// Copy NotResource slice
		if stmt.NotResource != nil {
			copied.Statement[i].NotResource = make([]string, len(stmt.NotResource))
			copy(copied.Statement[i].NotResource, stmt.NotResource)
		}

		// Copy condition map (shallow copy for now)
		if stmt.Condition != nil {
			copied.Statement[i].Condition = make(map[string]map[string]interface{})
			for k, v := range stmt.Condition {
				copied.Statement[i].Condition[k] = v
			}
		}
	}

	return copied
}

// FilerPolicyStore implements PolicyStore using SeaweedFS filer
type FilerPolicyStore struct {
	grpcDialOption grpc.DialOption
	basePath       string
}

// NewFilerPolicyStore creates a new filer-based policy store
func NewFilerPolicyStore(config map[string]interface{}) (*FilerPolicyStore, error) {
	store := &FilerPolicyStore{
		basePath: "/etc/iam/policies", // Default path for policy storage - aligned with /etc/ convention
	}

	// Parse configuration - only basePath and other settings, NOT filerAddress
	if config != nil {
		if basePath, ok := config["basePath"].(string); ok && basePath != "" {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	glog.V(2).Infof("Initialized FilerPolicyStore with basePath %s", store.basePath)

	return store, nil
}

// StorePolicy stores a policy document in filer
func (s *FilerPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	// Serialize policy to JSON
	policyData, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize policy: %v", err)
	}

	policyPath := s.getPolicyPath(name)

	// Store in filer
	return s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: s.basePath,
			Entry: &filer_pb.Entry{
				Name:        s.getPolicyFileName(name),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0600), // Read/write for owner only
					Uid:      uint32(0),
					Gid:      uint32(0),
				},
				Content: policyData,
			},
		}

		glog.V(3).Infof("Storing policy %s at %s", name, policyPath)
		_, err := client.CreateEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to store policy %s: %v", name, err)
		}

		return nil
	})
}

// GetPolicy retrieves a policy document from filer
func (s *FilerPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return nil, fmt.Errorf("policy name cannot be empty")
	}

	var policyData []byte
	err := s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: s.basePath,
			Name:      s.getPolicyFileName(name),
		}

		glog.V(3).Infof("Looking up policy %s", name)
		response, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("policy not found: %v", err)
		}

		if response.Entry == nil {
			return fmt.Errorf("policy not found")
		}

		policyData = response.Entry.Content
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Deserialize policy from JSON
	var policy PolicyDocument
	if err := json.Unmarshal(policyData, &policy); err != nil {
		return nil, fmt.Errorf("failed to deserialize policy: %v", err)
	}

	return &policy, nil
}

// DeletePolicy deletes a policy document from filer
func (s *FilerPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	return s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:            s.basePath,
			Name:                 s.getPolicyFileName(name),
			IsDeleteData:         true,
			IsRecursive:          false,
			IgnoreRecursiveError: false,
		}

		glog.V(3).Infof("Deleting policy %s", name)
		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			// Ignore "not found" errors - policy may already be deleted
			if strings.Contains(err.Error(), "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete policy %s: %v", name, err)
		}

		// Check response error
		if resp.Error != "" {
			// Ignore "not found" errors - policy may already be deleted
			if strings.Contains(resp.Error, "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete policy %s: %s", name, resp.Error)
		}

		return nil
	})
}

// ListPolicies lists all policy names in filer
func (s *FilerPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]string, error) {
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerPolicyStore")
	}

	var policyNames []string

	err := s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		// List all entries in the policy directory
		request := &filer_pb.ListEntriesRequest{
			Directory:          s.basePath,
			Prefix:             "policy_",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000, // Process in batches of 1000
		}

		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list policies: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			// Extract policy name from filename
			filename := resp.Entry.Name
			if strings.HasPrefix(filename, "policy_") && strings.HasSuffix(filename, ".json") {
				// Remove "policy_" prefix and ".json" suffix
				policyName := strings.TrimSuffix(strings.TrimPrefix(filename, "policy_"), ".json")
				policyNames = append(policyNames, policyName)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return policyNames, nil
}

// Helper methods

// withFilerClient executes a function with a filer client
func (s *FilerPolicyStore) withFilerClient(filerAddress string, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing SeaweedFS code
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), s.grpcDialOption, fn)
}

// getPolicyPath returns the full path for a policy
func (s *FilerPolicyStore) getPolicyPath(policyName string) string {
	return s.basePath + "/" + s.getPolicyFileName(policyName)
}

// getPolicyFileName returns the filename for a policy
func (s *FilerPolicyStore) getPolicyFileName(policyName string) string {
	return "policy_" + policyName + ".json"
}

// CachedFilerPolicyStore implements PolicyStore with TTL caching on top of FilerPolicyStore
type CachedFilerPolicyStore struct {
	filerStore *FilerPolicyStore
	cache      *ccache.Cache
	listCache  *ccache.Cache
	ttl        time.Duration
	listTTL    time.Duration
}

// CachedFilerPolicyStoreConfig holds configuration for the cached policy store
type CachedFilerPolicyStoreConfig struct {
	BasePath     string `json:"basePath,omitempty"`
	TTL          string `json:"ttl,omitempty"`          // e.g., "5m", "1h"
	ListTTL      string `json:"listTtl,omitempty"`      // e.g., "1m", "30s"
	MaxCacheSize int    `json:"maxCacheSize,omitempty"` // Maximum number of cached policies
}

// NewCachedFilerPolicyStore creates a new cached filer-based policy store
func NewCachedFilerPolicyStore(config map[string]interface{}) (*CachedFilerPolicyStore, error) {
	// Create underlying filer store
	filerStore, err := NewFilerPolicyStore(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create filer policy store: %w", err)
	}

	// Parse cache configuration with defaults
	cacheTTL := 5 * time.Minute // Default 5 minutes for policy cache
	listTTL := 1 * time.Minute  // Default 1 minute for list cache
	maxCacheSize := 500         // Default max 500 cached policies

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

	store := &CachedFilerPolicyStore{
		filerStore:   filerStore,
		cache:        make(map[string]*cachedPolicyItem),
		ttl:          cacheTTL,
		listTTL:      listTTL,
		maxCacheSize: maxCacheSize,
	}

	glog.V(2).Infof("Initialized CachedFilerPolicyStore with TTL %v, List TTL %v, Max Cache Size %d",
		cacheTTL, listTTL, maxCacheSize)

	return store, nil
}

// StorePolicy stores a policy document and invalidates the cache
func (c *CachedFilerPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	// Store in filer
	err := c.filerStore.StorePolicy(ctx, filerAddress, name, policy)
	if err != nil {
		return err
	}

	// Invalidate cache entry and list cache
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.cache, name)
	c.listCache = nil // Invalidate list cache

	glog.V(3).Infof("Stored and invalidated cache for policy %s", name)
	return nil
}

// GetPolicy retrieves a policy document with caching
func (c *CachedFilerPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	// Try to get from cache first
	item := c.cache.Get(name)
	if item != nil {
		// Cache hit - return cached policy (DO NOT extend TTL)
		policy := item.Value().(*PolicyDocument)
		glog.V(4).Infof("Cache hit for policy %s", name)
		return copyPolicyDocument(policy), nil
	}

	// Cache miss - fetch from filer
	glog.V(4).Infof("Cache miss for policy %s, fetching from filer", name)
	policy, err := c.filerStore.GetPolicy(ctx, filerAddress, name)
	if err != nil {
		return nil, err
	}

	// Cache the result with TTL
	c.cache.Set(name, copyPolicyDocument(policy), c.ttl)
	glog.V(3).Infof("Cached policy %s with TTL %v", name, c.ttl)
	return policy, nil
}

// DeletePolicy deletes a policy document and invalidates the cache
func (c *CachedFilerPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	// Delete from filer
	err := c.filerStore.DeletePolicy(ctx, filerAddress, name)
	if err != nil {
		return err
	}

	// Invalidate cache entry and list cache
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.cache, name)
	c.listCache = nil // Invalidate list cache

	glog.V(3).Infof("Deleted and invalidated cache for policy %s", name)
	return nil
}

// ListPolicies lists all policy names with caching
func (c *CachedFilerPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]string, error) {
	now := time.Now()

	// Try to get from cache first
	c.mutex.RLock()
	if c.listCache != nil && now.Before(c.listCache.expiresAt) {
		// Cache hit
		policiesCopy := make([]string, len(c.listCache.policies))
		copy(policiesCopy, c.listCache.policies)
		c.mutex.RUnlock()
		glog.V(4).Infof("Policy list cache hit, returning %d policies", len(policiesCopy))
		return policiesCopy, nil
	}
	c.mutex.RUnlock()

	// Cache miss - fetch from filer
	glog.V(4).Infof("Policy list cache miss, fetching from filer")
	policies, err := c.filerStore.ListPolicies(ctx, filerAddress)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.mutex.Lock()
	defer c.mutex.Unlock()

	policiesCopy := make([]string, len(policies))
	copy(policiesCopy, policies)

	c.listCache = &cachedPolicyList{
		policies:  policiesCopy,
		expiresAt: now.Add(c.listTTL),
	}

	glog.V(3).Infof("Cached policy list with %d entries, TTL %v", len(policies), c.listTTL)
	return policies, nil
}

// ClearCache clears all cached entries (for testing or manual cache invalidation)
func (c *CachedFilerPolicyStore) ClearCache() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache = make(map[string]*cachedPolicyItem)
	c.listCache = nil
	glog.V(2).Infof("Cleared all policy cache entries")
}

// GetCacheStats returns cache statistics
func (c *CachedFilerPolicyStore) GetCacheStats() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	now := time.Now()
	expiredCount := 0
	for _, item := range c.cache {
		if now.After(item.expiresAt) {
			expiredCount++
		}
	}

	return map[string]interface{}{
		"totalEntries":     len(c.cache),
		"expiredEntries":   expiredCount,
		"activeEntries":    len(c.cache) - expiredCount,
		"maxCacheSize":     c.maxCacheSize,
		"ttl":              c.ttl.String(),
		"listTTL":          c.listTTL.String(),
		"hasListCache":     c.listCache != nil,
		"listCacheExpired": c.listCache != nil && now.After(c.listCache.expiresAt),
	}
}

// evictOldestEntries removes expired entries and oldest entries if cache is still full
func (c *CachedFilerPolicyStore) evictOldestEntries() {
	now := time.Now()

	// First pass: remove expired entries
	for policyName, item := range c.cache {
		if now.After(item.expiresAt) {
			delete(c.cache, policyName)
		}
	}

	// Second pass: if still over limit, remove oldest entries
	if len(c.cache) >= c.maxCacheSize {
		// Find and remove 25% of oldest entries
		type policyAge struct {
			name      string
			expiresAt time.Time
		}

		var ages []policyAge
		for policyName, item := range c.cache {
			ages = append(ages, policyAge{name: policyName, expiresAt: item.expiresAt})
		}

		// Sort by expiration time (oldest first)
		for i := 0; i < len(ages)-1; i++ {
			for j := i + 1; j < len(ages); j++ {
				if ages[i].expiresAt.After(ages[j].expiresAt) {
					ages[i], ages[j] = ages[j], ages[i]
				}
			}
		}

		// Remove oldest 25% or until we're under the limit
		toRemove := len(ages) / 4
		if toRemove < 1 {
			toRemove = 1
		}

		for i := 0; i < toRemove && len(c.cache) >= c.maxCacheSize; i++ {
			delete(c.cache, ages[i].name)
		}

		glog.V(3).Infof("Evicted %d oldest policy cache entries", toRemove)
	}
}
