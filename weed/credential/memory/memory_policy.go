package memory

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// GetPolicies retrieves all IAM policies from memory
func (store *MemoryStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	// Create a copy of the policies map to avoid mutation issues
	policies := make(map[string]policy_engine.PolicyDocument)
	for name, doc := range store.policies {
		policies[name] = doc
	}

	return policies, nil
}

// GetPolicy retrieves a specific IAM policy by name from memory
func (store *MemoryStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if policy, exists := store.policies[name]; exists {
		return &policy, nil
	}

	return nil, nil // Policy not found
}

// CreatePolicy creates a new IAM policy in memory
func (store *MemoryStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	store.policies[name] = document
	return nil
}

// UpdatePolicy updates an existing IAM policy in memory
func (store *MemoryStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	store.policies[name] = document
	return nil
}

// PutPolicy creates or updates an IAM policy in memory
func (store *MemoryStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	store.policies[name] = document
	return nil
}

// DeletePolicy deletes an IAM policy from memory
func (store *MemoryStore) DeletePolicy(ctx context.Context, name string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	delete(store.policies, name)
	return nil
}
