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

// ListPolicyNames returns all stored policy names.
func (store *MemoryStore) ListPolicyNames(ctx context.Context) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	names := make([]string, 0, len(store.policies))
	for name := range store.policies {
		names = append(names, name)
	}

	return names, nil
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

// PutUserInlinePolicy stores a per-user inline policy document.
func (store *MemoryStore) PutUserInlinePolicy(ctx context.Context, userName, policyName string, document policy_engine.PolicyDocument) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	if store.inlinePolicies[userName] == nil {
		store.inlinePolicies[userName] = make(map[string]policy_engine.PolicyDocument)
	}
	store.inlinePolicies[userName][policyName] = document
	return nil
}

// GetUserInlinePolicy retrieves a per-user inline policy document.
func (store *MemoryStore) GetUserInlinePolicy(ctx context.Context, userName, policyName string) (*policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	if userPolicies := store.inlinePolicies[userName]; userPolicies != nil {
		if doc, exists := userPolicies[policyName]; exists {
			return &doc, nil
		}
	}
	return nil, nil
}

// DeleteUserInlinePolicy removes a per-user inline policy document.
func (store *MemoryStore) DeleteUserInlinePolicy(ctx context.Context, userName, policyName string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	if userPolicies := store.inlinePolicies[userName]; userPolicies != nil {
		delete(userPolicies, policyName)
		if len(userPolicies) == 0 {
			delete(store.inlinePolicies, userName)
		}
	}
	return nil
}

// ListUserInlinePolicies returns the names of all inline policies for a user.
func (store *MemoryStore) ListUserInlinePolicies(ctx context.Context, userName string) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	userPolicies := store.inlinePolicies[userName]
	names := make([]string, 0, len(userPolicies))
	for name := range userPolicies {
		names = append(names, name)
	}
	return names, nil
}

// LoadInlinePolicies returns all inline policies keyed by username then policy name.
func (store *MemoryStore) LoadInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	result := make(map[string]map[string]policy_engine.PolicyDocument, len(store.inlinePolicies))
	for userName, userPolicies := range store.inlinePolicies {
		copied := make(map[string]policy_engine.PolicyDocument, len(userPolicies))
		for policyName, doc := range userPolicies {
			copied[policyName] = doc
		}
		result[userName] = copied
	}
	return result, nil
}

// PutGroupInlinePolicy stores a per-group inline policy document.
func (store *MemoryStore) PutGroupInlinePolicy(ctx context.Context, groupName, policyName string, document policy_engine.PolicyDocument) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	if store.groupInlinePolicies[groupName] == nil {
		store.groupInlinePolicies[groupName] = make(map[string]policy_engine.PolicyDocument)
	}
	store.groupInlinePolicies[groupName][policyName] = document
	return nil
}

// GetGroupInlinePolicy retrieves a per-group inline policy document.
func (store *MemoryStore) GetGroupInlinePolicy(ctx context.Context, groupName, policyName string) (*policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	if groupPolicies := store.groupInlinePolicies[groupName]; groupPolicies != nil {
		if doc, exists := groupPolicies[policyName]; exists {
			return &doc, nil
		}
	}
	return nil, nil
}

// DeleteGroupInlinePolicy removes a per-group inline policy document.
func (store *MemoryStore) DeleteGroupInlinePolicy(ctx context.Context, groupName, policyName string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	if groupPolicies := store.groupInlinePolicies[groupName]; groupPolicies != nil {
		delete(groupPolicies, policyName)
		if len(groupPolicies) == 0 {
			delete(store.groupInlinePolicies, groupName)
		}
	}
	return nil
}

// ListGroupInlinePolicies returns the names of all inline policies for a group.
func (store *MemoryStore) ListGroupInlinePolicies(ctx context.Context, groupName string) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	groupPolicies := store.groupInlinePolicies[groupName]
	names := make([]string, 0, len(groupPolicies))
	for name := range groupPolicies {
		names = append(names, name)
	}
	return names, nil
}

// LoadGroupInlinePolicies returns all group inline policies keyed by group name then policy name.
func (store *MemoryStore) LoadGroupInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	result := make(map[string]map[string]policy_engine.PolicyDocument, len(store.groupInlinePolicies))
	for groupName, groupPolicies := range store.groupInlinePolicies {
		copied := make(map[string]policy_engine.PolicyDocument, len(groupPolicies))
		for policyName, doc := range groupPolicies {
			copied[policyName] = doc
		}
		result[groupName] = copied
	}
	return result, nil
}
