package policy

import (
	"context"
	"fmt"
	"sync"
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

// StorePolicy stores a policy document in memory
func (s *MemoryPolicyStore) StorePolicy(ctx context.Context, name string, policy *PolicyDocument) error {
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

// GetPolicy retrieves a policy document from memory
func (s *MemoryPolicyStore) GetPolicy(ctx context.Context, name string) (*PolicyDocument, error) {
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

// DeletePolicy deletes a policy document from memory
func (s *MemoryPolicyStore) DeletePolicy(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.policies, name)
	return nil
}

// ListPolicies lists all policy names in memory
func (s *MemoryPolicyStore) ListPolicies(ctx context.Context) ([]string, error) {
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
	basePath string
	// TODO: Add filer client when integrating with SeaweedFS
}

// NewFilerPolicyStore creates a new filer-based policy store
func NewFilerPolicyStore(config map[string]interface{}) (*FilerPolicyStore, error) {
	// TODO: Implement filer policy store
	// 1. Parse configuration for filer connection details
	// 2. Set up filer client
	// 3. Configure base path for policy storage

	return nil, fmt.Errorf("filer policy store not implemented yet")
}

// StorePolicy stores a policy document in filer
func (s *FilerPolicyStore) StorePolicy(ctx context.Context, name string, policy *PolicyDocument) error {
	// TODO: Implement filer policy storage
	// 1. Serialize policy to JSON
	// 2. Store in filer at basePath/policies/name.json
	// 3. Handle errors and retries

	return fmt.Errorf("filer policy storage not implemented yet")
}

// GetPolicy retrieves a policy document from filer
func (s *FilerPolicyStore) GetPolicy(ctx context.Context, name string) (*PolicyDocument, error) {
	// TODO: Implement filer policy retrieval
	// 1. Read policy file from filer
	// 2. Deserialize JSON to PolicyDocument
	// 3. Handle not found cases

	return nil, fmt.Errorf("filer policy retrieval not implemented yet")
}

// DeletePolicy deletes a policy document from filer
func (s *FilerPolicyStore) DeletePolicy(ctx context.Context, name string) error {
	// TODO: Implement filer policy deletion
	// 1. Delete policy file from filer
	// 2. Handle errors

	return fmt.Errorf("filer policy deletion not implemented yet")
}

// ListPolicies lists all policy names in filer
func (s *FilerPolicyStore) ListPolicies(ctx context.Context) ([]string, error) {
	// TODO: Implement filer policy listing
	// 1. List files in basePath/policies/
	// 2. Extract policy names from filenames
	// 3. Return sorted list

	return nil, fmt.Errorf("filer policy listing not implemented yet")
}
