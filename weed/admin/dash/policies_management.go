package dash

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

type IAMPolicy struct {
	Name         string                       `json:"name"`
	Document     policy_engine.PolicyDocument `json:"document"`
	DocumentJSON string                       `json:"document_json"`
	CreatedAt    time.Time                    `json:"created_at"`
	UpdatedAt    time.Time                    `json:"updated_at"`
}

type PoliciesCollection struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
}

type PoliciesData struct {
	Username      string      `json:"username"`
	Policies      []IAMPolicy `json:"policies"`
	TotalPolicies int         `json:"total_policies"`
	LastUpdated   time.Time   `json:"last_updated"`
}

// Policy management request structures
type CreatePolicyRequest struct {
	Name         string                       `json:"name" binding:"required"`
	Document     policy_engine.PolicyDocument `json:"document" binding:"required"`
	DocumentJSON string                       `json:"document_json"`
}

type UpdatePolicyRequest struct {
	Document     policy_engine.PolicyDocument `json:"document" binding:"required"`
	DocumentJSON string                       `json:"document_json"`
}

// PolicyManager interface is now in the credential package

// CredentialStorePolicyManager implements credential.PolicyManager by delegating to the credential store
type CredentialStorePolicyManager struct {
	credentialManager *credential.CredentialManager
}

// NewCredentialStorePolicyManager creates a new CredentialStorePolicyManager
func NewCredentialStorePolicyManager(credentialManager *credential.CredentialManager) *CredentialStorePolicyManager {
	return &CredentialStorePolicyManager{
		credentialManager: credentialManager,
	}
}

// GetPolicies retrieves all IAM policies via credential store
func (cspm *CredentialStorePolicyManager) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	// Get policies from credential store
	// We'll use the credential store to access the filer indirectly
	// Since policies are stored separately, we need to access the underlying store
	store := cspm.credentialManager.GetStore()
	glog.V(1).Infof("Getting policies from credential store: %T", store)

	// Check if the store supports policy management
	if policyStore, ok := store.(credential.PolicyManager); ok {
		glog.V(1).Infof("Store supports policy management, calling GetPolicies")
		policies, err := policyStore.GetPolicies(ctx)
		if err != nil {
			glog.Errorf("Error getting policies from store: %v", err)
			return nil, err
		}
		glog.V(1).Infof("Got %d policies from store", len(policies))
		return policies, nil
	} else {
		// Fallback: use empty policies for stores that don't support policies
		glog.V(1).Infof("Credential store doesn't support policy management, returning empty policies")
		return make(map[string]policy_engine.PolicyDocument), nil
	}
}

// CreatePolicy creates a new IAM policy via credential store
func (cspm *CredentialStorePolicyManager) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	store := cspm.credentialManager.GetStore()

	if policyStore, ok := store.(credential.PolicyManager); ok {
		return policyStore.CreatePolicy(ctx, name, document)
	}

	return fmt.Errorf("credential store doesn't support policy creation")
}

// UpdatePolicy updates an existing IAM policy via credential store
func (cspm *CredentialStorePolicyManager) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	store := cspm.credentialManager.GetStore()

	if policyStore, ok := store.(credential.PolicyManager); ok {
		return policyStore.UpdatePolicy(ctx, name, document)
	}

	return fmt.Errorf("credential store doesn't support policy updates")
}

// DeletePolicy deletes an IAM policy via credential store
func (cspm *CredentialStorePolicyManager) DeletePolicy(ctx context.Context, name string) error {
	store := cspm.credentialManager.GetStore()

	if policyStore, ok := store.(credential.PolicyManager); ok {
		return policyStore.DeletePolicy(ctx, name)
	}

	return fmt.Errorf("credential store doesn't support policy deletion")
}

// GetPolicy retrieves a specific IAM policy via credential store
func (cspm *CredentialStorePolicyManager) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	store := cspm.credentialManager.GetStore()

	if policyStore, ok := store.(credential.PolicyManager); ok {
		return policyStore.GetPolicy(ctx, name)
	}

	return nil, fmt.Errorf("credential store doesn't support policy retrieval")
}

// AdminServer policy management methods using credential.PolicyManager
func (s *AdminServer) GetPolicyManager() credential.PolicyManager {
	if s.credentialManager == nil {
		glog.V(1).Infof("Credential manager is nil, policy management not available")
		return nil
	}
	glog.V(1).Infof("Credential manager available, creating CredentialStorePolicyManager")
	return NewCredentialStorePolicyManager(s.credentialManager)
}

// GetPolicies retrieves all IAM policies
func (s *AdminServer) GetPolicies() ([]IAMPolicy, error) {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return nil, fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	policyMap, err := policyManager.GetPolicies(ctx)
	if err != nil {
		return nil, err
	}

	// Convert map[string]PolicyDocument to []IAMPolicy
	var policies []IAMPolicy
	for name, doc := range policyMap {
		policy := IAMPolicy{
			Name:         name,
			Document:     doc,
			DocumentJSON: "", // Will be populated if needed
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// CreatePolicy creates a new IAM policy
func (s *AdminServer) CreatePolicy(name string, document policy_engine.PolicyDocument) error {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	return policyManager.CreatePolicy(ctx, name, document)
}

// UpdatePolicy updates an existing IAM policy
func (s *AdminServer) UpdatePolicy(name string, document policy_engine.PolicyDocument) error {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	return policyManager.UpdatePolicy(ctx, name, document)
}

// DeletePolicy deletes an IAM policy
func (s *AdminServer) DeletePolicy(name string) error {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	return policyManager.DeletePolicy(ctx, name)
}

// GetPolicy retrieves a specific IAM policy
func (s *AdminServer) GetPolicy(name string) (*IAMPolicy, error) {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return nil, fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	policyDoc, err := policyManager.GetPolicy(ctx, name)
	if err != nil {
		return nil, err
	}

	if policyDoc == nil {
		return nil, nil
	}

	// Convert PolicyDocument to IAMPolicy
	policy := &IAMPolicy{
		Name:         name,
		Document:     *policyDoc,
		DocumentJSON: "", // Will be populated if needed
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	return policy, nil
}
