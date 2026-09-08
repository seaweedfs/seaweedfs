package dash

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// ErrPolicyStillAttached is returned when deleting a managed policy that is
// still attached to one or more users or groups.
var ErrPolicyStillAttached = errors.New("policy is still attached")

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
// Deprecated: This method delegates to the credential manager and will be moved/removed in a future release.
func (cspm *CredentialStorePolicyManager) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	return cspm.credentialManager.GetPolicies(ctx)
}

// CreatePolicy creates a new IAM policy via credential store
// Deprecated: This method delegates to the credential manager and will be moved/removed in a future release.
func (cspm *CredentialStorePolicyManager) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return cspm.credentialManager.CreatePolicy(ctx, name, document)
}

// UpdatePolicy updates an existing IAM policy via credential store
// Deprecated: This method delegates to the credential manager and will be moved/removed in a future release.
func (cspm *CredentialStorePolicyManager) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return cspm.credentialManager.UpdatePolicy(ctx, name, document)
}

// DeletePolicy deletes an IAM policy via credential store
// Deprecated: This method delegates to the credential manager and will be moved/removed in a future release.
func (cspm *CredentialStorePolicyManager) DeletePolicy(ctx context.Context, name string) error {
	return cspm.credentialManager.DeletePolicy(ctx, name)
}

// GetPolicy retrieves a specific IAM policy via credential store
// Deprecated: This method delegates to the credential manager and will be moved/removed in a future release.
func (cspm *CredentialStorePolicyManager) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	return cspm.credentialManager.GetPolicy(ctx, name)
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

// DeletePolicy deletes an IAM policy. Deletion is rejected while the policy is
// still attached to any user or group, matching AWS IAM behavior and the IAM
// API handler, so a deleted policy name never lingers in an attached policy
// list.
func (s *AdminServer) DeletePolicy(name string) error {
	policyManager := s.GetPolicyManager()
	if policyManager == nil {
		return fmt.Errorf("policy manager not available")
	}

	ctx := context.Background()
	attached, err := s.IsPolicyAttached(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to check policy attachments: %w", err)
	}
	if len(attached) > 0 {
		return fmt.Errorf("policy %s is still attached to: %s: %w", name, strings.Join(attached, ", "), ErrPolicyStillAttached)
	}

	return policyManager.DeletePolicy(ctx, name)
}

// IsPolicyAttached returns the names of users and groups that still have the
// given managed policy attached. The returned entries are prefixed with
// "user:" or "group:". Returns nil when the policy is not attached anywhere.
func (s *AdminServer) IsPolicyAttached(ctx context.Context, policyName string) ([]string, error) {
	if s.credentialManager == nil {
		return nil, fmt.Errorf("credential manager not available")
	}

	var attached []string

	usernames, err := s.credentialManager.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	for _, username := range usernames {
		policies, err := s.credentialManager.ListAttachedUserPolicies(ctx, username)
		if err != nil {
			return nil, fmt.Errorf("failed to list policies for user %s: %w", username, err)
		}
		for _, p := range policies {
			if p == policyName {
				attached = append(attached, "user:"+username)
				break
			}
		}
	}

	groupNames, err := s.credentialManager.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	for _, groupName := range groupNames {
		group, err := s.credentialManager.GetGroup(ctx, groupName)
		if err != nil {
			return nil, fmt.Errorf("failed to get group %s: %w", groupName, err)
		}
		for _, p := range group.PolicyNames {
			if p == policyName {
				attached = append(attached, "group:"+groupName)
				break
			}
		}
	}

	return attached, nil
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
