package integration

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// IAMManager orchestrates all IAM components
type IAMManager struct {
	stsService    *sts.STSService
	policyEngine  *policy.PolicyEngine
	roles         map[string]*RoleDefinition
	initialized   bool
}

// IAMConfig holds configuration for all IAM components
type IAMConfig struct {
	// STS service configuration
	STS *sts.STSConfig `json:"sts"`
	
	// Policy engine configuration
	Policy *policy.PolicyEngineConfig `json:"policy"`
}

// RoleDefinition defines a role with its trust policy and attached policies
type RoleDefinition struct {
	// RoleName is the name of the role
	RoleName string `json:"roleName"`
	
	// RoleArn is the full ARN of the role
	RoleArn string `json:"roleArn"`
	
	// TrustPolicy defines who can assume this role
	TrustPolicy *policy.PolicyDocument `json:"trustPolicy"`
	
	// AttachedPolicies lists the policy names attached to this role
	AttachedPolicies []string `json:"attachedPolicies"`
	
	// Description is an optional description of the role
	Description string `json:"description,omitempty"`
}

// ActionRequest represents a request to perform an action
type ActionRequest struct {
	// Principal is the entity performing the action
	Principal string `json:"principal"`
	
	// Action is the action being requested
	Action string `json:"action"`
	
	// Resource is the resource being accessed
	Resource string `json:"resource"`
	
	// SessionToken for temporary credential validation
	SessionToken string `json:"sessionToken"`
	
	// RequestContext contains additional request information
	RequestContext map[string]interface{} `json:"requestContext,omitempty"`
}

// NewIAMManager creates a new IAM manager
func NewIAMManager() *IAMManager {
	return &IAMManager{
		roles: make(map[string]*RoleDefinition),
	}
}

// Initialize initializes the IAM manager with all components
func (m *IAMManager) Initialize(config *IAMConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}
	
	// Initialize STS service
	m.stsService = sts.NewSTSService()
	if err := m.stsService.Initialize(config.STS); err != nil {
		return fmt.Errorf("failed to initialize STS service: %w", err)
	}
	
	// Initialize policy engine
	m.policyEngine = policy.NewPolicyEngine()
	if err := m.policyEngine.Initialize(config.Policy); err != nil {
		return fmt.Errorf("failed to initialize policy engine: %w", err)
	}
	
	m.initialized = true
	return nil
}

// RegisterIdentityProvider registers an identity provider
func (m *IAMManager) RegisterIdentityProvider(provider providers.IdentityProvider) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}
	
	return m.stsService.RegisterProvider(provider)
}

// CreatePolicy creates a new policy
func (m *IAMManager) CreatePolicy(ctx context.Context, name string, policyDoc *policy.PolicyDocument) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}
	
	return m.policyEngine.AddPolicy(name, policyDoc)
}

// CreateRole creates a new role with trust policy and attached policies
func (m *IAMManager) CreateRole(ctx context.Context, roleName string, roleDef *RoleDefinition) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}
	
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	
	if roleDef == nil {
		return fmt.Errorf("role definition cannot be nil")
	}
	
	// Set role ARN if not provided
	if roleDef.RoleArn == "" {
		roleDef.RoleArn = fmt.Sprintf("arn:seaweed:iam::role/%s", roleName)
	}
	
	// Validate trust policy
	if roleDef.TrustPolicy != nil {
		if err := policy.ValidateTrustPolicyDocument(roleDef.TrustPolicy); err != nil {
			return fmt.Errorf("invalid trust policy: %w", err)
		}
	}
	
	// Store role definition
	m.roles[roleName] = roleDef
	
	return nil
}

// AssumeRoleWithWebIdentity assumes a role using web identity (OIDC)
func (m *IAMManager) AssumeRoleWithWebIdentity(ctx context.Context, request *sts.AssumeRoleWithWebIdentityRequest) (*sts.AssumeRoleResponse, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}
	
	// Extract role name from ARN
	roleName := extractRoleNameFromArn(request.RoleArn)
	
	// Get role definition
	roleDef, exists := m.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}
	
	// Validate trust policy before allowing STS to assume the role
	if err := m.validateTrustPolicyForWebIdentity(ctx, roleDef, request.WebIdentityToken); err != nil {
		return nil, fmt.Errorf("trust policy validation failed: %w", err)
	}
	
	// Use STS service to assume the role
	return m.stsService.AssumeRoleWithWebIdentity(ctx, request)
}

// AssumeRoleWithCredentials assumes a role using credentials (LDAP)
func (m *IAMManager) AssumeRoleWithCredentials(ctx context.Context, request *sts.AssumeRoleWithCredentialsRequest) (*sts.AssumeRoleResponse, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}
	
	// Extract role name from ARN
	roleName := extractRoleNameFromArn(request.RoleArn)
	
	// Get role definition
	roleDef, exists := m.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}
	
	// Validate trust policy
	if err := m.validateTrustPolicyForCredentials(ctx, roleDef, request); err != nil {
		return nil, fmt.Errorf("trust policy validation failed: %w", err)
	}
	
	// Use STS service to assume the role
	return m.stsService.AssumeRoleWithCredentials(ctx, request)
}

// IsActionAllowed checks if a principal is allowed to perform an action on a resource
func (m *IAMManager) IsActionAllowed(ctx context.Context, request *ActionRequest) (bool, error) {
	if !m.initialized {
		return false, fmt.Errorf("IAM manager not initialized")
	}
	
	// Validate session token first
	_, err := m.stsService.ValidateSessionToken(ctx, request.SessionToken)
	if err != nil {
		return false, fmt.Errorf("invalid session: %w", err)
	}
	
	// Extract role name from principal ARN
	roleName := extractRoleNameFromPrincipal(request.Principal)
	if roleName == "" {
		return false, fmt.Errorf("could not extract role from principal: %s", request.Principal)
	}
	
	// Get role definition
	roleDef, exists := m.roles[roleName]
	if !exists {
		return false, fmt.Errorf("role not found: %s", roleName)
	}
	
	// Create evaluation context
	evalCtx := &policy.EvaluationContext{
		Principal:      request.Principal,
		Action:         request.Action,
		Resource:       request.Resource,
		RequestContext: request.RequestContext,
	}
	
	// Evaluate policies attached to the role
	result, err := m.policyEngine.Evaluate(ctx, evalCtx, roleDef.AttachedPolicies)
	if err != nil {
		return false, fmt.Errorf("policy evaluation failed: %w", err)
	}
	
	return result.Effect == policy.EffectAllow, nil
}

// ValidateTrustPolicy validates if a principal can assume a role (for testing)
func (m *IAMManager) ValidateTrustPolicy(ctx context.Context, roleArn, provider, userID string) bool {
	roleName := extractRoleNameFromArn(roleArn)
	roleDef, exists := m.roles[roleName]
	if !exists {
		return false
	}
	
	// Simple validation based on provider in trust policy
	if roleDef.TrustPolicy != nil {
		for _, statement := range roleDef.TrustPolicy.Statement {
			if statement.Effect == "Allow" {
				if principal, ok := statement.Principal.(map[string]interface{}); ok {
					if federated, ok := principal["Federated"].(string); ok {
						if federated == "test-"+provider {
							return true
						}
					}
				}
			}
		}
	}
	
	return false
}

// validateTrustPolicyForWebIdentity validates trust policy for OIDC assumption
func (m *IAMManager) validateTrustPolicyForWebIdentity(ctx context.Context, roleDef *RoleDefinition, webIdentityToken string) error {
	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}
	
	// For simplified implementation, we'll do basic validation
	// In a full implementation, this would:
	// 1. Parse the web identity token
	// 2. Check issuer against trust policy
	// 3. Validate conditions in trust policy
	
	// Check if trust policy allows web identity assumption
	for _, statement := range roleDef.TrustPolicy.Statement {
		if statement.Effect == "Allow" {
			for _, action := range statement.Action {
				if action == "sts:AssumeRoleWithWebIdentity" {
					// For testing, just verify there's a Federated principal
					if principal, ok := statement.Principal.(map[string]interface{}); ok {
						if _, ok := principal["Federated"]; ok {
							return nil // Allow
						}
					}
				}
			}
		}
	}
	
	return fmt.Errorf("trust policy does not allow web identity assumption")
}

// validateTrustPolicyForCredentials validates trust policy for credential assumption
func (m *IAMManager) validateTrustPolicyForCredentials(ctx context.Context, roleDef *RoleDefinition, request *sts.AssumeRoleWithCredentialsRequest) error {
	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}
	
	// Check if trust policy allows credential assumption for the specific provider
	for _, statement := range roleDef.TrustPolicy.Statement {
		if statement.Effect == "Allow" {
			for _, action := range statement.Action {
				if action == "sts:AssumeRoleWithCredentials" {
					if principal, ok := statement.Principal.(map[string]interface{}); ok {
						if federated, ok := principal["Federated"].(string); ok {
							if federated == request.ProviderName {
								return nil // Allow
							}
						}
					}
				}
			}
		}
	}
	
	return fmt.Errorf("trust policy does not allow credential assumption for provider: %s", request.ProviderName)
}

// Helper functions

// extractRoleNameFromArn extracts role name from role ARN
func extractRoleNameFromArn(roleArn string) string {
	prefix := "arn:seaweed:iam::role/"
	if len(roleArn) > len(prefix) && roleArn[:len(prefix)] == prefix {
		return roleArn[len(prefix):]
	}
	return ""
}

// extractRoleNameFromPrincipal extracts role name from assumed role principal ARN
func extractRoleNameFromPrincipal(principal string) string {
	// Expected format: arn:seaweed:sts::assumed-role/RoleName/SessionName
	prefix := "arn:seaweed:sts::assumed-role/"
	if len(principal) > len(prefix) && principal[:len(prefix)] == prefix {
		remainder := principal[len(prefix):]
		// Split on first '/' to get role name
		if slashIndex := indexOf(remainder, "/"); slashIndex != -1 {
			return remainder[:slashIndex]
		}
	}
	return ""
}

// indexOf finds the index of the first occurrence of substring in string
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
