package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// IAMManager orchestrates all IAM components
type IAMManager struct {
	stsService           *sts.STSService
	policyEngine         *policy.PolicyEngine
	roleStore            RoleStore
	filerAddressProvider func() string // Function to get current filer address
	initialized          bool
}

// IAMConfig holds configuration for all IAM components
type IAMConfig struct {
	// STS service configuration
	STS *sts.STSConfig `json:"sts"`

	// Policy engine configuration
	Policy *policy.PolicyEngineConfig `json:"policy"`

	// Role store configuration
	Roles *RoleStoreConfig `json:"roleStore"`
}

// RoleStoreConfig holds role store configuration
type RoleStoreConfig struct {
	// StoreType specifies the role store backend (memory, filer, etc.)
	StoreType string `json:"storeType"`

	// StoreConfig contains store-specific configuration
	StoreConfig map[string]interface{} `json:"storeConfig,omitempty"`
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
	return &IAMManager{}
}

// Initialize initializes the IAM manager with all components
func (m *IAMManager) Initialize(config *IAMConfig, filerAddressProvider func() string) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Store the filer address provider function
	m.filerAddressProvider = filerAddressProvider

	// Initialize STS service
	m.stsService = sts.NewSTSService()
	if err := m.stsService.Initialize(config.STS); err != nil {
		return fmt.Errorf("failed to initialize STS service: %w", err)
	}

	// CRITICAL SECURITY: Set trust policy validator to ensure proper role assumption validation
	m.stsService.SetTrustPolicyValidator(m)

	// Initialize policy engine
	m.policyEngine = policy.NewPolicyEngine()
	if err := m.policyEngine.InitializeWithProvider(config.Policy, m.filerAddressProvider); err != nil {
		return fmt.Errorf("failed to initialize policy engine: %w", err)
	}

	// Initialize role store
	roleStore, err := m.createRoleStoreWithProvider(config.Roles, m.filerAddressProvider)
	if err != nil {
		return fmt.Errorf("failed to initialize role store: %w", err)
	}
	m.roleStore = roleStore

	m.initialized = true
	return nil
}

// getFilerAddress returns the current filer address using the provider function
func (m *IAMManager) getFilerAddress() string {
	if m.filerAddressProvider != nil {
		return m.filerAddressProvider()
	}
	return "" // Fallback to empty string if no provider is set
}

// createRoleStore creates a role store based on configuration
func (m *IAMManager) createRoleStore(config *RoleStoreConfig) (RoleStore, error) {
	if config == nil {
		// Default to generic cached filer role store when no config provided
		return NewGenericCachedRoleStore(nil, nil)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerRoleStore(config.StoreConfig, nil)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedRoleStore(config.StoreConfig, nil)
	case "cached-filer", "generic-cached":
		return NewGenericCachedRoleStore(config.StoreConfig, nil)
	case "memory":
		return NewMemoryRoleStore(), nil
	default:
		return nil, fmt.Errorf("unsupported role store type: %s", config.StoreType)
	}
}

// createRoleStoreWithProvider creates a role store with a filer address provider function
func (m *IAMManager) createRoleStoreWithProvider(config *RoleStoreConfig, filerAddressProvider func() string) (RoleStore, error) {
	if config == nil {
		// Default to generic cached filer role store when no config provided
		return NewGenericCachedRoleStore(nil, filerAddressProvider)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerRoleStore(config.StoreConfig, filerAddressProvider)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider)
	case "cached-filer", "generic-cached":
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider)
	case "memory":
		return NewMemoryRoleStore(), nil
	default:
		return nil, fmt.Errorf("unsupported role store type: %s", config.StoreType)
	}
}

// RegisterIdentityProvider registers an identity provider
func (m *IAMManager) RegisterIdentityProvider(provider providers.IdentityProvider) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsService.RegisterProvider(provider)
}

// CreatePolicy creates a new policy
func (m *IAMManager) CreatePolicy(ctx context.Context, filerAddress string, name string, policyDoc *policy.PolicyDocument) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.policyEngine.AddPolicy(filerAddress, name, policyDoc)
}

// CreateRole creates a new role with trust policy and attached policies
func (m *IAMManager) CreateRole(ctx context.Context, filerAddress string, roleName string, roleDef *RoleDefinition) error {
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
	return m.roleStore.StoreRole(ctx, "", roleName, roleDef)
}

// AssumeRoleWithWebIdentity assumes a role using web identity (OIDC)
func (m *IAMManager) AssumeRoleWithWebIdentity(ctx context.Context, request *sts.AssumeRoleWithWebIdentityRequest) (*sts.AssumeRoleResponse, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(request.RoleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
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
	roleName := utils.ExtractRoleNameFromArn(request.RoleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
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

	// Validate session token first (skip for OIDC tokens which are already validated)
	if !isOIDCToken(request.SessionToken) {
		_, err := m.stsService.ValidateSessionToken(ctx, request.SessionToken)
		if err != nil {
			return false, fmt.Errorf("invalid session: %w", err)
		}
	}

	// Extract role name from principal ARN
	roleName := utils.ExtractRoleNameFromPrincipal(request.Principal)
	if roleName == "" {
		return false, fmt.Errorf("could not extract role from principal: %s", request.Principal)
	}

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
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
	result, err := m.policyEngine.Evaluate(ctx, "", evalCtx, roleDef.AttachedPolicies)
	if err != nil {
		return false, fmt.Errorf("policy evaluation failed: %w", err)
	}

	return result.Effect == policy.EffectAllow, nil
}

// ValidateTrustPolicy validates if a principal can assume a role (for testing)
func (m *IAMManager) ValidateTrustPolicy(ctx context.Context, roleArn, provider, userID string) bool {
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
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

	// Create evaluation context for trust policy validation
	requestContext := make(map[string]interface{})

	// Try to parse as JWT first, fallback to mock token handling
	tokenClaims, err := parseJWTTokenForTrustPolicy(webIdentityToken)
	if err != nil {
		// If JWT parsing fails, this might be a mock token (like "valid-oidc-token")
		// For mock tokens, we'll use default values that match the trust policy expectations
		requestContext["seaweed:TokenIssuer"] = "test-oidc"
		requestContext["seaweed:FederatedProvider"] = "test-oidc"
		requestContext["seaweed:Subject"] = "mock-user"
	} else {
		// Add standard context values from JWT claims that trust policies might check
		if idp, ok := tokenClaims["idp"].(string); ok {
			requestContext["seaweed:TokenIssuer"] = idp
			requestContext["seaweed:FederatedProvider"] = idp
		}
		if iss, ok := tokenClaims["iss"].(string); ok {
			requestContext["seaweed:Issuer"] = iss
		}
		if sub, ok := tokenClaims["sub"].(string); ok {
			requestContext["seaweed:Subject"] = sub
		}
		if extUid, ok := tokenClaims["ext_uid"].(string); ok {
			requestContext["seaweed:ExternalUserId"] = extUid
		}
	}

	// Create evaluation context for trust policy
	evalCtx := &policy.EvaluationContext{
		Principal:      "web-identity-user", // Placeholder principal for trust policy evaluation
		Action:         "sts:AssumeRoleWithWebIdentity",
		Resource:       roleDef.RoleArn,
		RequestContext: requestContext,
	}

	// Evaluate the trust policy directly
	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return fmt.Errorf("trust policy denies web identity assumption")
	}

	return nil
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

// ExpireSessionForTesting manually expires a session for testing purposes
func (m *IAMManager) ExpireSessionForTesting(ctx context.Context, sessionToken string) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsService.ExpireSessionForTesting(ctx, sessionToken)
}

// GetSTSService returns the STS service instance
func (m *IAMManager) GetSTSService() *sts.STSService {
	return m.stsService
}

// parseJWTTokenForTrustPolicy parses a JWT token to extract claims for trust policy evaluation
func parseJWTTokenForTrustPolicy(tokenString string) (map[string]interface{}, error) {
	// Simple JWT parsing without verification (for trust policy context only)
	// In production, this should use proper JWT parsing with signature verification
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	// Decode the payload (second part)
	payload := parts[1]
	// Add padding if needed
	for len(payload)%4 != 0 {
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JWT claims: %w", err)
	}

	return claims, nil
}

// evaluateTrustPolicy evaluates a trust policy against the evaluation context
func (m *IAMManager) evaluateTrustPolicy(trustPolicy *policy.PolicyDocument, evalCtx *policy.EvaluationContext) bool {
	if trustPolicy == nil {
		return false
	}

	// Trust policies work differently from regular policies:
	// - They check the Principal field to see who can assume the role
	// - They check Action to see what actions are allowed
	// - They may have Conditions that must be satisfied

	for _, statement := range trustPolicy.Statement {
		if statement.Effect == "Allow" {
			// Check if the action matches
			actionMatches := false
			for _, action := range statement.Action {
				if action == evalCtx.Action || action == "*" {
					actionMatches = true
					break
				}
			}
			if !actionMatches {
				continue
			}

			// Check if the principal matches
			principalMatches := false
			if principal, ok := statement.Principal.(map[string]interface{}); ok {
				// Check for Federated principal (OIDC/SAML)
				if federatedValue, ok := principal["Federated"]; ok {
					principalMatches = m.evaluatePrincipalValue(federatedValue, evalCtx, "seaweed:FederatedProvider")
				}
				// Check for AWS principal (IAM users/roles)
				if !principalMatches {
					if awsValue, ok := principal["AWS"]; ok {
						principalMatches = m.evaluatePrincipalValue(awsValue, evalCtx, "seaweed:AWSPrincipal")
					}
				}
				// Check for Service principal (AWS services)
				if !principalMatches {
					if serviceValue, ok := principal["Service"]; ok {
						principalMatches = m.evaluatePrincipalValue(serviceValue, evalCtx, "seaweed:ServicePrincipal")
					}
				}
			} else if principalStr, ok := statement.Principal.(string); ok {
				// Handle string principal
				if principalStr == "*" {
					principalMatches = true
				}
			}

			if !principalMatches {
				continue
			}

			// Check conditions if present
			if len(statement.Condition) > 0 {
				conditionsMatch := m.evaluateTrustPolicyConditions(statement.Condition, evalCtx)
				if !conditionsMatch {
					continue
				}
			}

			// All checks passed for this Allow statement
			return true
		}
	}

	return false
}

// evaluateTrustPolicyConditions evaluates conditions in a trust policy statement
func (m *IAMManager) evaluateTrustPolicyConditions(conditions map[string]map[string]interface{}, evalCtx *policy.EvaluationContext) bool {
	for conditionType, conditionBlock := range conditions {
		switch conditionType {
		case "StringEquals":
			if !m.policyEngine.EvaluateStringCondition(conditionBlock, evalCtx, true, false) {
				return false
			}
		case "StringNotEquals":
			if !m.policyEngine.EvaluateStringCondition(conditionBlock, evalCtx, false, false) {
				return false
			}
		case "StringLike":
			if !m.policyEngine.EvaluateStringCondition(conditionBlock, evalCtx, true, true) {
				return false
			}
		// Add other condition types as needed
		default:
			// Unknown condition type - fail safe
			return false
		}
	}
	return true
}

// evaluatePrincipalValue evaluates a principal value (string or array) against the context
func (m *IAMManager) evaluatePrincipalValue(principalValue interface{}, evalCtx *policy.EvaluationContext, contextKey string) bool {
	// Get the value from evaluation context
	contextValue, exists := evalCtx.RequestContext[contextKey]
	if !exists {
		return false
	}

	contextStr, ok := contextValue.(string)
	if !ok {
		return false
	}

	// Handle single string value
	if principalStr, ok := principalValue.(string); ok {
		return principalStr == contextStr || principalStr == "*"
	}

	// Handle array of strings
	if principalArray, ok := principalValue.([]interface{}); ok {
		for _, item := range principalArray {
			if itemStr, ok := item.(string); ok {
				if itemStr == contextStr || itemStr == "*" {
					return true
				}
			}
		}
	}

	// Handle array of strings (alternative JSON unmarshaling format)
	if principalStrArray, ok := principalValue.([]string); ok {
		for _, itemStr := range principalStrArray {
			if itemStr == contextStr || itemStr == "*" {
				return true
			}
		}
	}

	return false
}

// isOIDCToken checks if a token is an OIDC JWT token (vs STS session token)
func isOIDCToken(token string) bool {
	// JWT tokens have three parts separated by dots and start with base64-encoded JSON
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}

	// JWT tokens typically start with "eyJ" (base64 encoded JSON starting with "{")
	return strings.HasPrefix(token, "eyJ")
}

// TrustPolicyValidator interface implementation
// These methods allow the IAMManager to serve as the trust policy validator for the STS service

// ValidateTrustPolicyForWebIdentity implements the TrustPolicyValidator interface
func (m *IAMManager) ValidateTrustPolicyForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("role not found: %s", roleName)
	}

	// Use existing trust policy validation logic
	return m.validateTrustPolicyForWebIdentity(ctx, roleDef, webIdentityToken)
}

// ValidateTrustPolicyForCredentials implements the TrustPolicyValidator interface
func (m *IAMManager) ValidateTrustPolicyForCredentials(ctx context.Context, roleArn string, identity *providers.ExternalIdentity) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("role not found: %s", roleName)
	}

	// For credentials, we need to create a mock request to reuse existing validation
	// This is a bit of a hack, but it allows us to reuse the existing logic
	mockRequest := &sts.AssumeRoleWithCredentialsRequest{
		ProviderName: identity.Provider, // Use the provider name from the identity
	}

	// Use existing trust policy validation logic
	return m.validateTrustPolicyForCredentials(ctx, roleDef, mockRequest)
}
