package integration

import (
	"context"

	"fmt"
	"strings"


	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// IAMManager orchestrates all IAM components
type IAMManager struct {
	stsAdapter           STSAdapter
	policyEngine         *policy.PolicyEngine
	roleStore            RoleStore
	groupStore           GroupStore
	filerAddressProvider func() string // Function to get current filer address
	masterClient         *wdclient.MasterClient
	initialized          bool
}

// IAMConfig holds configuration for all IAM components

// STSConfig holds STS service configuration (local definition to decouple from sts package)
type STSConfig struct {
	TokenDuration    string `json:"tokenDuration"` 
	MaxSessionLength string `json:"maxSessionLength"`
	Issuer           string `json:"issuer"`
	SigningKey       []byte `json:"signingKey"`
	AccountId        string `json:"accountId"`
}

type IAMConfig struct {
	// STS service configuration
	STS *STSConfig `json:"sts"`

	// Policy engine configuration
	Policy *policy.PolicyEngineConfig `json:"policy"`

	// Role store configuration
	Roles *RoleStoreConfig `json:"roleStore"`

	// Group store configuration
	Groups *GroupStoreConfig `json:"groupStore"`
}

// RoleStoreConfig holds role store configuration
type RoleStoreConfig struct {
	// StoreType specifies the role store backend (memory, filer, etc.)
	StoreType string `json:"storeType"`

	// StoreConfig contains store-specific configuration
	StoreConfig map[string]interface{} `json:"storeConfig,omitempty"`
}

// GroupStoreConfig holds group store configuration
type GroupStoreConfig struct {
	// StoreType specifies the group store backend (memory, filer, etc.)
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

	// MaxSessionDuration is the maximum session duration in seconds (3600-43200)
	MaxSessionDuration int `json:"maxSessionDuration,omitempty"`

	// InlinePolicies maps policy names to URL-encoded policy document JSON
	InlinePolicies map[string]string `json:"inlinePolicies,omitempty"`
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

	// AttachedPolicies list of direct policies attached to the identity
	AttachedPolicies []string `json:"attachedPolicies,omitempty"`
}

// NewIAMManager creates a new IAM manager
func NewIAMManager() *IAMManager {
	return &IAMManager{}
}

// Initialize initializes the IAM manager with all components
func (m *IAMManager) Initialize(config *IAMConfig, filerAddressProvider func() string, masterClient *wdclient.MasterClient) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Store the filer address provider function
	m.filerAddressProvider = filerAddressProvider
	m.masterClient = masterClient

	// Initialize STS adapter with stub by default
	m.stsAdapter = NewStubSTSAdapter()
	// Logic to initialize real STS if config is provided will be handled by caller or SetSTSAdapter

	// Initialize policy engine
	m.policyEngine = policy.NewPolicyEngine()
	if err := m.policyEngine.InitializeWithProvider(config.Policy, m.filerAddressProvider, m.masterClient); err != nil {
		return fmt.Errorf("failed to initialize policy engine: %w", err)
	}

	// Initialize role store
	roleStore, err := m.createRoleStoreWithProvider(config.Roles, m.filerAddressProvider, m.masterClient)
	if err != nil {
		return fmt.Errorf("failed to initialize role store: %w", err)
	}
	m.roleStore = roleStore

	// Initialize group store
	groupStore, err := m.createGroupStoreWithProvider(config.Groups, m.filerAddressProvider, m.masterClient)
	if err != nil {
		return fmt.Errorf("failed to initialize group store: %w", err)
	}
	m.groupStore = groupStore

	m.initialized = true
	return nil
}

// IsInitialized returns whether the IAM manager is initialized
func (m *IAMManager) IsInitialized() bool {
	return m.initialized
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
		return NewGenericCachedRoleStore(nil, nil, nil)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerRoleStore(config.StoreConfig, nil, nil)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedRoleStore(config.StoreConfig, nil, nil)
	case "cached-filer", "generic-cached":
		return NewGenericCachedRoleStore(config.StoreConfig, nil, nil)
	case "memory":
		return NewMemoryRoleStore(), nil
	default:
		return nil, fmt.Errorf("unsupported role store type: %s", config.StoreType)
	}
}

// createRoleStoreWithProvider creates a role store with a filer address provider function
func (m *IAMManager) createRoleStoreWithProvider(config *RoleStoreConfig, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (RoleStore, error) {
	if config == nil {
		// Default to generic cached filer role store when no config provided
		return NewGenericCachedRoleStore(nil, filerAddressProvider, nil)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerRoleStore(config.StoreConfig, filerAddressProvider, masterClient)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider, masterClient)
	case "cached-filer", "generic-cached":
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider, masterClient)
	case "memory":
		return NewMemoryRoleStore(), nil
	default:
		return nil, fmt.Errorf("unsupported role store type: %s", config.StoreType)
	}
}

// createGroupStoreWithProvider creates a group store with a filer address provider function
func (m *IAMManager) createGroupStoreWithProvider(config *GroupStoreConfig, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (GroupStore, error) {
	if config == nil {
		// Default to generic cached filer group store when no config provided
		return NewGenericCachedGroupStore(nil, filerAddressProvider, masterClient)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerGroupStore(config.StoreConfig, filerAddressProvider, masterClient)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedGroupStore(config.StoreConfig, filerAddressProvider, masterClient)
	case "cached-filer", "generic-cached":
		return NewGenericCachedGroupStore(config.StoreConfig, filerAddressProvider, masterClient)
	case "memory":
		return NewMemoryGroupStore(), nil
	default:
		return nil, fmt.Errorf("unsupported group store type: %s", config.StoreType)
	}
}

// RegisterIdentityProvider registers an identity provider
func (m *IAMManager) RegisterIdentityProvider(provider providers.IdentityProvider) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsAdapter.RegisterProvider(provider)
}

// GetRoleStore returns the role store
func (m *IAMManager) GetRoleStore() RoleStore {
	return m.roleStore
}

// GetGroupStore returns the group store
func (m *IAMManager) GetGroupStore() GroupStore {
	return m.groupStore
}

// CreatePolicy creates a new policy
func (m *IAMManager) CreatePolicy(ctx context.Context, filerAddress string, name string, policyDoc *policy.PolicyDocument) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.policyEngine.AddPolicy(filerAddress, name, policyDoc)
}

// ListPolicies lists all available policies
func (m *IAMManager) ListPolicies(ctx context.Context) ([]string, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}

	metas, err := m.policyEngine.ListPolicies(ctx, m.getFilerAddress())
	if err != nil {
		return nil, err
	}

	var names []string
	for _, meta := range metas {
		names = append(names, meta.Name)
	}
	return names, nil
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
		// Use standard AWS format with "seaweedfs" account ID for local roles
		// This ensures compatibility with AWS IAM policy evaluation
		roleDef.RoleArn = fmt.Sprintf("arn:aws:iam::seaweedfs:role/%s", roleName)
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



// AssumeRoleWithCredentials assumes a role using credentials (LDAP)
func (m *IAMManager) AssumeRoleWithCredentials(ctx context.Context, request *AssumeRoleRequest) (*AssumeRoleResponse, error) {
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

	// Validate session duration
	// Validate session duration
	if roleDef.MaxSessionDuration > 0 && request.DurationSeconds != nil && *request.DurationSeconds > int64(roleDef.MaxSessionDuration) {
		return nil, fmt.Errorf("requested duration %d seconds exceeds max session duration %d seconds for role %s", *request.DurationSeconds, roleDef.MaxSessionDuration, roleName)
	}

	// Use STS service to assume the role
	resp, err := m.stsAdapter.AssumeRoleWithCredentials(ctx, request)
	if err != nil {
		stats.StsRequestCounter.WithLabelValues("assume_role_with_credentials", roleName, "failure").Inc()
		return nil, err
	}

	stats.StsRequestCounter.WithLabelValues("assume_role_with_credentials", roleName, "success").Inc()
	return resp, nil
}

// AssumeRole assumes a role from an authenticated context
func (m *IAMManager) AssumeRole(ctx context.Context, request *AssumeRoleRequest) (*AssumeRoleResponse, error) {
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
	// For AssumeRole (authenticated user), we must evaluate the trust policy
	// matching AWS behavior:
	// 1. Principal is the ARN of the calling identity
	// 2. Action is "sts:AssumeRole"
	// 3. Resource is the Role ARN
	
	// Construct caller's ARN (e.g., arn:aws:iam::seaweedfs:user/test-user)
	// We use "seaweedfs" as the account ID for all internal principals
	if request.Identity != nil && request.Identity.UserID != "" {
		// Log
		glog.V(4).Infof("ValidateTrustPolicy: Identity=%v", request.Identity)
	}
	callerArn := fmt.Sprintf("arn:aws:iam::seaweedfs:user/%s", request.Identity.UserID)
	
	evalCtx := &policy.EvaluationContext{
		Principal: callerArn,
		Action:    "sts:AssumeRole", 
		Resource:  roleDef.RoleArn,
		// Request context can be expanded later if needed
		RequestContext: map[string]interface{}{
			"aws:PrincipalArn":     callerArn,
			"aws:username":         request.Identity.UserID,
			"seaweed:AWSPrincipal": callerArn, // Required for trust policy evaluation
		},
	}

	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return nil, fmt.Errorf("trust policy does not allow assume role for principal: %s", callerArn)
	}

	// Validate session duration
	// Validate session duration
	if roleDef.MaxSessionDuration > 0 && request.DurationSeconds != nil && *request.DurationSeconds > int64(roleDef.MaxSessionDuration) {
		return nil, fmt.Errorf("requested duration %d seconds exceeds max session duration %d seconds for role %s", *request.DurationSeconds, roleDef.MaxSessionDuration, roleName)
	}

	// Use STS service to assume the role
	resp, err := m.stsAdapter.AssumeRole(ctx, request)
	if err != nil {
		stats.StsRequestCounter.WithLabelValues("assume_role", roleName, "failure").Inc()
		return nil, err
	}

	stats.StsRequestCounter.WithLabelValues("assume_role", roleName, "success").Inc()
	return resp, nil
}

// IsActionAllowed checks if a principal is allowed to perform an action on a resource
func (m *IAMManager) IsActionAllowed(ctx context.Context, request *ActionRequest) (bool, error) {
	if !m.initialized {
		return false, fmt.Errorf("IAM manager not initialized")
	}

	// Validate session token first
	if request.SessionToken != "" {
		_, err := m.stsAdapter.ValidateSessionToken(ctx, request.SessionToken)
		if err != nil {
			return false, fmt.Errorf("invalid session: %w", err)
		}
	}

	var policies []string

	// Extract role name from principal ARN
	roleName := utils.ExtractRoleNameFromPrincipal(request.Principal)
	if roleName != "" {
		// Get role definition
		roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
		if err != nil {
			// If role not found, we might still have attached policies (e.g. from session)
			// But traditionally if role is missing, we can't find its policies
			if len(request.AttachedPolicies) == 0 {
				return false, fmt.Errorf("role not found: %s", roleName)
			}
		} else {
			policies = append(policies, roleDef.AttachedPolicies...)
		}
	}

	// Add directly attached policies (e.g., from legacy users or session policies)
	if len(request.AttachedPolicies) > 0 {
		policies = append(policies, request.AttachedPolicies...)
	}

	// If no policies found and no role, we can't authorize
	if len(policies) == 0 && roleName == "" {
		return false, fmt.Errorf("could not extract role from principal and no attached policies provided: %s", request.Principal)
	}

	// Determine principal type from the ARN for policy condition evaluation
	// This is required for conditions like StringNotEquals(aws:PrincipalType, "AssumedRole")
	principalType := "Account" // Default
	if strings.Contains(request.Principal, "assumed-role") {
		principalType = "AssumedRole"
	} else if strings.Contains(request.Principal, ":user/") {
		principalType = "User"
	}
	
	// Ensure RequestContext exists and add principal type
	if request.RequestContext == nil {
		request.RequestContext = make(map[string]interface{})
	}
	request.RequestContext["aws:PrincipalType"] = principalType

	// Create evaluation context
	evalCtx := &policy.EvaluationContext{
		Principal:      request.Principal,
		Action:         request.Action,
		Resource:       request.Resource,
		RequestContext: request.RequestContext,
	}

	// Evaluate policies
	result, err := m.policyEngine.Evaluate(ctx, "", evalCtx, policies)
	if err != nil {
		// Extract username for lower cardinality metrics
		username := utils.ExtractUsernameFromPrincipal(request.Principal)
		stats.IamAccessDeniedCounter.WithLabelValues(username).Inc()
		return false, fmt.Errorf("policy evaluation failed: %w", err)
	}

	allowed := result.Effect == policy.EffectAllow
	if !allowed {
		username := utils.ExtractUsernameFromPrincipal(request.Principal)
		stats.IamAccessDeniedCounter.WithLabelValues(username).Inc()
	}

	return allowed, nil
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



// validateTrustPolicyForCredentials validates trust policy for credential assumption
func (m *IAMManager) validateTrustPolicyForCredentials(ctx context.Context, roleDef *RoleDefinition, request *AssumeRoleRequest) error {
	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}

	// Check if trust policy allows credential assumption for the specific provider
	for _, statement := range roleDef.TrustPolicy.Statement {
		if statement.Effect == "Allow" {
			for _, action := range statement.Action {
				if action == "sts:AssumeRoleWithCredentials" {
					if principal, ok := statement.Principal.(map[string]interface{}); ok {
						if trustedProvider, ok := principal["Federated"].(string); ok {
							// Check if incoming identity matches trusted provider
							if request.Identity != nil && request.Identity.Provider == trustedProvider {
								return nil
							}
						}
					}
				}
			}
		}
	}

	if request.Identity != nil {
		return fmt.Errorf("trust policy does not allow credential assumption for provider: %s", request.Identity.Provider)
	}
	return fmt.Errorf("trust policy does not allow credential assumption: no identity provided")
}

// Helper functions

// ExpireSessionForTesting manually expires a session for testing purposes
func (m *IAMManager) ExpireSessionForTesting(ctx context.Context, sessionToken string) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsAdapter.ExpireSessionForTesting(ctx, sessionToken)
}

// GetSTSAdapter returns the STS adapter instance
func (m *IAMManager) GetSTSAdapter() STSAdapter {
	return m.stsAdapter
}

// SetSTSAdapter sets the STS adapter
func (m *IAMManager) SetSTSAdapter(adapter STSAdapter) {
	m.stsAdapter = adapter
}



// evaluateTrustPolicy evaluates a trust policy against the evaluation context
func (m *IAMManager) evaluateTrustPolicy(trustPolicy *policy.PolicyDocument, evalCtx *policy.EvaluationContext) bool {
	glog.V(0).Infof("DEBUG: evaluateTrustPolicy ENTRY. Policy Document: %+v", trustPolicy)
	glog.V(0).Infof("DEBUG: evaluateTrustPolicy ENTRY. Evaluation Context: %+v", evalCtx)
	if trustPolicy == nil {
		glog.V(0).Infof("DEBUG: evaluateTrustPolicy FAILURE. Trust policy is nil")
		return false
	}

	// Trust policies work differently from regular policies:
	// - They check the Principal field to see who can assume the role
	// - They check Action to see what actions are allowed
	// - They may have Conditions that must be satisfied

	for i, statement := range trustPolicy.Statement {
		glog.V(0).Infof("DEBUG: Processing Statement [%d]: %+v", i, statement)
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
				glog.V(0).Infof("DEBUG: Found Principal Map: %+v", principal)

				// Check for AWS principal (IAM users/roles)
				if !principalMatches {
					if awsValue, ok := principal["AWS"]; ok {
						glog.V(0).Infof("Evaluating AWS Principal. Policy: %v, ContextKey: seaweed:AWSPrincipal", awsValue)
						principalMatches = m.evaluatePrincipalValue(awsValue, evalCtx, "seaweed:AWSPrincipal")
						glog.V(0).Infof("AWS Principal Match Result: %v", principalMatches)
					}
				}
				// Check for Service principal (AWS services)
				if !principalMatches {
					if serviceValue, ok := principal["Service"]; ok {
						glog.V(0).Infof("DEBUG: Evaluating Service Principal: %v", serviceValue)
						principalMatches = m.evaluatePrincipalValue(serviceValue, evalCtx, "seaweed:ServicePrincipal")
					}
				}
			} else if principalStr, ok := statement.Principal.(string); ok {
				glog.V(0).Infof("DEBUG: Found String Principal: %s", principalStr)
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
	glog.V(0).Infof("evaluatePrincipalValue: Key=%s, Exists=%v, ContextValue=%v, PrincipalValue=%v", contextKey, exists, contextValue, principalValue)

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



// TrustPolicyValidator interface implementation
// These methods allow the IAMManager to serve as the trust policy validator for the STS service

// ValidateTrustPolicyForWebIdentity implements the TrustPolicyValidator interface




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
		return fmt.Errorf("role '%s' not found: %w (role ARN: %s)", roleName, err, roleArn)
	}

	// Use "seaweedfs" provider check to distinguish between standard AssumeRole (internal users)
	// and AssumeRoleWithCredentials (external providers like LDAP/OIDC)
	if identity.Provider == "seaweedfs" {
		// This is an internal IAM user performing standard AssumeRole
		// We must validate against the Principal ARN and sts:AssumeRole action
		// matching AWS behavior

		// Construct caller's ARN (e.g., arn:aws:iam::seaweedfs:user/test-user)
		callerArn := fmt.Sprintf("arn:aws:iam::seaweedfs:user/%s", identity.UserID)

		evalCtx := &policy.EvaluationContext{
			Principal: callerArn,
			Action:    "sts:AssumeRole",
			Resource:  roleDef.RoleArn,
			RequestContext: map[string]interface{}{
				"aws:PrincipalArn":     callerArn,
				"aws:username":         identity.UserID,
				"seaweed:AWSPrincipal": callerArn,
			},
		}

		glog.V(2).Infof("Evaluating trust policy for role '%s', principal: %s, action: %s", roleName, callerArn, "sts:AssumeRole")

		if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
			// Provide detailed error message
			statementCount := 0
			if roleDef.TrustPolicy != nil {
				statementCount = len(roleDef.TrustPolicy.Statement)
			}
			return fmt.Errorf("trust policy evaluation failed for role '%s':\n  Principal: %s\n  Action: %s\n  Resource: %s\n  Policy has %d statement(s), none allowed this request.\n  Hint: Check that the trust policy Principal matches the caller ARN and Action includes 'sts:AssumeRole'",
				roleName, callerArn, "sts:AssumeRole", roleDef.RoleArn, statementCount)
		}
		glog.V(1).Infof("Trust policy allowed assume role for principal: %s on role: %s", callerArn, roleName)
		return nil
	}

	// For credentials (LDAP/OIDC/Custom), we need to create a mock request to reuse existing validation
	// This is a bit of a hack, but it allows us to reuse the existing logic
	mockRequest := &AssumeRoleRequest{
		Identity: identity,
	}

	// Use existing trust policy validation logic
	return m.validateTrustPolicyForCredentials(ctx, roleDef, mockRequest)
}

// InvalidateRoleCache invalidates the cache for a specific role
func (m *IAMManager) InvalidateRoleCache(roleName string) {
	if !m.initialized || m.roleStore == nil {
		return
	}
	m.roleStore.InvalidateCache(roleName)
}

// InvalidatePolicyCache invalidates the cache for a specific policy
func (m *IAMManager) InvalidatePolicyCache(policyName string) {
	if !m.initialized || m.policyEngine == nil {
		return
	}
	m.policyEngine.InvalidatePolicyCache(policyName)
}
