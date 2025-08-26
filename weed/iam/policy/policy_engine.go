package policy

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
)

// Effect represents the policy evaluation result
type Effect string

const (
	EffectAllow Effect = "Allow"
	EffectDeny  Effect = "Deny"
)

// PolicyEngine evaluates policies against requests
type PolicyEngine struct {
	config      *PolicyEngineConfig
	initialized bool
	store       PolicyStore
}

// PolicyEngineConfig holds policy engine configuration
type PolicyEngineConfig struct {
	// DefaultEffect when no policies match (Allow or Deny)
	DefaultEffect string `json:"defaultEffect"`

	// StoreType specifies the policy store backend (memory, filer, etc.)
	StoreType string `json:"storeType"`

	// StoreConfig contains store-specific configuration
	StoreConfig map[string]interface{} `json:"storeConfig,omitempty"`
}

// PolicyDocument represents an IAM policy document
type PolicyDocument struct {
	// Version of the policy language (e.g., "2012-10-17")
	Version string `json:"Version"`

	// Id is an optional policy identifier
	Id string `json:"Id,omitempty"`

	// Statement contains the policy statements
	Statement []Statement `json:"Statement"`
}

// Statement represents a single policy statement
type Statement struct {
	// Sid is an optional statement identifier
	Sid string `json:"Sid,omitempty"`

	// Effect specifies whether to Allow or Deny
	Effect string `json:"Effect"`

	// Principal specifies who the statement applies to (optional in role policies)
	Principal interface{} `json:"Principal,omitempty"`

	// NotPrincipal specifies who the statement does NOT apply to
	NotPrincipal interface{} `json:"NotPrincipal,omitempty"`

	// Action specifies the actions this statement applies to
	Action []string `json:"Action"`

	// NotAction specifies actions this statement does NOT apply to
	NotAction []string `json:"NotAction,omitempty"`

	// Resource specifies the resources this statement applies to
	Resource []string `json:"Resource"`

	// NotResource specifies resources this statement does NOT apply to
	NotResource []string `json:"NotResource,omitempty"`

	// Condition specifies conditions for when this statement applies
	Condition map[string]map[string]interface{} `json:"Condition,omitempty"`
}

// EvaluationContext provides context for policy evaluation
type EvaluationContext struct {
	// Principal making the request (e.g., "user:alice", "role:admin")
	Principal string `json:"principal"`

	// Action being requested (e.g., "s3:GetObject")
	Action string `json:"action"`

	// Resource being accessed (e.g., "arn:seaweed:s3:::bucket/key")
	Resource string `json:"resource"`

	// RequestContext contains additional request information
	RequestContext map[string]interface{} `json:"requestContext,omitempty"`
}

// EvaluationResult contains the result of policy evaluation
type EvaluationResult struct {
	// Effect is the final decision (Allow or Deny)
	Effect Effect `json:"effect"`

	// MatchingStatements contains statements that matched the request
	MatchingStatements []StatementMatch `json:"matchingStatements,omitempty"`

	// EvaluationDetails provides detailed evaluation information
	EvaluationDetails *EvaluationDetails `json:"evaluationDetails,omitempty"`
}

// StatementMatch represents a statement that matched during evaluation
type StatementMatch struct {
	// PolicyName is the name of the policy containing this statement
	PolicyName string `json:"policyName"`

	// StatementSid is the statement identifier
	StatementSid string `json:"statementSid,omitempty"`

	// Effect is the effect of this statement
	Effect Effect `json:"effect"`

	// Reason explains why this statement matched
	Reason string `json:"reason,omitempty"`
}

// EvaluationDetails provides detailed information about policy evaluation
type EvaluationDetails struct {
	// Principal that was evaluated
	Principal string `json:"principal"`

	// Action that was evaluated
	Action string `json:"action"`

	// Resource that was evaluated
	Resource string `json:"resource"`

	// PoliciesEvaluated lists all policies that were evaluated
	PoliciesEvaluated []string `json:"policiesEvaluated"`

	// ConditionsEvaluated lists all conditions that were evaluated
	ConditionsEvaluated []string `json:"conditionsEvaluated,omitempty"`
}

// PolicyStore defines the interface for storing and retrieving policies
type PolicyStore interface {
	// StorePolicy stores a policy document (filerAddress ignored for memory stores)
	StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error

	// GetPolicy retrieves a policy document (filerAddress ignored for memory stores)
	GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error)

	// DeletePolicy deletes a policy document (filerAddress ignored for memory stores)
	DeletePolicy(ctx context.Context, filerAddress string, name string) error

	// ListPolicies lists all policy names (filerAddress ignored for memory stores)
	ListPolicies(ctx context.Context, filerAddress string) ([]string, error)
}

// NewPolicyEngine creates a new policy engine
func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{}
}

// Initialize initializes the policy engine with configuration
func (e *PolicyEngine) Initialize(config *PolicyEngineConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if err := e.validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	e.config = config

	// Initialize policy store
	store, err := e.createPolicyStore(config)
	if err != nil {
		return fmt.Errorf("failed to create policy store: %w", err)
	}
	e.store = store

	e.initialized = true
	return nil
}

// validateConfig validates the policy engine configuration
func (e *PolicyEngine) validateConfig(config *PolicyEngineConfig) error {
	if config.DefaultEffect != "Allow" && config.DefaultEffect != "Deny" {
		return fmt.Errorf("invalid default effect: %s", config.DefaultEffect)
	}

	if config.StoreType == "" {
		config.StoreType = "filer" // Default to filer store for persistence
	}

	return nil
}

// createPolicyStore creates a policy store based on configuration
func (e *PolicyEngine) createPolicyStore(config *PolicyEngineConfig) (PolicyStore, error) {
	switch config.StoreType {
	case "memory":
		return NewMemoryPolicyStore(), nil
	case "filer":
		return NewFilerPolicyStore(config.StoreConfig)
	default:
		return nil, fmt.Errorf("unsupported store type: %s", config.StoreType)
	}
}

// IsInitialized returns whether the engine is initialized
func (e *PolicyEngine) IsInitialized() bool {
	return e.initialized
}

// AddPolicy adds a policy to the engine (filerAddress ignored for memory stores)
func (e *PolicyEngine) AddPolicy(filerAddress string, name string, policy *PolicyDocument) error {
	if !e.initialized {
		return fmt.Errorf("policy engine not initialized")
	}

	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	if err := ValidatePolicyDocument(policy); err != nil {
		return fmt.Errorf("invalid policy document: %w", err)
	}

	return e.store.StorePolicy(context.Background(), filerAddress, name, policy)
}

// Evaluate evaluates policies against a request context (filerAddress ignored for memory stores)
func (e *PolicyEngine) Evaluate(ctx context.Context, filerAddress string, evalCtx *EvaluationContext, policyNames []string) (*EvaluationResult, error) {
	if !e.initialized {
		return nil, fmt.Errorf("policy engine not initialized")
	}

	if evalCtx == nil {
		return nil, fmt.Errorf("evaluation context cannot be nil")
	}

	result := &EvaluationResult{
		Effect: Effect(e.config.DefaultEffect),
		EvaluationDetails: &EvaluationDetails{
			Principal:         evalCtx.Principal,
			Action:            evalCtx.Action,
			Resource:          evalCtx.Resource,
			PoliciesEvaluated: policyNames,
		},
	}

	var matchingStatements []StatementMatch
	explicitDeny := false
	hasAllow := false

	// Evaluate each policy
	for _, policyName := range policyNames {
		policy, err := e.store.GetPolicy(ctx, filerAddress, policyName)
		if err != nil {
			continue // Skip policies that can't be loaded
		}

		// Evaluate each statement in the policy
		for _, statement := range policy.Statement {
			if e.statementMatches(&statement, evalCtx) {
				match := StatementMatch{
					PolicyName:   policyName,
					StatementSid: statement.Sid,
					Effect:       Effect(statement.Effect),
					Reason:       "Action, Resource, and Condition matched",
				}
				matchingStatements = append(matchingStatements, match)

				if statement.Effect == "Deny" {
					explicitDeny = true
				} else if statement.Effect == "Allow" {
					hasAllow = true
				}
			}
		}
	}

	result.MatchingStatements = matchingStatements

	// AWS IAM evaluation logic:
	// 1. If there's an explicit Deny, the result is Deny
	// 2. If there's an Allow and no Deny, the result is Allow
	// 3. Otherwise, use the default effect
	if explicitDeny {
		result.Effect = EffectDeny
	} else if hasAllow {
		result.Effect = EffectAllow
	}

	return result, nil
}

// statementMatches checks if a statement matches the evaluation context
func (e *PolicyEngine) statementMatches(statement *Statement, evalCtx *EvaluationContext) bool {
	// Check action match
	if !e.matchesActions(statement.Action, evalCtx.Action) {
		return false
	}

	// Check resource match
	if !e.matchesResources(statement.Resource, evalCtx.Resource) {
		return false
	}

	// Check conditions
	if !e.matchesConditions(statement.Condition, evalCtx) {
		return false
	}

	return true
}

// matchesActions checks if any action in the list matches the requested action
func (e *PolicyEngine) matchesActions(actions []string, requestedAction string) bool {
	for _, action := range actions {
		if matchAction(action, requestedAction) {
			return true
		}
	}
	return false
}

// matchesResources checks if any resource in the list matches the requested resource
func (e *PolicyEngine) matchesResources(resources []string, requestedResource string) bool {
	for _, resource := range resources {
		if matchResource(resource, requestedResource) {
			return true
		}
	}
	return false
}

// matchesConditions checks if all conditions are satisfied
func (e *PolicyEngine) matchesConditions(conditions map[string]map[string]interface{}, evalCtx *EvaluationContext) bool {
	if len(conditions) == 0 {
		return true // No conditions means always match
	}

	for conditionType, conditionBlock := range conditions {
		if !e.evaluateConditionBlock(conditionType, conditionBlock, evalCtx) {
			return false
		}
	}

	return true
}

// evaluateConditionBlock evaluates a single condition block
func (e *PolicyEngine) evaluateConditionBlock(conditionType string, block map[string]interface{}, evalCtx *EvaluationContext) bool {
	switch conditionType {
	case "IpAddress":
		return e.evaluateIPCondition(block, evalCtx, true)
	case "NotIpAddress":
		return e.evaluateIPCondition(block, evalCtx, false)
	case "StringEquals":
		return e.evaluateStringCondition(block, evalCtx, true, false)
	case "StringNotEquals":
		return e.evaluateStringCondition(block, evalCtx, false, false)
	case "StringLike":
		return e.evaluateStringCondition(block, evalCtx, true, true)
	default:
		// Unknown condition types default to false (more secure)
		return false
	}
}

// evaluateIPCondition evaluates IP address conditions
func (e *PolicyEngine) evaluateIPCondition(block map[string]interface{}, evalCtx *EvaluationContext, shouldMatch bool) bool {
	sourceIP, exists := evalCtx.RequestContext["sourceIP"]
	if !exists {
		return !shouldMatch // If no IP in context, condition fails for positive match
	}

	sourceIPStr, ok := sourceIP.(string)
	if !ok {
		return !shouldMatch
	}

	sourceIPAddr := net.ParseIP(sourceIPStr)
	if sourceIPAddr == nil {
		return !shouldMatch
	}

	for key, value := range block {
		if key == "seaweed:SourceIP" {
			ranges, ok := value.([]string)
			if !ok {
				continue
			}

			for _, ipRange := range ranges {
				if strings.Contains(ipRange, "/") {
					// CIDR range
					_, cidr, err := net.ParseCIDR(ipRange)
					if err != nil {
						continue
					}
					if cidr.Contains(sourceIPAddr) {
						return shouldMatch
					}
				} else {
					// Single IP
					if sourceIPStr == ipRange {
						return shouldMatch
					}
				}
			}
		}
	}

	return !shouldMatch
}

// evaluateStringCondition evaluates string-based conditions
func (e *PolicyEngine) evaluateStringCondition(block map[string]interface{}, evalCtx *EvaluationContext, shouldMatch bool, useWildcard bool) bool {
	// Iterate through all condition keys in the block
	for conditionKey, conditionValue := range block {
		// Get the context values for this condition key
		contextValues, exists := evalCtx.RequestContext[conditionKey]
		if !exists {
			// If the context key doesn't exist, condition fails for positive match
			if shouldMatch {
				return false
			}
			continue
		}

		// Convert context value to string slice
		var contextStrings []string
		switch v := contextValues.(type) {
		case string:
			contextStrings = []string{v}
		case []string:
			contextStrings = v
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok {
					contextStrings = append(contextStrings, str)
				}
			}
		default:
			// Convert to string as fallback
			contextStrings = []string{fmt.Sprintf("%v", v)}
		}

		// Convert condition value to string slice
		var expectedStrings []string
		switch v := conditionValue.(type) {
		case string:
			expectedStrings = []string{v}
		case []string:
			expectedStrings = v
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok {
					expectedStrings = append(expectedStrings, str)
				} else {
					expectedStrings = append(expectedStrings, fmt.Sprintf("%v", item))
				}
			}
		default:
			expectedStrings = []string{fmt.Sprintf("%v", v)}
		}

		// Evaluate the condition
		conditionMet := false
		for _, expected := range expectedStrings {
			for _, contextValue := range contextStrings {
				if useWildcard {
					// Use wildcard matching for StringLike conditions
					matched, err := filepath.Match(expected, contextValue)
					if err == nil && matched {
						conditionMet = true
						break
					}
				} else {
					// Exact string matching for StringEquals/StringNotEquals
					if expected == contextValue {
						conditionMet = true
						break
					}
				}
			}
			if conditionMet {
				break
			}
		}

		// For shouldMatch=true (StringEquals, StringLike): condition must be met
		// For shouldMatch=false (StringNotEquals): condition must NOT be met
		if shouldMatch && !conditionMet {
			return false
		}
		if !shouldMatch && conditionMet {
			return false
		}
	}

	return true
}

// ValidatePolicyDocument validates a policy document structure
func ValidatePolicyDocument(policy *PolicyDocument) error {
	return ValidatePolicyDocumentWithType(policy, "resource")
}

// ValidateTrustPolicyDocument validates a trust policy document structure
func ValidateTrustPolicyDocument(policy *PolicyDocument) error {
	return ValidatePolicyDocumentWithType(policy, "trust")
}

// ValidatePolicyDocumentWithType validates a policy document for specific type
func ValidatePolicyDocumentWithType(policy *PolicyDocument, policyType string) error {
	if policy == nil {
		return fmt.Errorf("policy document cannot be nil")
	}

	if policy.Version == "" {
		return fmt.Errorf("version is required")
	}

	if len(policy.Statement) == 0 {
		return fmt.Errorf("at least one statement is required")
	}

	for i, statement := range policy.Statement {
		if err := validateStatementWithType(&statement, policyType); err != nil {
			return fmt.Errorf("statement %d is invalid: %w", i, err)
		}
	}

	return nil
}

// validateStatement validates a single statement (for backward compatibility)
func validateStatement(statement *Statement) error {
	return validateStatementWithType(statement, "resource")
}

// validateStatementWithType validates a single statement based on policy type
func validateStatementWithType(statement *Statement, policyType string) error {
	if statement.Effect != "Allow" && statement.Effect != "Deny" {
		return fmt.Errorf("invalid effect: %s (must be Allow or Deny)", statement.Effect)
	}

	if len(statement.Action) == 0 {
		return fmt.Errorf("at least one action is required")
	}

	// Trust policies don't require Resource field, but resource policies do
	if policyType == "resource" {
		if len(statement.Resource) == 0 {
			return fmt.Errorf("at least one resource is required")
		}
	} else if policyType == "trust" {
		// Trust policies should have Principal field
		if statement.Principal == nil {
			return fmt.Errorf("trust policy statement must have Principal field")
		}

		// Trust policies typically have specific actions
		validTrustActions := map[string]bool{
			"sts:AssumeRole":                true,
			"sts:AssumeRoleWithWebIdentity": true,
			"sts:AssumeRoleWithCredentials": true,
		}

		for _, action := range statement.Action {
			if !validTrustActions[action] {
				return fmt.Errorf("invalid action for trust policy: %s", action)
			}
		}
	}

	return nil
}

// matchResource checks if a resource pattern matches a requested resource
func matchResource(pattern, resource string) bool {
	if pattern == resource {
		return true
	}

	if pattern == "*" {
		return true
	}

	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(resource, prefix)
	}

	return false
}

// matchAction checks if an action pattern matches a requested action
func matchAction(pattern, action string) bool {
	if pattern == action {
		return true
	}

	if pattern == "*" {
		return true
	}

	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(action, prefix)
	}

	return false
}
