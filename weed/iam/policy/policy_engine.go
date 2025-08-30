package policy

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Effect represents the policy evaluation result
type Effect string

const (
	EffectAllow Effect = "Allow"
	EffectDeny  Effect = "Deny"
)

// Package-level regex cache for performance optimization
var (
	regexCache   = make(map[string]*regexp.Regexp)
	regexCacheMu sync.RWMutex
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

// InitializeWithProvider initializes the policy engine with configuration and a filer address provider
func (e *PolicyEngine) InitializeWithProvider(config *PolicyEngineConfig, filerAddressProvider func() string) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if err := e.validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	e.config = config

	// Initialize policy store with provider
	store, err := e.createPolicyStoreWithProvider(config, filerAddressProvider)
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
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerPolicyStore(config.StoreConfig, nil)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedPolicyStore(config.StoreConfig, nil)
	case "cached-filer", "generic-cached":
		return NewGenericCachedPolicyStore(config.StoreConfig, nil)
	default:
		return nil, fmt.Errorf("unsupported store type: %s", config.StoreType)
	}
}

// createPolicyStoreWithProvider creates a policy store with a filer address provider function
func (e *PolicyEngine) createPolicyStoreWithProvider(config *PolicyEngineConfig, filerAddressProvider func() string) (PolicyStore, error) {
	switch config.StoreType {
	case "memory":
		return NewMemoryPolicyStore(), nil
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerPolicyStore(config.StoreConfig, filerAddressProvider)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedPolicyStore(config.StoreConfig, filerAddressProvider)
	case "cached-filer", "generic-cached":
		return NewGenericCachedPolicyStore(config.StoreConfig, filerAddressProvider)
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
	if !e.matchesActions(statement.Action, evalCtx.Action, evalCtx) {
		return false
	}

	// Check resource match
	if !e.matchesResources(statement.Resource, evalCtx.Resource, evalCtx) {
		return false
	}

	// Check conditions
	if !e.matchesConditions(statement.Condition, evalCtx) {
		return false
	}

	return true
}

// matchesActions checks if any action in the list matches the requested action
func (e *PolicyEngine) matchesActions(actions []string, requestedAction string, evalCtx *EvaluationContext) bool {
	for _, action := range actions {
		if awsIAMMatch(action, requestedAction, evalCtx) {
			return true
		}
	}
	return false
}

// matchesResources checks if any resource in the list matches the requested resource
func (e *PolicyEngine) matchesResources(resources []string, requestedResource string, evalCtx *EvaluationContext) bool {
	for _, resource := range resources {
		if awsIAMMatch(resource, requestedResource, evalCtx) {
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
	// IP Address conditions
	case "IpAddress":
		return e.evaluateIPCondition(block, evalCtx, true)
	case "NotIpAddress":
		return e.evaluateIPCondition(block, evalCtx, false)

	// String conditions
	case "StringEquals":
		return e.EvaluateStringCondition(block, evalCtx, true, false)
	case "StringNotEquals":
		return e.EvaluateStringCondition(block, evalCtx, false, false)
	case "StringLike":
		return e.EvaluateStringCondition(block, evalCtx, true, true)
	case "StringEqualsIgnoreCase":
		return e.evaluateStringConditionIgnoreCase(block, evalCtx, true, false)
	case "StringNotEqualsIgnoreCase":
		return e.evaluateStringConditionIgnoreCase(block, evalCtx, false, false)
	case "StringLikeIgnoreCase":
		return e.evaluateStringConditionIgnoreCase(block, evalCtx, true, true)

	// Numeric conditions
	case "NumericEquals":
		return e.evaluateNumericCondition(block, evalCtx, "==")
	case "NumericNotEquals":
		return e.evaluateNumericCondition(block, evalCtx, "!=")
	case "NumericLessThan":
		return e.evaluateNumericCondition(block, evalCtx, "<")
	case "NumericLessThanEquals":
		return e.evaluateNumericCondition(block, evalCtx, "<=")
	case "NumericGreaterThan":
		return e.evaluateNumericCondition(block, evalCtx, ">")
	case "NumericGreaterThanEquals":
		return e.evaluateNumericCondition(block, evalCtx, ">=")

	// Date conditions
	case "DateEquals":
		return e.evaluateDateCondition(block, evalCtx, "==")
	case "DateNotEquals":
		return e.evaluateDateCondition(block, evalCtx, "!=")
	case "DateLessThan":
		return e.evaluateDateCondition(block, evalCtx, "<")
	case "DateLessThanEquals":
		return e.evaluateDateCondition(block, evalCtx, "<=")
	case "DateGreaterThan":
		return e.evaluateDateCondition(block, evalCtx, ">")
	case "DateGreaterThanEquals":
		return e.evaluateDateCondition(block, evalCtx, ">=")

	// Boolean conditions
	case "Bool":
		return e.evaluateBoolCondition(block, evalCtx)

	// Null conditions
	case "Null":
		return e.evaluateNullCondition(block, evalCtx)

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

// EvaluateStringCondition evaluates string-based conditions
func (e *PolicyEngine) EvaluateStringCondition(block map[string]interface{}, evalCtx *EvaluationContext, shouldMatch bool, useWildcard bool) bool {
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

		// Evaluate the condition using AWS IAM-compliant matching
		conditionMet := false
		for _, expected := range expectedStrings {
			for _, contextValue := range contextStrings {
				if useWildcard {
					// Use AWS IAM-compliant wildcard matching for StringLike conditions
					// This handles case-insensitivity and policy variables
					if awsIAMMatch(expected, contextValue, evalCtx) {
						conditionMet = true
						break
					}
				} else {
					// For StringEquals/StringNotEquals, also support policy variables but be case-sensitive
					expandedExpected := expandPolicyVariables(expected, evalCtx)
					if expandedExpected == contextValue {
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
// Uses hybrid approach: simple suffix wildcards for compatibility, filepath.Match for complex patterns
func matchResource(pattern, resource string) bool {
	if pattern == resource {
		return true
	}

	// Handle simple suffix wildcard (backward compatibility)
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(resource, prefix)
	}

	// For complex patterns, use filepath.Match for advanced wildcard support (*, ?, [])
	matched, err := filepath.Match(pattern, resource)
	if err != nil {
		// Fallback to exact match if pattern is malformed
		return pattern == resource
	}

	return matched
}

// awsIAMMatch performs AWS IAM-compliant pattern matching with case-insensitivity and policy variable support
func awsIAMMatch(pattern, value string, evalCtx *EvaluationContext) bool {
	// Step 1: Substitute policy variables (e.g., ${aws:username}, ${saml:username})
	expandedPattern := expandPolicyVariables(pattern, evalCtx)

	// Step 2: Handle special patterns
	if expandedPattern == "*" {
		return true // Universal wildcard
	}

	// Step 3: Case-insensitive exact match
	if strings.EqualFold(expandedPattern, value) {
		return true
	}

	// Step 4: Handle AWS-style wildcards (case-insensitive)
	if strings.Contains(expandedPattern, "*") || strings.Contains(expandedPattern, "?") {
		return AwsWildcardMatch(expandedPattern, value)
	}

	return false
}

// expandPolicyVariables substitutes AWS policy variables in the pattern
func expandPolicyVariables(pattern string, evalCtx *EvaluationContext) string {
	if evalCtx == nil || evalCtx.RequestContext == nil {
		return pattern
	}

	expanded := pattern

	// Common AWS policy variables that might be used in SeaweedFS
	variableMap := map[string]string{
		"${aws:username}":      getContextValue(evalCtx, "aws:username", ""),
		"${saml:username}":     getContextValue(evalCtx, "saml:username", ""),
		"${oidc:sub}":          getContextValue(evalCtx, "oidc:sub", ""),
		"${aws:userid}":        getContextValue(evalCtx, "aws:userid", ""),
		"${aws:principaltype}": getContextValue(evalCtx, "aws:principaltype", ""),
	}

	for variable, value := range variableMap {
		if value != "" {
			expanded = strings.ReplaceAll(expanded, variable, value)
		}
	}

	return expanded
}

// getContextValue safely gets a value from the evaluation context
func getContextValue(evalCtx *EvaluationContext, key, defaultValue string) string {
	if value, exists := evalCtx.RequestContext[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return defaultValue
}

// AwsWildcardMatch performs case-insensitive wildcard matching like AWS IAM
func AwsWildcardMatch(pattern, value string) bool {
	// Create regex pattern key for caching
	// First escape all regex metacharacters, then replace wildcards
	regexPattern := regexp.QuoteMeta(pattern)
	regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "\\?", ".")
	regexPattern = "^" + regexPattern + "$"
	regexKey := "(?i)" + regexPattern

	// Try to get compiled regex from cache
	regexCacheMu.RLock()
	regex, found := regexCache[regexKey]
	regexCacheMu.RUnlock()

	if !found {
		// Compile and cache the regex
		compiledRegex, err := regexp.Compile(regexKey)
		if err != nil {
			// Fallback to simple case-insensitive comparison if regex fails
			return strings.EqualFold(pattern, value)
		}

		// Store in cache with write lock
		regexCacheMu.Lock()
		// Double-check in case another goroutine added it
		if existingRegex, exists := regexCache[regexKey]; exists {
			regex = existingRegex
		} else {
			regexCache[regexKey] = compiledRegex
			regex = compiledRegex
		}
		regexCacheMu.Unlock()
	}

	return regex.MatchString(value)
}

// matchAction checks if an action pattern matches a requested action
// Uses hybrid approach: simple suffix wildcards for compatibility, filepath.Match for complex patterns
func matchAction(pattern, action string) bool {
	if pattern == action {
		return true
	}

	// Handle simple suffix wildcard (backward compatibility)
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(action, prefix)
	}

	// For complex patterns, use filepath.Match for advanced wildcard support (*, ?, [])
	matched, err := filepath.Match(pattern, action)
	if err != nil {
		// Fallback to exact match if pattern is malformed
		return pattern == action
	}

	return matched
}

// evaluateStringConditionIgnoreCase evaluates string conditions with case insensitivity
func (e *PolicyEngine) evaluateStringConditionIgnoreCase(block map[string]interface{}, evalCtx *EvaluationContext, shouldMatch bool, useWildcard bool) bool {
	for key, expectedValues := range block {
		contextValue, exists := evalCtx.RequestContext[key]
		if !exists {
			if !shouldMatch {
				continue // For NotEquals, missing key is OK
			}
			return false
		}

		contextStr, ok := contextValue.(string)
		if !ok {
			return false
		}

		contextStr = strings.ToLower(contextStr)
		matched := false

		// Handle different value types
		switch v := expectedValues.(type) {
		case string:
			expectedStr := strings.ToLower(v)
			if useWildcard {
				matched, _ = filepath.Match(expectedStr, contextStr)
			} else {
				matched = expectedStr == contextStr
			}
		case []interface{}:
			for _, val := range v {
				if valStr, ok := val.(string); ok {
					expectedStr := strings.ToLower(valStr)
					if useWildcard {
						if m, _ := filepath.Match(expectedStr, contextStr); m {
							matched = true
							break
						}
					} else {
						if expectedStr == contextStr {
							matched = true
							break
						}
					}
				}
			}
		}

		if shouldMatch && !matched {
			return false
		}
		if !shouldMatch && matched {
			return false
		}
	}
	return true
}

// evaluateNumericCondition evaluates numeric conditions
func (e *PolicyEngine) evaluateNumericCondition(block map[string]interface{}, evalCtx *EvaluationContext, operator string) bool {
	for key, expectedValues := range block {
		contextValue, exists := evalCtx.RequestContext[key]
		if !exists {
			return false
		}

		contextNum, err := parseNumeric(contextValue)
		if err != nil {
			return false
		}

		matched := false

		// Handle different value types
		switch v := expectedValues.(type) {
		case string:
			expectedNum, err := parseNumeric(v)
			if err != nil {
				return false
			}
			matched = compareNumbers(contextNum, expectedNum, operator)
		case []interface{}:
			for _, val := range v {
				expectedNum, err := parseNumeric(val)
				if err != nil {
					continue
				}
				if compareNumbers(contextNum, expectedNum, operator) {
					matched = true
					break
				}
			}
		}

		if !matched {
			return false
		}
	}
	return true
}

// evaluateDateCondition evaluates date conditions
func (e *PolicyEngine) evaluateDateCondition(block map[string]interface{}, evalCtx *EvaluationContext, operator string) bool {
	for key, expectedValues := range block {
		contextValue, exists := evalCtx.RequestContext[key]
		if !exists {
			return false
		}

		contextTime, err := parseDateTime(contextValue)
		if err != nil {
			return false
		}

		matched := false

		// Handle different value types
		switch v := expectedValues.(type) {
		case string:
			expectedTime, err := parseDateTime(v)
			if err != nil {
				return false
			}
			matched = compareDates(contextTime, expectedTime, operator)
		case []interface{}:
			for _, val := range v {
				expectedTime, err := parseDateTime(val)
				if err != nil {
					continue
				}
				if compareDates(contextTime, expectedTime, operator) {
					matched = true
					break
				}
			}
		}

		if !matched {
			return false
		}
	}
	return true
}

// evaluateBoolCondition evaluates boolean conditions
func (e *PolicyEngine) evaluateBoolCondition(block map[string]interface{}, evalCtx *EvaluationContext) bool {
	for key, expectedValues := range block {
		contextValue, exists := evalCtx.RequestContext[key]
		if !exists {
			return false
		}

		contextBool, err := parseBool(contextValue)
		if err != nil {
			return false
		}

		matched := false

		// Handle different value types
		switch v := expectedValues.(type) {
		case string:
			expectedBool, err := parseBool(v)
			if err != nil {
				return false
			}
			matched = contextBool == expectedBool
		case bool:
			matched = contextBool == v
		case []interface{}:
			for _, val := range v {
				expectedBool, err := parseBool(val)
				if err != nil {
					continue
				}
				if contextBool == expectedBool {
					matched = true
					break
				}
			}
		}

		if !matched {
			return false
		}
	}
	return true
}

// evaluateNullCondition evaluates null conditions
func (e *PolicyEngine) evaluateNullCondition(block map[string]interface{}, evalCtx *EvaluationContext) bool {
	for key, expectedValues := range block {
		_, exists := evalCtx.RequestContext[key]

		expectedNull := false
		switch v := expectedValues.(type) {
		case string:
			expectedNull = v == "true"
		case bool:
			expectedNull = v
		}

		// If we expect null (true) and key exists, or expect non-null (false) and key doesn't exist
		if expectedNull == exists {
			return false
		}
	}
	return true
}

// Helper functions for parsing and comparing values

// parseNumeric parses a value as a float64
func parseNumeric(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot parse %T as numeric", value)
	}
}

// compareNumbers compares two numbers using the given operator
func compareNumbers(a, b float64, operator string) bool {
	switch operator {
	case "==":
		return a == b
	case "!=":
		return a != b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case ">=":
		return a >= b
	default:
		return false
	}
}

// parseDateTime parses a value as a time.Time
func parseDateTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case string:
		// Try common date formats
		formats := []string{
			time.RFC3339,
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse date: %s", v)
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf("cannot parse %T as date", value)
	}
}

// compareDates compares two dates using the given operator
func compareDates(a, b time.Time, operator string) bool {
	switch operator {
	case "==":
		return a.Equal(b)
	case "!=":
		return !a.Equal(b)
	case "<":
		return a.Before(b)
	case "<=":
		return a.Before(b) || a.Equal(b)
	case ">":
		return a.After(b)
	case ">=":
		return a.After(b) || a.Equal(b)
	default:
		return false
	}
}

// parseBool parses a value as a boolean
func parseBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("cannot parse %T as boolean", value)
	}
}
