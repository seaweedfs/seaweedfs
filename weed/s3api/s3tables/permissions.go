package s3tables

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// Permission represents a specific action permission
type Permission string

// IAM Policy structures for evaluation
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// UnmarshalJSON handles both single statement object and array of statements
// AWS allows {"Statement": {...}} or {"Statement": [{...}]}
func (pd *PolicyDocument) UnmarshalJSON(data []byte) error {
	type Alias PolicyDocument
	aux := &struct {
		Statement interface{} `json:"Statement"`
		*Alias
	}{
		Alias: (*Alias)(pd),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Handle Statement as either a single object or array
	switch s := aux.Statement.(type) {
	case map[string]interface{}:
		// Single statement object - unmarshal to one Statement
		stmtData, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("failed to marshal single statement: %w", err)
		}
		var stmt Statement
		if err := json.Unmarshal(stmtData, &stmt); err != nil {
			return fmt.Errorf("failed to unmarshal single statement: %w", err)
		}
		pd.Statement = []Statement{stmt}
	case []interface{}:
		// Array of statements - normal handling
		stmtData, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("failed to marshal statement array: %w", err)
		}
		if err := json.Unmarshal(stmtData, &pd.Statement); err != nil {
			return fmt.Errorf("failed to unmarshal statement array: %w", err)
		}
	case nil:
		// No statements
		pd.Statement = []Statement{}
	default:
		return fmt.Errorf("Statement must be an object or array, got %T", aux.Statement)
	}

	return nil
}

type Statement struct {
	Effect    string                            `json:"Effect"`    // "Allow" or "Deny"
	Principal interface{}                       `json:"Principal"` // Can be string, []string, or map
	Action    interface{}                       `json:"Action"`    // Can be string or []string
	Resource  interface{}                       `json:"Resource"`  // Can be string or []string
	Condition map[string]map[string]interface{} `json:"Condition,omitempty"`
}

type PolicyContext struct {
	Namespace       string
	TableName       string
	TableBucketName string
	IdentityActions []string
	RequestTags     map[string]string
	ResourceTags    map[string]string
	TableBucketTags map[string]string
	TagKeys         []string
	SSEAlgorithm    string
	KMSKeyArn       string
	StorageClass    string
	DefaultAllow    bool
	// TrustedIssuers lists the OIDC issuers the deployment is configured to
	// trust, used to re-anchor owners/principals written before issuer-scoped
	// account IDs. A bare subject is only resolvable when exactly one issuer
	// is configured; more than one leaves it ambiguous, so it must not match.
	TrustedIssuers []string
	// IsLocalAccountID reports whether an account ID belongs to a configured
	// local identity. Such owner strings were never OIDC subjects and must not
	// be re-anchored; a nil predicate exempts nothing beyond the built-in
	// reserved, ARN, and scoped-id checks.
	IsLocalAccountID func(string) bool
}

// CheckPermissionWithContext checks permission with optional resource and condition context.
func CheckPermissionWithContext(operation, principal, owner, resourcePolicy, resourceARN string, ctx *PolicyContext) bool {
	// Deny access if identities are empty
	if principal == "" || owner == "" {
		return false
	}

	// Admin always has permission.
	if principal == s3_constants.AccountAdminId {
		return true
	}

	glog.V(2).Infof("S3Tables: CheckPermission operation=%s principal=%s owner=%s", operation, principal, owner)

	return checkPermission(operation, principal, owner, resourcePolicy, resourceARN, ctx)
}

// CheckTableOrBucketPermission combines a table or namespace policy with its
// table bucket's policy into one decision: a matching Deny on either policy
// vetoes the request even when the other policy would allow it.
func CheckTableOrBucketPermission(operation, principal, resourceOwner, resourcePolicy, resourceARN, bucketOwner, bucketPolicy, bucketARN string, ctx *PolicyContext) bool {
	if principal == "" {
		return false
	}
	if principal == s3_constants.AccountAdminId {
		return true
	}
	if matchesDenyStatement(operation, principal, resourcePolicy, resourceARN, ctx) ||
		matchesDenyStatement(operation, principal, bucketPolicy, bucketARN, ctx) {
		return false
	}
	// Each side fails closed when its owner is unknown, matching
	// CheckPermissionWithContext; an ownerless side contributes nothing.
	resourceOK := resourceOwner != "" &&
		checkPermission(operation, principal, resourceOwner, resourcePolicy, resourceARN, ctx)
	bucketOK := bucketOwner != "" &&
		checkPermission(operation, principal, bucketOwner, bucketPolicy, bucketARN, ctx)
	return resourceOK || bucketOK
}

// deniedByPolicy reports whether another applicable resource policy vetoes the
// request. The admin account is exempt, matching CheckPermissionWithContext.
func deniedByPolicy(operation, principal, resourcePolicy, resourceARN string, ctx *PolicyContext) bool {
	return principal != s3_constants.AccountAdminId &&
		matchesDenyStatement(operation, principal, resourcePolicy, resourceARN, ctx)
}

// matchesDenyStatement reports whether the JSON policy carries a statement
// matching this request with Effect Deny. Malformed policies return false;
// the caller's normal evaluation still denies them.
func matchesDenyStatement(operation, principal, resourcePolicy, resourceARN string, ctx *PolicyContext) bool {
	if resourcePolicy == "" {
		return false
	}
	fullAction := operation
	if !strings.Contains(operation, ":") {
		fullAction = "s3tables:" + operation
	}
	var policy PolicyDocument
	if err := json.Unmarshal([]byte(resourcePolicy), &policy); err != nil {
		return false
	}
	for _, stmt := range policy.Statement {
		if stmt.Effect != "Deny" ||
			!matchesCaller(stmt.Principal, principal, ctx) ||
			!matchesAction(stmt.Action, fullAction) ||
			(resourceARN != "" && !matchesResource(stmt.Resource, resourceARN)) ||
			!matchesConditions(stmt.Condition, ctx) {
			continue
		}
		return true
	}
	return false
}

func checkPermission(operation, principal, owner, resourcePolicy, resourceARN string, ctx *PolicyContext) bool {

	// An explicit Deny in the resource policy vetoes owner and identity
	// grants as well as allows within the same document.
	if matchesDenyStatement(operation, principal, resourcePolicy, resourceARN, ctx) {
		return false
	}

	// Owner always has permission.
	if principal == owner || matchesLegacySubject(owner, principal, ctx) {
		return true
	}

	if hasIdentityPermission(operation, ctx) {
		return true
	}

	// If no policy is provided, use default allow if enabled
	if resourcePolicy == "" {
		if ctx != nil && ctx.DefaultAllow {
			return true
		}
		return false
	}

	// Normalize operation to full IAM-style action name (e.g., "s3tables:CreateTableBucket")
	// if not already prefixed
	fullAction := operation
	if !strings.Contains(operation, ":") {
		fullAction = "s3tables:" + operation
	}

	// Parse and evaluate policy
	var policy PolicyDocument
	if err := json.Unmarshal([]byte(resourcePolicy), &policy); err != nil {
		return false
	}

	// Evaluate policy statements
	// Default is deny, so we need an explicit allow
	hasAllow := false

	for _, stmt := range policy.Statement {
		// Check if principal matches
		if !matchesCaller(stmt.Principal, principal, ctx) {
			continue
		}

		// Check if action matches (using normalized full action name)
		if !matchesAction(stmt.Action, fullAction) {
			continue
		}

		// Check if resource matches (if resourceARN specified and Resource field exists)
		if resourceARN != "" && !matchesResource(stmt.Resource, resourceARN) {
			continue
		}

		if !matchesConditions(stmt.Condition, ctx) {
			continue
		}

		// Statement matches - check effect
		switch stmt.Effect {
		case "Allow":
			hasAllow = true
		case "Deny":
			// Explicit deny always wins
			return false
		}
	}

	if hasAllow {
		return true
	}

	// If no statement matched, use default allow if enabled
	if ctx != nil && ctx.DefaultAllow {
		return true
	}

	return false
}

func hasIdentityPermission(operation string, ctx *PolicyContext) bool {
	if ctx == nil || len(ctx.IdentityActions) == 0 {
		return false
	}
	fullAction := operation
	if !strings.Contains(operation, ":") {
		fullAction = "s3tables:" + operation
	}
	if hasAdminAction(ctx.IdentityActions) {
		return true
	}
	candidates := []string{operation, fullAction}
	if ctx.TableBucketName != "" {
		candidates = append(candidates, operation+":"+ctx.TableBucketName, fullAction+":"+ctx.TableBucketName)
	}
	for _, action := range ctx.IdentityActions {
		for _, candidate := range candidates {
			if action == candidate {
				return true
			}
			if strings.ContainsAny(action, "*?") && wildcard.MatchesWildcard(action, candidate) {
				return true
			}
		}
	}
	return false
}

// hasAdminAction reports whether the action list grants blanket admin access.
// Legacy static identities may use broad markers or s3 wildcards; s3:* is treated
// as s3tables:* so shared admin policies still permit table access.
func hasAdminAction(actions []string) bool {
	for _, action := range actions {
		if action == "*" || action == s3_constants.ACTION_ADMIN || action == "s3:*" || action == "s3tables:*" {
			return true
		}
	}
	return false
}

// matchesCaller matches a statement's Principal against the caller's scoped
// principal or, for policies written before issuer scoping, a bare subject
// that resolves to it under the deployment's single trusted issuer.
func matchesCaller(principalSpec interface{}, principal string, ctx *PolicyContext) bool {
	if matchesPrincipal(principalSpec, principal) {
		return true
	}
	switch p := principalSpec.(type) {
	case string:
		return matchesLegacySubject(p, principal, ctx)
	case []interface{}:
		for _, item := range p {
			if str, ok := item.(string); ok && matchesLegacySubject(str, principal, ctx) {
				return true
			}
		}
	case map[string]interface{}:
		if aws, ok := p["AWS"]; ok {
			return matchesCaller(aws, principal, ctx)
		}
	}
	return false
}

// matchesLegacySubject reports whether a bare subject stored before issuer
// scoping belongs to the caller. Re-anchoring is only sound when exactly one
// OIDC issuer is configured: the stored subject is hashed under that issuer
// and compared to the caller's scoped principal. With multiple issuers a bare
// subject is ambiguous — matching it under the caller's own issuer would let
// a same-sub identity from any configured issuer claim it.
func matchesLegacySubject(legacy, principal string, ctx *PolicyContext) bool {
	if ctx == nil || len(ctx.TrustedIssuers) != 1 {
		return false
	}
	// The reserved accounts, ARN values, and already issuer-scoped ids are never
	// OIDC subjects — or, for strings that could be either a legacy subject or a
	// scoped id, resolving to scoped fails closed. A configured local account
	// id is excluded the same way once the caller supplies the predicate. Any
	// of them could otherwise be claimed by a token asserting that string as
	// sub — including the admin account or a victim's scoped id.
	if legacy == s3_constants.AccountAdminId || legacy == s3_constants.AccountAnonymousId ||
		strings.HasPrefix(legacy, "arn:") || sts.IsParentUserID(legacy) ||
		(ctx.IsLocalAccountID != nil && ctx.IsLocalAccountID(legacy)) {
		return false
	}
	return sts.ComputeParentUser(legacy, ctx.TrustedIssuers[0]) == principal
}

// matchesPrincipal checks if the principal matches the statement's principal
func matchesPrincipal(principalSpec interface{}, principal string) bool {
	if principalSpec == nil {
		return false
	}

	switch p := principalSpec.(type) {
	case string:
		// Direct string match or wildcard
		if p == "*" || p == principal {
			return true
		}
		// Support wildcard matching for principals (e.g., "arn:aws:iam::*:user/admin")
		return wildcard.MatchesWildcard(p, principal)
	case []interface{}:
		// Array of principals
		for _, item := range p {
			if str, ok := item.(string); ok {
				if str == "*" || str == principal {
					return true
				}
				// Support wildcard matching
				if wildcard.MatchesWildcard(str, principal) {
					return true
				}
			}
		}
	case map[string]interface{}:
		// AWS-style principal with service prefix, e.g., {"AWS": "arn:aws:iam::..."}
		// For S3 Tables, we primarily care about the AWS key
		if aws, ok := p["AWS"]; ok {
			return matchesPrincipal(aws, principal)
		}
	}

	return false
}

// matchesAction checks if the action matches the statement's action
func matchesAction(actionSpec interface{}, action string) bool {
	if actionSpec == nil {
		return false
	}

	switch a := actionSpec.(type) {
	case string:
		// Direct match or wildcard
		return matchesActionPattern(a, action)
	case []interface{}:
		// Array of actions
		for _, item := range a {
			if str, ok := item.(string); ok {
				if matchesActionPattern(str, action) {
					return true
				}
			}
		}
	}

	return false
}

// matchesActionPattern checks if an action matches a pattern (supports wildcards)
// This uses the policy_engine.MatchesWildcard function for full wildcard support,
// including middle wildcards (e.g., "s3tables:Get*Table") for complete IAM compatibility.
func matchesActionPattern(pattern, action string) bool {
	if pattern == "*" {
		return true
	}

	// Exact match
	if pattern == action {
		return true
	}

	// Wildcard match using policy engine's wildcard matcher
	// Supports both * (any sequence) and ? (single character) anywhere in the pattern
	return wildcard.MatchesWildcard(pattern, action)
}

func matchesConditions(conditions map[string]map[string]interface{}, ctx *PolicyContext) bool {
	if len(conditions) == 0 {
		return true
	}
	if ctx == nil {
		return false
	}
	for operator, conditionValues := range conditions {
		if !matchesConditionOperator(operator, conditionValues, ctx) {
			return false
		}
	}
	return true
}

func matchesConditionOperator(operator string, conditionValues map[string]interface{}, ctx *PolicyContext) bool {
	evaluator, err := policy_engine.GetConditionEvaluator(operator)
	if err != nil {
		return false
	}

	for key, value := range conditionValues {
		contextVals := getConditionContextValues(key, ctx)
		if !evaluator.Evaluate(value, contextVals) {
			return false
		}
	}
	return true
}

func getConditionContextValues(key string, ctx *PolicyContext) []string {
	switch key {
	case "s3tables:namespace":
		return []string{ctx.Namespace}
	case "s3tables:tableName":
		return []string{ctx.TableName}
	case "s3tables:tableBucketName":
		return []string{ctx.TableBucketName}
	case "s3tables:SSEAlgorithm":
		return []string{ctx.SSEAlgorithm}
	case "s3tables:KMSKeyArn":
		return []string{ctx.KMSKeyArn}
	case "s3tables:StorageClass":
		return []string{ctx.StorageClass}
	case "aws:TagKeys":
		return ctx.TagKeys
	}
	if strings.HasPrefix(key, "aws:RequestTag/") {
		tagKey := strings.TrimPrefix(key, "aws:RequestTag/")
		if val, ok := ctx.RequestTags[tagKey]; ok {
			return []string{val}
		}
	}
	if strings.HasPrefix(key, "aws:ResourceTag/") {
		tagKey := strings.TrimPrefix(key, "aws:ResourceTag/")
		if val, ok := ctx.ResourceTags[tagKey]; ok {
			return []string{val}
		}
	}
	if strings.HasPrefix(key, "s3tables:TableBucketTag/") {
		tagKey := strings.TrimPrefix(key, "s3tables:TableBucketTag/")
		if val, ok := ctx.TableBucketTags[tagKey]; ok {
			return []string{val}
		}
	}
	return nil
}

// matchesResource checks if the resource ARN matches the statement's resource specification
// Returns true if resource matches or if Resource is not specified (implicit match)
func matchesResource(resourceSpec interface{}, resourceARN string) bool {
	// If no Resource is specified, match all resources (implicit *)
	if resourceSpec == nil {
		return true
	}

	switch r := resourceSpec.(type) {
	case string:
		// Direct match or wildcard
		return matchesResourcePattern(r, resourceARN)
	case []interface{}:
		// Array of resources - match if any matches
		for _, item := range r {
			if str, ok := item.(string); ok {
				if matchesResourcePattern(str, resourceARN) {
					return true
				}
			}
		}
	}

	return false
}

// matchesResourcePattern checks if a resource ARN matches a pattern (supports wildcards)
func matchesResourcePattern(pattern, resourceARN string) bool {
	if pattern == "*" {
		return true
	}

	// Exact match
	if pattern == resourceARN {
		return true
	}

	// Wildcard match using policy engine's wildcard matcher
	return wildcard.MatchesWildcard(pattern, resourceARN)
}

// AuthError represents an authorization error
type AuthError struct {
	Operation string
	Principal string
	Message   string
}

func (e *AuthError) Error() string {
	return "unauthorized: " + e.Principal + " is not permitted to perform " + e.Operation + ": " + e.Message
}

// NewAuthError creates a new authorization error
func NewAuthError(operation, principal, message string) *AuthError {
	return &AuthError{
		Operation: operation,
		Principal: principal,
		Message:   message,
	}
}
