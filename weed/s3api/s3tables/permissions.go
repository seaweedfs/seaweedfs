package s3tables

import (
	"encoding/json"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// Permission represents a specific action permission
type Permission string

// IAM Policy structures for evaluation
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

type Statement struct {
	Effect    string      `json:"Effect"`    // "Allow" or "Deny"
	Principal interface{} `json:"Principal"` // Can be string, []string, or map
	Action    interface{} `json:"Action"`    // Can be string or []string
	Resource  interface{} `json:"Resource"`  // Can be string or []string
}

// CheckPermissionWithResource checks if a principal has permission to perform an operation on a specific resource
func CheckPermissionWithResource(operation, principal, owner, resourcePolicy, resourceARN string) bool {
	// Deny access if identities are empty
	if principal == "" || owner == "" {
		return false
	}

	// Owner always has permission
	if principal == owner {
		return true
	}

	// If no policy is provided, deny access (default deny)
	if resourcePolicy == "" {
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
		if !matchesPrincipal(stmt.Principal, principal) {
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

		// Statement matches - check effect
		if stmt.Effect == "Allow" {
			hasAllow = true
		} else if stmt.Effect == "Deny" {
			// Explicit deny always wins
			return false
		}
	}

	return hasAllow
}

// CheckPermission checks if a principal has permission to perform an operation
// (without resource-specific validation - for backward compatibility)
func CheckPermission(operation, principal, owner, resourcePolicy string) bool {
	// Deny access if identities are empty
	if principal == "" || owner == "" {
		return false
	}

	// Owner always has permission
	if principal == owner {
		return true
	}

	// If no policy is provided, deny access (default deny)
	if resourcePolicy == "" {
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
		if !matchesPrincipal(stmt.Principal, principal) {
			continue
		}

		// Check if action matches (using normalized full action name)
		if !matchesAction(stmt.Action, fullAction) {
			continue
		}

		// Statement matches - check effect
		if stmt.Effect == "Allow" {
			hasAllow = true
		} else if stmt.Effect == "Deny" {
			// Explicit deny always wins
			return false
		}
	}

	return hasAllow
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
		return policy_engine.MatchesWildcard(p, principal)
	case []interface{}:
		// Array of principals
		for _, item := range p {
			if str, ok := item.(string); ok {
				if str == "*" || str == principal {
					return true
				}
				// Support wildcard matching
				if policy_engine.MatchesWildcard(str, principal) {
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
	return policy_engine.MatchesWildcard(pattern, action)
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
	return policy_engine.MatchesWildcard(pattern, resourceARN)
}

// Helper functions for specific permissions

// CanCreateTableBucket checks if principal can create table buckets
func CanCreateTableBucket(principal, owner, resourcePolicy string) bool {
	return CheckPermission("CreateTableBucket", principal, owner, resourcePolicy)
}

// CanGetTableBucket checks if principal can get table bucket details
func CanGetTableBucket(principal, owner, resourcePolicy string) bool {
	return CheckPermission("GetTableBucket", principal, owner, resourcePolicy)
}

// CanListTableBuckets checks if principal can list table buckets
func CanListTableBuckets(principal, owner, resourcePolicy string) bool {
	return CheckPermission("ListTableBuckets", principal, owner, resourcePolicy)
}

// CanDeleteTableBucket checks if principal can delete table buckets
func CanDeleteTableBucket(principal, owner, resourcePolicy string) bool {
	return CheckPermission("DeleteTableBucket", principal, owner, resourcePolicy)
}

// CanPutTableBucketPolicy checks if principal can put table bucket policies
func CanPutTableBucketPolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("PutTableBucketPolicy", principal, owner, resourcePolicy)
}

// CanGetTableBucketPolicy checks if principal can get table bucket policies
func CanGetTableBucketPolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("GetTableBucketPolicy", principal, owner, resourcePolicy)
}

// CanDeleteTableBucketPolicy checks if principal can delete table bucket policies
func CanDeleteTableBucketPolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("DeleteTableBucketPolicy", principal, owner, resourcePolicy)
}

// CanCreateNamespace checks if principal can create namespaces
func CanCreateNamespace(principal, owner, resourcePolicy string) bool {
	return CheckPermission("CreateNamespace", principal, owner, resourcePolicy)
}

// CanGetNamespace checks if principal can get namespace details
func CanGetNamespace(principal, owner, resourcePolicy string) bool {
	return CheckPermission("GetNamespace", principal, owner, resourcePolicy)
}

// CanListNamespaces checks if principal can list namespaces
func CanListNamespaces(principal, owner, resourcePolicy string) bool {
	return CheckPermission("ListNamespaces", principal, owner, resourcePolicy)
}

// CanDeleteNamespace checks if principal can delete namespaces
func CanDeleteNamespace(principal, owner, resourcePolicy string) bool {
	return CheckPermission("DeleteNamespace", principal, owner, resourcePolicy)
}

// CanCreateTable checks if principal can create tables
func CanCreateTable(principal, owner, resourcePolicy string) bool {
	return CheckPermission("CreateTable", principal, owner, resourcePolicy)
}

// CanGetTable checks if principal can get table details
func CanGetTable(principal, owner, resourcePolicy string) bool {
	return CheckPermission("GetTable", principal, owner, resourcePolicy)
}

// CanListTables checks if principal can list tables
func CanListTables(principal, owner, resourcePolicy string) bool {
	return CheckPermission("ListTables", principal, owner, resourcePolicy)
}

// CanDeleteTable checks if principal can delete tables
func CanDeleteTable(principal, owner, resourcePolicy string) bool {
	return CheckPermission("DeleteTable", principal, owner, resourcePolicy)
}

// CanPutTablePolicy checks if principal can put table policies
func CanPutTablePolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("PutTablePolicy", principal, owner, resourcePolicy)
}

// CanGetTablePolicy checks if principal can get table policies
func CanGetTablePolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("GetTablePolicy", principal, owner, resourcePolicy)
}

// CanDeleteTablePolicy checks if principal can delete table policies
func CanDeleteTablePolicy(principal, owner, resourcePolicy string) bool {
	return CheckPermission("DeleteTablePolicy", principal, owner, resourcePolicy)
}

// CanTagResource checks if principal can tag a resource
func CanTagResource(principal, owner, resourcePolicy string) bool {
	return CheckPermission("TagResource", principal, owner, resourcePolicy)
}

// CanUntagResource checks if principal can untag a resource
func CanUntagResource(principal, owner, resourcePolicy string) bool {
	return CheckPermission("UntagResource", principal, owner, resourcePolicy)
}

// CanManageTags checks if principal can manage tags (tag or untag)
func CanManageTags(principal, owner, resourcePolicy string) bool {
	return CanTagResource(principal, owner, resourcePolicy) || CanUntagResource(principal, owner, resourcePolicy)
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
