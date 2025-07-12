package policy_engine

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// This file provides compatibility functions to convert between different policy type definitions
// to address the duplicate PolicyDocument types in the codebase.

// IamApiStatement represents the simpler Statement type from iamapi package
type IamApiStatement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

// IamApiPolicyDocument represents the simpler PolicyDocument type from iamapi package
type IamApiPolicyDocument struct {
	Version   string             `json:"Version"`
	Statement []*IamApiStatement `json:"Statement"`
}

// ConvertFromIamApiPolicyDocument converts an iamapi-style policy to our enhanced PolicyDocument
func ConvertFromIamApiPolicyDocument(iamPolicy IamApiPolicyDocument) (*PolicyDocument, error) {
	// Convert to our enhanced type
	enhanced := &PolicyDocument{
		Version:   iamPolicy.Version,
		Statement: make([]PolicyStatement, len(iamPolicy.Statement)),
	}

	for i, stmt := range iamPolicy.Statement {
		effect := PolicyEffectAllow
		if stmt.Effect == "Deny" {
			effect = PolicyEffectDeny
		}

		enhanced.Statement[i] = PolicyStatement{
			Effect:   effect,
			Action:   NewStringOrStringSlice(stmt.Action...),
			Resource: NewStringOrStringSlice(stmt.Resource...),
			// Principal and Condition are nil/empty for iamapi conversions
		}
	}

	return enhanced, nil
}

// ConvertToIamApiPolicyDocument converts our enhanced PolicyDocument to iamapi.PolicyDocument
func ConvertToIamApiPolicyDocument(policy *PolicyDocument) (*IamApiPolicyDocument, error) {
	simple := &IamApiPolicyDocument{
		Version:   policy.Version,
		Statement: make([]*IamApiStatement, len(policy.Statement)),
	}

	for i, stmt := range policy.Statement {
		// Warn if we're losing information in the conversion
		if stmt.Principal != nil {
			glog.Warningf("Losing Principal information when converting to iamapi format")
		}
		if len(stmt.Condition) > 0 {
			glog.Warningf("Losing Condition information when converting to iamapi format")
		}

		simple.Statement[i] = &IamApiStatement{
			Effect:   string(stmt.Effect),
			Action:   stmt.Action.Strings(),
			Resource: stmt.Resource.Strings(),
		}
	}

	return simple, nil
}

// IsCompatibleWithIamApi checks if a PolicyDocument can be safely converted to iamapi format
// without losing information
func IsCompatibleWithIamApi(policy *PolicyDocument) bool {
	for _, stmt := range policy.Statement {
		// Check for features not supported by iamapi
		if stmt.Principal != nil && len(stmt.Principal.Strings()) > 0 {
			return false
		}
		if len(stmt.Condition) > 0 {
			return false
		}
		if stmt.Sid != "" {
			return false // iamapi doesn't support Sid
		}
	}
	return true
}

// CreateIamApiCompatiblePolicy creates a policy document that's compatible with both systems
func CreateIamApiCompatiblePolicy(version string, statements []SimpleStatement) *PolicyDocument {
	policy := &PolicyDocument{
		Version:   version,
		Statement: make([]PolicyStatement, len(statements)),
	}

	for i, stmt := range statements {
		effect := PolicyEffectAllow
		if stmt.Effect == "Deny" {
			effect = PolicyEffectDeny
		}

		policy.Statement[i] = PolicyStatement{
			Effect:   effect,
			Action:   NewStringOrStringSlice(stmt.Action...),
			Resource: NewStringOrStringSlice(stmt.Resource...),
			// No Principal, Condition, or Sid for compatibility
		}
	}

	return policy
}

// SimpleStatement represents a basic policy statement compatible with both systems
type SimpleStatement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

// ConvertToSimpleStatements extracts simple statements from an enhanced policy
func ConvertToSimpleStatements(policy *PolicyDocument) []SimpleStatement {
	statements := make([]SimpleStatement, len(policy.Statement))

	for i, stmt := range policy.Statement {
		statements[i] = SimpleStatement{
			Effect:   string(stmt.Effect),
			Action:   stmt.Action.Strings(),
			Resource: stmt.Resource.Strings(),
		}
	}

	return statements
}

// MergeIamApiPolicies merges multiple iamapi-style policies into one enhanced policy
func MergeIamApiPolicies(policies ...IamApiPolicyDocument) (*PolicyDocument, error) {
	if len(policies) == 0 {
		return nil, fmt.Errorf("no policies to merge")
	}

	merged := &PolicyDocument{
		Version:   policies[0].Version,
		Statement: make([]PolicyStatement, 0),
	}

	for _, policy := range policies {
		enhanced, err := ConvertFromIamApiPolicyDocument(policy)
		if err != nil {
			return nil, fmt.Errorf("failed to convert policy: %v", err)
		}
		merged.Statement = append(merged.Statement, enhanced.Statement...)
	}

	return merged, nil
}
