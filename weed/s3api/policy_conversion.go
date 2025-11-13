package s3api

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// ConvertPolicyDocumentToPolicyEngine converts a policy.PolicyDocument to policy_engine.PolicyDocument
// This function provides efficient type conversion without JSON marshaling overhead.
// It handles the differences between the two types:
// - Converts []string fields to StringOrStringSlice
// - Maps Condition types
// - Handles optional fields (Id, NotPrincipal, NotAction, NotResource are ignored in policy_engine)
func ConvertPolicyDocumentToPolicyEngine(src *policy.PolicyDocument) *policy_engine.PolicyDocument {
	if src == nil {
		return nil
	}

	dest := &policy_engine.PolicyDocument{
		Version:   src.Version,
		Statement: make([]policy_engine.PolicyStatement, len(src.Statement)),
	}

	for i := range src.Statement {
		dest.Statement[i] = convertStatement(&src.Statement[i])
	}

	return dest
}

// convertStatement converts a policy.Statement to policy_engine.PolicyStatement
func convertStatement(src *policy.Statement) policy_engine.PolicyStatement {
	stmt := policy_engine.PolicyStatement{
		Sid:    src.Sid,
		Effect: policy_engine.PolicyEffect(src.Effect),
	}

	// Convert Action ([]string to StringOrStringSlice)
	if len(src.Action) > 0 {
		stmt.Action = policy_engine.NewStringOrStringSlice(src.Action...)
	}

	// Convert Resource ([]string to StringOrStringSlice)
	if len(src.Resource) > 0 {
		stmt.Resource = policy_engine.NewStringOrStringSlice(src.Resource...)
	}

	// Convert Principal (interface{} to *StringOrStringSlice)
	if src.Principal != nil {
		stmt.Principal = convertPrincipal(src.Principal)
	}

	// Convert Condition (map[string]map[string]interface{} to PolicyConditions)
	if len(src.Condition) > 0 {
		stmt.Condition = convertCondition(src.Condition)
	}

	return stmt
}

// convertPrincipal converts a Principal field to *StringOrStringSlice
func convertPrincipal(principal interface{}) *policy_engine.StringOrStringSlice {
	if principal == nil {
		return nil
	}

	var strs []string
	processed := true

	switch p := principal.(type) {
	case string:
		strs = []string{p}
	case []string:
		strs = p
	case []interface{}:
		// Convert []interface{} to []string
		strs = make([]string, 0, len(p))
		for _, v := range p {
			if v != nil {
				strs = append(strs, convertToString(v))
			}
		}
	case map[string]interface{}:
		// Handle AWS-style principal with service/user keys
		// Example: {"AWS": "arn:aws:iam::123456789012:user/Alice"}
		for _, v := range p {
			switch val := v.(type) {
			case string:
				strs = append(strs, val)
			case []string:
				strs = append(strs, val...)
			case []interface{}:
				for _, item := range val {
					if item != nil {
						strs = append(strs, convertToString(item))
					}
				}
			}
		}
	default:
		processed = false
	}

	if processed && len(strs) > 0 {
		result := policy_engine.NewStringOrStringSlice(strs...)
		return &result
	}

	return nil
}

// convertCondition converts policy conditions to PolicyConditions
func convertCondition(src map[string]map[string]interface{}) policy_engine.PolicyConditions {
	if len(src) == 0 {
		return nil
	}

	dest := make(policy_engine.PolicyConditions)
	for condType, condBlock := range src {
		destBlock := make(map[string]policy_engine.StringOrStringSlice)
		for key, value := range condBlock {
			destBlock[key] = convertConditionValue(value)
		}
		dest[condType] = destBlock
	}

	return dest
}

// convertConditionValue converts a condition value to StringOrStringSlice
func convertConditionValue(value interface{}) policy_engine.StringOrStringSlice {
	switch v := value.(type) {
	case string:
		return policy_engine.NewStringOrStringSlice(v)
	case []string:
		return policy_engine.NewStringOrStringSlice(v...)
	case []interface{}:
		strs := make([]string, 0, len(v))
		for _, item := range v {
			if item != nil {
				strs = append(strs, convertToString(item))
			}
		}
		return policy_engine.NewStringOrStringSlice(strs...)
	default:
		// For non-string types, convert to string
		// This handles numbers, booleans, etc.
		return policy_engine.NewStringOrStringSlice(convertToString(v))
	}
}

// convertToString converts any value to string representation
func convertToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		// Use fmt.Sprint for supported primitive types
		return fmt.Sprint(v)
	default:
		return ""
	}
}

