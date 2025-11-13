package s3api

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// ConvertPolicyDocumentToPolicyEngine converts a policy.PolicyDocument to policy_engine.PolicyDocument
// This function provides type-safe conversion with explicit field mapping and error handling.
// It handles the differences between the two types:
// - Converts []string fields to StringOrStringSlice
// - Maps Condition types with type validation
// - Converts Principal fields with support for AWS principals only
// - Handles optional fields (Id, NotPrincipal, NotAction, NotResource are ignored in policy_engine)
//
// Returns an error if the policy contains unsupported types or malformed data.
func ConvertPolicyDocumentToPolicyEngine(src *policy.PolicyDocument) (*policy_engine.PolicyDocument, error) {
	if src == nil {
		return nil, nil
	}

	// Warn if the policy document Id is being dropped
	if src.Id != "" {
		glog.Warningf("policy document Id %q is not supported and will be ignored", src.Id)
	}

	dest := &policy_engine.PolicyDocument{
		Version:   src.Version,
		Statement: make([]policy_engine.PolicyStatement, len(src.Statement)),
	}

	for i := range src.Statement {
		stmt, err := convertStatement(&src.Statement[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert statement %d: %w", i, err)
		}
		dest.Statement[i] = stmt
	}

	return dest, nil
}

// convertStatement converts a policy.Statement to policy_engine.PolicyStatement
func convertStatement(src *policy.Statement) (policy_engine.PolicyStatement, error) {
	// Check for unsupported fields that would fundamentally change policy semantics
	// These fields invert the logic and ignoring them could create security holes
	if len(src.NotAction) > 0 {
		return policy_engine.PolicyStatement{}, fmt.Errorf("statement %q: NotAction is not supported (would invert action logic, creating potential security risk)", src.Sid)
	}
	if len(src.NotResource) > 0 {
		return policy_engine.PolicyStatement{}, fmt.Errorf("statement %q: NotResource is not supported (would invert resource logic, creating potential security risk)", src.Sid)
	}
	if src.NotPrincipal != nil {
		return policy_engine.PolicyStatement{}, fmt.Errorf("statement %q: NotPrincipal is not supported (would invert principal logic, creating potential security risk)", src.Sid)
	}

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
		principal, err := convertPrincipal(src.Principal)
		if err != nil {
			return policy_engine.PolicyStatement{}, fmt.Errorf("failed to convert principal: %w", err)
		}
		stmt.Principal = principal
	}

	// Convert Condition (map[string]map[string]interface{} to PolicyConditions)
	if len(src.Condition) > 0 {
		condition, err := convertCondition(src.Condition)
		if err != nil {
			return policy_engine.PolicyStatement{}, fmt.Errorf("failed to convert condition: %w", err)
		}
		stmt.Condition = condition
	}

	return stmt, nil
}

// convertPrincipal converts a Principal field to *StringOrStringSlice
func convertPrincipal(principal interface{}) (*policy_engine.StringOrStringSlice, error) {
	if principal == nil {
		return nil, nil
	}

	switch p := principal.(type) {
	case string:
		if p == "" {
			return nil, fmt.Errorf("principal string cannot be empty")
		}
		result := policy_engine.NewStringOrStringSlice(p)
		return &result, nil
	case []string:
		if len(p) == 0 {
			return nil, nil
		}
		for _, s := range p {
			if s == "" {
				return nil, fmt.Errorf("principal string in slice cannot be empty")
			}
		}
		result := policy_engine.NewStringOrStringSlice(p...)
		return &result, nil
	case []interface{}:
		strs := make([]string, 0, len(p))
		for _, v := range p {
			if v != nil {
				str, err := convertToString(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert principal array item: %w", err)
				}
				if str == "" {
					return nil, fmt.Errorf("principal string in slice cannot be empty")
				}
				strs = append(strs, str)
			}
		}
		if len(strs) == 0 {
			return nil, nil
		}
		result := policy_engine.NewStringOrStringSlice(strs...)
		return &result, nil
	case map[string]interface{}:
		// Handle AWS-style principal with service/user keys
		// Example: {"AWS": "arn:aws:iam::123456789012:user/Alice"}
		// Only AWS principals are supported for now. Other types like Service or Federated need special handling.
		
		awsPrincipals, ok := p["AWS"]
		if !ok || len(p) != 1 {
			glog.Warningf("unsupported principal map, only a single 'AWS' key is supported: %v", p)
			return nil, fmt.Errorf("unsupported principal map, only a single 'AWS' key is supported, got keys: %v", getMapKeys(p))
		}
		
		// Recursively convert the AWS principal value
		res, err := convertPrincipal(awsPrincipals)
		if err != nil {
			return nil, fmt.Errorf("invalid 'AWS' principal value: %w", err)
		}
		return res, nil
	default:
		return nil, fmt.Errorf("unsupported principal type: %T", p)
	}
}

// convertCondition converts policy conditions to PolicyConditions
func convertCondition(src map[string]map[string]interface{}) (policy_engine.PolicyConditions, error) {
	if len(src) == 0 {
		return nil, nil
	}

	dest := make(policy_engine.PolicyConditions)
	for condType, condBlock := range src {
		destBlock := make(map[string]policy_engine.StringOrStringSlice)
		for key, value := range condBlock {
			condValue, err := convertConditionValue(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert condition %s[%s]: %w", condType, key, err)
			}
			destBlock[key] = condValue
		}
		dest[condType] = destBlock
	}

	return dest, nil
}

// convertConditionValue converts a condition value to StringOrStringSlice
func convertConditionValue(value interface{}) (policy_engine.StringOrStringSlice, error) {
	switch v := value.(type) {
	case string:
		return policy_engine.NewStringOrStringSlice(v), nil
	case []string:
		return policy_engine.NewStringOrStringSlice(v...), nil
	case []interface{}:
		strs := make([]string, 0, len(v))
		for _, item := range v {
			if item != nil {
				str, err := convertToString(item)
				if err != nil {
					return policy_engine.StringOrStringSlice{}, fmt.Errorf("failed to convert condition array item: %w", err)
				}
				strs = append(strs, str)
			}
		}
		return policy_engine.NewStringOrStringSlice(strs...), nil
	default:
		// For non-string types, convert to string
		// This handles numbers, booleans, etc.
		str, err := convertToString(v)
		if err != nil {
			return policy_engine.StringOrStringSlice{}, err
		}
		return policy_engine.NewStringOrStringSlice(str), nil
	}
}

// convertToString converts any value to string representation
// Returns an error for unsupported types to prevent silent data corruption
func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		// Use fmt.Sprint for supported primitive types
		return fmt.Sprint(v), nil
	default:
		glog.Warningf("unsupported type in policy conversion: %T", v)
		return "", fmt.Errorf("unsupported type in policy conversion: %T", v)
	}
}

// getMapKeys returns the keys of a map as a slice (helper for error messages)
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

