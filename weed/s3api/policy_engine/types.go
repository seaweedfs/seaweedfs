package policy_engine

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Policy Engine Types
//
// This package provides enhanced AWS S3-compatible policy types with improved type safety.
//
// MIGRATION COMPLETE:
// This is now the unified PolicyDocument type used throughout the SeaweedFS codebase.
// The previous duplicate PolicyDocument types in iamapi and credential packages have
// been migrated to use these enhanced types, providing:
// - Principal specifications
// - Complex conditions (IP, time, string patterns, etc.)
// - Flexible string/array types with proper JSON marshaling
// - Policy compilation for performance
//
// All policy operations now use this single, consistent type definition.

// StringOrStringSlice represents a value that can be either a string or []string
type StringOrStringSlice struct {
	values []string
}

// UnmarshalJSON implements json.Unmarshaler for StringOrStringSlice
func (s *StringOrStringSlice) UnmarshalJSON(data []byte) error {
	// Try unmarshaling as string first
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		s.values = []string{str}
		return nil
	}

	// Try unmarshaling as []string
	var strs []string
	if err := json.Unmarshal(data, &strs); err == nil {
		s.values = strs
		return nil
	}

	return fmt.Errorf("value must be string or []string")
}

// MarshalJSON implements json.Marshaler for StringOrStringSlice
func (s StringOrStringSlice) MarshalJSON() ([]byte, error) {
	if len(s.values) == 1 {
		return json.Marshal(s.values[0])
	}
	return json.Marshal(s.values)
}

// Strings returns the slice of strings
func (s StringOrStringSlice) Strings() []string {
	return s.values
}

// NewStringOrStringSlice creates a new StringOrStringSlice from strings
func NewStringOrStringSlice(values ...string) StringOrStringSlice {
	return StringOrStringSlice{values: values}
}

// PolicyConditions represents policy conditions with proper typing
type PolicyConditions map[string]map[string]StringOrStringSlice

// PolicyDocument represents an AWS S3 bucket policy document
type PolicyDocument struct {
	Version   string            `json:"Version"`
	Statement []PolicyStatement `json:"Statement"`
}

// PolicyStatement represents a single policy statement
type PolicyStatement struct {
	Sid       string               `json:"Sid,omitempty"`
	Effect    PolicyEffect         `json:"Effect"`
	Principal *StringOrStringSlice `json:"Principal,omitempty"`
	Action    StringOrStringSlice  `json:"Action"`
	Resource  StringOrStringSlice  `json:"Resource"`
	Condition PolicyConditions     `json:"Condition,omitempty"`
}

// PolicyEffect represents Allow or Deny
type PolicyEffect string

const (
	PolicyEffectAllow PolicyEffect = "Allow"
	PolicyEffectDeny  PolicyEffect = "Deny"
)

// PolicyEvaluationArgs contains the arguments for policy evaluation
type PolicyEvaluationArgs struct {
	Action     string
	Resource   string
	Principal  string
	Conditions map[string][]string
}

// PolicyCache for caching compiled policies
type PolicyCache struct {
	policies   map[string]*CompiledPolicy
	lastUpdate time.Time
}

// CompiledPolicy represents a policy that has been compiled for efficient evaluation
type CompiledPolicy struct {
	Document   *PolicyDocument
	Statements []CompiledStatement
}

// CompiledStatement represents a compiled policy statement
type CompiledStatement struct {
	Statement         *PolicyStatement
	ActionPatterns    []*regexp.Regexp
	ResourcePatterns  []*regexp.Regexp
	PrincipalPatterns []*regexp.Regexp
}

// NewPolicyCache creates a new policy cache
func NewPolicyCache() *PolicyCache {
	return &PolicyCache{
		policies: make(map[string]*CompiledPolicy),
	}
}

// ValidatePolicy validates a policy document
func ValidatePolicy(policyDoc *PolicyDocument) error {
	if policyDoc.Version != "2012-10-17" {
		return fmt.Errorf("unsupported policy version: %s", policyDoc.Version)
	}

	if len(policyDoc.Statement) == 0 {
		return fmt.Errorf("policy must contain at least one statement")
	}

	for i, stmt := range policyDoc.Statement {
		if err := validateStatement(&stmt); err != nil {
			return fmt.Errorf("invalid statement %d: %v", i, err)
		}
	}

	return nil
}

// validateStatement validates a single policy statement
func validateStatement(stmt *PolicyStatement) error {
	if stmt.Effect != PolicyEffectAllow && stmt.Effect != PolicyEffectDeny {
		return fmt.Errorf("invalid effect: %s", stmt.Effect)
	}

	if len(stmt.Action.Strings()) == 0 {
		return fmt.Errorf("action is required")
	}

	if len(stmt.Resource.Strings()) == 0 {
		return fmt.Errorf("resource is required")
	}

	return nil
}

// ParsePolicy parses a policy JSON string
func ParsePolicy(policyJSON string) (*PolicyDocument, error) {
	var policy PolicyDocument
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return nil, fmt.Errorf("failed to parse policy JSON: %v", err)
	}

	if err := ValidatePolicy(&policy); err != nil {
		return nil, fmt.Errorf("invalid policy: %v", err)
	}

	return &policy, nil
}

// CompilePolicy compiles a policy for efficient evaluation
func CompilePolicy(policy *PolicyDocument) (*CompiledPolicy, error) {
	compiled := &CompiledPolicy{
		Document:   policy,
		Statements: make([]CompiledStatement, len(policy.Statement)),
	}

	for i, stmt := range policy.Statement {
		compiledStmt, err := compileStatement(&stmt)
		if err != nil {
			return nil, fmt.Errorf("failed to compile statement %d: %v", i, err)
		}
		compiled.Statements[i] = *compiledStmt
	}

	return compiled, nil
}

// compileStatement compiles a single policy statement
func compileStatement(stmt *PolicyStatement) (*CompiledStatement, error) {
	compiled := &CompiledStatement{
		Statement: stmt,
	}

	// Compile action patterns
	for _, action := range stmt.Action.Strings() {
		pattern, err := compilePattern(action)
		if err != nil {
			return nil, fmt.Errorf("failed to compile action pattern %s: %v", action, err)
		}
		compiled.ActionPatterns = append(compiled.ActionPatterns, pattern)
	}

	// Compile resource patterns
	for _, resource := range stmt.Resource.Strings() {
		pattern, err := compilePattern(resource)
		if err != nil {
			return nil, fmt.Errorf("failed to compile resource pattern %s: %v", resource, err)
		}
		compiled.ResourcePatterns = append(compiled.ResourcePatterns, pattern)
	}

	// Compile principal patterns if present
	if stmt.Principal != nil && len(stmt.Principal.Strings()) > 0 {
		for _, principal := range stmt.Principal.Strings() {
			pattern, err := compilePattern(principal)
			if err != nil {
				return nil, fmt.Errorf("failed to compile principal pattern %s: %v", principal, err)
			}
			compiled.PrincipalPatterns = append(compiled.PrincipalPatterns, pattern)
		}
	}

	return compiled, nil
}

// compilePattern compiles a wildcard pattern to regex
func compilePattern(pattern string) (*regexp.Regexp, error) {
	// Escape special regex characters except * and ?
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped wildcards with regex equivalents
	escaped = strings.ReplaceAll(escaped, `\*`, `.*`)
	escaped = strings.ReplaceAll(escaped, `\?`, `.`)

	// Anchor the pattern
	escaped = "^" + escaped + "$"

	return regexp.Compile(escaped)
}

// normalizeToStringSlice converts various types to string slice - kept for backward compatibility
func normalizeToStringSlice(value interface{}) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = fmt.Sprintf("%v", item)
		}
		return result
	case StringOrStringSlice:
		return v.Strings()
	default:
		glog.Warningf("unexpected type for policy value: %T", v)
		return []string{fmt.Sprintf("%v", v)}
	}
}

// GetBucketFromResource extracts bucket name from resource ARN
func GetBucketFromResource(resource string) string {
	// Handle ARN format: arn:aws:s3:::bucket-name/object-path
	if strings.HasPrefix(resource, "arn:aws:s3:::") {
		parts := strings.SplitN(resource[13:], "/", 2)
		return parts[0]
	}
	return ""
}

// IsObjectResource checks if resource refers to objects
func IsObjectResource(resource string) bool {
	return strings.Contains(resource, "/")
}

// MatchesResource checks if a resource matches a pattern
func MatchesResource(pattern, resource string) bool {
	// Simple wildcard matching
	if pattern == "*" {
		return true
	}

	// Convert wildcards to regex
	regexPattern := strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, `.*`)
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, `.`)
	regexPattern = "^" + regexPattern + "$"

	match, err := regexp.MatchString(regexPattern, resource)
	if err != nil {
		glog.Errorf("Error matching resource pattern %s against %s: %v", pattern, resource, err)
		return false
	}

	return match
}

// S3Actions contains common S3 actions
var S3Actions = map[string]string{
	"GetObject":                        "s3:GetObject",
	"PutObject":                        "s3:PutObject",
	"DeleteObject":                     "s3:DeleteObject",
	"GetObjectVersion":                 "s3:GetObjectVersion",
	"DeleteObjectVersion":              "s3:DeleteObjectVersion",
	"ListBucket":                       "s3:ListBucket",
	"ListBucketVersions":               "s3:ListBucketVersions",
	"GetBucketLocation":                "s3:GetBucketLocation",
	"GetBucketVersioning":              "s3:GetBucketVersioning",
	"PutBucketVersioning":              "s3:PutBucketVersioning",
	"GetBucketAcl":                     "s3:GetBucketAcl",
	"PutBucketAcl":                     "s3:PutBucketAcl",
	"GetObjectAcl":                     "s3:GetObjectAcl",
	"PutObjectAcl":                     "s3:PutObjectAcl",
	"GetBucketPolicy":                  "s3:GetBucketPolicy",
	"PutBucketPolicy":                  "s3:PutBucketPolicy",
	"DeleteBucketPolicy":               "s3:DeleteBucketPolicy",
	"GetBucketCors":                    "s3:GetBucketCors",
	"PutBucketCors":                    "s3:PutBucketCors",
	"DeleteBucketCors":                 "s3:DeleteBucketCors",
	"GetBucketNotification":            "s3:GetBucketNotification",
	"PutBucketNotification":            "s3:PutBucketNotification",
	"GetBucketTagging":                 "s3:GetBucketTagging",
	"PutBucketTagging":                 "s3:PutBucketTagging",
	"DeleteBucketTagging":              "s3:DeleteBucketTagging",
	"GetObjectTagging":                 "s3:GetObjectTagging",
	"PutObjectTagging":                 "s3:PutObjectTagging",
	"DeleteObjectTagging":              "s3:DeleteObjectTagging",
	"ListMultipartUploads":             "s3:ListMultipartUploads",
	"AbortMultipartUpload":             "s3:AbortMultipartUpload",
	"ListParts":                        "s3:ListParts",
	"GetObjectRetention":               "s3:GetObjectRetention",
	"PutObjectRetention":               "s3:PutObjectRetention",
	"GetObjectLegalHold":               "s3:GetObjectLegalHold",
	"PutObjectLegalHold":               "s3:PutObjectLegalHold",
	"GetBucketObjectLockConfiguration": "s3:GetBucketObjectLockConfiguration",
	"PutBucketObjectLockConfiguration": "s3:PutBucketObjectLockConfiguration",
}
