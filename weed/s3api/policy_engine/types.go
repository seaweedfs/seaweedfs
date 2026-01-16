package policy_engine

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
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

// Constants for policy validation
const (
	// PolicyVersion2012_10_17 is the standard AWS policy version
	PolicyVersion2012_10_17 = "2012-10-17"
)

var (
	// PolicyVariableRegex detects AWS IAM policy variables like ${aws:username}
	PolicyVariableRegex = regexp.MustCompile(`\$\{([^}]+)\}`)
)

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
	Sid         string               `json:"Sid,omitempty"`
	Effect      PolicyEffect         `json:"Effect"`
	Principal   *StringOrStringSlice `json:"Principal,omitempty"`
	Action      StringOrStringSlice  `json:"Action"`
	Resource    StringOrStringSlice  `json:"Resource,omitempty"`
	NotResource StringOrStringSlice  `json:"NotResource,omitempty"`
	Condition   PolicyConditions     `json:"Condition,omitempty"`
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
	// ObjectEntry is the object's metadata from entry.Extended.
	// Used for evaluating conditions like s3:ExistingObjectTag/<tag-key>.
	// Tags are stored with s3_constants.AmzObjectTaggingPrefix (X-Amz-Tagging-) prefix.
	// Can be nil for bucket-level operations or when object doesn't exist.
	ObjectEntry map[string][]byte
	// Claims are JWT claims for jwt:* policy variables (can be nil)
	Claims map[string]interface{}
}

// PolicyCache for caching compiled policies
type PolicyCache struct {
	policies   map[string]*CompiledPolicy
	lastUpdate time.Time
}

// CompiledPolicy represents a policy that has been compiled for efficient evaluation
type CompiledPolicy struct {
	Document   *PolicyDocument
	Statements []*CompiledStatement
}

// CompiledStatement represents a compiled policy statement
type CompiledStatement struct {
	Statement         *PolicyStatement
	ActionMatchers    []*WildcardMatcher
	ResourceMatchers  []*WildcardMatcher
	PrincipalMatchers []*WildcardMatcher
	// Keep regex patterns for backward compatibility
	ActionPatterns    []*regexp.Regexp
	ResourcePatterns  []*regexp.Regexp
	PrincipalPatterns []*regexp.Regexp

	// dynamic patterns that require variable substitution before matching
	DynamicActionPatterns    []string
	DynamicResourcePatterns  []string
	DynamicPrincipalPatterns []string

	// NotResource patterns (resource should NOT match these)
	NotResourcePatterns        []*regexp.Regexp
	NotResourceMatchers        []*WildcardMatcher
	DynamicNotResourcePatterns []string
}

// NewPolicyCache creates a new policy cache
func NewPolicyCache() *PolicyCache {
	return &PolicyCache{
		policies: make(map[string]*CompiledPolicy),
	}
}

// ValidatePolicy validates a policy document
func ValidatePolicy(policyDoc *PolicyDocument) error {
	if policyDoc.Version != PolicyVersion2012_10_17 {
		return fmt.Errorf("unsupported policy version: %s", policyDoc.Version)
	}

	if len(policyDoc.Statement) == 0 {
		return fmt.Errorf("policy must contain at least one statement")
	}

	for i := range policyDoc.Statement {
		if err := validateStatement(&policyDoc.Statement[i]); err != nil {
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

	if len(stmt.Resource.Strings()) == 0 && len(stmt.NotResource.Strings()) == 0 {
		return fmt.Errorf("statement must specify Resource or NotResource")
	}

	return nil
}

// ParsePolicy parses a policy JSON string
func ParsePolicy(policyJSON string) (*PolicyDocument, error) {
	var policy PolicyDocument
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return nil, fmt.Errorf("failed to parse policy JSON: %w", err)
	}

	if err := ValidatePolicy(&policy); err != nil {
		return nil, fmt.Errorf("invalid policy: %w", err)
	}

	return &policy, nil
}

// CompilePolicy compiles a policy for efficient evaluation
func CompilePolicy(policy *PolicyDocument) (*CompiledPolicy, error) {
	compiled := &CompiledPolicy{
		Document:   policy,
		Statements: make([]*CompiledStatement, len(policy.Statement)),
	}

	for i := range policy.Statement {
		stmt := &policy.Statement[i]
		compiledStmt, err := compileStatement(stmt)
		if err != nil {
			return nil, fmt.Errorf("failed to compile statement %d: %v", i, err)
		}
		compiled.Statements[i] = compiledStmt
	}

	return compiled, nil
}

// compileStatement compiles a single policy statement
func compileStatement(stmt *PolicyStatement) (*CompiledStatement, error) {
	resStrings := slices.Clone(stmt.Resource.Strings())
	notResStrings := slices.Clone(stmt.NotResource.Strings())
	compiled := &CompiledStatement{
		Statement: &PolicyStatement{
			Sid:    stmt.Sid,
			Effect: stmt.Effect,
			Action: stmt.Action,
		},
	}

	// Deep clone Principal if present
	if stmt.Principal != nil {
		principalClone := *stmt.Principal
		principalClone.values = slices.Clone(stmt.Principal.values)
		compiled.Statement.Principal = &principalClone
	}

	// Deep clone Resource/NotResource into the internal statement as well for completeness
	compiled.Statement.Resource.values = slices.Clone(stmt.Resource.values)
	compiled.Statement.NotResource.values = slices.Clone(stmt.NotResource.values)

	// Deep clone Condition map
	if stmt.Condition != nil {
		compiled.Statement.Condition = make(PolicyConditions)
		for k, v := range stmt.Condition {
			innerMap := make(map[string]StringOrStringSlice)
			for ik, iv := range v {
				innerMap[ik] = StringOrStringSlice{values: slices.Clone(iv.values)}
			}
			compiled.Statement.Condition[k] = innerMap
		}
	}

	// Compile action patterns and matchers
	for _, action := range stmt.Action.Strings() {
		if action == "" {
			continue
		}
		// Check for dynamic variables
		if PolicyVariableRegex.MatchString(action) {
			compiled.DynamicActionPatterns = append(compiled.DynamicActionPatterns, action)
			continue
		}

		pattern, err := compilePattern(action)
		if err != nil {
			return nil, fmt.Errorf("failed to compile action pattern %s: %v", action, err)
		}
		compiled.ActionPatterns = append(compiled.ActionPatterns, pattern)

		matcher, err := NewWildcardMatcher(action)
		if err != nil {
			return nil, fmt.Errorf("failed to create action matcher %s: %v", action, err)
		}
		compiled.ActionMatchers = append(compiled.ActionMatchers, matcher)
	}

	// Compile resource patterns and matchers
	for _, resource := range resStrings {
		if resource == "" {
			continue
		}
		// Check for dynamic variables
		if PolicyVariableRegex.MatchString(resource) {
			compiled.DynamicResourcePatterns = append(compiled.DynamicResourcePatterns, resource)
			continue
		}

		pattern, err := compilePattern(resource)
		if err != nil {
			return nil, fmt.Errorf("failed to compile resource pattern %s: %v", resource, err)
		}
		compiled.ResourcePatterns = append(compiled.ResourcePatterns, pattern)

		matcher, err := NewWildcardMatcher(resource)
		if err != nil {
			return nil, fmt.Errorf("failed to create resource matcher %s: %v", resource, err)
		}
		compiled.ResourceMatchers = append(compiled.ResourceMatchers, matcher)
	}

	// Compile principal patterns and matchers if present
	if stmt.Principal != nil && len(stmt.Principal.Strings()) > 0 {
		for _, principal := range stmt.Principal.Strings() {
			if principal == "" {
				continue
			}
			// Check for dynamic variables
			if PolicyVariableRegex.MatchString(principal) {
				compiled.DynamicPrincipalPatterns = append(compiled.DynamicPrincipalPatterns, principal)
				continue
			}

			pattern, err := compilePattern(principal)
			if err != nil {
				return nil, fmt.Errorf("failed to compile principal pattern %s: %v", principal, err)
			}
			compiled.PrincipalPatterns = append(compiled.PrincipalPatterns, pattern)

			matcher, err := NewWildcardMatcher(principal)
			if err != nil {
				return nil, fmt.Errorf("failed to create principal matcher %s: %v", principal, err)
			}
			compiled.PrincipalMatchers = append(compiled.PrincipalMatchers, matcher)
		}
	}

	// Compile NotResource patterns (resource should NOT match these)
	if len(notResStrings) > 0 {
		for _, notResource := range notResStrings {
			if notResource == "" {
				continue
			}
			// Check for dynamic variables
			if PolicyVariableRegex.MatchString(notResource) {
				compiled.DynamicNotResourcePatterns = append(compiled.DynamicNotResourcePatterns, notResource)
				continue
			}

			pattern, err := compilePattern(notResource)
			if err != nil {
				return nil, fmt.Errorf("failed to compile NotResource pattern %s: %v", notResource, err)
			}
			compiled.NotResourcePatterns = append(compiled.NotResourcePatterns, pattern)

			matcher, err := NewWildcardMatcher(notResource)
			if err != nil {
				return nil, fmt.Errorf("failed to create NotResource matcher %s: %v", notResource, err)
			}
			compiled.NotResourceMatchers = append(compiled.NotResourceMatchers, matcher)

			// Debug log
			// fmt.Printf("Compiled NotResource: %s\n", notResource)
		}
	}

	return compiled, nil
}

// compilePattern compiles a wildcard pattern to regex
func compilePattern(pattern string) (*regexp.Regexp, error) {
	return CompileWildcardPattern(pattern)
}

// normalizeToStringSlice converts various types to string slice - kept for backward compatibility
func normalizeToStringSlice(value interface{}) []string {
	result, err := normalizeToStringSliceWithError(value)
	if err != nil {
		glog.Warningf("unexpected type for policy value: %T, error: %v", value, err)
		return []string{fmt.Sprintf("%v", value)}
	}
	return result
}

// normalizeToStringSliceWithError converts various types to string slice with proper error handling
func normalizeToStringSliceWithError(value interface{}) ([]string, error) {
	switch v := value.(type) {
	case string:
		return []string{v}, nil
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = fmt.Sprintf("%v", item)
		}
		return result, nil
	case StringOrStringSlice:
		return v.Strings(), nil
	default:
		return nil, fmt.Errorf("unexpected type for policy value: %T", v)
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
	"BypassGovernanceRetention":        "s3:BypassGovernanceRetention",
}

// MatchesAction checks if an action matches any of the compiled action matchers
func (cs *CompiledStatement) MatchesAction(action string) bool {
	for _, matcher := range cs.ActionMatchers {
		if matcher.Match(action) {
			return true
		}
	}
	return false
}

// MatchesResource checks if a resource matches any of the compiled resource matchers
func (cs *CompiledStatement) MatchesResource(resource string) bool {
	for _, matcher := range cs.ResourceMatchers {
		if matcher.Match(resource) {
			return true
		}
	}
	return false
}

// MatchesPrincipal checks if a principal matches any of the compiled principal matchers
func (cs *CompiledStatement) MatchesPrincipal(principal string) bool {
	// If no principals specified, match all
	if len(cs.PrincipalMatchers) == 0 {
		return true
	}

	for _, matcher := range cs.PrincipalMatchers {
		if matcher.Match(principal) {
			return true
		}
	}
	return false
}

// EvaluateStatement evaluates a compiled statement against the given arguments
func (cs *CompiledStatement) EvaluateStatement(args *PolicyEvaluationArgs) bool {
	// Check if action matches
	if !cs.MatchesAction(args.Action) {
		return false
	}

	// Check if resource matches
	if !cs.MatchesResource(args.Resource) {
		return false
	}

	// Check if principal matches
	if !cs.MatchesPrincipal(args.Principal) {
		return false
	}

	return true
}

// EvaluatePolicy evaluates a compiled policy against the given arguments
func (cp *CompiledPolicy) EvaluatePolicy(args *PolicyEvaluationArgs) (bool, PolicyEffect) {
	var explicitAllow, explicitDeny bool

	// Evaluate each statement
	for _, stmt := range cp.Statements {
		if stmt.EvaluateStatement(args) {
			if stmt.Statement.Effect == PolicyEffectAllow {
				explicitAllow = true
			} else if stmt.Statement.Effect == PolicyEffectDeny {
				explicitDeny = true
			}
		}
	}

	// AWS policy evaluation logic: explicit deny overrides allow
	if explicitDeny {
		return false, PolicyEffectDeny
	}
	if explicitAllow {
		return true, PolicyEffectAllow
	}

	// No matching statements - implicit deny
	return false, PolicyEffectDeny
}

// FastMatchesWildcard uses cached WildcardMatcher for performance
func FastMatchesWildcard(pattern, str string) bool {
	matcher, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		glog.Errorf("Error getting cached WildcardMatcher for pattern %s: %v", pattern, err)
		// Fall back to the original implementation
		return MatchesWildcard(pattern, str)
	}
	return matcher.Match(str)
}
