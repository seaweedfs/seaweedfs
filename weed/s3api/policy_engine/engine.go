package policy_engine

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// PolicyEvaluationResult represents the result of policy evaluation
type PolicyEvaluationResult int

const (
	PolicyResultDeny PolicyEvaluationResult = iota
	PolicyResultAllow
	PolicyResultIndeterminate
)

// PolicyEvaluationContext manages policy evaluation for a bucket
type PolicyEvaluationContext struct {
	bucketName string
	policy     *CompiledPolicy
	cache      *PolicyCache
	mutex      sync.RWMutex
}

// PolicyEngine is the main policy evaluation engine
type PolicyEngine struct {
	contexts map[string]*PolicyEvaluationContext
	mutex    sync.RWMutex
}

// NewPolicyEngine creates a new policy evaluation engine
func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{
		contexts: make(map[string]*PolicyEvaluationContext),
	}
}

// SetBucketPolicy sets the policy for a bucket
func (engine *PolicyEngine) SetBucketPolicy(bucketName string, policyJSON string) error {
	policy, err := ParsePolicy(policyJSON)
	if err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}

	compiled, err := CompilePolicy(policy)
	if err != nil {
		return fmt.Errorf("failed to compile policy: %w", err)
	}

	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	context := &PolicyEvaluationContext{
		bucketName: bucketName,
		policy:     compiled,
		cache:      NewPolicyCache(),
	}

	engine.contexts[bucketName] = context
	glog.V(4).Infof("SetBucketPolicy: Successfully cached policy for bucket=%s, statements=%d", bucketName, len(compiled.Statements))
	return nil
}

// GetBucketPolicy gets the policy for a bucket
func (engine *PolicyEngine) GetBucketPolicy(bucketName string) (*PolicyDocument, error) {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	context, exists := engine.contexts[bucketName]
	if !exists {
		return nil, fmt.Errorf("no policy found for bucket %s", bucketName)
	}

	return context.policy.Document, nil
}

// DeleteBucketPolicy deletes the policy for a bucket
func (engine *PolicyEngine) DeleteBucketPolicy(bucketName string) error {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	delete(engine.contexts, bucketName)
	glog.V(2).Infof("Deleted bucket policy for %s", bucketName)
	return nil
}

// HasPolicyForBucket checks if a bucket has a policy configured
func (engine *PolicyEngine) HasPolicyForBucket(bucketName string) bool {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()
	_, exists := engine.contexts[bucketName]
	return exists
}

// EvaluatePolicy evaluates a policy for the given arguments
func (engine *PolicyEngine) EvaluatePolicy(bucketName string, args *PolicyEvaluationArgs) PolicyEvaluationResult {
	engine.mutex.RLock()
	context, exists := engine.contexts[bucketName]
	engine.mutex.RUnlock()

	if !exists {
		glog.V(4).Infof("EvaluatePolicy: No policy found for bucket=%s (PolicyResultIndeterminate)", bucketName)
		return PolicyResultIndeterminate
	}

	glog.V(4).Infof("EvaluatePolicy: Found policy for bucket=%s, evaluating with action=%s resource=%s principal=%s",
		bucketName, args.Action, args.Resource, args.Principal)
	return engine.evaluateCompiledPolicy(context.policy, args)
}

// evaluateCompiledPolicy evaluates a compiled policy
func (engine *PolicyEngine) evaluateCompiledPolicy(policy *CompiledPolicy, args *PolicyEvaluationArgs) PolicyEvaluationResult {
	// AWS Policy evaluation logic:
	// 1. Check for explicit Deny - if found, return Deny
	// 2. Check for explicit Allow - if found, return Allow
	// 3. If no matching statements, return Indeterminate (fall through to IAM)

	hasExplicitAllow := false

	for _, stmt := range policy.Statements {
		if engine.evaluateStatement(stmt, args) {
			if stmt.Statement.Effect == PolicyEffectDeny {
				return PolicyResultDeny // Explicit deny trumps everything
			}
			if stmt.Statement.Effect == PolicyEffectAllow {
				hasExplicitAllow = true
			}
		}
	}

	if hasExplicitAllow {
		return PolicyResultAllow
	}

	// No matching statements - return Indeterminate to fall through to IAM
	// This allows IAM policies to grant access even when bucket policy doesn't mention the action
	return PolicyResultIndeterminate
}

// matchesDynamicPatterns checks if a value matches any of the dynamic patterns after variable substitution
func (engine *PolicyEngine) matchesDynamicPatterns(patterns []string, value string, args *PolicyEvaluationArgs) bool {
	for _, pattern := range patterns {
		substituted := SubstituteVariables(pattern, args.Conditions, args.Claims)
		if FastMatchesWildcard(substituted, value) {
			return true
		}
	}
	return false
}

// evaluateStatement evaluates a single policy statement
func (engine *PolicyEngine) evaluateStatement(stmt *CompiledStatement, args *PolicyEvaluationArgs) bool {
	// Check if action matches
	matchedAction := engine.matchesPatterns(stmt.ActionPatterns, args.Action)
	if !matchedAction {
		matchedAction = engine.matchesDynamicPatterns(stmt.DynamicActionPatterns, args.Action, args)
	}
	if !matchedAction {
		return false
	}

	// Check if resource matches
	hasResource := len(stmt.ResourcePatterns) > 0 || len(stmt.DynamicResourcePatterns) > 0
	hasNotResource := len(stmt.NotResourcePatterns) > 0 || len(stmt.DynamicNotResourcePatterns) > 0
	if hasResource {
		matchedResource := engine.matchesPatterns(stmt.ResourcePatterns, args.Resource)
		if !matchedResource {
			matchedResource = engine.matchesDynamicPatterns(stmt.DynamicResourcePatterns, args.Resource, args)
		}
		if !matchedResource {
			return false
		}
	}

	if hasNotResource {
		matchedNotResource := false
		for _, matcher := range stmt.NotResourceMatchers {
			if matcher.Match(args.Resource) {
				matchedNotResource = true
				break
			}
		}

		if !matchedNotResource {
			matchedNotResource = engine.matchesDynamicPatterns(stmt.DynamicNotResourcePatterns, args.Resource, args)
		}

		if matchedNotResource {
			return false
		}
	}

	// Check if principal matches
	if len(stmt.PrincipalPatterns) > 0 || len(stmt.DynamicPrincipalPatterns) > 0 {
		matchedPrincipal := engine.matchesPatterns(stmt.PrincipalPatterns, args.Principal)
		if !matchedPrincipal {
			matchedPrincipal = engine.matchesDynamicPatterns(stmt.DynamicPrincipalPatterns, args.Principal, args)
		}
		if !matchedPrincipal {
			return false
		}
	}

	// Check conditions
	if len(stmt.Statement.Condition) > 0 {
		match := EvaluateConditions(stmt.Statement.Condition, args.Conditions, args.ObjectEntry, args.Claims)
		if !match {
			return false
		}
	}

	return true
}

// matchesPatterns checks if a value matches any of the compiled patterns
func (engine *PolicyEngine) matchesPatterns(patterns []*regexp.Regexp, value string) bool {
	for _, pattern := range patterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return false
}

// SubstituteVariables replaces ${variable} in a pattern with values from context and claims
// Supports:
//   - Standard context variables (aws:SourceIp, s3:prefix, etc.)
//   - JWT claims (jwt:preferred_username, jwt:sub, jwt:*)
//   - LDAP claims (ldap:username, ldap:dn, ldap:*)
func SubstituteVariables(pattern string, context map[string][]string, claims map[string]interface{}) string {
	result := PolicyVariableRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		// match is like "${aws:username}"
		// extract variable name "aws:username"
		variable := match[2 : len(match)-1]

		// Check standard context first
		if values, ok := context[variable]; ok && len(values) > 0 {
			return values[0]
		}

		// Check JWT claims for jwt:* variables
		if strings.HasPrefix(variable, "jwt:") {
			claimName := variable[4:] // Remove "jwt:" prefix
			if claimValue, ok := claims[claimName]; ok {
				switch v := claimValue.(type) {
				case string:
					return v
				case float64:
					// JWT numbers are often float64
					if v == float64(int64(v)) {
						return fmt.Sprintf("%d", int64(v))
					}
					return fmt.Sprintf("%g", v)
				case bool:
					return fmt.Sprintf("%t", v)
				case int:
					return fmt.Sprintf("%d", v)
				case int32:
					return fmt.Sprintf("%d", v)
				case int64:
					return fmt.Sprintf("%d", v)
				default:
					return fmt.Sprintf("%v", v)
				}
			}
		}

		// Check LDAP claims for ldap:* variables
		// FALLBACK MECHANISM: Try both prefixed and unprefixed keys
		// Some LDAP providers store claims with the "ldap:" prefix (e.g., "ldap:username")
		// while others store them without the prefix (e.g., "username").
		// We check the prefixed key first for consistency, then fall back to unprefixed.
		if strings.HasPrefix(variable, "ldap:") {
			claimName := variable[5:] // Remove "ldap:" prefix
			// Try prefixed key first (e.g., "ldap:username"), then unprefixed
			var claimValue interface{}
			var ok bool
			if claimValue, ok = claims[variable]; !ok {
				claimValue, ok = claims[claimName]
			}
			if ok {
				switch v := claimValue.(type) {
				case string:
					return v
				case float64:
					if v == float64(int64(v)) {
						return fmt.Sprintf("%d", int64(v))
					}
					return fmt.Sprintf("%g", v)
				case bool:
					return fmt.Sprintf("%t", v)
				case int:
					return fmt.Sprintf("%d", v)
				case int32:
					return fmt.Sprintf("%d", v)
				case int64:
					return fmt.Sprintf("%d", v)
				default:
					return fmt.Sprintf("%v", v)
				}
			}
		}

		// Variable not found, leave as-is to avoid unexpected matching
		return match
	})
	return result
}

// ExtractPrincipalVariables extracts policy variables from a principal ARN
func ExtractPrincipalVariables(principal string) map[string][]string {
	vars := make(map[string][]string)

	// Handle non-ARN principals (e.g., "*" or simple usernames)
	if !strings.HasPrefix(principal, "arn:aws:") {
		return vars
	}

	// Parse ARN: arn:aws:service::account:resource
	parts := strings.Split(principal, ":")
	if len(parts) < 6 {
		return vars
	}

	account := parts[4]      // account ID
	resourcePart := parts[5] // user/username or assumed-role/role/session

	// Set aws:PrincipalAccount if account is present
	if account != "" {
		vars["aws:PrincipalAccount"] = []string{account}
	}

	resourceParts := strings.Split(resourcePart, "/")
	if len(resourceParts) < 2 {
		return vars
	}

	resourceType := resourceParts[0] // "user", "role", "assumed-role"

	// Set aws:principaltype and extract username/userid based on resource type
	switch resourceType {
	case "user":
		vars["aws:principaltype"] = []string{"IAMUser"}
		// For users with paths like "user/path/to/username", use the last segment
		username := resourceParts[len(resourceParts)-1]
		vars["aws:username"] = []string{username}
		vars["aws:userid"] = []string{username} // In SeaweedFS, userid is same as username
	case "role":
		vars["aws:principaltype"] = []string{"IAMRole"}
		// For roles with paths like "role/path/to/rolename", use the last segment
		// Note: IAM Roles do NOT have aws:userid, but aws:PrincipalAccount is kept for condition evaluations
		if len(resourceParts) >= 2 {
			roleName := resourceParts[len(resourceParts)-1]
			vars["aws:username"] = []string{roleName}
		}
	case "assumed-role":
		vars["aws:principaltype"] = []string{"AssumedRole"}
		// For assumed roles: assumed-role/RoleName/SessionName or assumed-role/path/to/RoleName/SessionName
		// The session name is always the last segment
		if len(resourceParts) >= 3 {
			sessionName := resourceParts[len(resourceParts)-1]
			vars["aws:username"] = []string{sessionName}
			vars["aws:userid"] = []string{sessionName}
		}
	}

	// Note: principaltype is already set correctly in the switch above based on resource type

	return vars
}

// ExtractConditionValuesFromRequest extracts condition values from HTTP request
func ExtractConditionValuesFromRequest(r *http.Request) map[string][]string {
	values := make(map[string][]string)

	// AWS condition keys
	values["aws:SourceIp"] = []string{extractSourceIP(r)}
	values["aws:SecureTransport"] = []string{fmt.Sprintf("%t", r.TLS != nil)}
	// Use AWS standard condition key for current time
	values["aws:CurrentTime"] = []string{time.Now().Format(time.RFC3339)}
	// Keep RequestTime for backward compatibility
	values["aws:RequestTime"] = []string{time.Now().Format(time.RFC3339)}

	// S3 specific condition keys
	if userAgent := r.Header.Get("User-Agent"); userAgent != "" {
		values["aws:UserAgent"] = []string{userAgent}
	}

	if referer := r.Header.Get("Referer"); referer != "" {
		values["aws:Referer"] = []string{referer}
	}

	// Note: s3:ExistingObjectTag/<key> conditions are evaluated using objectEntry
	// passed to EvaluatePolicy, not extracted from the request.

	// S3 bucket-level conditions
	if delimiter := r.URL.Query().Get("delimiter"); delimiter != "" {
		values["s3:delimiter"] = []string{delimiter}
	}

	if prefix := r.URL.Query().Get("prefix"); prefix != "" {
		values["s3:prefix"] = []string{prefix}
	}

	if maxKeys := r.URL.Query().Get("max-keys"); maxKeys != "" {
		values["s3:max-keys"] = []string{maxKeys}
	}

	// Authentication method
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
			values["s3:authType"] = []string{"REST-HEADER"}
		} else if strings.HasPrefix(authHeader, "AWS ") {
			values["s3:authType"] = []string{"REST-HEADER"}
		}
	} else if r.URL.Query().Get("AWSAccessKeyId") != "" {
		values["s3:authType"] = []string{"REST-QUERY-STRING"}
	}

	// HTTP method
	values["s3:RequestMethod"] = []string{r.Method}

	// Extract custom headers
	for key, headerValues := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-") {
			values[strings.ToLower(key)] = headerValues
		}
	}

	return values
}

// extractSourceIP returns the best-effort client IP address for condition evaluation.
// Preference order: X-Forwarded-For (first valid IP), X-Real-Ip, then RemoteAddr.
// IMPORTANT: X-Forwarded-For and X-Real-Ip are trusted without validation.
// When the service is exposed directly, clients can spoof aws:SourceIp unless a
// reverse proxy overwrites these headers.
func extractSourceIP(r *http.Request) string {
	if r == nil {
		return ""
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, candidate := range strings.Split(xff, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			if ip := net.ParseIP(candidate); ip != nil {
				return ip.String()
			}
		}
	}

	if xRealIP := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xRealIP != "" {
		if ip := net.ParseIP(xRealIP); ip != nil {
			return ip.String()
		}
	}

	remoteAddr := strings.TrimSpace(r.RemoteAddr)
	if remoteAddr == "" {
		return ""
	}

	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip.String()
		}
		// Do not return DNS names; fall through to the fallback path.
	} else {
		ipCandidate := remoteAddr
		if len(remoteAddr) > 1 && remoteAddr[0] == '[' && remoteAddr[len(remoteAddr)-1] == ']' {
			ipCandidate = remoteAddr[1 : len(remoteAddr)-1]
		}
		if ip := net.ParseIP(ipCandidate); ip != nil {
			return ip.String()
		}
	}

	// Fall back to unix socket markers or other non-IP placeholders.
	if remoteAddr == "@" {
		return remoteAddr
	}

	return ""
}

// BuildResourceArn builds an ARN for the given bucket and object
func BuildResourceArn(bucketName, objectName string) string {
	if objectName == "" {
		return fmt.Sprintf("arn:aws:s3:::%s", bucketName)
	}
	return fmt.Sprintf("arn:aws:s3:::%s/%s", bucketName, objectName)
}

// BuildActionName builds a standardized action name
func BuildActionName(action string) string {
	if strings.HasPrefix(action, "s3:") {
		return action
	}
	return fmt.Sprintf("s3:%s", action)
}

// IsReadAction checks if an action is a read action
func IsReadAction(action string) bool {
	readActions := []string{
		"s3:GetObject",
		"s3:GetObjectVersion",
		"s3:GetObjectAcl",
		"s3:GetObjectVersionAcl",
		"s3:GetObjectTagging",
		"s3:GetObjectVersionTagging",
		"s3:ListBucket",
		"s3:ListBucketVersions",
		"s3:GetBucketLocation",
		"s3:GetBucketVersioning",
		"s3:GetBucketAcl",
		"s3:GetBucketCors",
		"s3:GetBucketPolicy",
		"s3:GetBucketTagging",
		"s3:GetBucketNotification",
		"s3:GetBucketObjectLockConfiguration",
		"s3:GetObjectRetention",
		"s3:GetObjectLegalHold",
	}

	for _, readAction := range readActions {
		if action == readAction {
			return true
		}
	}
	return false
}

// IsWriteAction checks if an action is a write action
func IsWriteAction(action string) bool {
	writeActions := []string{
		"s3:PutObject",
		"s3:PutObjectAcl",
		"s3:PutObjectTagging",
		"s3:DeleteObject",
		"s3:DeleteObjectVersion",
		"s3:DeleteObjectTagging",
		"s3:AbortMultipartUpload",
		"s3:ListMultipartUploads",
		"s3:ListParts",
		"s3:PutBucketAcl",
		"s3:PutBucketCors",
		"s3:PutBucketPolicy",
		"s3:PutBucketTagging",
		"s3:PutBucketNotification",
		"s3:PutBucketVersioning",
		"s3:DeleteBucketPolicy",
		"s3:DeleteBucketTagging",
		"s3:DeleteBucketCors",
		"s3:PutBucketObjectLockConfiguration",
		"s3:PutObjectRetention",
		"s3:PutObjectLegalHold",
		"s3:BypassGovernanceRetention",
	}

	for _, writeAction := range writeActions {
		if action == writeAction {
			return true
		}
	}
	return false
}

// GetBucketNameFromArn extracts bucket name from ARN
func GetBucketNameFromArn(arn string) string {
	if strings.HasPrefix(arn, "arn:aws:s3:::") {
		parts := strings.SplitN(arn[13:], "/", 2)
		return parts[0]
	}
	return ""
}

// GetObjectNameFromArn extracts object name from ARN
func GetObjectNameFromArn(arn string) string {
	if strings.HasPrefix(arn, "arn:aws:s3:::") {
		parts := strings.SplitN(arn[13:], "/", 2)
		if len(parts) > 1 {
			return parts[1]
		}
	}
	return ""
}

// GetPolicyStatements returns all policy statements for a bucket
func (engine *PolicyEngine) GetPolicyStatements(bucketName string) []PolicyStatement {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	context, exists := engine.contexts[bucketName]
	if !exists {
		return nil
	}

	return context.policy.Document.Statement
}

// ValidatePolicyForBucket validates if a policy is valid for a bucket
func (engine *PolicyEngine) ValidatePolicyForBucket(bucketName string, policyJSON string) error {
	policy, err := ParsePolicy(policyJSON)
	if err != nil {
		return err
	}

	// Additional validation specific to the bucket
	for _, stmt := range policy.Statement {
		resources := normalizeToStringSlice(stmt.Resource)
		for _, resource := range resources {
			if resourceBucket := GetBucketFromResource(resource); resourceBucket != "" {
				if resourceBucket != bucketName {
					return fmt.Errorf("policy resource %s does not match bucket %s", resource, bucketName)
				}
			}
		}
	}

	return nil
}

// ClearAllPolicies clears all bucket policies
func (engine *PolicyEngine) ClearAllPolicies() {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	engine.contexts = make(map[string]*PolicyEvaluationContext)
	glog.V(2).Info("Cleared all bucket policies")
}

// GetAllBucketsWithPolicies returns all buckets that have policies
func (engine *PolicyEngine) GetAllBucketsWithPolicies() []string {
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()

	buckets := make([]string, 0, len(engine.contexts))
	for bucketName := range engine.contexts {
		buckets = append(buckets, bucketName)
	}
	return buckets
}

// EvaluatePolicyForRequest evaluates policy for an HTTP request
func (engine *PolicyEngine) EvaluatePolicyForRequest(bucketName, objectName, action, principal string, r *http.Request) PolicyEvaluationResult {
	resource := BuildResourceArn(bucketName, objectName)
	actionName := BuildActionName(action)
	conditions := ExtractConditionValuesFromRequest(r)

	// Extract principal information for variables
	principalVars := ExtractPrincipalVariables(principal)
	for k, v := range principalVars {
		conditions[k] = v
	}

	args := &PolicyEvaluationArgs{
		Action:     actionName,
		Resource:   resource,
		Principal:  principal,
		Conditions: conditions,
	}

	return engine.EvaluatePolicy(bucketName, args)
}
