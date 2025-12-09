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
	glog.V(2).Infof("Set bucket policy for %s", bucketName)
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
		return PolicyResultIndeterminate
	}

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
		if engine.evaluateStatement(&stmt, args) {
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

// evaluateStatement evaluates a single policy statement
func (engine *PolicyEngine) evaluateStatement(stmt *CompiledStatement, args *PolicyEvaluationArgs) bool {
	// Check if action matches
	if !engine.matchesPatterns(stmt.ActionPatterns, args.Action) {
		return false
	}

	// Check if resource matches
	if !engine.matchesPatterns(stmt.ResourcePatterns, args.Resource) {
		return false
	}

	// Check if principal matches (if specified)
	if len(stmt.PrincipalPatterns) > 0 {
		if !engine.matchesPatterns(stmt.PrincipalPatterns, args.Principal) {
			return false
		}
	}

	// Check conditions
	if len(stmt.Statement.Condition) > 0 {
		if !EvaluateConditions(stmt.Statement.Condition, args.Conditions, args.ObjectEntry) {
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

// ExtractConditionValuesFromRequest extracts condition values from HTTP request
func ExtractConditionValuesFromRequest(r *http.Request) map[string][]string {
	values := make(map[string][]string)

	// AWS condition keys
	// Extract IP address without port for proper IP matching
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// Log a warning if splitting fails
		glog.Warningf("Failed to parse IP address from RemoteAddr %q: %v", r.RemoteAddr, err)
		// If splitting fails, use the original RemoteAddr (might be just IP without port)
		host = r.RemoteAddr
	}
	values["aws:SourceIp"] = []string{host}
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

	// S3 object-level conditions
	if r.Method == "GET" || r.Method == "HEAD" {
		values["s3:ExistingObjectTag"] = extractObjectTags(r)
	}

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

// extractObjectTags extracts object tags from request (placeholder implementation)
func extractObjectTags(r *http.Request) []string {
	// This would need to be implemented based on how object tags are stored
	// For now, return empty slice
	return []string{}
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

	args := &PolicyEvaluationArgs{
		Action:     actionName,
		Resource:   resource,
		Principal:  principal,
		Conditions: conditions,
	}

	return engine.EvaluatePolicy(bucketName, args)
}
