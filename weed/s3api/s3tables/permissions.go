package s3tables

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

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
		Statement json.RawMessage `json:"Statement"`
		*Alias
	}{
		Alias: (*Alias)(pd),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	var statement interface{}
	if len(aux.Statement) > 0 {
		if err := json.Unmarshal(aux.Statement, &statement); err != nil {
			return err
		}
	}

	// Handle Statement as either a single object or array
	raws := []json.RawMessage{aux.Statement}
	switch s := statement.(type) {
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
		if err := json.Unmarshal(aux.Statement, &raws); err != nil {
			return err
		}
	case nil:
		// No statements
		pd.Statement = []Statement{}
	default:
		return fmt.Errorf("Statement must be an object or array, got %T", statement)
	}

	// The re-marshal above keeps only the last of a repeated key, so the
	// Condition keys are counted from the original bytes.
	for i := range pd.Statement {
		if i < len(raws) {
			pd.Statement[i].nullCondition, pd.Statement[i].splitCondition = conditionKeys(raws[i])
		}
	}
	return nil
}

type Statement struct {
	Effect    string                            `json:"Effect"`    // "Allow" or "Deny"
	Principal interface{}                       `json:"Principal"` // Can be string, []string, or map
	Action    interface{}                       `json:"Action"`    // Can be string or []string
	Resource  interface{}                       `json:"Resource"`  // Can be string or []string
	Condition map[string]map[string]interface{} `json:"Condition,omitempty"`
	// nullCondition records a literal "Condition": null, which decodes the
	// same as no Condition.
	nullCondition bool
	// splitCondition records more than one Condition key, in any spelling.
	// Those objects decode into one map in which an operator they share keeps
	// only the last one's keys.
	splitCondition bool
}

// conditionKeys walks a statement object's keys, which a map would collapse,
// and reports a null Condition and a Condition under more than one key.
func conditionKeys(data []byte) (null, split bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return false, false
	}
	conditions := 0
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			break
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		if name, _ := key.(string); strings.EqualFold(name, "Condition") {
			conditions++
			null = null || string(raw) == "null"
		}
	}
	return null, conditions > 1
}

// SplitCondition reports a statement with more than one Condition key, which
// the evaluator never grants on and always vetoes with.
func (s *Statement) SplitCondition() bool {
	return s.splitCondition
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
	if principal == s3_constants.AccountAdminId && adminCaller(operation, ctx) {
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
	if principal == s3_constants.AccountAdminId && adminCaller(operation, ctx) {
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

// validatePolicyDocument rejects a policy the evaluator could not honor: one
// that does not parse, whose actions and resources name the vector service,
// whose conditions the evaluator cannot compare, or whose Allow carries a null
// Condition. Any of them stored here would be a rule the writer believes is
// enforced — vector requests authorize their s3tables action on a s3tables
// ARN, an unusable condition is skipped on an Allow and vetoes everything on a
// Deny, and a null Condition reads as none, so the Allow grants unconditionally.
func validatePolicyDocument(raw string) error {
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return fmt.Errorf("invalid policy document: %w", err)
	}
	for _, stmt := range doc.Statement {
		// A null Condition on a Deny only widens the veto, so it stays allowed.
		if stmt.Effect == "Allow" && stmt.nullCondition {
			return fmt.Errorf("invalid policy document: an Allow statement's Condition must not be null")
		}
		if stmt.splitCondition {
			return fmt.Errorf("invalid policy document: a statement's Condition must be under one key, spelled Condition")
		}
		for operator, keys := range stmt.Condition {
			if _, err := policy_engine.GetConditionEvaluator(operator); err != nil {
				return fmt.Errorf("invalid policy document: condition operator %q is not supported", operator)
			}
			// An operator with no keys holds for every request.
			if len(keys) == 0 {
				return fmt.Errorf("invalid policy document: condition %s needs at least one key", operator)
			}
			for key, value := range keys {
				if !tableConditionKey(key) {
					return fmt.Errorf("invalid policy document: condition key %q is not supported in a table policy", key)
				}
				if !ConditionValue(operator, value) {
					return fmt.Errorf("invalid policy document: condition %s on %q needs values %s can read (a number for Numeric*, an RFC 3339 time for Date*, true or false for Bool and Null, an IP or CIDR for IpAddress)", operator, key, operator)
				}
			}
		}
		for _, action := range statementActions(stmt.Action) {
			if strings.HasPrefix(strings.ToLower(action), "s3vectors:") {
				return fmt.Errorf("action %q is not enforceable in a resource policy", action)
			}
		}
		for _, resource := range statementActions(stmt.Resource) {
			if strings.EqualFold(arnService(resource), "s3vectors") {
				return fmt.Errorf("resource %q is not enforceable in a resource policy", resource)
			}
		}
	}
	return nil
}

// ConditionValue reports whether operator's evaluator can read v: a JSON
// type it can compare, and a value it parses rather than skips. A null
// value would panic it, an empty list never matches, and an unreadable
// value is skipped — dropping a Deny or matching a negated Allow.
func ConditionValue(operator string, v interface{}) bool {
	list, ok := v.([]interface{})
	if !ok {
		list = []interface{}{v}
	}
	base, qualified := strings.CutPrefix(operator, "ForAllValues:")
	if !qualified {
		base, _ = strings.CutPrefix(operator, "ForAnyValue:")
	}
	for _, item := range list {
		switch item.(type) {
		case string, float64, bool:
		default:
			return false
		}
		if !conditionValueReadable(base, item) {
			return false
		}
	}
	return len(list) > 0
}

// conditionValueReadable reports whether the evaluator for base parses item
// the way it reads it. An operator without a case here is refused, so one
// added to the policy engine cannot be accepted before it is checked.
func conditionValueReadable(base string, item interface{}) bool {
	switch base {
	case "StringEquals", "StringNotEquals", "StringLike", "StringNotLike",
		"StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase", "ArnEquals", "ArnLike":
		return true
	case "NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
		"NumericGreaterThan", "NumericGreaterThanEquals":
		switch v := item.(type) {
		case float64:
			return true
		case string:
			f, err := strconv.ParseFloat(v, 64)
			return err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
		}
	case "DateEquals", "DateNotEquals", "DateLessThan", "DateLessThanEquals",
		"DateGreaterThan", "DateGreaterThanEquals":
		if v, ok := item.(string); ok {
			_, err := time.Parse(time.RFC3339, v)
			return err == nil
		}
	case "Bool", "Null":
		switch v := item.(type) {
		case bool:
			return true
		case string:
			return strings.EqualFold(v, "true") || strings.EqualFold(v, "false")
		}
	case "IpAddress", "NotIpAddress":
		if v, ok := item.(string); ok {
			if _, _, err := net.ParseCIDR(v); err == nil {
				return true
			}
			return net.ParseIP(v) != nil
		}
	}
	return false
}

// arnService returns the service field of an ARN in any partition, so a
// resource naming s3vectors is caught for aws-cn, aws-us-gov, and friends —
// not just the commercial partition.
func arnService(resource string) string {
	parts := strings.SplitN(resource, ":", 4)
	if len(parts) == 4 && parts[0] == "arn" {
		return parts[2]
	}
	return ""
}

func statementActions(spec interface{}) []string {
	switch a := spec.(type) {
	case string:
		return []string{a}
	case []interface{}:
		var actions []string
		for _, item := range a {
			if str, ok := item.(string); ok {
				actions = append(actions, str)
			}
		}
		return actions
	}
	return nil
}

// deniedByPolicy reports whether another applicable resource policy vetoes the
// request. The admin account is exempt, matching CheckPermissionWithContext.
func deniedByPolicy(operation, principal, resourcePolicy, resourceARN string, ctx *PolicyContext) bool {
	return (principal != s3_constants.AccountAdminId || !adminCaller(operation, ctx)) &&
		matchesDenyStatement(operation, principal, resourcePolicy, resourceARN, ctx)
}

// adminCaller reports whether the admin principal bypass applies. Trusted
// transports carry no action list, while an identified caller always has one
// — possibly empty — and holds the bypass only when the identity administers
// the operation's service: a principal that merely normalizes to the admin
// id does not bypass policy denies.
func adminCaller(operation string, ctx *PolicyContext) bool {
	return ctx == nil || ctx.IdentityActions == nil || hasAdminActionFor(operation, ctx.IdentityActions)
}

// matchesDenyStatement reports whether the JSON policy carries a statement
// matching this request with Effect Deny. Malformed policies return false;
// the caller's normal evaluation still denies them. A Deny whose condition
// cannot be evaluated applies: it was written to protect the resource, and
// dropping it would widen access.
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
			!matchesDenyCaller(stmt.Principal, principal, ctx) ||
			!matchesAction(stmt.Action, fullAction) ||
			(resourceARN != "" && !matchesResource(stmt.Resource, resourceARN)) {
			continue
		}
		matched, usable := matchesConditions(stmt.Condition, ctx)
		if !usable || stmt.splitCondition {
			glog.Warningf("S3Tables: policy on %q has a Deny condition that cannot be evaluated; denying %s for %s until the policy is replaced", resourceARN, fullAction, principal)
			return true
		}
		if matched {
			return true
		}
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

		// A condition that cannot be evaluated never grants, nor does a null
		// or split one stored before validatePolicyDocument refused it.
		if stmt.Effect == "Allow" && (stmt.nullCondition || stmt.splitCondition) {
			continue
		}
		if matched, usable := matchesConditions(stmt.Condition, ctx); !usable || !matched {
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
	if hasAdminActionFor(operation, ctx.IdentityActions) {
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

// hasAdminActionFor reports whether the action list administers the
// operation's own service: the global admin markers always apply, while a
// service wildcard covers only that service — s3tables:* never reaches an
// s3vectors operation.
func hasAdminActionFor(operation string, actions []string) bool {
	if !strings.Contains(operation, ":") {
		operation = "s3tables:" + operation
	}
	serviceWildcard := operation[:strings.Index(operation, ":")] + ":*"
	for _, action := range actions {
		if action == "*" || action == s3_constants.ACTION_ADMIN || action == "s3:*" || action == serviceWildcard {
			return true
		}
	}
	return false
}

// matchesCaller matches a statement's Principal against the caller's scoped
// principal or, for policies written before issuer scoping, a bare subject
// that resolves to it under the deployment's single trusted issuer.
func matchesCaller(principalSpec interface{}, principal string, ctx *PolicyContext) bool {
	return matchCaller(principalSpec, principal, ctx, false)
}

// matchesDenyCaller is matchesCaller for deny evaluation: an ARN-scoped
// statement also reaches a caller stored under the ARN's trailing name, so a
// deny cannot be ducked through normalization the way an allow must not grant
// through it.
func matchesDenyCaller(principalSpec interface{}, principal string, ctx *PolicyContext) bool {
	return matchCaller(principalSpec, principal, ctx, true)
}

func matchCaller(principalSpec interface{}, principal string, ctx *PolicyContext, forDeny bool) bool {
	if matchPrincipal(principalSpec, principal, forDeny) {
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
			return matchCaller(aws, principal, ctx, forDeny)
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
	return matchPrincipal(principalSpec, principal, false)
}

func matchPrincipal(principalSpec interface{}, principal string, forDeny bool) bool {
	if principalSpec == nil {
		return false
	}

	switch p := principalSpec.(type) {
	case string:
		return matchPrincipalString(p, principal, forDeny)
	case []interface{}:
		// Array of principals
		for _, item := range p {
			if str, ok := item.(string); ok && matchPrincipalString(str, principal, forDeny) {
				return true
			}
		}
	case map[string]interface{}:
		// AWS-style principal with service prefix, e.g., {"AWS": "arn:aws:iam::..."}
		// For S3 Tables, we primarily care about the AWS key
		if aws, ok := p["AWS"]; ok {
			return matchPrincipal(aws, principal, forDeny)
		}
	}

	return false
}

// matchPrincipalString reports whether a single principal spec matches the
// caller. An ARN-scoped statement reaches a caller recorded under the ARN's
// trailing name only for denies: normalization drops the ARN's account and
// path, so an allow matched that way could grant a same-named caller the
// statement never verified, while a deny must still land so it cannot be
// ducked through the same collapse.
func matchPrincipalString(spec, principal string, forDeny bool) bool {
	if spec == "*" || spec == principal {
		return true
	}
	if wildcard.MatchesWildcard(spec, principal) {
		return true
	}
	return forDeny && strings.HasPrefix(spec, "arn:") && !strings.HasPrefix(principal, "arn:") &&
		wildcard.MatchesWildcard(normalizePrincipalID(spec), principal)
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

// matchesConditions reports whether every condition holds for ctx, and
// whether the block can be evaluated at all: one unsupported operator, keyless
// operator or uncomparable value leaves it unusable, and the caller decides
// what that means for the statement's Effect. Every operator is checked
// before answering, so the verdict does not depend on map iteration order.
func matchesConditions(conditions map[string]map[string]interface{}, ctx *PolicyContext) (matched, usable bool) {
	if len(conditions) == 0 {
		return true, true
	}
	matched = true
	for operator, conditionValues := range conditions {
		ok, valid := matchesConditionOperator(operator, conditionValues, ctx)
		if !valid {
			return false, false
		}
		matched = matched && ok
	}
	return matched, true
}

func matchesConditionOperator(operator string, conditionValues map[string]interface{}, ctx *PolicyContext) (matched, usable bool) {
	evaluator, err := policy_engine.GetConditionEvaluator(operator)
	if err != nil {
		return false, false
	}
	// With no keys the loop below would hold for every request; a stored
	// keyless operator is unusable, so it never lets an Allow grant.
	if len(conditionValues) == 0 {
		return false, false
	}
	for _, value := range conditionValues {
		if !ConditionValue(operator, value) {
			return false, false
		}
	}
	if ctx == nil {
		return false, true
	}

	for key, value := range conditionValues {
		contextVals := getConditionContextValues(key, ctx)
		if !evaluator.Evaluate(value, contextVals) {
			return false, true
		}
	}
	return true, true
}

// tableConditionKey reports whether getConditionContextValues supplies key.
func tableConditionKey(key string) bool {
	switch key {
	case "s3tables:namespace", "s3tables:tableName", "s3tables:tableBucketName",
		"s3tables:SSEAlgorithm", "s3tables:KMSKeyArn", "s3tables:StorageClass", "aws:TagKeys":
		return true
	}
	for _, prefix := range []string{"aws:RequestTag/", "aws:ResourceTag/", "s3tables:TableBucketTag/", "s3vectors:VectorBucketTag/"} {
		if tagKey, ok := strings.CutPrefix(key, prefix); ok {
			return tagKey != ""
		}
	}
	return false
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
	tagKey, ok := strings.CutPrefix(key, "s3tables:TableBucketTag/")
	if !ok {
		tagKey, ok = strings.CutPrefix(key, "s3vectors:VectorBucketTag/")
	}
	if ok {
		if val, found := ctx.TableBucketTags[tagKey]; found {
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
