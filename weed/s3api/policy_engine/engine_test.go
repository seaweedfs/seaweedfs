package policy_engine

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// tagsToEntry converts a map of tag key-value pairs to the entry.Extended format
// used for s3:ExistingObjectTag/<key> condition evaluation
func tagsToEntry(tags map[string]string) map[string][]byte {
	if tags == nil {
		return nil
	}
	entry := make(map[string][]byte)
	for k, v := range tags {
		entry[s3_constants.AmzObjectTaggingPrefix+k] = []byte(v)
	}
	return entry
}

func TestPolicyEngine(t *testing.T) {
	engine := NewPolicyEngine()

	// Test policy JSON
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": ["s3:GetObject", "s3:PutObject"],
				"Resource": ["arn:aws:s3:::test-bucket/*"]
			},
			{
				"Effect": "Deny",
				"Action": ["s3:DeleteObject"],
				"Resource": ["arn:aws:s3:::test-bucket/*"],
				"Condition": {
					"StringEquals": {
						"s3:RequestMethod": ["DELETE"]
					}
				}
			}
		]
	}`

	// Set bucket policy
	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Test Allow case
	args := &PolicyEvaluationArgs{
		Action:     "s3:GetObject",
		Resource:   "arn:aws:s3:::test-bucket/test-object",
		Principal:  "user1",
		Conditions: map[string][]string{},
	}

	result := engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow, got %v", result)
	}

	// Test Deny case
	args = &PolicyEvaluationArgs{
		Action:    "s3:DeleteObject",
		Resource:  "arn:aws:s3:::test-bucket/test-object",
		Principal: "user1",
		Conditions: map[string][]string{
			"s3:RequestMethod": {"DELETE"},
		},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultDeny {
		t.Errorf("Expected Deny, got %v", result)
	}

	// Test non-matching action
	args = &PolicyEvaluationArgs{
		Action:     "s3:ListBucket",
		Resource:   "arn:aws:s3:::test-bucket",
		Principal:  "user1",
		Conditions: map[string][]string{},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate for non-matching action (should fall through to IAM), got %v", result)
	}

	// Test GetBucketPolicy
	policy, err := engine.GetBucketPolicy("test-bucket")
	if err != nil {
		t.Fatalf("Failed to get bucket policy: %v", err)
	}
	if policy.Version != "2012-10-17" {
		t.Errorf("Expected version 2012-10-17, got %s", policy.Version)
	}

	// Test DeleteBucketPolicy
	err = engine.DeleteBucketPolicy("test-bucket")
	if err != nil {
		t.Fatalf("Failed to delete bucket policy: %v", err)
	}

	// Test policy is gone
	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate after policy deletion, got %v", result)
	}
}

func TestConditionEvaluators(t *testing.T) {
	tests := []struct {
		name           string
		operator       string
		conditionValue interface{}
		contextValues  []string
		expected       bool
	}{
		{
			name:           "StringEquals - match",
			operator:       "StringEquals",
			conditionValue: "test-value",
			contextValues:  []string{"test-value"},
			expected:       true,
		},
		{
			name:           "StringEquals - no match",
			operator:       "StringEquals",
			conditionValue: "test-value",
			contextValues:  []string{"other-value"},
			expected:       false,
		},
		{
			name:           "StringLike - wildcard match",
			operator:       "StringLike",
			conditionValue: "test-*",
			contextValues:  []string{"test-value"},
			expected:       true,
		},
		{
			name:           "StringLike - wildcard no match",
			operator:       "StringLike",
			conditionValue: "test-*",
			contextValues:  []string{"other-value"},
			expected:       false,
		},
		{
			name:           "NumericEquals - match",
			operator:       "NumericEquals",
			conditionValue: "42",
			contextValues:  []string{"42"},
			expected:       true,
		},
		{
			name:           "NumericLessThan - match",
			operator:       "NumericLessThan",
			conditionValue: "100",
			contextValues:  []string{"50"},
			expected:       true,
		},
		{
			name:           "NumericLessThan - no match",
			operator:       "NumericLessThan",
			conditionValue: "100",
			contextValues:  []string{"150"},
			expected:       false,
		},
		{
			name:           "IpAddress - CIDR match",
			operator:       "IpAddress",
			conditionValue: "192.168.1.0/24",
			contextValues:  []string{"192.168.1.100"},
			expected:       true,
		},
		{
			name:           "IpAddress - CIDR no match",
			operator:       "IpAddress",
			conditionValue: "192.168.1.0/24",
			contextValues:  []string{"10.0.0.1"},
			expected:       false,
		},
		{
			name:           "Bool - true match",
			operator:       "Bool",
			conditionValue: "true",
			contextValues:  []string{"true"},
			expected:       true,
		},
		{
			name:           "Bool - false match",
			operator:       "Bool",
			conditionValue: "false",
			contextValues:  []string{"false"},
			expected:       true,
		},
		{
			name:           "Bool - no match",
			operator:       "Bool",
			conditionValue: "true",
			contextValues:  []string{"false"},
			expected:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := GetConditionEvaluator(tt.operator)
			if err != nil {
				t.Fatalf("Failed to get condition evaluator: %v", err)
			}

			result := evaluator.Evaluate(tt.conditionValue, tt.contextValues)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestConvertIdentityToPolicy(t *testing.T) {
	identityActions := []string{
		"Read:bucket1/*",
		"Write:bucket1/*",
		"Admin:bucket2",
	}

	policy, err := ConvertIdentityToPolicy(identityActions, "bucket1")
	if err != nil {
		t.Fatalf("Failed to convert identity to policy: %v", err)
	}

	if policy.Version != "2012-10-17" {
		t.Errorf("Expected version 2012-10-17, got %s", policy.Version)
	}

	if len(policy.Statement) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(policy.Statement))
	}

	// Check first statement (Read)
	stmt := policy.Statement[0]
	if stmt.Effect != PolicyEffectAllow {
		t.Errorf("Expected Allow effect, got %s", stmt.Effect)
	}

	actions := normalizeToStringSlice(stmt.Action)
	if len(actions) != 3 {
		t.Errorf("Expected 3 read actions, got %d", len(actions))
	}

	resources := normalizeToStringSlice(stmt.Resource)
	if len(resources) != 2 {
		t.Errorf("Expected 2 resources, got %d", len(resources))
	}
}

func TestPolicyValidation(t *testing.T) {
	tests := []struct {
		name        string
		policyJSON  string
		expectError bool
	}{
		{
			name: "Valid policy",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Action": "s3:GetObject",
						"Resource": "arn:aws:s3:::test-bucket/*"
					}
				]
			}`,
			expectError: false,
		},
		{
			name: "Invalid version",
			policyJSON: `{
				"Version": "2008-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Action": "s3:GetObject",
						"Resource": "arn:aws:s3:::test-bucket/*"
					}
				]
			}`,
			expectError: true,
		},
		{
			name: "Missing action",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Resource": "arn:aws:s3:::test-bucket/*"
					}
				]
			}`,
			expectError: true,
		},
		{
			name: "Invalid JSON",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Action": "s3:GetObject",
						"Resource": "arn:aws:s3:::test-bucket/*"
					}
				]
			}extra`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePolicy(tt.policyJSON)
			if (err != nil) != tt.expectError {
				t.Errorf("Expected error: %v, got error: %v", tt.expectError, err)
			}
		})
	}
}

func TestPatternMatching(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		value    string
		expected bool
	}{
		{
			name:     "Exact match",
			pattern:  "s3:GetObject",
			value:    "s3:GetObject",
			expected: true,
		},
		{
			name:     "Wildcard match",
			pattern:  "s3:Get*",
			value:    "s3:GetObject",
			expected: true,
		},
		{
			name:     "Wildcard no match",
			pattern:  "s3:Put*",
			value:    "s3:GetObject",
			expected: false,
		},
		{
			name:     "Full wildcard",
			pattern:  "*",
			value:    "anything",
			expected: true,
		},
		{
			name:     "Question mark wildcard",
			pattern:  "s3:GetObjec?",
			value:    "s3:GetObject",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled, err := compilePattern(tt.pattern)
			if err != nil {
				t.Fatalf("Failed to compile pattern %s: %v", tt.pattern, err)
			}

			result := compiled.MatchString(tt.value)
			if result != tt.expected {
				t.Errorf("Pattern %s against %s: expected %v, got %v", tt.pattern, tt.value, tt.expected, result)
			}
		})
	}
}

func TestExtractConditionValuesFromRequest(t *testing.T) {
	// Create a test request
	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path:     "/test-bucket/test-object",
			RawQuery: "prefix=test&delimiter=/",
		},
		Header: map[string][]string{
			"User-Agent":        {"test-agent"},
			"X-Amz-Copy-Source": {"source-bucket/source-object"},
		},
		RemoteAddr: "192.168.1.100:12345",
	}

	values := ExtractConditionValuesFromRequest(req)

	// Check extracted values
	if len(values["aws:SourceIp"]) != 1 || values["aws:SourceIp"][0] != "192.168.1.100" {
		t.Errorf("Expected SourceIp to be 192.168.1.100, got %v", values["aws:SourceIp"])
	}

	if len(values["aws:UserAgent"]) != 1 || values["aws:UserAgent"][0] != "test-agent" {
		t.Errorf("Expected UserAgent to be test-agent, got %v", values["aws:UserAgent"])
	}

	if len(values["s3:prefix"]) != 1 || values["s3:prefix"][0] != "test" {
		t.Errorf("Expected prefix to be test, got %v", values["s3:prefix"])
	}

	if len(values["s3:delimiter"]) != 1 || values["s3:delimiter"][0] != "/" {
		t.Errorf("Expected delimiter to be /, got %v", values["s3:delimiter"])
	}

	if len(values["s3:RequestMethod"]) != 1 || values["s3:RequestMethod"][0] != "GET" {
		t.Errorf("Expected RequestMethod to be GET, got %v", values["s3:RequestMethod"])
	}

	if len(values["x-amz-copy-source"]) != 1 || values["x-amz-copy-source"][0] != "source-bucket/source-object" {
		t.Errorf("Expected X-Amz-Copy-Source header to be extracted, got %v", values["x-amz-copy-source"])
	}

	// Check that aws:CurrentTime is properly set
	if len(values["aws:CurrentTime"]) != 1 {
		t.Errorf("Expected aws:CurrentTime to be set, got %v", values["aws:CurrentTime"])
	}

	// Check that aws:RequestTime is still available for backward compatibility
	if len(values["aws:RequestTime"]) != 1 {
		t.Errorf("Expected aws:RequestTime to be set for backward compatibility, got %v", values["aws:RequestTime"])
	}
}

func TestPolicyEvaluationWithConditions(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy with IP condition
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"IpAddress": {
						"aws:SourceIp": "192.168.1.0/24"
					}
				}
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Test matching IP
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::test-bucket/test-object",
		Principal: "user1",
		Conditions: map[string][]string{
			"aws:SourceIp": {"192.168.1.100"},
		},
	}

	result := engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow for matching IP, got %v", result)
	}

	// Test non-matching IP
	args.Conditions["aws:SourceIp"] = []string{"10.0.0.1"}
	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate for non-matching IP (should fall through to IAM), got %v", result)
	}
}

func TestResourceArn(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		objectName string
		expected   string
	}{
		{
			name:       "Bucket only",
			bucketName: "test-bucket",
			objectName: "",
			expected:   "arn:aws:s3:::test-bucket",
		},
		{
			name:       "Bucket and object",
			bucketName: "test-bucket",
			objectName: "test-object",
			expected:   "arn:aws:s3:::test-bucket/test-object",
		},
		{
			name:       "Bucket and nested object",
			bucketName: "test-bucket",
			objectName: "folder/subfolder/test-object",
			expected:   "arn:aws:s3:::test-bucket/folder/subfolder/test-object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildResourceArn(tt.bucketName, tt.objectName)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestActionConversion(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		expected string
	}{
		{
			name:     "Already has s3 prefix",
			action:   "s3:GetObject",
			expected: "s3:GetObject",
		},
		{
			name:     "Add s3 prefix",
			action:   "GetObject",
			expected: "s3:GetObject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildActionName(tt.action)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestPolicyEngineForRequest(t *testing.T) {
	engine := NewPolicyEngine()

	// Set up a policy
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:RequestMethod": "GET"
					}
				}
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Create test request
	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path: "/test-bucket/test-object",
		},
		Header:     make(map[string][]string),
		RemoteAddr: "192.168.1.100:12345",
	}

	// Test the request
	result := engine.EvaluatePolicyForRequest("test-bucket", "test-object", "GetObject", "user1", req)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow for matching request, got %v", result)
	}
}

func TestWildcardMatching(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{
			name:     "Exact match",
			pattern:  "test",
			str:      "test",
			expected: true,
		},
		{
			name:     "Single wildcard",
			pattern:  "*",
			str:      "anything",
			expected: true,
		},
		{
			name:     "Prefix wildcard",
			pattern:  "test*",
			str:      "test123",
			expected: true,
		},
		{
			name:     "Suffix wildcard",
			pattern:  "*test",
			str:      "123test",
			expected: true,
		},
		{
			name:     "Middle wildcard",
			pattern:  "test*123",
			str:      "testABC123",
			expected: true,
		},
		{
			name:     "No match",
			pattern:  "test*",
			str:      "other",
			expected: false,
		},
		{
			name:     "Multiple wildcards",
			pattern:  "test*abc*123",
			str:      "testXYZabcDEF123",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesWildcard(tt.pattern, tt.str)
			if result != tt.expected {
				t.Errorf("Pattern %s against %s: expected %v, got %v", tt.pattern, tt.str, tt.expected, result)
			}
		})
	}
}

func TestCompilePolicy(t *testing.T) {
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": ["s3:GetObject", "s3:PutObject"],
				"Resource": "arn:aws:s3:::test-bucket/*"
			}
		]
	}`

	policy, err := ParsePolicy(policyJSON)
	if err != nil {
		t.Fatalf("Failed to parse policy: %v", err)
	}

	compiled, err := CompilePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to compile policy: %v", err)
	}

	if len(compiled.Statements) != 1 {
		t.Errorf("Expected 1 compiled statement, got %d", len(compiled.Statements))
	}

	stmt := compiled.Statements[0]
	if len(stmt.ActionPatterns) != 2 {
		t.Errorf("Expected 2 action patterns, got %d", len(stmt.ActionPatterns))
	}

	if len(stmt.ResourcePatterns) != 1 {
		t.Errorf("Expected 1 resource pattern, got %d", len(stmt.ResourcePatterns))
	}
}

// TestNewPolicyBackedIAMWithLegacy tests the constructor overload
func TestNewPolicyBackedIAMWithLegacy(t *testing.T) {
	// Mock legacy IAM
	mockLegacyIAM := &MockLegacyIAM{}

	// Test the new constructor
	policyBackedIAM := NewPolicyBackedIAMWithLegacy(mockLegacyIAM)

	// Verify that the legacy IAM is set
	if policyBackedIAM.legacyIAM != mockLegacyIAM {
		t.Errorf("Expected legacy IAM to be set, but it wasn't")
	}

	// Verify that the policy engine is initialized
	if policyBackedIAM.policyEngine == nil {
		t.Errorf("Expected policy engine to be initialized, but it wasn't")
	}

	// Compare with the traditional approach
	traditionalIAM := NewPolicyBackedIAM()
	traditionalIAM.SetLegacyIAM(mockLegacyIAM)

	// Both should behave the same
	if policyBackedIAM.legacyIAM != traditionalIAM.legacyIAM {
		t.Errorf("Expected both approaches to result in the same legacy IAM")
	}
}

// MockLegacyIAM implements the LegacyIAM interface for testing
type MockLegacyIAM struct{}

func (m *MockLegacyIAM) authRequest(r *http.Request, action Action) (Identity, s3err.ErrorCode) {
	return nil, s3err.ErrNone
}

// TestExistingObjectTagCondition tests s3:ExistingObjectTag/<tag-key> condition support
func TestExistingObjectTagCondition(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy that allows GetObject only for objects with specific tag
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:ExistingObjectTag/status": ["public"]
					}
				}
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	tests := []struct {
		name       string
		objectTags map[string]string
		expected   PolicyEvaluationResult
	}{
		{
			name:       "Matching tag value - should allow",
			objectTags: map[string]string{"status": "public"},
			expected:   PolicyResultAllow,
		},
		{
			name:       "Non-matching tag value - should be indeterminate",
			objectTags: map[string]string{"status": "private"},
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "Missing tag - should be indeterminate",
			objectTags: map[string]string{"other": "value"},
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "No tags - should be indeterminate",
			objectTags: nil,
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "Empty tags - should be indeterminate",
			objectTags: map[string]string{},
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "Multiple tags with matching one - should allow",
			objectTags: map[string]string{"status": "public", "owner": "admin"},
			expected:   PolicyResultAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := &PolicyEvaluationArgs{
				Action:      "s3:GetObject",
				Resource:   "arn:aws:s3:::test-bucket/test-object",
				Principal:  "*",
				ObjectEntry: tagsToEntry(tt.objectTags),
			}

			result := engine.EvaluatePolicy("test-bucket", args)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestExistingObjectTagConditionMultipleTags tests policies with multiple tag conditions
func TestExistingObjectTagConditionMultipleTags(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy that requires multiple tag conditions
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:ExistingObjectTag/status": ["public"],
						"s3:ExistingObjectTag/tier": ["free", "premium"]
					}
				}
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	tests := []struct {
		name       string
		objectTags map[string]string
		expected   PolicyEvaluationResult
	}{
		{
			name:       "Both tags match - should allow",
			objectTags: map[string]string{"status": "public", "tier": "free"},
			expected:   PolicyResultAllow,
		},
		{
			name:       "Both tags match (premium tier) - should allow",
			objectTags: map[string]string{"status": "public", "tier": "premium"},
			expected:   PolicyResultAllow,
		},
		{
			name:       "Only status matches - should be indeterminate",
			objectTags: map[string]string{"status": "public"},
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "Only tier matches - should be indeterminate",
			objectTags: map[string]string{"tier": "free"},
			expected:   PolicyResultIndeterminate,
		},
		{
			name:       "Neither tag matches - should be indeterminate",
			objectTags: map[string]string{"status": "private", "tier": "basic"},
			expected:   PolicyResultIndeterminate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := &PolicyEvaluationArgs{
				Action:      "s3:GetObject",
				Resource:   "arn:aws:s3:::test-bucket/test-object",
				Principal:  "*",
				ObjectEntry: tagsToEntry(tt.objectTags),
			}

			result := engine.EvaluatePolicy("test-bucket", args)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestExistingObjectTagDenyPolicy tests deny policies with tag conditions
func TestExistingObjectTagDenyPolicy(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy that denies access to objects with confidential tag
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*"
			},
			{
				"Effect": "Deny",
				"Principal": "*",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/*",
				"Condition": {
					"StringEquals": {
						"s3:ExistingObjectTag/classification": ["confidential"]
					}
				}
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	tests := []struct {
		name       string
		objectTags map[string]string
		expected   PolicyEvaluationResult
	}{
		{
			name:       "No tags - allow by default statement",
			objectTags: nil,
			expected:   PolicyResultAllow,
		},
		{
			name:       "Non-confidential tag - allow",
			objectTags: map[string]string{"classification": "public"},
			expected:   PolicyResultAllow,
		},
		{
			name:       "Confidential tag - deny",
			objectTags: map[string]string{"classification": "confidential"},
			expected:   PolicyResultDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := &PolicyEvaluationArgs{
				Action:      "s3:GetObject",
				Resource:   "arn:aws:s3:::test-bucket/test-object",
				Principal:  "*",
				ObjectEntry: tagsToEntry(tt.objectTags),
			}

			result := engine.EvaluatePolicy("test-bucket", args)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
