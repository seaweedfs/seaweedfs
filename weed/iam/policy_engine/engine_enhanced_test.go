package policy_engine

import (
	"testing"
)

func TestExtractPrincipalVariables(t *testing.T) {
	tests := []struct {
		name      string
		principal string
		expected  map[string][]string
	}{
		{
			name:      "IAM User ARN",
			principal: "arn:aws:iam::123456789012:user/alice",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"IAMUser"},
				"aws:username":         {"alice"},
				"aws:userid":           {"alice"},
			},
		},
		{
			name:      "Assumed Role ARN",
			principal: "arn:aws:sts::123456789012:assumed-role/MyRole/session-alice",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"AssumedRole"},
				"aws:username":         {"session-alice"},
				"aws:userid":           {"session-alice"},
			},
		},
		{
			name:      "IAM Role ARN",
			principal: "arn:aws:iam::123456789012:role/MyRole",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"IAMRole"},
				"aws:username":         {"MyRole"},
			},
		},
		{
			name:      "Non-ARN principal",
			principal: "user:alice",
			expected:  map[string][]string{},
		},
		{
			name:      "Wildcard principal",
			principal: "*",
			expected:  map[string][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractPrincipalVariables(tt.principal)

			// Check that all expected keys are present with correct values
			for key, expectedValues := range tt.expected {
				actualValues, ok := result[key]
				if !ok {
					t.Errorf("Expected key %s not found in result", key)
					continue
				}

				if len(actualValues) != len(expectedValues) {
					t.Errorf("For key %s: expected %d values, got %d", key, len(expectedValues), len(actualValues))
					continue
				}

				for i, expectedValue := range expectedValues {
					if actualValues[i] != expectedValue {
						t.Errorf("For key %s[%d]: expected %s, got %s", key, i, expectedValue, actualValues[i])
					}
				}
			}

			// Check that there are no unexpected keys
			for key := range result {
				if _, ok := tt.expected[key]; !ok {
					t.Errorf("Unexpected key %s in result", key)
				}
			}
		})
	}
}

func TestSubstituteVariablesWithClaims(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		context  map[string][]string
		claims   map[string]interface{}
		expected string
	}{
		{
			name:    "Standard context variable",
			pattern: "arn:aws:s3:::bucket/${aws:username}/*",
			context: map[string][]string{
				"aws:username": {"alice"},
			},
			claims:   nil,
			expected: "arn:aws:s3:::bucket/alice/*",
		},
		{
			name:    "JWT claim substitution",
			pattern: "arn:aws:s3:::bucket/${jwt:preferred_username}/*",
			context: map[string][]string{},
			claims: map[string]interface{}{
				"preferred_username": "bob",
			},
			expected: "arn:aws:s3:::bucket/bob/*",
		},
		{
			name:    "Mixed variables",
			pattern: "arn:aws:s3:::bucket/${jwt:sub}/files/${aws:principaltype}",
			context: map[string][]string{
				"aws:principaltype": {"IAMUser"},
			},
			claims: map[string]interface{}{
				"sub": "user123",
			},
			expected: "arn:aws:s3:::bucket/user123/files/IAMUser",
		},
		{
			name:     "Variable not found",
			pattern:  "arn:aws:s3:::bucket/${jwt:missing}/*",
			context:  map[string][]string{},
			claims:   map[string]interface{}{},
			expected: "arn:aws:s3:::bucket/${jwt:missing}/*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SubstituteVariables(tt.pattern, tt.context, tt.claims)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestPolicyVariablesWithPrincipalType(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy that requires specific principal type
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": "s3:*",
			"Resource": "arn:aws:s3:::bucket/*",
			"Condition": {
				"StringEquals": {
					"aws:principaltype": "IAMUser"
				}
			}
		}]
	}`

	err := engine.SetBucketPolicy("bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Test with IAM User - should allow
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::bucket/file.txt",
		Principal: "arn:aws:iam::123456789012:user/alice",
		Conditions: map[string][]string{
			"aws:principaltype": {"IAMUser"},
			"aws:username":      {"alice"},
			"aws:userid":        {"alice"},
		},
	}

	result := engine.EvaluatePolicy("bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow for IAMUser principal, got %v", result)
	}

	// Test with AssumedRole - should return Indeterminate (condition doesn't match)
	args.Principal = "arn:aws:sts::123456789012:assumed-role/MyRole/session"
	args.Conditions["aws:principaltype"] = []string{"AssumedRole"}

	result = engine.EvaluatePolicy("bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate for AssumedRole principal, got %v", result)
	}
}

func TestPolicyVariablesWithJWTClaims(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy using JWT claim in resource
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": "s3:*",
			"Resource": "arn:aws:s3:::bucket/${jwt:preferred_username}/*"
		}]
	}`

	err := engine.SetBucketPolicy("bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Test with matching JWT claim
	args := &PolicyEvaluationArgs{
		Action:     "s3:GetObject",
		Resource:   "arn:aws:s3:::bucket/alice/file.txt",
		Principal:  "arn:aws:iam::123456789012:user/alice",
		Conditions: map[string][]string{},
		Claims: map[string]interface{}{
			"preferred_username": "alice",
		},
	}

	result := engine.EvaluatePolicy("bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow when JWT claim matches resource, got %v", result)
	}

	// Test with mismatched JWT claim
	args.Resource = "arn:aws:s3:::bucket/bob/file.txt"

	result = engine.EvaluatePolicy("bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate when JWT claim doesn't match resource, got %v", result)
	}
}

func TestExtractPrincipalVariablesWithAccount(t *testing.T) {
	principal := "arn:aws:iam::123456789012:user/alice"
	vars := ExtractPrincipalVariables(principal)

	if account, ok := vars["aws:PrincipalAccount"]; !ok {
		t.Errorf("Expected aws:PrincipalAccount to be present")
	} else if len(account) == 0 {
		t.Errorf("Expected aws:PrincipalAccount to have values")
	} else if account[0] != "123456789012" {
		t.Errorf("Expected aws:PrincipalAccount=123456789012, got %v", account[0])
	}
}

func TestSubstituteVariablesWithLDAP(t *testing.T) {
	pattern := "arn:aws:s3:::bucket/${ldap:username}/*"
	context := map[string][]string{}
	claims := map[string]interface{}{
		"username": "jdoe",
	}

	result := SubstituteVariables(pattern, context, claims)
	expected := "arn:aws:s3:::bucket/jdoe/*"

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	// Test ldap:dn
	pattern = "arn:aws:s3:::bucket/${ldap:dn}/*"
	claims = map[string]interface{}{
		"dn": "uid=jdoe,ou=people,dc=example,dc=com",
	}
	result = SubstituteVariables(pattern, context, claims)
	expected = "arn:aws:s3:::bucket/uid=jdoe,ou=people,dc=example,dc=com/*"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestSubstituteVariablesSpecialChars(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		context  map[string][]string
		claims   map[string]interface{}
		expected string
	}{
		{
			name:    "Comparison operators in claims/vars",
			pattern: "resource/${jwt:scope}",
			context: map[string][]string{},
			claims: map[string]interface{}{
				"scope": "read/write",
			},
			expected: "resource/read/write",
		},
		{
			name:    "Path traversal attempt (should just substitute text)",
			pattern: "bucket/${jwt:user}",
			context: map[string][]string{},
			claims: map[string]interface{}{
				"user": "../../../etc/passwd",
			},
			expected: "bucket/../../../etc/passwd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SubstituteVariables(tt.pattern, tt.context, tt.claims)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
