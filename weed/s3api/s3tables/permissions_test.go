package s3tables

import (
	"encoding/json"
	"testing"
)

func TestMatchesActionPattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		action   string
		expected bool
	}{
		// Exact matches
		{"exact match", "GetTable", "GetTable", true},
		{"no match", "GetTable", "DeleteTable", false},

		// Universal wildcard
		{"universal wildcard", "*", "anything", true},

		// Suffix wildcards
		{"suffix wildcard match", "s3tables:*", "s3tables:GetTable", true},
		{"suffix wildcard no match", "s3tables:*", "iam:GetUser", false},

		// Middle wildcards (new capability from policy_engine)
		{"middle wildcard Get*Table", "s3tables:Get*Table", "s3tables:GetTable", true},
		{"middle wildcard Get*Table no match GetTableBucket", "s3tables:Get*Table", "s3tables:GetTableBucket", false},
		{"middle wildcard Get*Table no match DeleteTable", "s3tables:Get*Table", "s3tables:DeleteTable", false},
		{"middle wildcard *Table*", "s3tables:*Table*", "s3tables:GetTableBucket", true},
		{"middle wildcard *Table* match CreateTable", "s3tables:*Table*", "s3tables:CreateTable", true},

		// Question mark wildcards
		{"question mark single char", "GetTable?", "GetTableX", true},
		{"question mark no match", "GetTable?", "GetTableXY", false},

		// Combined wildcards
		{"combined * and ? singular", "s3tables:Get?able*", "s3tables:GetTable", true},
		{"combined * and ? plural", "s3tables:Get?able*", "s3tables:GetTables", true},
		{"combined no match - ? needs 1 char", "s3tables:Get?able*", "s3tables:Getable", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesActionPattern(tt.pattern, tt.action)
			if result != tt.expected {
				t.Errorf("matchesActionPattern(%q, %q) = %v, want %v", tt.pattern, tt.action, result, tt.expected)
			}
		})
	}
}

func TestMatchesPrincipal(t *testing.T) {
	tests := []struct {
		name          string
		principalSpec interface{}
		principal     string
		expected      bool
	}{
		// String principals
		{"exact match", "user123", "user123", true},
		{"no match", "user123", "user456", false},
		{"universal wildcard", "*", "anyone", true},

		// Wildcard principals
		{"prefix wildcard", "arn:aws:iam::123456789012:user/*", "arn:aws:iam::123456789012:user/admin", true},
		{"prefix wildcard no match", "arn:aws:iam::123456789012:user/*", "arn:aws:iam::987654321098:user/admin", false},
		{"middle wildcard", "arn:aws:iam::*:user/admin", "arn:aws:iam::123456789012:user/admin", true},

		// Array of principals
		{"array match first", []interface{}{"user1", "user2"}, "user1", true},
		{"array match second", []interface{}{"user1", "user2"}, "user2", true},
		{"array no match", []interface{}{"user1", "user2"}, "user3", false},
		{"array wildcard", []interface{}{"user1", "arn:aws:iam::*:user/admin"}, "arn:aws:iam::123:user/admin", true},

		// Map-style AWS principals
		{"AWS map exact", map[string]interface{}{"AWS": "user123"}, "user123", true},
		{"AWS map wildcard", map[string]interface{}{"AWS": "arn:aws:iam::*:user/admin"}, "arn:aws:iam::123:user/admin", true},
		{"AWS map array", map[string]interface{}{"AWS": []interface{}{"user1", "user2"}}, "user1", true},

		// Nil/empty cases
		{"nil principal", nil, "user123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesPrincipal(tt.principalSpec, tt.principal)
			if result != tt.expected {
				t.Errorf("matchesPrincipal(%v, %q) = %v, want %v", tt.principalSpec, tt.principal, result, tt.expected)
			}
		})
	}
}

func TestEvaluatePolicyWithConditions(t *testing.T) {
	policy := &PolicyDocument{
		Statement: []Statement{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3tables:GetTable",
				Condition: map[string]map[string]interface{}{
					"StringEquals": {
						"s3tables:namespace": "default",
					},
					"StringLike": {
						"s3tables:tableName": "test_*",
					},
					"NumericGreaterThan": {
						"aws:RequestTag/priority": "10",
					},
					"Bool": {
						"aws:ResourceTag/is_public": "true",
					},
				},
			},
		},
	}
	policyBytes, _ := json.Marshal(policy)
	policyStr := string(policyBytes)

	tests := []struct {
		name     string
		ctx      *PolicyContext
		expected bool
	}{
		{
			"all conditions match",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			true,
		},
		{
			"namespace mismatch",
			&PolicyContext{
				Namespace: "other",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"table name mismatch",
			&PolicyContext{
				Namespace: "default",
				TableName: "other_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"numeric condition failure",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "5",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"bool condition failure",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "false",
				},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// principal="user123", owner="owner123"
			result := CheckPermissionWithContext("s3tables:GetTable", "user123", "owner123", policyStr, "", tt.ctx)
			if result != tt.expected {
				t.Errorf("CheckPermissionWithContext() = %v, want %v", result, tt.expected)
			}
		})
	}
}
func TestCheckPermissionWithDefaultAllow(t *testing.T) {
	tests := []struct {
		name         string
		defaultAllow bool
		policy       string
		expected     bool
	}{
		{
			"default deny (no policy, DefaultAllow=false)",
			false,
			"",
			false,
		},
		{
			"default allow (no policy, DefaultAllow=true)",
			true,
			"",
			true,
		},
		{
			"explicit deny overrides DefaultAllow=true",
			true,
			`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable"}]}`,
			false,
		},
		{
			"explicit allow works with DefaultAllow=false",
			false,
			`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable"}]}`,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &PolicyContext{
				DefaultAllow: tt.defaultAllow,
			}
			result := CheckPermissionWithContext("s3tables:GetTable", "user123", "owner123", tt.policy, "", ctx)
			if result != tt.expected {
				t.Errorf("CheckPermissionWithContext() = %v, want %v (DefaultAllow=%v, Policy=%s)", result, tt.expected, tt.defaultAllow, tt.policy)
			}
		})
	}
}
