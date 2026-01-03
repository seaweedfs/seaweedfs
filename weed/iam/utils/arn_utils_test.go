package utils

import "testing"

// TestExtractRoleNameFromArn tests the ExtractRoleNameFromArn function with
// comprehensive test cases covering:
//   - Legacy IAM role ARN format (arn:aws:iam::role/RoleName)
//   - Standard AWS IAM role ARN format (arn:aws:iam::ACCOUNT:role/RoleName)
//   - Role names with path components (e.g., role/Path/To/RoleName)
//   - Invalid and edge case ARNs (missing prefix, wrong service, empty strings)
//
// The test uses table-driven test pattern with multiple scenarios for each
// format to ensure robust handling of both legacy and modern AWS ARN formats.
func TestExtractRoleNameFromArn(t *testing.T) {
	testCases := []struct {
		name     string
		roleArn  string
		expected string
	}{
		// Legacy format (without account ID)
		{
			name:     "legacy_format_simple_role_name",
			roleArn:  "arn:aws:iam::role/default",
			expected: "default",
		},
		{
			name:     "legacy_format_custom_role_name",
			roleArn:  "arn:aws:iam::role/MyRole",
			expected: "MyRole",
		},
		{
			name:     "legacy_format_with_path",
			roleArn:  "arn:aws:iam::role/Path/MyRole",
			expected: "Path/MyRole",
		},
		{
			name:     "legacy_format_with_nested_path",
			roleArn:  "arn:aws:iam::role/Division/Team/Role",
			expected: "Division/Team/Role",
		},
		// Standard AWS format (with account ID)
		{
			name:     "standard_format_simple_role_name",
			roleArn:  "arn:aws:iam::123456789012:role/default",
			expected: "default",
		},
		{
			name:     "standard_format_custom_role_name",
			roleArn:  "arn:aws:iam::999999999999:role/MyRole",
			expected: "MyRole",
		},
		{
			name:     "standard_format_with_path",
			roleArn:  "arn:aws:iam::123456789012:role/Path/MyRole",
			expected: "Path/MyRole",
		},
		{
			name:     "standard_format_with_nested_path",
			roleArn:  "arn:aws:iam::123456789012:role/Division/Team/Role",
			expected: "Division/Team/Role",
		},
		// Edge cases and invalid formats
		{
			name:     "invalid_arn_missing_prefix",
			roleArn:  "invalid-arn",
			expected: "",
		},
		{
			name:     "invalid_arn_incomplete",
			roleArn:  "arn:aws:iam::",
			expected: "",
		},
		{
			name:     "invalid_arn_no_role_marker",
			roleArn:  "arn:aws:iam::123456789012:user/username",
			expected: "",
		},
		{
			name:     "invalid_arn_wrong_service",
			roleArn:  "arn:aws:sts::assumed-role/Role/Session",
			expected: "",
		},
		{
			name:     "empty_string",
			roleArn:  "",
			expected: "",
		},
		{
			name:     "role_marker_no_name",
			roleArn:  "arn:aws:iam::role/",
			expected: "",
		},
		{
			name:     "standard_format_role_marker_no_name",
			roleArn:  "arn:aws:iam::123456789012:role/",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractRoleNameFromArn(tc.roleArn)
			if result != tc.expected {
				t.Errorf("ExtractRoleNameFromArn(%q) = %q, want %q", tc.roleArn, result, tc.expected)
			}
		})
	}
}

// TestExtractRoleNameFromPrincipal tests the ExtractRoleNameFromPrincipal function
// with comprehensive test cases covering:
//   - STS assumed role ARN format (arn:aws:sts::assumed-role/RoleName/SessionName)
//   - Standard AWS STS format (arn:aws:sts::ACCOUNT:assumed-role/RoleName/SessionName)
//   - IAM role ARN format delegated to ExtractRoleNameFromArn
//   - Both legacy and standard IAM role formats with and without paths
//   - Invalid and edge case principals (wrong format, empty strings)
//
// The test ensures that ExtractRoleNameFromPrincipal correctly handles both
// STS temporary credentials and permanent IAM role ARNs used in different
// authentication and authorization workflows.
func TestExtractRoleNameFromPrincipal(t *testing.T) {
	testCases := []struct {
		name      string
		principal string
		expected  string
	}{
		// STS assumed role format (legacy)
		{
			name:      "sts_assumed_role_legacy",
			principal: "arn:aws:sts::assumed-role/RoleName/SessionName",
			expected:  "RoleName",
		},
		{
			name:      "sts_assumed_role_legacy_no_session",
			principal: "arn:aws:sts::assumed-role/RoleName",
			expected:  "RoleName",
		},
		// STS assumed role format (standard with account ID)
		{
			name:      "sts_assumed_role_standard",
			principal: "arn:aws:sts::123456789012:assumed-role/RoleName/SessionName",
			expected:  "RoleName",
		},
		{
			name:      "sts_assumed_role_standard_no_session",
			principal: "arn:aws:sts::123456789012:assumed-role/RoleName",
			expected:  "RoleName",
		},
		// IAM role format (legacy)
		{
			name:      "iam_role_legacy",
			principal: "arn:aws:iam::role/RoleName",
			expected:  "RoleName",
		},
		{
			name:      "iam_role_legacy_with_path",
			principal: "arn:aws:iam::role/Path/RoleName",
			expected:  "Path/RoleName",
		},
		// IAM role format (standard)
		{
			name:      "iam_role_standard",
			principal: "arn:aws:iam::123456789012:role/RoleName",
			expected:  "RoleName",
		},
		{
			name:      "iam_role_standard_with_path",
			principal: "arn:aws:iam::123456789012:role/Path/RoleName",
			expected:  "Path/RoleName",
		},
		// Invalid formats
		{
			name:      "invalid_principal",
			principal: "invalid-arn",
			expected:  "",
		},
		{
			name:      "empty_string",
			principal: "",
			expected:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractRoleNameFromPrincipal(tc.principal)
			if result != tc.expected {
				t.Errorf("ExtractRoleNameFromPrincipal(%q) = %q, want %q", tc.principal, result, tc.expected)
			}
		})
	}
}

// TestParseRoleARN tests the ParseRoleARN function with structured ARNInfo output
func TestParseRoleARN(t *testing.T) {
	testCases := []struct {
		name     string
		roleArn  string
		expected ARNInfo
	}{
		{
			name:    "legacy_format_simple_role",
			roleArn: "arn:aws:iam::role/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::role/MyRole",
				RoleName:  "MyRole",
				AccountID: "",
			},
		},
		{
			name:    "legacy_format_with_path",
			roleArn: "arn:aws:iam::role/Division/Team/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::role/Division/Team/MyRole",
				RoleName:  "Division/Team/MyRole",
				AccountID: "",
			},
		},
		{
			name:    "standard_format_simple_role",
			roleArn: "arn:aws:iam::123456789012:role/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::123456789012:role/MyRole",
				RoleName:  "MyRole",
				AccountID: "123456789012",
			},
		},
		{
			name:    "standard_format_with_path",
			roleArn: "arn:aws:iam::999999999999:role/Path/To/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::999999999999:role/Path/To/MyRole",
				RoleName:  "Path/To/MyRole",
				AccountID: "999999999999",
			},
		},
		{
			name:    "invalid_arn_missing_prefix",
			roleArn: "invalid-arn",
			expected: ARNInfo{
				Original:  "invalid-arn",
				RoleName:  "",
				AccountID: "",
			},
		},
		{
			name:    "invalid_arn_no_role_marker",
			roleArn: "arn:aws:iam::123456789012:user/username",
			expected: ARNInfo{
				Original:  "arn:aws:iam::123456789012:user/username",
				RoleName:  "",
				AccountID: "",
			},
		},
		{
			name:    "invalid_arn_empty_role_name",
			roleArn: "arn:aws:iam::role/",
			expected: ARNInfo{
				Original:  "arn:aws:iam::role/",
				RoleName:  "",
				AccountID: "",
			},
		},
		{
			name:    "invalid_arn_empty_role_name_standard_format",
			roleArn: "arn:aws:iam::123456789012:role/",
			expected: ARNInfo{
				Original:  "arn:aws:iam::123456789012:role/",
				RoleName:  "",
				AccountID: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseRoleARN(tc.roleArn)
			if result.Original != tc.expected.Original {
				t.Errorf("ParseRoleARN(%q).Original = %q, want %q", tc.roleArn, result.Original, tc.expected.Original)
			}
			if result.RoleName != tc.expected.RoleName {
				t.Errorf("ParseRoleARN(%q).RoleName = %q, want %q", tc.roleArn, result.RoleName, tc.expected.RoleName)
			}
			if result.AccountID != tc.expected.AccountID {
				t.Errorf("ParseRoleARN(%q).AccountID = %q, want %q", tc.roleArn, result.AccountID, tc.expected.AccountID)
			}
		})
	}
}

// TestParsePrincipalARN tests the ParsePrincipalARN function with structured ARNInfo output
func TestParsePrincipalARN(t *testing.T) {
	testCases := []struct {
		name      string
		principal string
		expected  ARNInfo
	}{
		{
			name:      "sts_assumed_role_legacy",
			principal: "arn:aws:sts::assumed-role/MyRole/SessionName",
			expected: ARNInfo{
				Original:  "arn:aws:sts::assumed-role/MyRole/SessionName",
				RoleName:  "MyRole",
				AccountID: "",
			},
		},
		{
			name:      "sts_assumed_role_standard",
			principal: "arn:aws:sts::123456789012:assumed-role/MyRole/SessionName",
			expected: ARNInfo{
				Original:  "arn:aws:sts::123456789012:assumed-role/MyRole/SessionName",
				RoleName:  "MyRole",
				AccountID: "123456789012",
			},
		},
		{
			name:      "sts_assumed_role_no_session",
			principal: "arn:aws:sts::assumed-role/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:sts::assumed-role/MyRole",
				RoleName:  "MyRole",
				AccountID: "",
			},
		},
		{
			name:      "iam_role_legacy",
			principal: "arn:aws:iam::role/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::role/MyRole",
				RoleName:  "MyRole",
				AccountID: "",
			},
		},
		{
			name:      "iam_role_standard",
			principal: "arn:aws:iam::123456789012:role/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::123456789012:role/MyRole",
				RoleName:  "MyRole",
				AccountID: "123456789012",
			},
		},
		{
			name:      "iam_role_with_path",
			principal: "arn:aws:iam::999999999999:role/Division/Team/MyRole",
			expected: ARNInfo{
				Original:  "arn:aws:iam::999999999999:role/Division/Team/MyRole",
				RoleName:  "Division/Team/MyRole",
				AccountID: "999999999999",
			},
		},
		{
			name:      "invalid_principal",
			principal: "invalid-arn",
			expected: ARNInfo{
				Original:  "invalid-arn",
				RoleName:  "",
				AccountID: "",
			},
		},
		{
			name:      "invalid_sts_empty_role_name",
			principal: "arn:aws:sts::assumed-role/",
			expected: ARNInfo{
				Original:  "arn:aws:sts::assumed-role/",
				RoleName:  "",
				AccountID: "",
			},
		},
		{
			name:      "invalid_sts_empty_role_name_standard_format",
			principal: "arn:aws:sts::123456789012:assumed-role/",
			expected: ARNInfo{
				Original:  "arn:aws:sts::123456789012:assumed-role/",
				RoleName:  "",
				AccountID: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParsePrincipalARN(tc.principal)
			if result.Original != tc.expected.Original {
				t.Errorf("ParsePrincipalARN(%q).Original = %q, want %q", tc.principal, result.Original, tc.expected.Original)
			}
			if result.RoleName != tc.expected.RoleName {
				t.Errorf("ParsePrincipalARN(%q).RoleName = %q, want %q", tc.principal, result.RoleName, tc.expected.RoleName)
			}
			if result.AccountID != tc.expected.AccountID {
				t.Errorf("ParsePrincipalARN(%q).AccountID = %q, want %q", tc.principal, result.AccountID, tc.expected.AccountID)
			}
		})
	}
}

// TestSecurityMaliciousUserARNs tests that user ARNs with "role/" in the path are correctly rejected
// to prevent security vulnerabilities where malicious ARNs could bypass validation.
func TestSecurityMaliciousUserARNs(t *testing.T) {
	maliciousARNs := []struct {
		arn         string
		description string
	}{
		{"arn:aws:iam::123456789012:user/role/malicious", "user ARN with role/ in path"},
		{"arn:aws:iam::123456789012:policy/role/some-policy", "policy ARN with role/ in name"},
		{"arn:aws:iam::123456789012:group/role/some-group", "group ARN with role/ in name"},
		{"arn:aws:iam::user/role/test", "legacy user ARN with role/ in path"},
	}

	for _, tc := range maliciousARNs {
		t.Run(tc.description, func(t *testing.T) {
			roleName := ExtractRoleNameFromArn(tc.arn)
			if roleName != "" {
				t.Errorf("Security issue: %s was accepted and returned role name '%s'", tc.description, roleName)
			}

			arnInfo := ParseRoleARN(tc.arn)
			if arnInfo.RoleName != "" {
				t.Errorf("Security issue: %s was accepted by ParseRoleARN and returned role name '%s'", tc.description, arnInfo.RoleName)
			}
		})
	}
}

// TestSecurityMaliciousSTSUserARNs tests that STS user ARNs with "assumed-role/" are correctly rejected
// to prevent security vulnerabilities.
func TestSecurityMaliciousSTSUserARNs(t *testing.T) {
	maliciousARNs := []struct {
		arn         string
		description string
	}{
		{"arn:aws:sts::123456789012:user/assumed-role/malicious", "STS user with assumed-role in path"},
		{"arn:aws:sts::user/assumed-role/test", "legacy STS user with assumed-role in path"},
	}

	for _, tc := range maliciousARNs {
		t.Run(tc.description, func(t *testing.T) {
			roleName := ExtractRoleNameFromPrincipal(tc.arn)
			if roleName != "" {
				t.Errorf("Security issue: %s was accepted and returned role name '%s'", tc.description, roleName)
			}

			arnInfo := ParsePrincipalARN(tc.arn)
			if arnInfo.RoleName != "" {
				t.Errorf("Security issue: %s was accepted by ParsePrincipalARN and returned role name '%s'", tc.description, arnInfo.RoleName)
			}
		})
	}
}
