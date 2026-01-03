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
