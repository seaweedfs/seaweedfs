package utils

import "strings"

// ExtractRoleNameFromPrincipal extracts role name from principal ARN
// Handles both STS assumed role and IAM role formats with or without account ID:
// - arn:aws:sts::assumed-role/Role/Session (legacy)
// - arn:aws:sts::ACCOUNT:assumed-role/Role/Session (standard)
// - arn:aws:iam::role/Role (legacy)
// - arn:aws:iam::ACCOUNT:role/Role (standard)
func ExtractRoleNameFromPrincipal(principal string) string {
	// Handle STS assumed role format
	if strings.HasPrefix(principal, "arn:aws:sts::") {
		remainder := principal[len("arn:aws:sts::"):]
		if idx := strings.Index(remainder, "assumed-role/"); idx != -1 {
			afterMarker := remainder[idx+len("assumed-role/"):]
			if slash := strings.Index(afterMarker, "/"); slash != -1 {
				return afterMarker[:slash]
			}
			return afterMarker
		}
	}

	// Handle IAM role format
	return ExtractRoleNameFromArn(principal)
}

// ExtractRoleNameFromArn extracts role name from an IAM role ARN
// Handles both formats:
// - arn:aws:iam::role/RoleName (legacy, without account ID)
// - arn:aws:iam::ACCOUNT:role/RoleName (standard AWS format)
func ExtractRoleNameFromArn(roleArn string) string {
	if !strings.HasPrefix(roleArn, "arn:aws:iam::") {
		return ""
	}
	remainder := roleArn[len("arn:aws:iam::"):]
	if idx := strings.Index(remainder, "role/"); idx != -1 {
		return remainder[idx+len("role/"):]
	}
	return ""
}
