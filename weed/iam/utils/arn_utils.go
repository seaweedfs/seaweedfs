package utils

import "strings"

// ExtractRoleNameFromPrincipal extracts role name from principal ARN
// Handles both STS assumed role and IAM role formats
func ExtractRoleNameFromPrincipal(principal string) string {
	// Handle STS assumed role format: arn:aws:sts::assumed-role/RoleName/SessionName
	stsPrefix := "arn:aws:sts::assumed-role/"
	if strings.HasPrefix(principal, stsPrefix) {
		remainder := principal[len(stsPrefix):]
		// Split on first '/' to get role name
		if slashIndex := strings.Index(remainder, "/"); slashIndex != -1 {
			return remainder[:slashIndex]
		}
		// If no slash found, return the remainder (edge case)
		return remainder
	}

	// Handle IAM role format: arn:aws:iam::role/RoleName
	iamPrefix := "arn:aws:iam::role/"
	if strings.HasPrefix(principal, iamPrefix) {
		return principal[len(iamPrefix):]
	}

	// Return empty string to signal invalid ARN format
	// This allows callers to handle the error explicitly instead of masking it
	return ""
}

// ExtractRoleNameFromArn extracts role name from an IAM role ARN
// Specifically handles: arn:aws:iam::role/RoleName
func ExtractRoleNameFromArn(roleArn string) string {
	prefix := "arn:aws:iam::role/"
	if strings.HasPrefix(roleArn, prefix) && len(roleArn) > len(prefix) {
		return roleArn[len(prefix):]
	}
	return ""
}
