package utils

import (
	"fmt"
	"strings"
)

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
	// And standard AWS format: arn:aws:iam::123456789012:role/RoleName
	rolePrefix := ":role/"
	if idx := strings.LastIndex(principal, rolePrefix); idx != -1 {
		return principal[idx+len(rolePrefix):]
	}

	// Fallback for simple ARN format without account ID (legacy)
	iamPrefix := "arn:aws:iam::role/"
	if strings.HasPrefix(principal, iamPrefix) {
		return principal[len(iamPrefix):]
	}

	// Return empty string to signal invalid ARN format
	// This allows callers to handle the error explicitly instead of masking it
	return ""
}

// ExtractRoleNameFromArn extracts role name from an IAM role ARN
// Specifically handles: arn:aws:iam::role/RoleName and arn:aws:iam::account:role/RoleName
//
// IMPORTANT: This function does basic extraction without strict validation.
// For strict validation, use ValidateRoleARN() before calling this function.
func ExtractRoleNameFromArn(roleArn string) string {
	// Handle standard format: arn:aws:iam::account:role/RoleName
	rolePrefix := ":role/"
	if idx := strings.LastIndex(roleArn, rolePrefix); idx != -1 {
		return roleArn[idx+len(rolePrefix):]
	}

	// Handle legacy format: arn:aws:iam::role/RoleName
	prefix := "arn:aws:iam::role/"
	if strings.HasPrefix(roleArn, prefix) && len(roleArn) > len(prefix) {
		return roleArn[len(prefix):]
	}
	return ""
}

// ExtractRoleNameFromArnWithValidation extracts role name after validating the ARN format
func ExtractRoleNameFromArnWithValidation(roleArn string) (string, error) {
	// Validate ARN format first
	if err := ValidateRoleARN(roleArn); err != nil {
		return "", err
	}
	
	roleName := ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		return "", fmt.Errorf("failed to extract role name from ARN: %s", roleArn)
	}
	
	return roleName, nil
}

// ExtractUsernameFromPrincipal extracts a username/identifier from a principal ARN
// for low-cardinality metric labels. Handles multiple ARN formats:
// - arn:aws:iam::account:user/username -> username
// - arn:aws:sts::assumed-role/RoleName/SessionName -> RoleName
// - arn:aws:iam::seaweedfs:role/RoleName -> RoleName
func ExtractUsernameFromPrincipal(principal string) string {
	// Handle user ARN: arn:aws:iam::account:user/username
	userPrefix := ":user/"
	if idx := strings.LastIndex(principal, userPrefix); idx != -1 {
		return principal[idx+len(userPrefix):]
	}

	// Handle assumed role: arn:aws:sts::assumed-role/RoleName/SessionName
	stsPrefix := "arn:aws:sts::assumed-role/"
	if strings.HasPrefix(principal, stsPrefix) {
		remainder := principal[len(stsPrefix):]
		// Return role name (before first slash)
		if slashIndex := strings.Index(remainder, "/"); slashIndex != -1 {
			return remainder[:slashIndex]
		}
		return remainder
	}

	// Handle role ARN: arn:aws:iam::account:role/RoleName
	rolePrefix := ":role/"
	if idx := strings.LastIndex(principal, rolePrefix); idx != -1 {
		return principal[idx+len(rolePrefix):]
	}

	// Fallback: return the full principal (shouldn't happen with valid ARNs)
	return principal
}
