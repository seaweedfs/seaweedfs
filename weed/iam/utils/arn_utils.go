// Package utils provides utility functions for AWS IAM ARN parsing and role extraction.
package utils

import "strings"

// ARN parsing constants for AWS IAM and STS services
const (
	// stsPrefix is the common prefix for all AWS STS ARNs
	stsPrefix = "arn:aws:sts::"

	// stsAssumedRoleMarker is the marker that identifies assumed role ARNs
	stsAssumedRoleMarker = "assumed-role/"

	// iamPrefix is the common prefix for all AWS IAM ARNs
	iamPrefix = "arn:aws:iam::"

	// iamRoleMarker is the marker that identifies IAM role ARNs
	iamRoleMarker = "role/"
)

// ExtractRoleNameFromPrincipal extracts the role name from an AWS principal ARN.
//
// It handles both STS assumed role and IAM role ARN formats, supporting both
// legacy (without account ID) and standard AWS (with account ID) formats:
//   - arn:aws:sts::assumed-role/RoleName/SessionName (legacy STS format)
//   - arn:aws:sts::ACCOUNT:assumed-role/RoleName/SessionName (standard STS format)
//   - arn:aws:iam::role/RoleName (legacy IAM format)
//   - arn:aws:iam::ACCOUNT:role/RoleName (standard IAM format)
//
// For STS assumed-role ARNs, it extracts the role name from the "assumed-role/"
// component, which is the second part of the path (e.g., "Role" from
// "arn:aws:sts::ACCOUNT:assumed-role/Role/Session"). For IAM role ARNs, it
// delegates to ExtractRoleNameFromArn.
//
// Returns an empty string if the principal format is invalid or unrecognized.
//
// Parameters:
//   - principal: The AWS principal ARN string to extract the role name from
//
// Returns:
//   - The extracted role name (without "role/" prefix)
//   - Empty string if the principal is invalid or no role name is found
func ExtractRoleNameFromPrincipal(principal string) string {
	// Handle STS assumed role format
	if strings.HasPrefix(principal, stsPrefix) {
		remainder := principal[len(stsPrefix):]
		if idx := strings.Index(remainder, stsAssumedRoleMarker); idx != -1 {
			afterMarker := remainder[idx+len(stsAssumedRoleMarker):]
			if slash := strings.Index(afterMarker, "/"); slash != -1 {
				return afterMarker[:slash]
			}
			return afterMarker
		}
	}

	// Handle IAM role format
	return ExtractRoleNameFromArn(principal)
}

// ExtractRoleNameFromArn extracts the role name from an AWS IAM role ARN.
//
// It handles both legacy and standard AWS IAM role ARN formats:
//   - arn:aws:iam::role/RoleName (legacy format without account ID)
//   - arn:aws:iam::ACCOUNT:role/RoleName (standard AWS format with account ID)
//
// The function uses a flexible approach to locate the "role/" marker within
// the ARN, allowing it to support both formats without explicit format validation.
// If the ARN contains a path component (e.g., "role/Division/Team/RoleName"),
// the entire path is returned after "role/".
//
// Returns an empty string if:
//   - The ARN does not start with "arn:aws:iam::"
//   - The "role/" marker is not found in the ARN
//   - The input is empty or invalid
//
// This function is commonly used in STS (Security Token Service) role assumption
// validation to extract the role name from principal ARNs or to validate IAM
// role ARNs during credential assumption checks.
//
// Parameters:
//   - roleArn: The IAM role ARN string to extract the role name from
//
// Returns:
//   - The extracted role name (without "role/" prefix, may include path)
//   - Empty string if the ARN is invalid or no role name is found
func ExtractRoleNameFromArn(roleArn string) string {
	if !strings.HasPrefix(roleArn, iamPrefix) {
		return ""
	}
	remainder := roleArn[len(iamPrefix):]
	if idx := strings.Index(remainder, iamRoleMarker); idx != -1 {
		return remainder[idx+len(iamRoleMarker):]
	}
	return ""
}
