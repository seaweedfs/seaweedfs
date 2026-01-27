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

// ARNInfo contains structured information about a parsed AWS ARN.
// This provides more context than a simple string extraction, making it
// easier to debug issues and support different ARN formats.
type ARNInfo struct {
	// Original is the original ARN string that was parsed
	Original string

	// RoleName is the extracted role name (without "role/" prefix)
	// May include path components (e.g., "Division/Team/RoleName")
	// Empty string indicates an invalid ARN
	RoleName string

	// AccountID is the AWS account ID if present in the ARN
	// Empty string for legacy format ARNs (arn:aws:iam::role/Name)
	// Non-empty for standard format ARNs (arn:aws:iam::ACCOUNT:role/Name)
	AccountID string
}

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

		// Validate ARN structure: should be either "assumed-role/..." or "ACCOUNT:assumed-role/..."
		// Split on ':' to separate account ID (if present) from resource type
		resourcePart := remainder
		if colonIdx := strings.Index(remainder, ":"); colonIdx != -1 {
			// Standard format with account ID: "ACCOUNT:assumed-role/..."
			resourcePart = remainder[colonIdx+1:]
		}

		// Verify the resource type is exactly "assumed-role/"
		if !strings.HasPrefix(resourcePart, stsAssumedRoleMarker) {
			return ""
		}

		// Extract role name after "assumed-role/"
		afterMarker := resourcePart[len(stsAssumedRoleMarker):]
		if slash := strings.Index(afterMarker, "/"); slash != -1 {
			return afterMarker[:slash]
		}
		return afterMarker
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
// The function validates the ARN structure to ensure the resource type is exactly
// "role", preventing security issues where malicious ARNs like
// "arn:aws:iam::123456789012:user/role/malicious" could be accepted.
//
// If the ARN contains a path component (e.g., "role/Division/Team/RoleName"),
// the entire path is returned after "role/".
//
// Returns an empty string if:
//   - The ARN does not start with "arn:aws:iam::"
//   - The resource type is not exactly "role"
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

	// Validate ARN structure: should be either "role/..." or "ACCOUNT:role/..."
	// Split on ':' to separate account ID (if present) from resource type
	resourcePart := remainder
	if colonIdx := strings.Index(remainder, ":"); colonIdx != -1 {
		// Standard format with account ID: "ACCOUNT:role/..."
		resourcePart = remainder[colonIdx+1:]
	}

	// Verify the resource type is exactly "role/"
	if !strings.HasPrefix(resourcePart, iamRoleMarker) {
		return ""
	}

	// Extract role name after "role/"
	return resourcePart[len(iamRoleMarker):]
}

// ParseRoleARN parses an AWS IAM role ARN and returns structured information.
//
// It handles both legacy and standard AWS IAM role ARN formats:
//   - arn:aws:iam::role/RoleName (legacy format without account ID)
//   - arn:aws:iam::ACCOUNT:role/RoleName (standard AWS format with account ID)
//
// The function validates the ARN structure to ensure the resource type is exactly
// "role", extracting the role name and account ID (if present).
//
// Parameters:
//   - roleArn: The IAM role ARN string to parse
//
// Returns:
//   - ARNInfo struct containing parsed information
//   - RoleName will be empty if parsing fails or ARN is malformed
func ParseRoleARN(roleArn string) ARNInfo {
	info := ARNInfo{
		Original: roleArn,
	}

	if !strings.HasPrefix(roleArn, iamPrefix) {
		return info
	}

	remainder := roleArn[len(iamPrefix):]

	// Validate ARN structure: should be either "role/..." or "ACCOUNT:role/..."
	// Split on ':' to separate account ID (if present) from resource type
	resourcePart := remainder
	accountPart := ""

	if colonIdx := strings.Index(remainder, ":"); colonIdx != -1 {
		// Standard format with account ID: "ACCOUNT:role/..."
		accountPart = remainder[:colonIdx]
		resourcePart = remainder[colonIdx+1:]
	}

	// Verify the resource type is exactly "role/"
	if !strings.HasPrefix(resourcePart, iamRoleMarker) {
		// Invalid resource type
		return info
	}

	// Extract role name (everything after "role/")
	info.RoleName = resourcePart[len(iamRoleMarker):]
	if info.RoleName == "" {
		// Empty role name is invalid
		return info
	}

	// Set account ID if present
	info.AccountID = accountPart

	return info
}

// ParsePrincipalARN parses an AWS principal ARN (STS or IAM) and returns structured information.
//
// It handles both STS assumed role and IAM role ARN formats:
//   - arn:aws:sts::assumed-role/RoleName/SessionName (legacy STS format)
//   - arn:aws:sts::ACCOUNT:assumed-role/RoleName/SessionName (standard STS format)
//   - arn:aws:iam::role/RoleName (legacy IAM format)
//   - arn:aws:iam::ACCOUNT:role/RoleName (standard IAM format)
//
// The function validates the ARN structure to ensure the resource type is exactly
// "assumed-role" for STS or delegates to ParseRoleARN for IAM ARNs.
//
// Parameters:
//   - principal: The AWS principal ARN string to parse
//
// Returns:
//   - ARNInfo struct containing parsed information
//   - RoleName will be empty if parsing fails or ARN is malformed
func ParsePrincipalARN(principal string) ARNInfo {
	// Handle STS assumed role format
	if strings.HasPrefix(principal, stsPrefix) {
		info := ARNInfo{
			Original: principal,
		}

		remainder := principal[len(stsPrefix):]

		// Validate ARN structure: should be either "assumed-role/..." or "ACCOUNT:assumed-role/..."
		// Split on ':' to separate account ID (if present) from resource type
		resourcePart := remainder
		accountPart := ""

		if colonIdx := strings.Index(remainder, ":"); colonIdx != -1 {
			// Standard format with account ID: "ACCOUNT:assumed-role/..."
			accountPart = remainder[:colonIdx]
			resourcePart = remainder[colonIdx+1:]
		}

		// Verify the resource type is exactly "assumed-role/"
		if !strings.HasPrefix(resourcePart, stsAssumedRoleMarker) {
			// Invalid resource type
			return info
		}

		// Extract role name (between "assumed-role/" and next "/")
		afterMarker := resourcePart[len(stsAssumedRoleMarker):]
		if slash := strings.Index(afterMarker, "/"); slash != -1 {
			info.RoleName = afterMarker[:slash]
		} else {
			info.RoleName = afterMarker
		}

		// Validate that role name is not empty
		if info.RoleName == "" {
			return info
		}

		// Set account ID if present
		info.AccountID = accountPart

		return info
	}

	// Handle IAM role format
	return ParseRoleARN(principal)
}
