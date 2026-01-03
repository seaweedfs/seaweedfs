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

// ARNFormat represents the format type of an AWS ARN
type ARNFormat string

const (
	// ARNFormatLegacy represents ARNs without account ID (arn:aws:iam::role/Name)
	ARNFormatLegacy ARNFormat = "legacy"

	// ARNFormatStandard represents ARNs with account ID (arn:aws:iam::ACCOUNT:role/Name)
	ARNFormatStandard ARNFormat = "standard"

	// ARNFormatInvalid represents invalid or unparseable ARNs
	ARNFormatInvalid ARNFormat = "invalid"
)

// ARNInfo contains structured information about a parsed AWS ARN.
// This provides more context than a simple string extraction, making it
// easier to debug issues and support different ARN formats.
type ARNInfo struct {
	// Original is the original ARN string that was parsed
	Original string

	// RoleName is the extracted role name (without "role/" prefix)
	// May include path components (e.g., "Division/Team/RoleName")
	RoleName string

	// AccountID is the AWS account ID if present in the ARN
	// Empty string for legacy format ARNs
	AccountID string

	// Format indicates whether this is a legacy or standard AWS ARN format
	Format ARNFormat
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

// ParseRoleARN parses an AWS IAM role ARN and returns structured information.
//
// It handles both legacy and standard AWS IAM role ARN formats:
//   - arn:aws:iam::role/RoleName (legacy format without account ID)
//   - arn:aws:iam::ACCOUNT:role/RoleName (standard AWS format with account ID)
//
// The function extracts the role name, account ID (if present), and determines
// the ARN format type. This provides more context than simple string extraction.
//
// Parameters:
//   - roleArn: The IAM role ARN string to parse
//
// Returns:
//   - ARNInfo struct containing parsed information
//   - RoleName will be empty if parsing fails
//   - Format will be ARNFormatInvalid if the ARN is malformed
func ParseRoleARN(roleArn string) ARNInfo {
	info := ARNInfo{
		Original: roleArn,
		Format:   ARNFormatInvalid,
	}

	if !strings.HasPrefix(roleArn, iamPrefix) {
		return info
	}

	remainder := roleArn[len(iamPrefix):]

	// Find the role marker
	roleIdx := strings.Index(remainder, iamRoleMarker)
	if roleIdx == -1 {
		return info
	}

	// Extract role name (everything after "role/")
	info.RoleName = remainder[roleIdx+len(iamRoleMarker):]
	if info.RoleName == "" {
		return info
	}

	// Determine format and extract account ID if present
	// Legacy format: remainder starts with "role/"
	// Standard format: remainder is "ACCOUNT:role/"
	if roleIdx == 0 {
		// Legacy format (no account ID)
		info.Format = ARNFormatLegacy
	} else {
		// Standard format (has account ID)
		// Extract account ID (everything before ":role/")
		accountPart := remainder[:roleIdx]
		// Remove trailing colon if present
		if strings.HasSuffix(accountPart, ":") {
			info.AccountID = accountPart[:len(accountPart)-1]
		} else {
			info.AccountID = accountPart
		}
		info.Format = ARNFormatStandard
	}

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
// Parameters:
//   - principal: The AWS principal ARN string to parse
//
// Returns:
//   - ARNInfo struct containing parsed information
func ParsePrincipalARN(principal string) ARNInfo {
	// Handle STS assumed role format
	if strings.HasPrefix(principal, stsPrefix) {
		info := ARNInfo{
			Original: principal,
			Format:   ARNFormatInvalid,
		}

		remainder := principal[len(stsPrefix):]
		assumedRoleIdx := strings.Index(remainder, stsAssumedRoleMarker)
		
		if assumedRoleIdx == -1 {
			return info
		}

		// Determine if account ID is present
		if assumedRoleIdx == 0 {
			// Legacy format
			info.Format = ARNFormatLegacy
		} else {
			// Standard format - extract account ID
			accountPart := remainder[:assumedRoleIdx]
			if strings.HasSuffix(accountPart, ":") {
				info.AccountID = accountPart[:len(accountPart)-1]
			} else {
				info.AccountID = accountPart
			}
			info.Format = ARNFormatStandard
		}

		// Extract role name (between "assumed-role/" and next "/")
		afterMarker := remainder[assumedRoleIdx+len(stsAssumedRoleMarker):]
		if slash := strings.Index(afterMarker, "/"); slash != -1 {
			info.RoleName = afterMarker[:slash]
		} else {
			info.RoleName = afterMarker
		}

		return info
	}

	// Handle IAM role format
	return ParseRoleARN(principal)
}
