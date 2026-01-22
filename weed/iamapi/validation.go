package iamapi

import (
	"fmt"
	"regexp"
	"strings"
)

// AWS IAM naming and validation constants
const (
	// MaxRoleNameLength is the maximum length for a role name (AWS limit)
	MaxRoleNameLength = 64

	// MaxPolicyNameLength is the maximum length for a policy name (AWS limit)
	MaxPolicyNameLength = 128

	// MaxDescriptionLength is the maximum length for description fields (AWS limit)
	MaxDescriptionLength = 1000

	// MaxPolicyDocumentSize is the maximum size for a policy document in bytes (AWS limit: 10KB)
	MaxPolicyDocumentSize = 10 * 1024

	// MaxPathLength is the maximum length for IAM paths (AWS limit)
	MaxPathLength = 512
)

var (
	// AWS-compliant regex patterns for validation
	// Pattern: ^[a-zA-Z0-9+=,.@_-]+$
	iamNamePattern = regexp.MustCompile(`^[a-zA-Z0-9+=,.@_-]+$`)

	// ARN pattern for basic validation
	arnPattern = regexp.MustCompile(`^arn:aws:iam::[0-9]{12}:(role|user|policy|group)/(.+)$`)

	// Path pattern: must start and end with /
	pathPattern = regexp.MustCompile(`^/[a-zA-Z0-9/_-]*/$`)
)

// ValidationError represents an input validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// ValidateRoleName validates a role name according to AWS IAM rules
func ValidateRoleName(name string) error {
	if name == "" {
		return &ValidationError{Field: "RoleName", Message: "role name cannot be empty"}
	}
	if len(name) > MaxRoleNameLength {
		return &ValidationError{
			Field:   "RoleName",
			Message: fmt.Sprintf("role name must be %d characters or fewer", MaxRoleNameLength),
		}
	}
	if !iamNamePattern.MatchString(name) {
		return &ValidationError{
			Field:   "RoleName",
			Message: "role name must match pattern ^[a-zA-Z0-9+=,.@_-]+$",
		}
	}
	return nil
}

// ValidatePolicyName validates a policy name according to AWS IAM rules
func ValidatePolicyName(name string) error {
	if name == "" {
		return &ValidationError{Field: "PolicyName", Message: "policy name cannot be empty"}
	}
	if len(name) > MaxPolicyNameLength {
		return &ValidationError{
			Field:   "PolicyName",
			Message: fmt.Sprintf("policy name must be %d characters or fewer", MaxPolicyNameLength),
		}
	}
	if !iamNamePattern.MatchString(name) {
		return &ValidationError{
			Field:   "PolicyName",
			Message: "policy name must match pattern ^[a-zA-Z0-9+=,.@_-]+$",
		}
	}
	return nil
}

// ValidateUserName validates a user name according to AWS IAM rules
func ValidateUserName(name string) error {
	if name == "" {
		return &ValidationError{Field: "UserName", Message: "user name cannot be empty"}
	}
	if len(name) > MaxRoleNameLength { // User names have same length limit as role names
		return &ValidationError{
			Field:   "UserName",
			Message: fmt.Sprintf("user name must be %d characters or fewer", MaxRoleNameLength),
		}
	}
	if !iamNamePattern.MatchString(name) {
		return &ValidationError{
			Field:   "UserName",
			Message: "user name must match pattern ^[a-zA-Z0-9+=,.@_-]+$",
		}
	}
	return nil
}

// ValidateGroupName validates a group name according to AWS IAM rules
func ValidateGroupName(name string) error {
	if name == "" {
		return &ValidationError{Field: "GroupName", Message: "group name cannot be empty"}
	}
	if len(name) > MaxPolicyNameLength { // Groups have same limit as policies
		return &ValidationError{
			Field:   "GroupName",
			Message: fmt.Sprintf("group name must be %d characters or fewer", MaxPolicyNameLength),
		}
	}
	if !iamNamePattern.MatchString(name) {
		return &ValidationError{
			Field:   "GroupName",
			Message: "group name must match pattern ^[a-zA-Z0-9+=,.@_-]+$",
		}
	}
	return nil
}

// ValidateDescription validates a description field
func ValidateDescription(description string) error {
	if len(description) > MaxDescriptionLength {
		return &ValidationError{
			Field:   "Description",
			Message: fmt.Sprintf("description must be %d characters or fewer", MaxDescriptionLength),
		}
	}
	// Sanitize: remove control characters
	for _, r := range description {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return &ValidationError{
				Field:   "Description",
				Message: "description contains invalid control characters",
			}
		}
	}
	return nil
}

// ValidatePolicyDocument validates a policy document size
func ValidatePolicyDocument(document string) error {
	if document == "" {
		return &ValidationError{Field: "PolicyDocument", Message: "policy document cannot be empty"}
	}
	if len(document) > MaxPolicyDocumentSize {
		return &ValidationError{
			Field:   "PolicyDocument",
			Message: fmt.Sprintf("policy document must be %d bytes or fewer", MaxPolicyDocumentSize),
		}
	}
	return nil
}

// ValidateRoleARN validates a role ARN format
func ValidateRoleARN(arn string) error {
	if arn == "" {
		return &ValidationError{Field: "RoleArn", Message: "role ARN cannot be empty"}
	}
	if !arnPattern.MatchString(arn) {
		return &ValidationError{
			Field:   "RoleArn",
			Message: "role ARN must match format arn:aws:iam::{account-id}:role/{role-name}",
		}
	}
	// Ensure it's specifically a role ARN
	if !strings.Contains(arn, ":role/") {
		return &ValidationError{
			Field:   "RoleArn",
			Message: "ARN must be a role ARN (contain :role/)",
		}
	}
	return nil
}

// ValidatePath validates an IAM path
func ValidatePath(path string) error {
	if path == "" {
		return nil // Path is optional, empty means default path "/"
	}
	if len(path) > MaxPathLength {
		return &ValidationError{
			Field:   "Path",
			Message: fmt.Sprintf("path must be %d characters or fewer", MaxPathLength),
		}
	}
	if !pathPattern.MatchString(path) {
		return &ValidationError{
			Field:   "Path",
			Message: "path must start and end with / and contain only [a-zA-Z0-9/_-]",
		}
	}
	return nil
}

// SanitizeString removes potentially dangerous characters from input
// This is a basic sanitization for logging and error messages
func SanitizeString(s string) string {
	// Remove null bytes and other control characters
	var result strings.Builder
	for _, r := range s {
		if r >= 32 || r == '\n' || r == '\t' {
			result.WriteRune(r)
		}
	}
	return result.String()
}
