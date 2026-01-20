package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// ARN represents an Amazon Resource Name structure
type ARN struct {
	Partition string // "aws"
	Service   string // "iam" or "sts"
	Region    string // empty for IAM/STS
	Account   string // account ID or "seaweedfs"
	Resource  string // "role/name", "user/name", "assumed-role/name/session"
}

// ARN format: arn:partition:service:region:account:resource
var arnRegex = regexp.MustCompile(`^arn:([^:]+):([^:]+):([^:]*):([^:]*):(.+)$`)

// ParseARN parses an ARN string into its components
func ParseARN(arn string) (*ARN, error) {
	if arn == "" {
		return nil, fmt.Errorf("ARN cannot be empty")
	}

	matches := arnRegex.FindStringSubmatch(arn)
	if matches == nil {
		return nil, fmt.Errorf("invalid ARN format: %s (expected: arn:partition:service:region:account:resource)", arn)
	}

	return &ARN{
		Partition: matches[1],
		Service:   matches[2],
		Region:    matches[3],
		Account:   matches[4],
		Resource:  matches[5],
	}, nil
}

// Validate checks if the ARN has valid values
func (a *ARN) Validate() error {
	if a.Partition == "" {
		return fmt.Errorf("ARN partition cannot be empty")
	}
	if a.Service == "" {
		return fmt.Errorf("ARN service cannot be empty")
	}
	if a.Resource == "" {
		return fmt.Errorf("ARN resource cannot be empty")
	}
	
	// IAM and STS should not have regions
	if (a.Service == "iam" || a.Service == "sts") && a.Region != "" {
		return fmt.Errorf("IAM/STS ARNs should not specify a region")
	}
	
	return nil
}

// ValidateRoleARN validates that the ARN is a valid IAM role ARN
func ValidateRoleARN(arn string) error {
	parsed, err := ParseARN(arn)
	if err != nil {
		return fmt.Errorf("invalid role ARN: %w", err)
	}

	if err := parsed.Validate(); err != nil {
		return fmt.Errorf("invalid role ARN: %w", err)
	}

	if parsed.Service != "iam" {
		return fmt.Errorf("role ARN must use 'iam' service (got: %s)", parsed.Service)
	}

	// Resource must start with "role/"
	if !strings.HasPrefix(parsed.Resource, "role/") {
		return fmt.Errorf("role ARN resource must start with 'role/' (got: %s)", parsed.Resource)
	}

	// Extract role name (everything after "role/")
	roleName := strings.TrimPrefix(parsed.Resource, "role/")
	if roleName == "" {
		return fmt.Errorf("role ARN must contain a role name after 'role/'")
	}

	// Validate role name format (alphanumeric, hyphens, underscores)
	if !isValidIAMName(roleName) {
		return fmt.Errorf("invalid role name: %s (must contain only alphanumeric characters, hyphens, or underscores)", roleName)
	}

	return nil
}

// ValidateUserARN validates that the ARN is a valid IAM user ARN
func ValidateUserARN(arn string) error {
	parsed, err := ParseARN(arn)
	if err != nil {
		return fmt.Errorf("invalid user ARN: %w", err)
	}

	if err := parsed.Validate(); err != nil {
		return fmt.Errorf("invalid user ARN: %w", err)
	}

	if parsed.Service != "iam" {
		return fmt.Errorf("user ARN must use 'iam' service (got: %s)", parsed.Service)
	}

	// Resource must start with "user/"
	if !strings.HasPrefix(parsed.Resource, "user/") {
		return fmt.Errorf("user ARN resource must start with 'user/' (got: %s)", parsed.Resource)
	}

	// Extract user name
	userName := strings.TrimPrefix(parsed.Resource, "user/")
	if userName == "" {
		return fmt.Errorf("user ARN must contain a user name after 'user/'")
	}

	// Validate user name format
	if !isValidIAMName(userName) {
		return fmt.Errorf("invalid user name: %s (must contain only alphanumeric characters, hyphens, or underscores)", userName)
	}

	return nil
}

// isValidIAMName checks if a name contains only valid IAM characters
func isValidIAMName(name string) bool {
	// AWS IAM names can contain alphanumeric characters, hyphens, and underscores
	// They cannot start with a hyphen
	if name == "" || name[0] == '-' {
		return false
	}
	
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	
	return true
}
