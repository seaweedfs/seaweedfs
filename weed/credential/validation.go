package credential

import (
	"fmt"
	"regexp"
)

var (
	PolicyNamePattern       = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	ServiceAccountIdPattern = regexp.MustCompile(`^sa:[A-Za-z0-9_-]+:[a-z0-9-]+$`)
)

func ValidatePolicyName(name string) error {
	if !PolicyNamePattern.MatchString(name) {
		return fmt.Errorf("invalid policy name: %s", name)
	}
	return nil
}

func ValidateServiceAccountId(id string) error {
	if id == "" {
		return fmt.Errorf("service account ID cannot be empty")
	}
	if !ServiceAccountIdPattern.MatchString(id) {
		return fmt.Errorf("invalid service account ID: %s (expected format sa:<user>:<uuid>)", id)
	}
	return nil
}
