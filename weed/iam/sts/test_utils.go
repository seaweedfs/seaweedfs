package sts

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// MockTrustPolicyValidator is a simple mock for testing STS functionality
type MockTrustPolicyValidator struct{}

// ValidateTrustPolicyForWebIdentity allows valid JWT test tokens for STS testing
func (m *MockTrustPolicyValidator) ValidateTrustPolicyForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string) error {
	// Reject non-existent roles for testing
	if strings.Contains(roleArn, "NonExistentRole") {
		return fmt.Errorf("trust policy validation failed: role does not exist")
	}

	// For STS unit tests, allow JWT tokens that look valid (contain dots for JWT structure)
	// In real implementation, this would validate against actual trust policies
	if len(webIdentityToken) > 20 && strings.Count(webIdentityToken, ".") >= 2 {
		// This appears to be a JWT token - allow it for testing
		return nil
	}

	// Legacy support for specific test tokens during migration
	if webIdentityToken == "valid_test_token" || webIdentityToken == "valid-oidc-token" {
		return nil
	}

	// Reject invalid tokens
	if webIdentityToken == "invalid_token" || webIdentityToken == "expired_token" || webIdentityToken == "invalid-token" {
		return fmt.Errorf("trust policy denies token")
	}

	return nil
}

// ValidateTrustPolicyForCredentials allows valid test identities for STS testing
func (m *MockTrustPolicyValidator) ValidateTrustPolicyForCredentials(ctx context.Context, roleArn string, identity *providers.ExternalIdentity) error {
	// Reject non-existent roles for testing
	if strings.Contains(roleArn, "NonExistentRole") {
		return fmt.Errorf("trust policy validation failed: role does not exist")
	}

	// For STS unit tests, allow test identities
	if identity != nil && identity.UserID != "" {
		return nil
	}
	return fmt.Errorf("invalid identity for role assumption")
}
