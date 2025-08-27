package sts

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// MockTrustPolicyValidator is a simple mock for testing STS functionality
type MockTrustPolicyValidator struct{}

// ValidateTrustPolicyForWebIdentity allows valid test tokens for STS testing
func (m *MockTrustPolicyValidator) ValidateTrustPolicyForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string) error {
	// For STS unit tests, allow valid test tokens
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
	// For STS unit tests, allow test identities
	if identity != nil && identity.UserID != "" {
		return nil
	}
	return fmt.Errorf("invalid identity for role assumption")
}
