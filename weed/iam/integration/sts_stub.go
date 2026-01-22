package integration

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// StubSTSAdapter returns "not implemented" for all STS operations
// This allows IAM to build without STS being present
type StubSTSAdapter struct{}

func NewStubSTSAdapter() *StubSTSAdapter {
	return &StubSTSAdapter{}
}

func (s *StubSTSAdapter) AssumeRoleWithCredentials(ctx context.Context, req *AssumeRoleRequest) (*AssumeRoleResponse, error) {
	return nil, fmt.Errorf("STS service not configured - will be available in next release")
}

func (s *StubSTSAdapter) AssumeRole(ctx context.Context, req *AssumeRoleRequest) (*AssumeRoleResponse, error) {
	return nil, fmt.Errorf("STS service not configured - will be available in next release")
}

func (s *StubSTSAdapter) ValidateSessionToken(ctx context.Context, token string) (*SessionInfo, error) {
	return nil, fmt.Errorf("STS service not configured - will be available in next release")
}

func (s *StubSTSAdapter) RegisterProvider(provider providers.IdentityProvider) error {
	// No-op for stub
	return nil
}

func (s *StubSTSAdapter) ExpireSessionForTesting(ctx context.Context, sessionToken string) error {
	return nil
}
