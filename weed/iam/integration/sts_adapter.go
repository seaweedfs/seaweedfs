package integration

import (
	"context"
	"time"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// STSAdapter defines the interface for STS operations
// This allows IAM to work without STS being present
type STSAdapter interface {
	// AssumeRoleWithCredentials generates temporary credentials
	AssumeRoleWithCredentials(ctx context.Context, req *AssumeRoleRequest) (*AssumeRoleResponse, error)
	
	// AssumeRole assumes a role from an authenticated context
	AssumeRole(ctx context.Context, req *AssumeRoleRequest) (*AssumeRoleResponse, error)

	// ValidateSessionToken validates a session token
	ValidateSessionToken(ctx context.Context, token string) (*SessionInfo, error)
	
	// RegisterProvider registers an identity provider
	RegisterProvider(provider providers.IdentityProvider) error

	// ExpireSessionForTesting manually expires a session
	ExpireSessionForTesting(ctx context.Context, sessionToken string) error
}

// AssumeRoleRequest/Response are IAM-owned types (not STS-owned)
type AssumeRoleRequest struct {
	RoleArn         string
	Identity        *providers.ExternalIdentity
	DurationSeconds *int64
	SessionName     string
}

type AssumeRoleResponse struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

type SessionInfo struct {
	RoleArn         string
	Principal       string
	Expiration      time.Time
	AccessKeyId     string
	SecretAccessKey string
	Subject         string
}
