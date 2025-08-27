package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
)

func TestS3IAMIntegration_isSTSIssuer(t *testing.T) {
	// Create test STS service
	stsService := sts.NewSTSService()

	// Create S3IAM integration with STS service
	s3iam := &S3IAMIntegration{
		iamManager:   &integration.IAMManager{}, // Mock
		stsService:   stsService,
		filerAddress: "test-filer:8888",
		enabled:      true,
	}

	tests := []struct {
		name     string
		issuer   string
		expected bool
	}{
		// STS issuers (should return true)
		{
			name:     "explicit STS issuer",
			issuer:   "seaweedfs-sts",
			expected: true,
		},
		{
			name:     "STS in issuer name",
			issuer:   "https://mycompany-sts.example.com",
			expected: true,
		},
		{
			name:     "seaweed in issuer name",
			issuer:   "https://seaweed-prod.company.com",
			expected: true,
		},
		{
			name:     "localhost for development",
			issuer:   "http://localhost:9333/sts",
			expected: true,
		},
		{
			name:     "case insensitive STS",
			issuer:   "SEAWEEDFS-STS-PROD",
			expected: true,
		},
		// External OIDC issuers (should return false)
		{
			name:     "Google OIDC",
			issuer:   "https://accounts.google.com",
			expected: false,
		},
		{
			name:     "Azure AD",
			issuer:   "https://login.microsoftonline.com/tenant-id/v2.0",
			expected: false,
		},
		{
			name:     "Auth0",
			issuer:   "https://mycompany.auth0.com",
			expected: false,
		},
		{
			name:     "Keycloak",
			issuer:   "https://keycloak.mycompany.com/auth/realms/master",
			expected: false,
		},
		{
			name:     "Generic OIDC provider",
			issuer:   "https://oidc.provider.com",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s3iam.isSTSIssuer(tt.issuer)
			assert.Equal(t, tt.expected, result, "isSTSIssuer should correctly identify issuer type")
		})
	}
}

func TestS3IAMIntegration_isSTSIssuer_NoSTSService(t *testing.T) {
	// Create S3IAM integration without STS service
	s3iam := &S3IAMIntegration{
		iamManager:   &integration.IAMManager{},
		stsService:   nil, // No STS service
		filerAddress: "test-filer:8888",
		enabled:      true,
	}

	// Should return false when STS service is not available
	result := s3iam.isSTSIssuer("seaweedfs-sts")
	assert.False(t, result, "isSTSIssuer should return false when STS service is nil")
}
