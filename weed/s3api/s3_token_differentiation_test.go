package s3api

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
)

func TestS3IAMIntegration_isSTSIssuer(t *testing.T) {
	// Create test STS service with configuration
	stsService := sts.NewSTSService()

	// Set up STS configuration with a specific issuer
	testIssuer := "https://seaweedfs-prod.company.com/sts"
	stsConfig := &sts.STSConfig{
		Issuer:           testIssuer,
		SigningKey:       []byte("test-signing-key-32-characters-long"),
		TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
		MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour}, // Required field
	}

	// Initialize STS service with config (this sets the Config field)
	err := stsService.Initialize(stsConfig)
	assert.NoError(t, err)

	// Create S3IAM integration with configured STS service
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
		// Only exact match should return true
		{
			name:     "exact match with configured issuer",
			issuer:   testIssuer,
			expected: true,
		},
		// All other issuers should return false (exact matching)
		{
			name:     "similar but not exact issuer",
			issuer:   "https://seaweedfs-prod.company.com/sts2",
			expected: false,
		},
		{
			name:     "substring of configured issuer",
			issuer:   "seaweedfs-prod.company.com",
			expected: false,
		},
		{
			name:     "contains configured issuer as substring",
			issuer:   "prefix-" + testIssuer + "-suffix",
			expected: false,
		},
		{
			name:     "case sensitive - different case",
			issuer:   strings.ToUpper(testIssuer),
			expected: false,
		},
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
			name:     "Empty string",
			issuer:   "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s3iam.isSTSIssuer(tt.issuer)
			assert.Equal(t, tt.expected, result, "isSTSIssuer should use exact matching against configured issuer")
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
