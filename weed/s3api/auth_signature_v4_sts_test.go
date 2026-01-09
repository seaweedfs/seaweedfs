package s3api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// MockIAMIntegration is a mock implementation of IAM integration for testing
type MockIAMIntegration struct {
	authorizeFunc func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode
	authCalled    bool
}

func (m *MockIAMIntegration) AuthorizeAction(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
	m.authCalled = true
	if m.authorizeFunc != nil {
		return m.authorizeFunc(ctx, identity, action, bucket, object, r)
	}
	return s3err.ErrNone
}

func (m *MockIAMIntegration) AuthenticateJWT(ctx context.Context, r *http.Request) (*IAMIdentity, s3err.ErrorCode) {
	return nil, s3err.ErrNotImplemented
}

func (m *MockIAMIntegration) ValidateSessionToken(ctx context.Context, token string) (*sts.SessionInfo, error) {
	return nil, nil // Not needed for these tests
}

// TestVerifyV4SignatureWithSTSIdentity tests that verifyV4Signature properly handles STS identities
// by falling back to IAM authorization when shouldCheckPermissions is true
func TestVerifyV4SignatureWithSTSIdentity(t *testing.T) {
	tests := []struct {
		name                   string
		identity               *Identity
		shouldCheckPermissions bool
		iamIntegration         *MockIAMIntegration
		expectedError          s3err.ErrorCode
		description            string
	}{
		{
			name: "STS identity with IAM integration - should authorize via IAM",
			identity: &Identity{
				Name:         "arn:aws:sts::assumed-role/adminRole/s3-session",
				Account:      &AccountAdmin,
				Actions:      []Action{}, // Empty actions = STS identity
				PrincipalArn: "arn:aws:sts::assumed-role/adminRole/s3-session",
			},
			shouldCheckPermissions: true,
			iamIntegration: &MockIAMIntegration{
				authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
					// Simulate successful IAM authorization
					return s3err.ErrNone
				},
			},
			expectedError: s3err.ErrNone,
			description:   "STS identity should be authorized via IAM when Actions is empty",
		},
		{
			name: "STS identity with IAM integration - IAM denies",
			identity: &Identity{
				Name:         "arn:aws:sts::assumed-role/readOnlyRole/s3-session",
				Account:      &AccountAdmin,
				Actions:      []Action{}, // Empty actions = STS identity
				PrincipalArn: "arn:aws:sts::assumed-role/readOnlyRole/s3-session",
			},
			shouldCheckPermissions: true,
			iamIntegration: &MockIAMIntegration{
				authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
					// Simulate IAM denying access (e.g., read-only role trying to write)
					if action == s3_constants.ACTION_WRITE {
						return s3err.ErrAccessDenied
					}
					return s3err.ErrNone
				},
			},
			expectedError: s3err.ErrAccessDenied,
			description:   "STS identity should be denied when IAM denies access",
		},
		{
			name: "STS identity without IAM integration - should deny",
			identity: &Identity{
				Name:         "arn:aws:sts::assumed-role/adminRole/s3-session",
				Account:      &AccountAdmin,
				Actions:      []Action{}, // Empty actions = STS identity
				PrincipalArn: "arn:aws:sts::assumed-role/adminRole/s3-session",
			},
			shouldCheckPermissions: true,
			iamIntegration:         nil, // No IAM integration
			expectedError:          s3err.ErrAccessDenied,
			description:            "STS identity should be denied when no IAM integration is available",
		},
		{
			name: "Traditional identity with Actions - should use canDo",
			identity: &Identity{
				Name:    "traditional-user",
				Account: &AccountAdmin,
				Actions: []Action{s3_constants.ACTION_WRITE}, // Has actions = traditional identity
			},
			shouldCheckPermissions: true,
			iamIntegration:         nil, // IAM integration not needed for traditional identities
			expectedError:          s3err.ErrNone,
			description:            "Traditional identity with Actions should use canDo check",
		},
		{
			name: "Traditional identity with Actions - canDo denies",
			identity: &Identity{
				Name:    "read-only-user",
				Account: &AccountAdmin,
				Actions: []Action{s3_constants.ACTION_READ}, // Only has READ action
			},
			shouldCheckPermissions: true,
			iamIntegration:         nil,
			expectedError:          s3err.ErrAccessDenied,
			description:            "Traditional identity should be denied when canDo fails (PUT requires WRITE)",
		},
		{
			name: "shouldCheckPermissions false - skip authorization",
			identity: &Identity{
				Name:         "arn:aws:sts::assumed-role/adminRole/s3-session",
				Account:      &AccountAdmin,
				Actions:      []Action{}, // Empty actions = STS identity
				PrincipalArn: "arn:aws:sts::assumed-role/adminRole/s3-session",
			},
			shouldCheckPermissions: false, // Skip permission check
			iamIntegration:         nil,
			expectedError:          s3err.ErrNone,
			description:            "When shouldCheckPermissions is false, authorization should be skipped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock request for PUT operation (requires WRITE action)
			req, err := http.NewRequest("PUT", "http://s3.amazonaws.com/test-bucket/test-object", nil)
			require.NoError(t, err)
			req.Header.Set("Host", "s3.amazonaws.com")

			// Mock the permission check logic from verifyV4Signature (now centralized in VerifyActionPermission)
			var errCode s3err.ErrorCode
			if tt.shouldCheckPermissions {
				bucket, object := s3_constants.GetBucketAndObject(req)
				action := s3_constants.ACTION_READ
				if req.Method != http.MethodGet && req.Method != http.MethodHead {
					action = s3_constants.ACTION_WRITE
				}

				// Create minimal IAM instance with mock integration
				var integration IAMIntegration
				if tt.iamIntegration != nil {
					integration = tt.iamIntegration
				}
				iam := &IdentityAccessManagement{
					iamIntegration: integration,
				}
				errCode = iam.VerifyActionPermission(req, tt.identity, Action(action), bucket, object)
			}

			// Verify the result
			assert.Equal(t, tt.expectedError, errCode, tt.description)

			// Additional verification for STS identities
			if len(tt.identity.Actions) == 0 && tt.shouldCheckPermissions {
				if tt.iamIntegration != nil {
					// When IAM integration exists, it should have been called
					assert.True(t, tt.iamIntegration.authCalled, "IAM integration should have been called for STS identity")
				} else {
					assert.Equal(t, s3err.ErrAccessDenied, errCode, "STS identity should be denied without IAM integration")
				}
			}
		})
	}
}

// TestVerifyV4SignatureSTSStreamingUpload tests the specific scenario from the bug report:
// STS identities in streaming/chunked uploads should be authorized via IAM
func TestVerifyV4SignatureSTSStreamingUpload(t *testing.T) {
	// Create an STS identity (empty Actions)
	stsIdentity := &Identity{
		Name:         "arn:aws:sts::assumed-role/adminRole/s3-session",
		Account:      &AccountAdmin,
		Actions:      []Action{}, // Empty - this is an STS identity
		PrincipalArn: "arn:aws:sts::assumed-role/adminRole/s3-session",
	}

	// Track whether IAM authorization was called
	iamAuthCalled := false

	// Create IAM integration mock that allows the action
	iamMock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			iamAuthCalled = true
			// Verify we're checking the right identity
			assert.Equal(t, "arn:aws:sts::assumed-role/adminRole/s3-session", identity.Name)
			assert.Equal(t, s3_constants.ACTION_WRITE, string(action))
			assert.Equal(t, "test-bucket", bucket)
			assert.Equal(t, "test-object", object)
			return s3err.ErrNone
		},
	}

	// Create a streaming upload request (PUT with streaming content)
	req, err := http.NewRequest("PUT", "/test-bucket/test-object", nil)
	require.NoError(t, err)
	req.Host = "s3.amazonaws.com"
	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Security-Token", "test-session-token")

	// Simulate the permission check with shouldCheckPermissions=true
	// This is what happens in calculateSeedSignature -> verifyV4Signature(r, true)
	bucket := "test-bucket"
	object := "test-object"
	action := s3_constants.ACTION_WRITE

	var errCode s3err.ErrorCode

	// Create minimal IAM instance logic
	iam := &IdentityAccessManagement{
		iamIntegration: iamMock,
	}

	errCode = iam.VerifyActionPermission(req, stsIdentity, Action(action), bucket, object)

	// Verify that the STS identity is authorized via IAM
	assert.Equal(t, s3err.ErrNone, errCode, "STS identity should be authorized via IAM for streaming upload")
	assert.True(t, iamAuthCalled, "IAM authorization should have been called for STS identity")
}

// TestVerifyV4SignatureActionDetermination tests that the correct action is determined
// based on the HTTP method
func TestVerifyV4SignatureActionDetermination(t *testing.T) {
	tests := []struct {
		method         string
		expectedAction string
	}{
		{http.MethodGet, s3_constants.ACTION_READ},
		{http.MethodHead, s3_constants.ACTION_READ},
		{http.MethodPut, s3_constants.ACTION_WRITE},
		{http.MethodPost, s3_constants.ACTION_WRITE},
		{http.MethodDelete, s3_constants.ACTION_WRITE},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://s3.amazonaws.com/bucket/object", nil)
			require.NoError(t, err)

			// Determine action the same way verifyV4Signature does
			action := s3_constants.ACTION_READ
			if req.Method != http.MethodGet && req.Method != http.MethodHead {
				action = s3_constants.ACTION_WRITE
			}

			assert.Equal(t, tt.expectedAction, action, "Action should match expected for method %s", tt.method)
		})
	}
}
