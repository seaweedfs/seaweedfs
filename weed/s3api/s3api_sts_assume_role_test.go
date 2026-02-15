package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAssumeRole_CallerIdentityFallback tests the fallback logic when RoleArn is missing
func TestAssumeRole_CallerIdentityFallback(t *testing.T) {
	// Setup STS service
	stsService, _ := setupTestSTSService(t)

	// Create IAM integration mock
	iamMock := &MockIAMIntegration{
		authorizeFunc: func(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
			// Allow global sts:AssumeRole
			if action == "sts:AssumeRole" {
				return s3err.ErrNone
			}
			return s3err.ErrAccessDenied
		},
		validateTrustPolicyFunc: func(ctx context.Context, roleArn, principalArn string) error {
			// Allow all trust policies for this test
			return nil
		},
	}

	// Create IAM service with the mock integration
	iam := &IdentityAccessManagement{
		iamIntegration: iamMock,
	}

	// Create STS handlers
	stsHandlers := NewSTSHandlers(stsService, iam)

	// Test case 1: Caller is an IAM User, RoleArn is missing
	t.Run("Caller is IAM User, No RoleArn", func(t *testing.T) {
		// Mock request
		req, err := http.NewRequest("POST", "/", nil)
		require.NoError(t, err)
		req.Form = url.Values{}
		req.Form.Set("Action", "AssumeRole")
		req.Form.Set("RoleSessionName", "test-session")
		req.Form.Set("Version", "2011-06-15")

		// Mock the authenticated identity (IAM User)
		callerIdentity := &Identity{
			Name:         "alice",
			Account:      &AccountAdmin,
			PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/alice", defaultAccountID),
		}

		// 1. Test prepareSTSCredentials with NO RoleArn (simulating the fallback logic having passed PrincipalArn)
		// expected RoleArn passed to prepareSTSCredentials would be the caller's PrincipalArn
		fallbackRoleArn := callerIdentity.PrincipalArn

		stsCreds, assumedUser, err := stsHandlers.prepareSTSCredentials(fallbackRoleArn, "test-session", nil, "", nil)
		require.NoError(t, err)

		// Assertions
		// The role name should be extracted from the user ARN ("alice")
		assert.Contains(t, assumedUser.Arn, fmt.Sprintf("assumed-role/alice/test-session"))
		assert.Contains(t, assumedUser.AssumedRoleId, "alice:test-session")

		// Verify token claims using ValidateSessionToken
		sessionInfo, err := stsService.ValidateSessionToken(context.Background(), stsCreds.SessionToken)
		require.NoError(t, err)

		// The RoleArn in session info should match the fallback ARN (user ARN)
		assert.Equal(t, fallbackRoleArn, sessionInfo.RoleArn)
	})

	// Test case 2: Caller is an STS Assumed Role, No RoleArn
	t.Run("Caller is STS Assumed Role, No RoleArn", func(t *testing.T) {
		// Mock identity
		callerIdentity := &Identity{
			Name:         "arn:aws:sts::111122223333:assumed-role/admin/session1",
			Account:      &AccountAdmin,
			PrincipalArn: "arn:aws:sts::111122223333:assumed-role/admin/session1",
		}

		fallbackRoleArn := callerIdentity.PrincipalArn

		stsCreds, assumedUser, err := stsHandlers.prepareSTSCredentials(fallbackRoleArn, "nested-session", nil, "", nil)
		require.NoError(t, err)

		// The role name should be extracted from the assumed role ARN ("admin")
		assert.Contains(t, assumedUser.Arn, "assumed-role/admin/nested-session")
		assert.Contains(t, assumedUser.AssumedRoleId, "admin:nested-session")

		// Check claims
		sessionInfo, err := stsService.ValidateSessionToken(context.Background(), stsCreds.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, fallbackRoleArn, sessionInfo.RoleArn)
	})

	// Test case 3: Explicit RoleArn provided (Standard AssumeRole)
	t.Run("Explicit RoleArn Provided", func(t *testing.T) {
		explicitRoleArn := "arn:aws:iam::111122223333:role/TargetRole"

		stsCreds, assumedUser, err := stsHandlers.prepareSTSCredentials(explicitRoleArn, "explicit-session", nil, "", nil)
		require.NoError(t, err)

		// Role name should be "TargetRole"
		assert.Contains(t, assumedUser.Arn, "assumed-role/TargetRole/explicit-session")

		// Check claims
		sessionInfo, err := stsService.ValidateSessionToken(context.Background(), stsCreds.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, explicitRoleArn, sessionInfo.RoleArn)
	})

	// Test case 4: Malformed ARN (Edge case)
	t.Run("Malformed ARN", func(t *testing.T) {
		malformedArn := "invalid-arn"

		stsCreds, assumedUser, err := stsHandlers.prepareSTSCredentials(malformedArn, "bad-session", nil, "", nil)
		require.NoError(t, err)

		// Fallback behavior: use full string as role name if extraction fails
		assert.Contains(t, assumedUser.Arn, "assumed-role/invalid-arn/bad-session")

		sessionInfo, err := stsService.ValidateSessionToken(context.Background(), stsCreds.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, malformedArn, sessionInfo.RoleArn)
	})
}
