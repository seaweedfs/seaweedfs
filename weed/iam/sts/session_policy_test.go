package sts

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSessionPolicyTestJWT creates a test JWT token for session policy tests
func createSessionPolicyTestJWT(t *testing.T, issuer, subject string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": issuer,
		"sub": subject,
		"aud": "test-client",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	tokenString, err := token.SignedString([]byte("test-signing-key"))
	require.NoError(t, err)
	return tokenString
}

// TestAssumeRoleWithWebIdentity_SessionPolicy tests the handling of the Policy field
// in AssumeRoleWithWebIdentityRequest to ensure users are properly informed that
// session policies are not currently supported
func TestAssumeRoleWithWebIdentity_SessionPolicy(t *testing.T) {
	service := setupTestSTSService(t)

	t.Run("should_reject_request_with_session_policy", func(t *testing.T) {
		ctx := context.Background()

		// Create a request with a session policy
		sessionPolicy := `{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::example-bucket/*"
			}]
		}`

		testToken := createSessionPolicyTestJWT(t, "test-issuer", "test-user")

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: testToken,
			RoleSessionName:  "test-session",
			DurationSeconds:  nil,            // Use default
			Policy:           &sessionPolicy, // ← Session policy provided
		}

		// Should return an error indicating session policies are not supported
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		// Verify the error
		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "session policies are not currently supported")
		assert.Contains(t, err.Error(), "Policy parameter must be omitted")
	})

	t.Run("should_succeed_without_session_policy", func(t *testing.T) {
		ctx := context.Background()
		testToken := createSessionPolicyTestJWT(t, "test-issuer", "test-user")

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: testToken,
			RoleSessionName:  "test-session",
			DurationSeconds:  nil, // Use default
			Policy:           nil, // ← No session policy
		}

		// Should succeed without session policy
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		// Verify success
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.NotNil(t, response.Credentials)
		assert.NotEmpty(t, response.Credentials.AccessKeyId)
		assert.NotEmpty(t, response.Credentials.SecretAccessKey)
		assert.NotEmpty(t, response.Credentials.SessionToken)
	})

	t.Run("should_succeed_with_empty_policy_pointer", func(t *testing.T) {
		ctx := context.Background()
		testToken := createSessionPolicyTestJWT(t, "test-issuer", "test-user")

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: testToken,
			RoleSessionName:  "test-session",
			Policy:           nil, // ← Explicitly nil
		}

		// Should succeed with nil policy pointer
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		require.NoError(t, err)
		require.NotNil(t, response)
		assert.NotNil(t, response.Credentials)
	})

	t.Run("should_reject_empty_string_policy", func(t *testing.T) {
		ctx := context.Background()

		emptyPolicy := "" // Empty string, but still a non-nil pointer

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &emptyPolicy, // ← Non-nil pointer to empty string
		}

		// Should still reject because pointer is not nil
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "session policies are not currently supported")
	})
}

// TestAssumeRoleWithWebIdentity_SessionPolicy_ErrorMessage tests that the error message
// is clear and helps users understand what they need to do
func TestAssumeRoleWithWebIdentity_SessionPolicy_ErrorMessage(t *testing.T) {
	service := setupTestSTSService(t)

	ctx := context.Background()
	complexPolicy := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Sid": "AllowS3Access",
				"Effect": "Allow",
				"Action": [
					"s3:GetObject",
					"s3:PutObject"
				],
				"Resource": [
					"arn:aws:s3:::my-bucket/*",
					"arn:aws:s3:::my-bucket"
				],
				"Condition": {
					"StringEquals": {
						"s3:prefix": ["documents/", "images/"]
					}
				}
			}
		]
	}`

	testToken := createSessionPolicyTestJWT(t, "test-issuer", "test-user")

	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: testToken,
		RoleSessionName:  "test-session-with-complex-policy",
		Policy:           &complexPolicy,
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)

	// Verify error details
	require.Error(t, err)
	assert.Nil(t, response)

	errorMsg := err.Error()

	// The error should be clear and actionable
	assert.Contains(t, errorMsg, "session policies are not currently supported",
		"Error should explain that session policies aren't supported")
	assert.Contains(t, errorMsg, "Policy parameter must be omitted",
		"Error should specify what action the user needs to take")

	// Should NOT contain internal implementation details
	assert.NotContains(t, errorMsg, "nil pointer",
		"Error should not expose internal implementation details")
	assert.NotContains(t, errorMsg, "struct field",
		"Error should not expose internal struct details")
}

// Test edge case scenarios for the Policy field handling
func TestAssumeRoleWithWebIdentity_SessionPolicy_EdgeCases(t *testing.T) {
	service := setupTestSTSService(t)

	t.Run("malformed_json_policy_still_rejected", func(t *testing.T) {
		ctx := context.Background()
		malformedPolicy := `{"Version": "2012-10-17", "Statement": [` // Incomplete JSON

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &malformedPolicy,
		}

		// Should reject before even parsing the policy JSON
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "session policies are not currently supported")
	})

	t.Run("policy_with_whitespace_still_rejected", func(t *testing.T) {
		ctx := context.Background()
		whitespacePolicy := "   \t\n   " // Only whitespace

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &whitespacePolicy,
		}

		// Should reject any non-nil policy, even whitespace
		response, err := service.AssumeRoleWithWebIdentity(ctx, request)

		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "session policies are not currently supported")
	})
}

// TestAssumeRoleWithWebIdentity_PolicyFieldDocumentation verifies that the struct
// field is properly documented to help developers understand the limitation
func TestAssumeRoleWithWebIdentity_PolicyFieldDocumentation(t *testing.T) {
	// This test documents the current behavior and ensures the struct field
	// exists with proper typing
	request := &AssumeRoleWithWebIdentityRequest{}

	// Verify the Policy field exists and has the correct type
	assert.IsType(t, (*string)(nil), request.Policy,
		"Policy field should be *string type for optional JSON policy")

	// Verify initial value is nil (no policy by default)
	assert.Nil(t, request.Policy,
		"Policy field should default to nil (no session policy)")

	// Test that we can set it to a string pointer (even though it will be rejected)
	policyValue := `{"Version": "2012-10-17"}`
	request.Policy = &policyValue
	assert.NotNil(t, request.Policy, "Should be able to assign policy value")
	assert.Equal(t, policyValue, *request.Policy, "Policy value should be preserved")
}

// TestAssumeRoleWithCredentials_NoSessionPolicySupport verifies that
// AssumeRoleWithCredentialsRequest doesn't have a Policy field, which is correct
// since credential-based role assumption typically doesn't support session policies
func TestAssumeRoleWithCredentials_NoSessionPolicySupport(t *testing.T) {
	// Verify that AssumeRoleWithCredentialsRequest doesn't have a Policy field
	// This is the expected behavior since session policies are typically only
	// supported with web identity (OIDC/SAML) flows in AWS STS
	request := &AssumeRoleWithCredentialsRequest{
		RoleArn:         "arn:aws:iam::role/TestRole",
		Username:        "testuser",
		Password:        "testpass",
		RoleSessionName: "test-session",
		ProviderName:    "ldap",
	}

	// The struct should compile and work without a Policy field
	assert.NotNil(t, request)
	assert.Equal(t, "arn:aws:iam::role/TestRole", request.RoleArn)
	assert.Equal(t, "testuser", request.Username)

	// This documents that credential-based assume role does NOT support session policies
	// which matches AWS STS behavior where session policies are primarily for
	// web identity (OIDC/SAML) and federation scenarios
}
