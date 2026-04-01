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

// TestAssumeRoleWithWebIdentity_SessionPolicy verifies inline session policies are preserved in tokens.
func TestAssumeRoleWithWebIdentity_SessionPolicy(t *testing.T) {
	service := setupTestSTSService(t)
	ctx := context.Background()

	sessionPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::example-bucket/*"}]}`
	testToken := createSessionPolicyTestJWT(t, "test-issuer", "test-user")

	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: testToken,
		RoleSessionName:  "test-session",
		Policy:           &sessionPolicy,
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	sessionInfo, err := service.ValidateSessionToken(ctx, response.Credentials.SessionToken)
	require.NoError(t, err)

	normalized, err := NormalizeSessionPolicy(sessionPolicy)
	require.NoError(t, err)
	assert.Equal(t, normalized, sessionInfo.SessionPolicy)

	t.Run("should_succeed_without_session_policy", func(t *testing.T) {
		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
		}

		response, err := service.AssumeRoleWithWebIdentity(ctx, request)
		require.NoError(t, err)
		require.NotNil(t, response)

		sessionInfo, err := service.ValidateSessionToken(ctx, response.Credentials.SessionToken)
		require.NoError(t, err)
		assert.Empty(t, sessionInfo.SessionPolicy)
	})
}

// Test edge case scenarios for the Policy field handling
func TestAssumeRoleWithWebIdentity_SessionPolicy_EdgeCases(t *testing.T) {
	service := setupTestSTSService(t)
	ctx := context.Background()

	t.Run("malformed_json_policy_rejected", func(t *testing.T) {
		malformedPolicy := `{"Version": "2012-10-17", "Statement": [` // Incomplete JSON

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &malformedPolicy,
		}

		response, err := service.AssumeRoleWithWebIdentity(ctx, request)
		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "invalid session policy JSON")
	})

	t.Run("invalid_policy_document_rejected", func(t *testing.T) {
		invalidPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &invalidPolicy,
		}

		response, err := service.AssumeRoleWithWebIdentity(ctx, request)
		assert.Error(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), "invalid session policy document")
	})

	t.Run("whitespace_policy_ignored", func(t *testing.T) {
		whitespacePolicy := "   \t\n   "

		request := &AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/TestRole",
			WebIdentityToken: createSessionPolicyTestJWT(t, "test-issuer", "test-user"),
			RoleSessionName:  "test-session",
			Policy:           &whitespacePolicy,
		}

		response, err := service.AssumeRoleWithWebIdentity(ctx, request)
		require.NoError(t, err)
		require.NotNil(t, response)

		sessionInfo, err := service.ValidateSessionToken(ctx, response.Credentials.SessionToken)
		require.NoError(t, err)
		assert.Empty(t, sessionInfo.SessionPolicy)
	})
}

// TestAssumeRoleWithWebIdentity_PolicyFieldDocumentation verifies that the struct field exists and is optional.
func TestAssumeRoleWithWebIdentity_PolicyFieldDocumentation(t *testing.T) {
	request := &AssumeRoleWithWebIdentityRequest{}

	assert.IsType(t, (*string)(nil), request.Policy,
		"Policy field should be *string type for optional JSON policy")
	assert.Nil(t, request.Policy,
		"Policy field should default to nil (no session policy)")

	policyValue := `{"Version": "2012-10-17"}`
	request.Policy = &policyValue
	assert.NotNil(t, request.Policy, "Should be able to assign policy value")
	assert.Equal(t, policyValue, *request.Policy, "Policy value should be preserved")
}

// TestAssumeRoleWithCredentials_SessionPolicy verifies session policy support for credentials-based flow.
func TestAssumeRoleWithCredentials_SessionPolicy(t *testing.T) {
	service := setupTestSTSService(t)
	ctx := context.Background()

	sessionPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"filer:CreateEntry","Resource":"arn:aws:filer::path/user-docs/*"}]}`
	request := &AssumeRoleWithCredentialsRequest{
		RoleArn:         "arn:aws:iam::role/TestRole",
		Username:        "testuser",
		Password:        "testpass",
		RoleSessionName: "test-session",
		ProviderName:    "test-ldap",
		Policy:          &sessionPolicy,
	}

	response, err := service.AssumeRoleWithCredentials(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	sessionInfo, err := service.ValidateSessionToken(ctx, response.Credentials.SessionToken)
	require.NoError(t, err)

	normalized, err := NormalizeSessionPolicy(sessionPolicy)
	require.NoError(t, err)
	assert.Equal(t, normalized, sessionInfo.SessionPolicy)
}
