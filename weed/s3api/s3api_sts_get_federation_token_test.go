package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetFederationToken_BasicFlow tests basic credential generation for GetFederationToken
func TestGetFederationToken_BasicFlow(t *testing.T) {
	stsService, _ := setupTestSTSService(t)

	iam := &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	}
	stsHandlers := NewSTSHandlers(stsService, iam)

	// Simulate the core logic of handleGetFederationToken
	name := "BobApp"
	callerIdentity := &Identity{
		Name:         "alice",
		PrincipalArn: fmt.Sprintf("arn:aws:iam::%s:user/alice", defaultAccountID),
		PolicyNames:  []string{"S3ReadPolicy"},
	}

	accountID := stsHandlers.getAccountID()

	// Generate session ID and credentials
	sessionId, err := sts.GenerateSessionId()
	require.NoError(t, err)

	expiration := time.Now().Add(12 * time.Hour)
	federatedUserArn := fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", accountID, name)
	federatedUserId := fmt.Sprintf("%s:%s", accountID, name)

	claims := sts.NewSTSSessionClaims(sessionId, stsService.Config.Issuer, expiration).
		WithSessionName(name).
		WithRoleInfo(callerIdentity.PrincipalArn, federatedUserId, federatedUserArn).
		WithPolicies(callerIdentity.PolicyNames)

	sessionToken, err := stsService.GetTokenGenerator().GenerateJWTWithClaims(claims)
	require.NoError(t, err)

	// Validate the session token
	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)

	// Verify the session info contains caller's policies
	assert.Equal(t, []string{"S3ReadPolicy"}, sessionInfo.Policies)

	// Verify principal is the federated user ARN
	assert.Equal(t, federatedUserArn, sessionInfo.Principal)

	// Verify the RoleArn points to the caller's identity (for policy resolution)
	assert.Equal(t, callerIdentity.PrincipalArn, sessionInfo.RoleArn)

	// Verify session name
	assert.Equal(t, name, sessionInfo.SessionName)
}

// TestGetFederationToken_WithSessionPolicy tests session policy scoping
func TestGetFederationToken_WithSessionPolicy(t *testing.T) {
	stsService, _ := setupTestSTSService(t)

	stsHandlers := NewSTSHandlers(stsService, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	accountID := stsHandlers.getAccountID()
	name := "ScopedApp"

	sessionPolicyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::my-bucket/*"]}]}`
	normalizedPolicy, err := sts.NormalizeSessionPolicy(sessionPolicyJSON)
	require.NoError(t, err)

	sessionId, err := sts.GenerateSessionId()
	require.NoError(t, err)

	expiration := time.Now().Add(12 * time.Hour)
	federatedUserArn := fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", accountID, name)
	federatedUserId := fmt.Sprintf("%s:%s", accountID, name)

	claims := sts.NewSTSSessionClaims(sessionId, stsService.Config.Issuer, expiration).
		WithSessionName(name).
		WithRoleInfo("arn:aws:iam::000000000000:user/caller", federatedUserId, federatedUserArn).
		WithPolicies([]string{"S3FullAccess"}).
		WithSessionPolicy(normalizedPolicy)

	sessionToken, err := stsService.GetTokenGenerator().GenerateJWTWithClaims(claims)
	require.NoError(t, err)

	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)

	// Verify session policy is embedded
	assert.NotEmpty(t, sessionInfo.SessionPolicy)
	assert.Contains(t, sessionInfo.SessionPolicy, "s3:GetObject")

	// Verify caller's policies are still present
	assert.Equal(t, []string{"S3FullAccess"}, sessionInfo.Policies)
}

// TestGetFederationToken_RejectTemporaryCredentials tests that requests with
// session tokens are rejected.
func TestGetFederationToken_RejectTemporaryCredentials(t *testing.T) {
	stsService, _ := setupTestSTSService(t)
	stsHandlers := NewSTSHandlers(stsService, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	tests := []struct {
		name        string
		setToken    func(r *http.Request)
		description string
	}{
		{
			name: "SessionTokenInHeader",
			setToken: func(r *http.Request) {
				r.Header.Set("X-Amz-Security-Token", "some-session-token")
			},
			description: "Session token in X-Amz-Security-Token header should be rejected",
		},
		{
			name: "SessionTokenInQuery",
			setToken: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Security-Token", "some-session-token")
				r.URL.RawQuery = q.Encode()
			},
			description: "Session token in query string should be rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			form := url.Values{}
			form.Set("Action", "GetFederationToken")
			form.Set("Name", "TestUser")
			form.Set("Version", "2011-06-15")

			req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			tt.setToken(req)

			// Parse form so the handler can read it
			require.NoError(t, req.ParseForm())
			// Re-set values after parse
			req.Form.Set("Action", "GetFederationToken")
			req.Form.Set("Name", "TestUser")
			req.Form.Set("Version", "2011-06-15")

			rr := httptest.NewRecorder()
			stsHandlers.HandleSTSRequest(rr, req)

			// The handler should reject before SigV4 check or after it,
			// but either way return 403 AccessDenied
			assert.Equal(t, http.StatusForbidden, rr.Code, tt.description)
			assert.Contains(t, rr.Body.String(), "AccessDenied")
		})
	}
}

// TestGetFederationToken_MissingName tests that a missing Name parameter returns an error
func TestGetFederationToken_MissingName(t *testing.T) {
	stsService, _ := setupTestSTSService(t)
	stsHandlers := NewSTSHandlers(stsService, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	req := httptest.NewRequest("POST", "/", nil)
	req.Form = url.Values{}
	req.Form.Set("Action", "GetFederationToken")
	req.Form.Set("Version", "2011-06-15")
	// Name is intentionally omitted

	rr := httptest.NewRecorder()
	stsHandlers.HandleSTSRequest(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Name is required")
}

// TestGetFederationToken_NameValidation tests Name parameter validation
func TestGetFederationToken_NameValidation(t *testing.T) {
	stsService, _ := setupTestSTSService(t)
	stsHandlers := NewSTSHandlers(stsService, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	tests := []struct {
		name        string
		federName   string
		expectError bool
		errContains string
	}{
		{
			name:        "TooShort",
			federName:   "A",
			expectError: true,
			errContains: "between 2 and 64",
		},
		{
			name:        "TooLong",
			federName:   strings.Repeat("A", 65),
			expectError: true,
			errContains: "between 2 and 64",
		},
		{
			name:        "MinLength",
			federName:   "AB",
			expectError: false,
		},
		{
			name:        "MaxLength",
			federName:   strings.Repeat("A", 64),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", nil)
			req.Form = url.Values{}
			req.Form.Set("Action", "GetFederationToken")
			req.Form.Set("Name", tt.federName)
			req.Form.Set("Version", "2011-06-15")

			rr := httptest.NewRecorder()
			stsHandlers.HandleSTSRequest(rr, req)

			if tt.expectError {
				assert.Equal(t, http.StatusBadRequest, rr.Code)
				assert.Contains(t, rr.Body.String(), tt.errContains)
			} else {
				// Valid name should proceed past validation — will fail at SigV4
				// (returns 403 because we have no real signature)
				assert.NotEqual(t, http.StatusBadRequest, rr.Code,
					"Valid name should not produce a 400 for name validation")
			}
		})
	}
}

// TestGetFederationToken_DurationValidation tests DurationSeconds validation
func TestGetFederationToken_DurationValidation(t *testing.T) {
	stsService, _ := setupTestSTSService(t)
	stsHandlers := NewSTSHandlers(stsService, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	tests := []struct {
		name        string
		duration    string
		expectError bool
		errContains string
	}{
		{
			name:        "BelowMinimum",
			duration:    "899",
			expectError: true,
			errContains: "between",
		},
		{
			name:        "AboveMaximum",
			duration:    "129601",
			expectError: true,
			errContains: "between",
		},
		{
			name:        "InvalidFormat",
			duration:    "not-a-number",
			expectError: true,
			errContains: "invalid DurationSeconds",
		},
		{
			name:        "MinimumValid",
			duration:    "900",
			expectError: false,
		},
		{
			name:        "MaximumValid_36Hours",
			duration:    "129600",
			expectError: false,
		},
		{
			name:        "Default12Hours",
			duration:    "43200",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", nil)
			req.Form = url.Values{}
			req.Form.Set("Action", "GetFederationToken")
			req.Form.Set("Name", "TestUser")
			req.Form.Set("DurationSeconds", tt.duration)
			req.Form.Set("Version", "2011-06-15")

			rr := httptest.NewRecorder()
			stsHandlers.HandleSTSRequest(rr, req)

			if tt.expectError {
				assert.Equal(t, http.StatusBadRequest, rr.Code)
				assert.Contains(t, rr.Body.String(), tt.errContains)
			} else {
				// Valid duration should proceed past validation — will fail at SigV4
				assert.NotEqual(t, http.StatusBadRequest, rr.Code,
					"Valid duration should not produce a 400 for duration validation")
			}
		})
	}
}

// TestGetFederationToken_ResponseFormat tests the XML response structure
func TestGetFederationToken_ResponseFormat(t *testing.T) {
	// Verify the response XML structure matches AWS format
	response := GetFederationTokenResponse{
		Result: GetFederationTokenResult{
			Credentials: STSCredentials{
				AccessKeyId:     "ASIA1234567890",
				SecretAccessKey: "secret123",
				SessionToken:    "token123",
				Expiration:      "2026-04-02T12:00:00Z",
			},
			FederatedUser: FederatedUser{
				FederatedUserId: "000000000000:BobApp",
				Arn:             "arn:aws:sts::000000000000:federated-user/BobApp",
			},
		},
	}
	response.ResponseMetadata.RequestId = "test-request-id"

	data, err := xml.MarshalIndent(response, "", "  ")
	require.NoError(t, err)

	xmlStr := string(data)
	assert.Contains(t, xmlStr, "GetFederationTokenResponse")
	assert.Contains(t, xmlStr, "GetFederationTokenResult")
	assert.Contains(t, xmlStr, "FederatedUser")
	assert.Contains(t, xmlStr, "FederatedUserId")
	assert.Contains(t, xmlStr, "federated-user/BobApp")
	assert.Contains(t, xmlStr, "ASIA1234567890")
	assert.Contains(t, xmlStr, "test-request-id")

	// Verify it can be unmarshaled back
	var parsed GetFederationTokenResponse
	err = xml.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "ASIA1234567890", parsed.Result.Credentials.AccessKeyId)
	assert.Equal(t, "arn:aws:sts::000000000000:federated-user/BobApp", parsed.Result.FederatedUser.Arn)
	assert.Equal(t, "000000000000:BobApp", parsed.Result.FederatedUser.FederatedUserId)
}

// TestGetFederationToken_PolicyEmbedding tests that the caller's policies are embedded
// into the session token using the IAM integration manager
func TestGetFederationToken_PolicyEmbedding(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)

	// Create a policy that the user has attached
	userPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect:   "Allow",
				Action:   []string{"s3:GetObject", "s3:PutObject"},
				Resource: []string{"arn:aws:s3:::user-bucket/*"},
			},
		},
	}
	require.NoError(t, manager.CreatePolicy(ctx, "", "UserS3Policy", userPolicy))

	stsService := manager.GetSTSService()

	// Simulate what handleGetFederationToken does for policy embedding
	name := "AppClient"
	callerPolicies := []string{"UserS3Policy"}

	sessionId, err := sts.GenerateSessionId()
	require.NoError(t, err)

	expiration := time.Now().Add(12 * time.Hour)
	accountID := defaultAccountID
	federatedUserArn := fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", accountID, name)
	federatedUserId := fmt.Sprintf("%s:%s", accountID, name)

	claims := sts.NewSTSSessionClaims(sessionId, stsService.Config.Issuer, expiration).
		WithSessionName(name).
		WithRoleInfo("arn:aws:iam::000000000000:user/caller", federatedUserId, federatedUserArn).
		WithPolicies(callerPolicies)

	sessionToken, err := stsService.GetTokenGenerator().GenerateJWTWithClaims(claims)
	require.NoError(t, err)

	sessionInfo, err := stsService.ValidateSessionToken(ctx, sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)

	// Verify the caller's policy names are embedded
	assert.Equal(t, []string{"UserS3Policy"}, sessionInfo.Policies)
}

// TestGetFederationToken_PolicyIntersection tests that both the caller's base policies
// and the restrictive session policy are embedded in the token, enabling the
// authorization layer to compute their intersection at request time.
func TestGetFederationToken_PolicyIntersection(t *testing.T) {
	ctx := context.Background()
	manager := newTestSTSIntegrationManager(t)

	// Create a broad policy for the caller
	broadPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect:   "Allow",
				Action:   []string{"s3:*"},
				Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
			},
		},
	}
	require.NoError(t, manager.CreatePolicy(ctx, "", "S3FullAccess", broadPolicy))

	stsService := manager.GetSTSService()

	// Session policy restricts to one bucket and one action
	sessionPolicyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::restricted-bucket/*"]}]}`
	normalizedPolicy, err := sts.NormalizeSessionPolicy(sessionPolicyJSON)
	require.NoError(t, err)

	sessionId, err := sts.GenerateSessionId()
	require.NoError(t, err)

	expiration := time.Now().Add(12 * time.Hour)
	name := "RestrictedApp"
	accountID := defaultAccountID
	federatedUserArn := fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", accountID, name)
	federatedUserId := fmt.Sprintf("%s:%s", accountID, name)

	claims := sts.NewSTSSessionClaims(sessionId, stsService.Config.Issuer, expiration).
		WithSessionName(name).
		WithRoleInfo("arn:aws:iam::000000000000:user/caller", federatedUserId, federatedUserArn).
		WithPolicies([]string{"S3FullAccess"}).
		WithSessionPolicy(normalizedPolicy)

	sessionToken, err := stsService.GetTokenGenerator().GenerateJWTWithClaims(claims)
	require.NoError(t, err)

	sessionInfo, err := stsService.ValidateSessionToken(ctx, sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)

	// Verify both the broad base policies and the restrictive session policy are embedded
	// The authorization layer computes intersection at request time
	assert.Equal(t, []string{"S3FullAccess"}, sessionInfo.Policies,
		"Caller's base policies should be embedded in token")
	assert.Contains(t, sessionInfo.SessionPolicy, "restricted-bucket",
		"Session policy should restrict to specific bucket")
	assert.Contains(t, sessionInfo.SessionPolicy, "s3:GetObject",
		"Session policy should restrict to specific action")
}

// TestGetFederationToken_MalformedPolicy tests that invalid policy JSON is rejected
// by the session policy normalization used in the handler
func TestGetFederationToken_MalformedPolicy(t *testing.T) {
	tests := []struct {
		name      string
		policyStr string
		expectErr bool
	}{
		{
			name:      "InvalidJSON",
			policyStr: "not-valid-json",
			expectErr: true,
		},
		{
			name:      "EmptyObject",
			policyStr: "{}",
			expectErr: true,
		},
		{
			name:      "TooLarge",
			policyStr: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["` + strings.Repeat("a", 2048) + `"]}]}`,
			expectErr: true,
		},
		{
			name:      "ValidPolicy",
			policyStr: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}`,
			expectErr: false,
		},
		{
			name:      "EmptyString",
			policyStr: "",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sts.NormalizeSessionPolicy(tt.policyStr)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGetFederationToken_STSNotReady tests that the handler returns 503 when STS is not initialized
func TestGetFederationToken_STSNotReady(t *testing.T) {
	// Create handlers with nil STS service
	stsHandlers := NewSTSHandlers(nil, &IdentityAccessManagement{
		iamIntegration: &MockIAMIntegration{},
	})

	req := httptest.NewRequest("POST", "/", nil)
	req.Form = url.Values{}
	req.Form.Set("Action", "GetFederationToken")
	req.Form.Set("Name", "TestUser")
	req.Form.Set("Version", "2011-06-15")

	rr := httptest.NewRecorder()
	stsHandlers.HandleSTSRequest(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.Contains(t, rr.Body.String(), "ServiceUnavailable")
}

// TestGetFederationToken_DefaultDuration tests that the default duration is 12 hours
func TestGetFederationToken_DefaultDuration(t *testing.T) {
	assert.Equal(t, int64(43200), defaultFederationDurationSeconds, "Default duration should be 12 hours (43200 seconds)")
	assert.Equal(t, int64(129600), maxFederationDurationSeconds, "Max duration should be 36 hours (129600 seconds)")
}
