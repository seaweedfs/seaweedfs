package sts

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSTSTestJWT creates a test JWT token for STS service tests
func createSTSTestJWT(t *testing.T, issuer, subject string) string {
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

// TestSTSServiceInitialization tests STS service initialization
func TestSTSServiceInitialization(t *testing.T) {
	tests := []struct {
		name    string
		config  *STSConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &STSConfig{
				TokenDuration:    FlexibleDuration{time.Hour},
				MaxSessionLength: FlexibleDuration{time.Hour * 12},
				Issuer:           "seaweedfs-sts",
				SigningKey:       []byte("test-signing-key"),
			},
			wantErr: false,
		},
		{
			name: "missing signing key",
			config: &STSConfig{
				TokenDuration: FlexibleDuration{time.Hour},
				Issuer:        "seaweedfs-sts",
			},
			wantErr: true,
		},
		{
			name: "invalid token duration",
			config: &STSConfig{
				TokenDuration: FlexibleDuration{-time.Hour},
				Issuer:        "seaweedfs-sts",
				SigningKey:    []byte("test-key"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewSTSService()

			err := service.Initialize(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.True(t, service.IsInitialized())
			}
		})
	}
}

// TestAssumeRoleWithWebIdentity tests role assumption with OIDC tokens
func TestAssumeRoleWithWebIdentity(t *testing.T) {
	service := setupTestSTSService(t)

	tests := []struct {
		name             string
		roleArn          string
		webIdentityToken string
		sessionName      string
		durationSeconds  *int64
		wantErr          bool
		expectedSubject  string
	}{
		{
			name:             "successful role assumption",
			roleArn:          "arn:aws:iam::role/TestRole",
			webIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user-id"),
			sessionName:      "test-session",
			durationSeconds:  nil, // Use default
			wantErr:          false,
			expectedSubject:  "test-user-id",
		},
		{
			name:             "invalid web identity token",
			roleArn:          "arn:aws:iam::role/TestRole",
			webIdentityToken: "invalid-token",
			sessionName:      "test-session",
			wantErr:          true,
		},
		{
			name:             "non-existent role",
			roleArn:          "arn:aws:iam::role/NonExistentRole",
			webIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user"),
			sessionName:      "test-session",
			wantErr:          true,
		},
		{
			name:             "custom session duration",
			roleArn:          "arn:aws:iam::role/TestRole",
			webIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user"),
			sessionName:      "test-session",
			durationSeconds:  int64Ptr(7200), // 2 hours
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			request := &AssumeRoleWithWebIdentityRequest{
				RoleArn:          tt.roleArn,
				WebIdentityToken: tt.webIdentityToken,
				RoleSessionName:  tt.sessionName,
				DurationSeconds:  tt.durationSeconds,
			}

			response, err := service.AssumeRoleWithWebIdentity(ctx, request)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.NotNil(t, response.Credentials)
				assert.NotNil(t, response.AssumedRoleUser)

				// Verify credentials
				creds := response.Credentials
				assert.NotEmpty(t, creds.AccessKeyId)
				assert.NotEmpty(t, creds.SecretAccessKey)
				assert.NotEmpty(t, creds.SessionToken)
				assert.True(t, creds.Expiration.After(time.Now()))

				// Verify assumed role user
				user := response.AssumedRoleUser
				assert.Equal(t, tt.roleArn, user.AssumedRoleId)
				assert.Contains(t, user.Arn, tt.sessionName)

				if tt.expectedSubject != "" {
					assert.Equal(t, tt.expectedSubject, user.Subject)
				}
			}
		})
	}
}

// TestAssumeRoleWithLDAP tests role assumption with LDAP credentials
func TestAssumeRoleWithLDAP(t *testing.T) {
	service := setupTestSTSService(t)

	tests := []struct {
		name        string
		roleArn     string
		username    string
		password    string
		sessionName string
		wantErr     bool
	}{
		{
			name:        "successful LDAP role assumption",
			roleArn:     "arn:aws:iam::role/LDAPRole",
			username:    "testuser",
			password:    "testpass",
			sessionName: "ldap-session",
			wantErr:     false,
		},
		{
			name:        "invalid LDAP credentials",
			roleArn:     "arn:aws:iam::role/LDAPRole",
			username:    "testuser",
			password:    "wrongpass",
			sessionName: "ldap-session",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			request := &AssumeRoleWithCredentialsRequest{
				RoleArn:         tt.roleArn,
				Username:        tt.username,
				Password:        tt.password,
				RoleSessionName: tt.sessionName,
				ProviderName:    "test-ldap",
			}

			response, err := service.AssumeRoleWithCredentials(ctx, request)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.NotNil(t, response.Credentials)
			}
		})
	}
}

// TestSessionTokenValidation tests session token validation
func TestSessionTokenValidation(t *testing.T) {
	service := setupTestSTSService(t)
	ctx := context.Background()

	// First, create a session
	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user"),
		RoleSessionName:  "test-session",
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	sessionToken := response.Credentials.SessionToken

	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{
			name:    "valid session token",
			token:   sessionToken,
			wantErr: false,
		},
		{
			name:    "invalid session token",
			token:   "invalid-session-token",
			wantErr: true,
		},
		{
			name:    "empty session token",
			token:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, err := service.ValidateSessionToken(ctx, tt.token)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, session)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, session)
				assert.Equal(t, "test-session", session.SessionName)
				assert.Equal(t, "arn:aws:iam::role/TestRole", session.RoleArn)
			}
		})
	}
}

// TestSessionTokenPersistence tests that JWT tokens remain valid throughout their lifetime
// Note: In the stateless JWT design, tokens cannot be revoked and remain valid until expiration
func TestSessionTokenPersistence(t *testing.T) {
	service := setupTestSTSService(t)
	ctx := context.Background()

	// Create a session first
	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user"),
		RoleSessionName:  "test-session",
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken

	// Verify token is valid initially
	session, err := service.ValidateSessionToken(ctx, sessionToken)
	assert.NoError(t, err)
	assert.NotNil(t, session)
	assert.Equal(t, "test-session", session.SessionName)

	// In a stateless JWT system, tokens remain valid throughout their lifetime
	// Multiple validations should all succeed as long as the token hasn't expired
	session2, err := service.ValidateSessionToken(ctx, sessionToken)
	assert.NoError(t, err, "Token should remain valid in stateless system")
	assert.NotNil(t, session2, "Session should be returned from JWT token")
	assert.Equal(t, session.SessionId, session2.SessionId, "Session ID should be consistent")
}

// Helper functions

func setupTestSTSService(t *testing.T) *STSService {
	service := NewSTSService()

	config := &STSConfig{
		TokenDuration:    FlexibleDuration{time.Hour},
		MaxSessionLength: FlexibleDuration{time.Hour * 12},
		Issuer:           "test-sts",
		SigningKey:       []byte("test-signing-key-32-characters-long"),
	}

	err := service.Initialize(config)
	require.NoError(t, err)

	// Set up mock trust policy validator (required for STS testing)
	mockValidator := &MockTrustPolicyValidator{}
	service.SetTrustPolicyValidator(mockValidator)

	// Register test providers
	mockOIDCProvider := &MockIdentityProvider{
		name: "test-oidc",
		validTokens: map[string]*providers.TokenClaims{
			createSTSTestJWT(t, "test-issuer", "test-user"): {
				Subject: "test-user-id",
				Issuer:  "test-issuer",
				Claims: map[string]interface{}{
					"email": "test@example.com",
					"name":  "Test User",
				},
			},
		},
	}

	mockLDAPProvider := &MockIdentityProvider{
		name: "test-ldap",
		validCredentials: map[string]string{
			"testuser": "testpass",
		},
	}

	service.RegisterProvider(mockOIDCProvider)
	service.RegisterProvider(mockLDAPProvider)

	return service
}

func int64Ptr(v int64) *int64 {
	return &v
}

// Mock identity provider for testing
type MockIdentityProvider struct {
	name             string
	validTokens      map[string]*providers.TokenClaims
	validCredentials map[string]string
}

func (m *MockIdentityProvider) Name() string {
	return m.name
}

func (m *MockIdentityProvider) GetIssuer() string {
	return "test-issuer" // This matches the issuer in the token claims
}

func (m *MockIdentityProvider) Initialize(config interface{}) error {
	return nil
}

func (m *MockIdentityProvider) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	// First try to parse as JWT token
	if len(token) > 20 && strings.Count(token, ".") >= 2 {
		parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
		if err == nil {
			if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok {
				issuer, _ := claims["iss"].(string)
				subject, _ := claims["sub"].(string)

				// Verify the issuer matches what we expect
				if issuer == "test-issuer" && subject != "" {
					return &providers.ExternalIdentity{
						UserID:      subject,
						Email:       subject + "@test-domain.com",
						DisplayName: "Test User " + subject,
						Provider:    m.name,
					}, nil
				}
			}
		}
	}

	// Handle legacy OIDC tokens (for backwards compatibility)
	if claims, exists := m.validTokens[token]; exists {
		email, _ := claims.GetClaimString("email")
		name, _ := claims.GetClaimString("name")

		return &providers.ExternalIdentity{
			UserID:      claims.Subject,
			Email:       email,
			DisplayName: name,
			Provider:    m.name,
		}, nil
	}

	// Handle LDAP credentials (username:password format)
	if m.validCredentials != nil {
		parts := strings.Split(token, ":")
		if len(parts) == 2 {
			username, password := parts[0], parts[1]
			if expectedPassword, exists := m.validCredentials[username]; exists && expectedPassword == password {
				return &providers.ExternalIdentity{
					UserID:      username,
					Email:       username + "@" + m.name + ".com",
					DisplayName: "Test User " + username,
					Provider:    m.name,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("unknown test token: %s", token)
}

func (m *MockIdentityProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	return &providers.ExternalIdentity{
		UserID:   userID,
		Email:    userID + "@" + m.name + ".com",
		Provider: m.name,
	}, nil
}

func (m *MockIdentityProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	if claims, exists := m.validTokens[token]; exists {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}

// TestSessionDurationCappedByTokenExpiration tests that session duration is capped by the source token's exp claim
func TestSessionDurationCappedByTokenExpiration(t *testing.T) {
	service := NewSTSService()

	config := &STSConfig{
		TokenDuration:    FlexibleDuration{time.Hour}, // Default: 1 hour
		MaxSessionLength: FlexibleDuration{time.Hour * 12},
		Issuer:           "test-sts",
		SigningKey:       []byte("test-signing-key-32-characters-long"),
	}

	err := service.Initialize(config)
	require.NoError(t, err)

	tests := []struct {
		name               string
		durationSeconds    *int64
		tokenExpiration    *time.Time
		expectedMaxSeconds int64
		description        string
	}{
		{
			name:               "no token expiration - use default duration",
			durationSeconds:    nil,
			tokenExpiration:    nil,
			expectedMaxSeconds: 3600, // 1 hour default
			description:        "When no token expiration is set, use the configured default duration",
		},
		{
			name:               "token expires before default duration",
			durationSeconds:    nil,
			tokenExpiration:    timePtr(time.Now().Add(30 * time.Minute)),
			expectedMaxSeconds: 30 * 60, // 30 minutes
			description:        "When token expires in 30 min, session should be capped at 30 min",
		},
		{
			name:               "token expires after default duration - use default",
			durationSeconds:    nil,
			tokenExpiration:    timePtr(time.Now().Add(2 * time.Hour)),
			expectedMaxSeconds: 3600, // 1 hour default, since it's less than 2 hour token expiry
			description:        "When token expires after default duration, use the default duration",
		},
		{
			name:               "requested duration shorter than token expiry",
			durationSeconds:    int64Ptr(1800), // 30 min requested
			tokenExpiration:    timePtr(time.Now().Add(time.Hour)),
			expectedMaxSeconds: 1800, // 30 minutes as requested
			description:        "When requested duration is shorter than token expiry, use requested duration",
		},
		{
			name:               "requested duration longer than token expiry - cap at token expiry",
			durationSeconds:    int64Ptr(3600), // 1 hour requested
			tokenExpiration:    timePtr(time.Now().Add(15 * time.Minute)),
			expectedMaxSeconds: 15 * 60, // Capped at 15 minutes
			description:        "When requested duration exceeds token expiry, cap at token expiry",
		},
		{
			name:               "GitLab CI short-lived token scenario",
			durationSeconds:    nil,
			tokenExpiration:    timePtr(time.Now().Add(5 * time.Minute)),
			expectedMaxSeconds: 5 * 60, // 5 minutes
			description:        "GitLab CI job with 5 minute timeout should result in 5 minute session",
		},
		{
			name:               "already expired token - defense in depth",
			durationSeconds:    nil,
			tokenExpiration:    timePtr(time.Now().Add(-5 * time.Minute)), // Expired 5 minutes ago
			expectedMaxSeconds: 60,                                        // 1 minute minimum
			description:        "Already expired token should result in minimal 1 minute session",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := service.calculateSessionDuration(tt.durationSeconds, tt.tokenExpiration)

			// Allow 5 second tolerance for time calculations
			maxExpected := time.Duration(tt.expectedMaxSeconds+5) * time.Second
			minExpected := time.Duration(tt.expectedMaxSeconds-5) * time.Second

			assert.GreaterOrEqual(t, duration, minExpected,
				"%s: duration %v should be >= %v", tt.description, duration, minExpected)
			assert.LessOrEqual(t, duration, maxExpected,
				"%s: duration %v should be <= %v", tt.description, duration, maxExpected)
		})
	}
}

// TestAssumeRoleWithWebIdentityRespectsTokenExpiration tests end-to-end that session duration is capped
func TestAssumeRoleWithWebIdentityRespectsTokenExpiration(t *testing.T) {
	service := NewSTSService()

	config := &STSConfig{
		TokenDuration:    FlexibleDuration{time.Hour},
		MaxSessionLength: FlexibleDuration{time.Hour * 12},
		Issuer:           "test-sts",
		SigningKey:       []byte("test-signing-key-32-characters-long"),
	}

	err := service.Initialize(config)
	require.NoError(t, err)

	// Set up mock trust policy validator
	mockValidator := &MockTrustPolicyValidator{}
	service.SetTrustPolicyValidator(mockValidator)

	// Create a mock provider that returns tokens with short expiration
	shortLivedTokenExpiration := time.Now().Add(10 * time.Minute)
	mockProvider := &MockIdentityProviderWithExpiration{
		name:            "short-lived-issuer",
		tokenExpiration: &shortLivedTokenExpiration,
	}
	service.RegisterProvider(mockProvider)

	ctx := context.Background()

	// Create a JWT token with short expiration
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "short-lived-issuer",
		"sub": "test-user",
		"aud": "test-client",
		"exp": shortLivedTokenExpiration.Unix(),
		"iat": time.Now().Unix(),
	})
	tokenString, err := token.SignedString([]byte("test-signing-key"))
	require.NoError(t, err)

	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: tokenString,
		RoleSessionName:  "test-session",
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Verify the session expires at or before the token expiration
	// Allow 5 second tolerance
	assert.True(t, response.Credentials.Expiration.Before(shortLivedTokenExpiration.Add(5*time.Second)),
		"Session expiration (%v) should not exceed token expiration (%v)",
		response.Credentials.Expiration, shortLivedTokenExpiration)
}

// MockIdentityProviderWithExpiration is a mock provider that returns tokens with configurable expiration
type MockIdentityProviderWithExpiration struct {
	name            string
	tokenExpiration *time.Time
}

func (m *MockIdentityProviderWithExpiration) Name() string {
	return m.name
}

func (m *MockIdentityProviderWithExpiration) GetIssuer() string {
	return m.name
}

func (m *MockIdentityProviderWithExpiration) Initialize(config interface{}) error {
	return nil
}

func (m *MockIdentityProviderWithExpiration) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	// Parse the token to get subject
	parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims")
	}

	subject, _ := claims["sub"].(string)

	identity := &providers.ExternalIdentity{
		UserID:          subject,
		Email:           subject + "@example.com",
		DisplayName:     "Test User",
		Provider:        m.name,
		TokenExpiration: m.tokenExpiration,
	}

	return identity, nil
}

func (m *MockIdentityProviderWithExpiration) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	return &providers.ExternalIdentity{
		UserID:   userID,
		Provider: m.name,
	}, nil
}

func (m *MockIdentityProviderWithExpiration) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	claims := &providers.TokenClaims{
		Subject: "test-user",
		Issuer:  m.name,
	}
	if m.tokenExpiration != nil {
		claims.ExpiresAt = *m.tokenExpiration
	}
	return claims, nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// TestAssumeRoleWithWebIdentity_PreservesAttributes tests that attributes from the identity provider
// are correctly propagated to the session token's request context
func TestAssumeRoleWithWebIdentity_PreservesAttributes(t *testing.T) {
	service := setupTestSTSService(t)

	// Create a mock provider that returns a user with attributes
	mockProvider := &MockIdentityProviderWithAttributes{
		name: "attr-provider",
		attributes: map[string]string{
			"preferred_username": "my-user",
			"department":         "engineering",
			"project":            "seaweedfs",
		},
	}
	service.RegisterProvider(mockProvider)

	// Create a valid JWT token for the provider
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "attr-provider",
		"sub": "test-user-id",
		"aud": "test-client",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	tokenString, err := token.SignedString([]byte("test-signing-key"))
	require.NoError(t, err)

	ctx := context.Background()
	request := &AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/TestRole",
		WebIdentityToken: tokenString,
		RoleSessionName:  "test-session",
	}

	response, err := service.AssumeRoleWithWebIdentity(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Validate the session token to check claims
	sessionInfo, err := service.ValidateSessionToken(ctx, response.Credentials.SessionToken)
	require.NoError(t, err)

	// Check that attributes are present in RequestContext
	require.NotNil(t, sessionInfo.RequestContext, "RequestContext should not be nil")
	assert.Equal(t, "my-user", sessionInfo.RequestContext["preferred_username"])
	assert.Equal(t, "engineering", sessionInfo.RequestContext["department"])
	assert.Equal(t, "seaweedfs", sessionInfo.RequestContext["project"])

	// Check standard claims are also present
	assert.Equal(t, "test-user-id", sessionInfo.RequestContext["sub"])
	assert.Equal(t, "test@example.com", sessionInfo.RequestContext["email"])
}

// MockIdentityProviderWithAttributes is a mock provider that returns configured attributes
type MockIdentityProviderWithAttributes struct {
	name       string
	attributes map[string]string
}

func (m *MockIdentityProviderWithAttributes) Name() string {
	return m.name
}

func (m *MockIdentityProviderWithAttributes) GetIssuer() string {
	return m.name
}

func (m *MockIdentityProviderWithAttributes) Initialize(config interface{}) error {
	return nil
}

func (m *MockIdentityProviderWithAttributes) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	return &providers.ExternalIdentity{
		UserID:      "test-user-id",
		Email:       "test@example.com",
		DisplayName: "Test User",
		Provider:    m.name,
		Attributes:  m.attributes,
	}, nil
}

func (m *MockIdentityProviderWithAttributes) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	return nil, nil
}

func (m *MockIdentityProviderWithAttributes) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	return &providers.TokenClaims{
		Subject: "test-user-id",
		Issuer:  m.name,
	}, nil
}
