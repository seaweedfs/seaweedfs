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
			roleArn:          "arn:seaweed:iam::role/TestRole",
			webIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user-id"),
			sessionName:      "test-session",
			durationSeconds:  nil, // Use default
			wantErr:          false,
			expectedSubject:  "test-user-id",
		},
		{
			name:             "invalid web identity token",
			roleArn:          "arn:seaweed:iam::role/TestRole",
			webIdentityToken: "invalid-token",
			sessionName:      "test-session",
			wantErr:          true,
		},
		{
			name:             "non-existent role",
			roleArn:          "arn:seaweed:iam::role/NonExistentRole",
			webIdentityToken: createSTSTestJWT(t, "test-issuer", "test-user"),
			sessionName:      "test-session",
			wantErr:          true,
		},
		{
			name:             "custom session duration",
			roleArn:          "arn:seaweed:iam::role/TestRole",
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
			roleArn:     "arn:seaweed:iam::role/LDAPRole",
			username:    "testuser",
			password:    "testpass",
			sessionName: "ldap-session",
			wantErr:     false,
		},
		{
			name:        "invalid LDAP credentials",
			roleArn:     "arn:seaweed:iam::role/LDAPRole",
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
		RoleArn:          "arn:seaweed:iam::role/TestRole",
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
				assert.Equal(t, "arn:seaweed:iam::role/TestRole", session.RoleArn)
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
		RoleArn:          "arn:seaweed:iam::role/TestRole",
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
