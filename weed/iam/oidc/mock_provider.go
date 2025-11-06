// This file contains mock OIDC provider implementations for testing only.
// These should NOT be used in production environments.

package oidc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// MockOIDCProvider is a mock implementation for testing
type MockOIDCProvider struct {
	*OIDCProvider
	TestTokens map[string]*providers.TokenClaims
	TestUsers  map[string]*providers.ExternalIdentity
}

// NewMockOIDCProvider creates a mock OIDC provider for testing
func NewMockOIDCProvider(name string) *MockOIDCProvider {
	return &MockOIDCProvider{
		OIDCProvider: NewOIDCProvider(name),
		TestTokens:   make(map[string]*providers.TokenClaims),
		TestUsers:    make(map[string]*providers.ExternalIdentity),
	}
}

// AddTestToken adds a test token with expected claims
func (m *MockOIDCProvider) AddTestToken(token string, claims *providers.TokenClaims) {
	m.TestTokens[token] = claims
}

// AddTestUser adds a test user with expected identity
func (m *MockOIDCProvider) AddTestUser(userID string, identity *providers.ExternalIdentity) {
	m.TestUsers[userID] = identity
}

// Authenticate overrides the parent Authenticate method to use mock data
func (m *MockOIDCProvider) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	if !m.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	// Validate token using mock validation
	claims, err := m.ValidateToken(ctx, token)
	if err != nil {
		return nil, err
	}

	// Map claims to external identity
	email, _ := claims.GetClaimString("email")
	displayName, _ := claims.GetClaimString("name")
	groups, _ := claims.GetClaimStringSlice("groups")

	return &providers.ExternalIdentity{
		UserID:      claims.Subject,
		Email:       email,
		DisplayName: displayName,
		Groups:      groups,
		Provider:    m.name,
	}, nil
}

// ValidateToken validates tokens using test data
func (m *MockOIDCProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	if !m.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	// Special test tokens
	if token == "expired_token" {
		return nil, fmt.Errorf("token has expired")
	}
	if token == "invalid_token" {
		return nil, fmt.Errorf("invalid token")
	}

	// Try to parse as JWT token first
	if len(token) > 20 && strings.Count(token, ".") >= 2 {
		parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
		if err == nil {
			if jwtClaims, ok := parsedToken.Claims.(jwt.MapClaims); ok {
				issuer, _ := jwtClaims["iss"].(string)
				subject, _ := jwtClaims["sub"].(string)
				audience, _ := jwtClaims["aud"].(string)

				// Verify the issuer matches our configuration
				if issuer == m.config.Issuer && subject != "" {
					// Extract expiration and issued at times
					var expiresAt, issuedAt time.Time
					if exp, ok := jwtClaims["exp"].(float64); ok {
						expiresAt = time.Unix(int64(exp), 0)
					}
					if iat, ok := jwtClaims["iat"].(float64); ok {
						issuedAt = time.Unix(int64(iat), 0)
					}

					return &providers.TokenClaims{
						Subject:   subject,
						Issuer:    issuer,
						Audience:  audience,
						ExpiresAt: expiresAt,
						IssuedAt:  issuedAt,
						Claims: map[string]interface{}{
							"email": subject + "@test-domain.com",
							"name":  "Test User " + subject,
						},
					}, nil
				}
			}
		}
	}

	// Check test tokens
	if claims, exists := m.TestTokens[token]; exists {
		return claims, nil
	}

	// Default test token for basic testing
	if token == "valid_test_token" {
		return &providers.TokenClaims{
			Subject:   "test-user-id",
			Issuer:    m.config.Issuer,
			Audience:  m.config.ClientID,
			ExpiresAt: time.Now().Add(time.Hour),
			IssuedAt:  time.Now(),
			Claims: map[string]interface{}{
				"email":  "test@example.com",
				"name":   "Test User",
				"groups": []string{"developers", "users"},
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown test token: %s", token)
}

// GetUserInfo returns test user info
func (m *MockOIDCProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	if !m.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}

	// Check test users
	if identity, exists := m.TestUsers[userID]; exists {
		return identity, nil
	}

	// Default test user
	return &providers.ExternalIdentity{
		UserID:      userID,
		Email:       userID + "@example.com",
		DisplayName: "Test User " + userID,
		Provider:    m.name,
	}, nil
}

// SetupDefaultTestData configures common test data
func (m *MockOIDCProvider) SetupDefaultTestData() {
	// Create default token claims
	defaultClaims := &providers.TokenClaims{
		Subject:   "test-user-123",
		Issuer:    "https://test-issuer.com",
		Audience:  "test-client-id",
		ExpiresAt: time.Now().Add(time.Hour),
		IssuedAt:  time.Now(),
		Claims: map[string]interface{}{
			"email":  "testuser@example.com",
			"name":   "Test User",
			"groups": []string{"developers"},
		},
	}

	// Add multiple token variants for compatibility
	m.AddTestToken("valid_token", defaultClaims)
	m.AddTestToken("valid-oidc-token", defaultClaims) // For integration tests
	m.AddTestToken("valid_test_token", defaultClaims) // For STS tests

	// Add default test users
	m.AddTestUser("test-user-123", &providers.ExternalIdentity{
		UserID:      "test-user-123",
		Email:       "testuser@example.com",
		DisplayName: "Test User",
		Groups:      []string{"developers"},
		Provider:    m.name,
	})
}
