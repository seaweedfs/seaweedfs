package oidc

import (
	"context"
	"fmt"
	"time"

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
	// Add default test tokens
	m.AddTestToken("valid_token", &providers.TokenClaims{
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
	})

	// Add default test users
	m.AddTestUser("test-user-123", &providers.ExternalIdentity{
		UserID:      "test-user-123",
		Email:       "testuser@example.com",
		DisplayName: "Test User",
		Groups:      []string{"developers"},
		Provider:    m.name,
	})
}
