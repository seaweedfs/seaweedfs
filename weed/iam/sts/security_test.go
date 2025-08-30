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

// TestSecurityIssuerToProviderMapping tests the security fix that ensures JWT tokens
// with specific issuer claims can only be validated by the provider registered for that issuer
func TestSecurityIssuerToProviderMapping(t *testing.T) {
	ctx := context.Background()

	// Create STS service with two mock providers
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

	// Create two mock providers with different issuers
	providerA := &MockIdentityProviderWithIssuer{
		name:   "provider-a",
		issuer: "https://provider-a.com",
		validTokens: map[string]bool{
			"token-for-provider-a": true,
		},
	}

	providerB := &MockIdentityProviderWithIssuer{
		name:   "provider-b",
		issuer: "https://provider-b.com",
		validTokens: map[string]bool{
			"token-for-provider-b": true,
		},
	}

	// Register both providers
	err = service.RegisterProvider(providerA)
	require.NoError(t, err)
	err = service.RegisterProvider(providerB)
	require.NoError(t, err)

	// Create JWT tokens with specific issuer claims
	tokenForProviderA := createTestJWT(t, "https://provider-a.com", "user-a")
	tokenForProviderB := createTestJWT(t, "https://provider-b.com", "user-b")

	t.Run("jwt_token_with_issuer_a_only_validated_by_provider_a", func(t *testing.T) {
		// This should succeed - token has issuer A and provider A is registered
		identity, provider, err := service.validateWebIdentityToken(ctx, tokenForProviderA)
		assert.NoError(t, err)
		assert.NotNil(t, identity)
		assert.Equal(t, "provider-a", provider.Name())
	})

	t.Run("jwt_token_with_issuer_b_only_validated_by_provider_b", func(t *testing.T) {
		// This should succeed - token has issuer B and provider B is registered
		identity, provider, err := service.validateWebIdentityToken(ctx, tokenForProviderB)
		assert.NoError(t, err)
		assert.NotNil(t, identity)
		assert.Equal(t, "provider-b", provider.Name())
	})

	t.Run("jwt_token_with_unregistered_issuer_fails", func(t *testing.T) {
		// Create token with unregistered issuer
		tokenWithUnknownIssuer := createTestJWT(t, "https://unknown-issuer.com", "user-x")

		// This should fail - no provider registered for this issuer
		identity, provider, err := service.validateWebIdentityToken(ctx, tokenWithUnknownIssuer)
		assert.Error(t, err)
		assert.Nil(t, identity)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "no identity provider registered for issuer: https://unknown-issuer.com")
	})

	t.Run("non_jwt_tokens_are_rejected", func(t *testing.T) {
		// Non-JWT tokens should be rejected - no fallback mechanism exists for security
		identity, provider, err := service.validateWebIdentityToken(ctx, "token-for-provider-a")
		assert.Error(t, err)
		assert.Nil(t, identity)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "web identity token must be a valid JWT token")
	})
}

// createTestJWT creates a test JWT token with the specified issuer and subject
func createTestJWT(t *testing.T, issuer, subject string) string {
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

// MockIdentityProviderWithIssuer is a mock provider that supports issuer mapping
type MockIdentityProviderWithIssuer struct {
	name        string
	issuer      string
	validTokens map[string]bool
}

func (m *MockIdentityProviderWithIssuer) Name() string {
	return m.name
}

func (m *MockIdentityProviderWithIssuer) GetIssuer() string {
	return m.issuer
}

func (m *MockIdentityProviderWithIssuer) Initialize(config interface{}) error {
	return nil
}

func (m *MockIdentityProviderWithIssuer) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	// For JWT tokens, parse and validate the token format
	if len(token) > 50 && strings.Contains(token, ".") {
		// This looks like a JWT - parse it to get the subject
		parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
		if err != nil {
			return nil, fmt.Errorf("invalid JWT token")
		}

		claims, ok := parsedToken.Claims.(jwt.MapClaims)
		if !ok {
			return nil, fmt.Errorf("invalid claims")
		}

		issuer, _ := claims["iss"].(string)
		subject, _ := claims["sub"].(string)

		// Verify the issuer matches what we expect
		if issuer != m.issuer {
			return nil, fmt.Errorf("token issuer %s does not match provider issuer %s", issuer, m.issuer)
		}

		return &providers.ExternalIdentity{
			UserID:   subject,
			Email:    subject + "@" + m.name + ".com",
			Provider: m.name,
		}, nil
	}

	// For non-JWT tokens, check our simple token list
	if m.validTokens[token] {
		return &providers.ExternalIdentity{
			UserID:   "test-user",
			Email:    "test@" + m.name + ".com",
			Provider: m.name,
		}, nil
	}

	return nil, fmt.Errorf("invalid token")
}

func (m *MockIdentityProviderWithIssuer) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	return &providers.ExternalIdentity{
		UserID:   userID,
		Email:    userID + "@" + m.name + ".com",
		Provider: m.name,
	}, nil
}

func (m *MockIdentityProviderWithIssuer) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	if m.validTokens[token] {
		return &providers.TokenClaims{
			Subject: "test-user",
			Issuer:  m.issuer,
		}, nil
	}
	return nil, fmt.Errorf("invalid token")
}
