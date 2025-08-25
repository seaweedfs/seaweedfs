package providers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIdentityProviderInterface tests the core identity provider interface
func TestIdentityProviderInterface(t *testing.T) {
	tests := []struct {
		name     string
		provider IdentityProvider
		wantErr  bool
	}{
		// We'll add test cases as we implement providers
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test provider name
			name := tt.provider.Name()
			assert.NotEmpty(t, name, "Provider name should not be empty")

			// Test initialization
			err := tt.provider.Initialize(nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Test authentication with invalid token
			ctx := context.Background()
			_, err = tt.provider.Authenticate(ctx, "invalid-token")
			assert.Error(t, err, "Should fail with invalid token")
		})
	}
}

// TestExternalIdentityValidation tests external identity structure validation
func TestExternalIdentityValidation(t *testing.T) {
	tests := []struct {
		name     string
		identity *ExternalIdentity
		wantErr  bool
	}{
		{
			name: "valid identity",
			identity: &ExternalIdentity{
				UserID:      "user123",
				Email:       "user@example.com",
				DisplayName: "Test User",
				Groups:      []string{"group1", "group2"},
				Attributes:  map[string]string{"dept": "engineering"},
				Provider:    "test-provider",
			},
			wantErr: false,
		},
		{
			name: "missing user id",
			identity: &ExternalIdentity{
				Email:    "user@example.com",
				Provider: "test-provider",
			},
			wantErr: true,
		},
		{
			name: "missing provider",
			identity: &ExternalIdentity{
				UserID: "user123",
				Email:  "user@example.com",
			},
			wantErr: true,
		},
		{
			name: "invalid email",
			identity: &ExternalIdentity{
				UserID:   "user123",
				Email:    "invalid-email",
				Provider: "test-provider",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.identity.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestTokenClaimsValidation tests token claims structure
func TestTokenClaimsValidation(t *testing.T) {
	tests := []struct {
		name   string
		claims *TokenClaims
		valid  bool
	}{
		{
			name: "valid claims",
			claims: &TokenClaims{
				Subject:   "user123",
				Issuer:    "https://provider.example.com",
				Audience:  "seaweedfs",
				ExpiresAt: time.Now().Add(time.Hour),
				IssuedAt:  time.Now().Add(-time.Minute),
				Claims:    map[string]interface{}{"email": "user@example.com"},
			},
			valid: true,
		},
		{
			name: "expired token",
			claims: &TokenClaims{
				Subject:   "user123",
				Issuer:    "https://provider.example.com",
				Audience:  "seaweedfs",
				ExpiresAt: time.Now().Add(-time.Hour), // Expired
				IssuedAt:  time.Now().Add(-time.Hour * 2),
				Claims:    map[string]interface{}{"email": "user@example.com"},
			},
			valid: false,
		},
		{
			name: "future issued token",
			claims: &TokenClaims{
				Subject:   "user123",
				Issuer:    "https://provider.example.com",
				Audience:  "seaweedfs",
				ExpiresAt: time.Now().Add(time.Hour),
				IssuedAt:  time.Now().Add(time.Hour), // Future
				Claims:    map[string]interface{}{"email": "user@example.com"},
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.claims.IsValid()
			assert.Equal(t, tt.valid, valid)
		})
	}
}

// TestProviderRegistry tests provider registration and discovery
func TestProviderRegistry(t *testing.T) {
	// Clear registry for test
	registry := NewProviderRegistry()

	t.Run("register provider", func(t *testing.T) {
		mockProvider := &MockProvider{name: "test-provider"}

		err := registry.RegisterProvider(mockProvider)
		assert.NoError(t, err)

		// Test duplicate registration
		err = registry.RegisterProvider(mockProvider)
		assert.Error(t, err, "Should not allow duplicate registration")
	})

	t.Run("get provider", func(t *testing.T) {
		provider, exists := registry.GetProvider("test-provider")
		assert.True(t, exists)
		assert.Equal(t, "test-provider", provider.Name())

		// Test non-existent provider
		_, exists = registry.GetProvider("non-existent")
		assert.False(t, exists)
	})

	t.Run("list providers", func(t *testing.T) {
		providers := registry.ListProviders()
		assert.Len(t, providers, 1)
		assert.Equal(t, "test-provider", providers[0])
	})
}

// MockProvider for testing
type MockProvider struct {
	name        string
	initialized bool
	shouldError bool
}

func (m *MockProvider) Name() string {
	return m.name
}

func (m *MockProvider) Initialize(config interface{}) error {
	if m.shouldError {
		return assert.AnError
	}
	m.initialized = true
	return nil
}

func (m *MockProvider) Authenticate(ctx context.Context, token string) (*ExternalIdentity, error) {
	if !m.initialized {
		return nil, assert.AnError
	}
	if token == "invalid-token" {
		return nil, assert.AnError
	}
	return &ExternalIdentity{
		UserID:      "test-user",
		Email:       "test@example.com",
		DisplayName: "Test User",
		Provider:    m.name,
	}, nil
}

func (m *MockProvider) GetUserInfo(ctx context.Context, userID string) (*ExternalIdentity, error) {
	if !m.initialized || userID == "" {
		return nil, assert.AnError
	}
	return &ExternalIdentity{
		UserID:      userID,
		Email:       userID + "@example.com",
		DisplayName: "User " + userID,
		Provider:    m.name,
	}, nil
}

func (m *MockProvider) ValidateToken(ctx context.Context, token string) (*TokenClaims, error) {
	if !m.initialized || token == "invalid-token" {
		return nil, assert.AnError
	}
	return &TokenClaims{
		Subject:   "test-user",
		Issuer:    "test-issuer",
		Audience:  "seaweedfs",
		ExpiresAt: time.Now().Add(time.Hour),
		IssuedAt:  time.Now(),
		Claims:    map[string]interface{}{"email": "test@example.com"},
	}, nil
}
