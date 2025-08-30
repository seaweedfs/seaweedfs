package ldap

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// MockLDAPProvider is a mock implementation for testing
// This is a standalone mock that doesn't depend on production LDAP code
type MockLDAPProvider struct {
	name            string
	initialized     bool
	TestUsers       map[string]*providers.ExternalIdentity
	TestCredentials map[string]string // username -> password
}

// NewMockLDAPProvider creates a mock LDAP provider for testing
func NewMockLDAPProvider(name string) *MockLDAPProvider {
	return &MockLDAPProvider{
		name:            name,
		initialized:     true, // Mock is always initialized
		TestUsers:       make(map[string]*providers.ExternalIdentity),
		TestCredentials: make(map[string]string),
	}
}

// Name returns the provider name
func (m *MockLDAPProvider) Name() string {
	return m.name
}

// Initialize initializes the mock provider (no-op for testing)
func (m *MockLDAPProvider) Initialize(config interface{}) error {
	m.initialized = true
	return nil
}

// AddTestUser adds a test user with credentials
func (m *MockLDAPProvider) AddTestUser(username, password string, identity *providers.ExternalIdentity) {
	m.TestCredentials[username] = password
	m.TestUsers[username] = identity
}

// Authenticate authenticates using test data
func (m *MockLDAPProvider) Authenticate(ctx context.Context, credentials string) (*providers.ExternalIdentity, error) {
	if !m.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if credentials == "" {
		return nil, fmt.Errorf("credentials cannot be empty")
	}

	// Parse credentials (username:password format)
	parts := strings.SplitN(credentials, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid credentials format (expected username:password)")
	}

	username, password := parts[0], parts[1]

	// Check test credentials
	expectedPassword, userExists := m.TestCredentials[username]
	if !userExists {
		return nil, fmt.Errorf("user not found")
	}

	if password != expectedPassword {
		return nil, fmt.Errorf("invalid credentials")
	}

	// Return test user identity
	if identity, exists := m.TestUsers[username]; exists {
		return identity, nil
	}

	return nil, fmt.Errorf("user identity not found")
}

// GetUserInfo returns test user info
func (m *MockLDAPProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
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

	// Return default test user if not found
	return &providers.ExternalIdentity{
		UserID:      userID,
		Email:       userID + "@test-ldap.com",
		DisplayName: "Test LDAP User " + userID,
		Groups:      []string{"test-group"},
		Provider:    m.name,
	}, nil
}

// ValidateToken validates credentials using test data
func (m *MockLDAPProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	if !m.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	// Parse credentials (username:password format)
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token format (expected username:password)")
	}

	username, password := parts[0], parts[1]

	// Check test credentials
	expectedPassword, userExists := m.TestCredentials[username]
	if !userExists {
		return nil, fmt.Errorf("user not found")
	}

	if password != expectedPassword {
		return nil, fmt.Errorf("invalid credentials")
	}

	// Return test claims
	identity := m.TestUsers[username]
	return &providers.TokenClaims{
		Subject: username,
		Claims: map[string]interface{}{
			"ldap_dn":  "CN=" + username + ",DC=test,DC=com",
			"email":    identity.Email,
			"name":     identity.DisplayName,
			"groups":   identity.Groups,
			"provider": m.name,
		},
	}, nil
}

// SetupDefaultTestData configures common test data
func (m *MockLDAPProvider) SetupDefaultTestData() {
	// Add default test user
	m.AddTestUser("testuser", "testpass", &providers.ExternalIdentity{
		UserID:      "testuser",
		Email:       "testuser@ldap-test.com",
		DisplayName: "Test LDAP User",
		Groups:      []string{"developers", "users"},
		Provider:    m.name,
		Attributes: map[string]string{
			"department": "Engineering",
			"location":   "Test City",
		},
	})

	// Add admin test user
	m.AddTestUser("admin", "adminpass", &providers.ExternalIdentity{
		UserID:      "admin",
		Email:       "admin@ldap-test.com",
		DisplayName: "LDAP Administrator",
		Groups:      []string{"admins", "users"},
		Provider:    m.name,
		Attributes: map[string]string{
			"department": "IT",
			"role":       "administrator",
		},
	})

	// Add readonly user
	m.AddTestUser("readonly", "readpass", &providers.ExternalIdentity{
		UserID:      "readonly",
		Email:       "readonly@ldap-test.com",
		DisplayName: "Read Only User",
		Groups:      []string{"readonly"},
		Provider:    m.name,
	})
}
