package ldap

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLDAPProviderInitialization tests LDAP provider initialization
func TestLDAPProviderInitialization(t *testing.T) {
	tests := []struct {
		name    string
		config  *LDAPConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &LDAPConfig{
				Server:      "ldap://localhost:389",
				BaseDN:      "DC=example,DC=com",
				BindDN:      "CN=admin,DC=example,DC=com",
				BindPass:    "password",
				UserFilter:  "(sAMAccountName=%s)",
				GroupFilter: "(member=%s)",
			},
			wantErr: false,
		},
		{
			name: "missing server",
			config: &LDAPConfig{
				BaseDN: "DC=example,DC=com",
			},
			wantErr: true,
		},
		{
			name: "missing base DN",
			config: &LDAPConfig{
				Server: "ldap://localhost:389",
			},
			wantErr: true,
		},
		{
			name: "invalid server URL",
			config: &LDAPConfig{
				Server: "invalid-url",
				BaseDN: "DC=example,DC=com",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := NewLDAPProvider("test-ldap")

			err := provider.Initialize(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, "test-ldap", provider.Name())
			}
		})
	}
}

// TestLDAPProviderAuthentication tests LDAP authentication
func TestLDAPProviderAuthentication(t *testing.T) {
	// Skip if no LDAP test server available
	if testing.Short() {
		t.Skip("Skipping LDAP integration test in short mode")
	}

	provider := NewLDAPProvider("test-ldap")
	config := &LDAPConfig{
		Server:      "ldap://localhost:389",
		BaseDN:      "DC=example,DC=com",
		BindDN:      "CN=admin,DC=example,DC=com",
		BindPass:    "password",
		UserFilter:  "(sAMAccountName=%s)",
		GroupFilter: "(member=%s)",
		Attributes: map[string]string{
			"email":       "mail",
			"displayName": "displayName",
			"groups":      "memberOf",
		},
		RoleMapping: &providers.RoleMapping{
			Rules: []providers.MappingRule{
				{
					Claim: "groups",
					Value: "*CN=Admins*",
					Role:  "arn:seaweed:iam::role/AdminRole",
				},
				{
					Claim: "groups",
					Value: "*CN=Users*",
					Role:  "arn:seaweed:iam::role/UserRole",
				},
			},
			DefaultRole: "arn:seaweed:iam::role/GuestRole",
		},
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("authenticate with username/password", func(t *testing.T) {
		// This would require an actual LDAP server for integration testing
		credentials := "user:password" // Basic auth format

		identity, err := provider.Authenticate(context.Background(), credentials)
		if err != nil {
			t.Skip("LDAP server not available for testing")
		}

		assert.NoError(t, err)
		assert.Equal(t, "user", identity.UserID)
		assert.Equal(t, "test-ldap", identity.Provider)
		assert.NotEmpty(t, identity.Email)
	})

	t.Run("authenticate with invalid credentials", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), "invalid:credentials")
		assert.Error(t, err)
	})
}

// TestLDAPProviderUserInfo tests LDAP user info retrieval
func TestLDAPProviderUserInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping LDAP integration test in short mode")
	}

	provider := NewLDAPProvider("test-ldap")
	config := &LDAPConfig{
		Server:     "ldap://localhost:389",
		BaseDN:     "DC=example,DC=com",
		BindDN:     "CN=admin,DC=example,DC=com",
		BindPass:   "password",
		UserFilter: "(sAMAccountName=%s)",
		Attributes: map[string]string{
			"email":       "mail",
			"displayName": "displayName",
			"department":  "department",
		},
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("get user info", func(t *testing.T) {
		identity, err := provider.GetUserInfo(context.Background(), "testuser")
		if err != nil {
			t.Skip("LDAP server not available for testing")
		}

		assert.NoError(t, err)
		assert.Equal(t, "testuser", identity.UserID)
		assert.Equal(t, "test-ldap", identity.Provider)
		assert.NotEmpty(t, identity.Email)
		assert.NotEmpty(t, identity.DisplayName)
	})

	t.Run("get user info with empty username", func(t *testing.T) {
		_, err := provider.GetUserInfo(context.Background(), "")
		assert.Error(t, err)
	})

	t.Run("get user info for non-existent user", func(t *testing.T) {
		_, err := provider.GetUserInfo(context.Background(), "nonexistent")
		assert.Error(t, err)
	})
}

// TestLDAPAttributeMapping tests LDAP attribute mapping
func TestLDAPAttributeMapping(t *testing.T) {
	provider := NewLDAPProvider("test-ldap")
	config := &LDAPConfig{
		Server: "ldap://localhost:389",
		BaseDN: "DC=example,DC=com",
		Attributes: map[string]string{
			"email":       "mail",
			"displayName": "cn",
			"department":  "departmentNumber",
			"groups":      "memberOf",
		},
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("map LDAP attributes to identity", func(t *testing.T) {
		ldapAttrs := map[string][]string{
			"mail":             {"user@example.com"},
			"cn":               {"John Doe"},
			"departmentNumber": {"IT"},
			"memberOf": {
				"CN=Users,OU=Groups,DC=example,DC=com",
				"CN=Developers,OU=Groups,DC=example,DC=com",
			},
		}

		identity := provider.mapLDAPAttributes("testuser", ldapAttrs)

		assert.Equal(t, "testuser", identity.UserID)
		assert.Equal(t, "user@example.com", identity.Email)
		assert.Equal(t, "John Doe", identity.DisplayName)
		assert.Equal(t, "test-ldap", identity.Provider)

		// Check groups
		assert.Contains(t, identity.Groups, "CN=Users,OU=Groups,DC=example,DC=com")
		assert.Contains(t, identity.Groups, "CN=Developers,OU=Groups,DC=example,DC=com")

		// Check attributes
		assert.Equal(t, "IT", identity.Attributes["department"])
	})
}

// TestLDAPGroupFiltering tests LDAP group filtering and role mapping
func TestLDAPGroupFiltering(t *testing.T) {
	provider := NewLDAPProvider("test-ldap")
	config := &LDAPConfig{
		Server: "ldap://localhost:389",
		BaseDN: "DC=example,DC=com",
		RoleMapping: &providers.RoleMapping{
			Rules: []providers.MappingRule{
				{
					Claim: "groups",
					Value: "*Admins*",
					Role:  "arn:seaweed:iam::role/AdminRole",
				},
				{
					Claim: "groups",
					Value: "*Users*",
					Role:  "arn:seaweed:iam::role/UserRole",
				},
			},
			DefaultRole: "arn:seaweed:iam::role/GuestRole",
		},
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	tests := []struct {
		name           string
		groups         []string
		expectedRole   string
		expectedClaims map[string]interface{}
	}{
		{
			name:         "admin user",
			groups:       []string{"CN=Admins,OU=Groups,DC=example,DC=com", "CN=Users,OU=Groups,DC=example,DC=com"},
			expectedRole: "arn:seaweed:iam::role/AdminRole",
		},
		{
			name:         "regular user",
			groups:       []string{"CN=Users,OU=Groups,DC=example,DC=com"},
			expectedRole: "arn:seaweed:iam::role/UserRole",
		},
		{
			name:         "guest user",
			groups:       []string{"CN=Guests,OU=Groups,DC=example,DC=com"},
			expectedRole: "arn:seaweed:iam::role/GuestRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := &providers.ExternalIdentity{
				UserID:   "testuser",
				Groups:   tt.groups,
				Provider: "test-ldap",
			}

			role := provider.mapUserToRole(identity)
			assert.Equal(t, tt.expectedRole, role)
		})
	}
}

// TestLDAPConnectionPool tests LDAP connection pooling
func TestLDAPConnectionPool(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping LDAP connection pool test in short mode")
	}

	provider := NewLDAPProvider("test-ldap")
	config := &LDAPConfig{
		Server:         "ldap://localhost:389",
		BaseDN:         "DC=example,DC=com",
		BindDN:         "CN=admin,DC=example,DC=com",
		BindPass:       "password",
		MaxConnections: 5,
		ConnTimeout:    30,
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("connection pool management", func(t *testing.T) {
		// Test that multiple concurrent requests work
		// This would require actual LDAP server for full testing
		pool := provider.getConnectionPool()
		
		// In CI environments where no LDAP server is available, pool might be nil
		// Skip the test if we can't establish a connection
		conn, err := provider.getConnection()
		if err != nil {
			t.Skip("LDAP server not available - skipping connection pool test")
			return
		}

		// Only test if we successfully got a connection
		assert.NotNil(t, pool)
		assert.NotNil(t, conn)
		provider.releaseConnection(conn)
	})
}

// MockLDAPServer for unit testing (without external dependencies)
type MockLDAPServer struct {
	users map[string]map[string][]string
}

func NewMockLDAPServer() *MockLDAPServer {
	return &MockLDAPServer{
		users: map[string]map[string][]string{
			"testuser": {
				"mail":       {"testuser@example.com"},
				"cn":         {"Test User"},
				"department": {"Engineering"},
				"memberOf":   {"CN=Users,OU=Groups,DC=example,DC=com"},
			},
			"admin": {
				"mail":       {"admin@example.com"},
				"cn":         {"Administrator"},
				"department": {"IT"},
				"memberOf":   {"CN=Admins,OU=Groups,DC=example,DC=com", "CN=Users,OU=Groups,DC=example,DC=com"},
			},
		},
	}
}

func (m *MockLDAPServer) Authenticate(username, password string) bool {
	_, exists := m.users[username]
	return exists && password == "password" // Mock authentication
}

func (m *MockLDAPServer) GetUserAttributes(username string) (map[string][]string, error) {
	if attrs, exists := m.users[username]; exists {
		return attrs, nil
	}
	return nil, fmt.Errorf("user not found: %s", username)
}
