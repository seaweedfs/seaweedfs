// Package auth provides authentication and authorization functionality for the SFTP server
package auth

import (
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
)

// Provider defines the interface for authentication providers
type Provider interface {
	// GetAuthMethods returns the SSH server auth methods
	GetAuthMethods() []ssh.AuthMethod
}

// Manager handles authentication and authorization
type Manager struct {
	userStore          user.Store
	passwordAuth       *PasswordAuthenticator
	publicKeyAuth      *PublicKeyAuthenticator
	enabledAuthMethods []string
}

// NewManager creates a new authentication manager
func NewManager(userStore user.Store, enabledAuthMethods []string) *Manager {
	manager := &Manager{
		userStore:          userStore,
		enabledAuthMethods: enabledAuthMethods,
	}

	// Initialize authenticators based on enabled methods
	passwordEnabled := false
	publicKeyEnabled := false

	for _, method := range enabledAuthMethods {
		switch method {
		case "password":
			passwordEnabled = true
		case "publickey":
			publicKeyEnabled = true
		}
	}

	manager.passwordAuth = NewPasswordAuthenticator(userStore, passwordEnabled)
	manager.publicKeyAuth = NewPublicKeyAuthenticator(userStore, publicKeyEnabled)

	return manager
}

// GetSSHServerConfig returns an SSH server config with the appropriate authentication methods
func (m *Manager) GetSSHServerConfig() *ssh.ServerConfig {
	config := &ssh.ServerConfig{}

	// Add password authentication if enabled
	if m.passwordAuth.Enabled() {
		config.PasswordCallback = m.passwordAuth.Authenticate
	}

	// Add public key authentication if enabled
	if m.publicKeyAuth.Enabled() {
		config.PublicKeyCallback = m.publicKeyAuth.Authenticate
	}

	return config
}

// GetUser retrieves a user from the user store
func (m *Manager) GetUser(username string) (*user.User, error) {
	return m.userStore.GetUser(username)
}
