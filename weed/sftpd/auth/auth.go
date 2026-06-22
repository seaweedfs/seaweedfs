// Package auth provides authentication and authorization functionality for the SFTP server
package auth

import (
	"fmt"

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
	certificateAuth    *CertificateAuthenticator
	enabledAuthMethods []string
}

// NewManager creates a new authentication manager.
//
// trustedUserCAKeysFile is the path to a file containing trusted CA public
// keys (OpenSSH authorized_keys format). It is required when "certificate"
// is listed in enabledAuthMethods and ignored otherwise.
func NewManager(userStore user.Store, enabledAuthMethods []string, trustedUserCAKeysFile string) (*Manager, error) {
	manager := &Manager{
		userStore:          userStore,
		enabledAuthMethods: enabledAuthMethods,
	}

	// Initialize authenticators based on enabled methods
	passwordEnabled := false
	publicKeyEnabled := false
	certificateEnabled := false

	for _, method := range enabledAuthMethods {
		switch method {
		case "password":
			passwordEnabled = true
		case "publickey":
			publicKeyEnabled = true
		case "certificate":
			certificateEnabled = true
		}
	}

	manager.passwordAuth = NewPasswordAuthenticator(userStore, passwordEnabled)
	manager.publicKeyAuth = NewPublicKeyAuthenticator(userStore, publicKeyEnabled)

	certAuth, err := NewCertificateAuthenticator(userStore, certificateEnabled, trustedUserCAKeysFile)
	if err != nil {
		return nil, fmt.Errorf("init certificate auth: %w", err)
	}
	manager.certificateAuth = certAuth

	return manager, nil
}

// GetSSHServerConfig returns an SSH server config with the appropriate authentication methods
func (m *Manager) GetSSHServerConfig() *ssh.ServerConfig {
	config := &ssh.ServerConfig{}

	// Add password authentication if enabled
	if m.passwordAuth.Enabled() {
		config.PasswordCallback = m.passwordAuth.Authenticate
	}

	// Wire the public-key channel. Certificate auth, when enabled, takes
	// over the channel entirely (MinIO/OpenSSH-style): plain public keys
	// are rejected even if "publickey" is also listed in enabledAuthMethods.
	switch {
	case m.certificateAuth.Enabled():
		config.PublicKeyCallback = m.certificateAuth.Authenticate
	case m.publicKeyAuth.Enabled():
		config.PublicKeyCallback = m.publicKeyAuth.Authenticate
	}

	return config
}

// GetUser retrieves a user from the user store
func (m *Manager) GetUser(username string) (*user.User, error) {
	return m.userStore.GetUser(username)
}
