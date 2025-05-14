package auth

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
)

// PublicKeyAuthenticator handles public key-based authentication
type PublicKeyAuthenticator struct {
	userStore user.Store
	enabled   bool
}

// NewPublicKeyAuthenticator creates a new public key authenticator
func NewPublicKeyAuthenticator(userStore user.Store, enabled bool) *PublicKeyAuthenticator {
	return &PublicKeyAuthenticator{
		userStore: userStore,
		enabled:   enabled,
	}
}

// Enabled returns whether public key authentication is enabled
func (a *PublicKeyAuthenticator) Enabled() bool {
	return a.enabled
}

// Authenticate validates a public key for a user
func (a *PublicKeyAuthenticator) Authenticate(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	username := conn.User()

	// Check if public key auth is enabled
	if !a.enabled {
		return nil, fmt.Errorf("public key authentication disabled")
	}

	// Convert key to string format for comparison
	keyData := string(key.Marshal())

	// Validate public key
	if a.userStore.ValidatePublicKey(username, keyData) {
		return &ssh.Permissions{
			Extensions: map[string]string{
				"username": username,
			},
		}, nil
	}

	return nil, fmt.Errorf("authentication failed")
}
