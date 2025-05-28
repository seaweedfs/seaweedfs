package auth

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"golang.org/x/crypto/ssh"
)

// PasswordAuthenticator handles password-based authentication
type PasswordAuthenticator struct {
	userStore user.Store
	enabled   bool
}

// NewPasswordAuthenticator creates a new password authenticator
func NewPasswordAuthenticator(userStore user.Store, enabled bool) *PasswordAuthenticator {
	return &PasswordAuthenticator{
		userStore: userStore,
		enabled:   enabled,
	}
}

// Enabled returns whether password authentication is enabled
func (a *PasswordAuthenticator) Enabled() bool {
	return a.enabled
}

// Authenticate validates a password for a user
func (a *PasswordAuthenticator) Authenticate(conn ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
	username := conn.User()

	// Check if password auth is enabled
	if !a.enabled {
		return nil, fmt.Errorf("password authentication disabled")
	}

	// Validate password against user store
	if a.userStore.ValidatePassword(username, password) {
		return &ssh.Permissions{
			Extensions: map[string]string{
				"username": username,
			},
		}, nil
	}

	// Add delay to prevent brute force attacks
	time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)

	return nil, fmt.Errorf("authentication failed")
}
