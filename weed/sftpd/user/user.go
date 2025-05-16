// Package user provides user management functionality for the SFTP server
package user

import (
	"math/rand"
	"path/filepath"
)

// User represents an SFTP user with authentication and permission details
type User struct {
	Username    string              // Username for authentication
	Password    string              // Plaintext password
	PublicKeys  []string            // Authorized public keys
	HomeDir     string              // User's home directory
	Permissions map[string][]string // path -> permissions (read, write, list, etc.)
	Uid         uint32              // User ID for file ownership
	Gid         uint32              // Group ID for file ownership
}

// NewUser creates a new user with default settings
func NewUser(username string) *User {
	// Generate a random UID/GID between 1000 and 60000
	// This range is typically safe for regular users in most systems
	// 0-999 are often reserved for system users
	randomId := 1000 + rand.Intn(59000)

	return &User{
		Username:    username,
		Permissions: make(map[string][]string),
		HomeDir:     filepath.Join("/home", username),
		Uid:         uint32(randomId),
		Gid:         uint32(randomId),
	}
}

// SetPassword sets a plaintext password for the user
func (u *User) SetPassword(password string) {
	u.Password = password
}

// AddPublicKey adds a public key to the user
func (u *User) AddPublicKey(key string) {
	// Check if key already exists
	for _, existingKey := range u.PublicKeys {
		if existingKey == key {
			return
		}
	}
	u.PublicKeys = append(u.PublicKeys, key)
}

// RemovePublicKey removes a public key from the user
func (u *User) RemovePublicKey(key string) bool {
	for i, existingKey := range u.PublicKeys {
		if existingKey == key {
			// Remove the key by replacing it with the last element and truncating
			u.PublicKeys[i] = u.PublicKeys[len(u.PublicKeys)-1]
			u.PublicKeys = u.PublicKeys[:len(u.PublicKeys)-1]
			return true
		}
	}
	return false
}

// SetPermission sets permissions for a specific path
func (u *User) SetPermission(path string, permissions []string) {
	u.Permissions[path] = permissions
}

// RemovePermission removes permissions for a specific path
func (u *User) RemovePermission(path string) bool {
	if _, exists := u.Permissions[path]; exists {
		delete(u.Permissions, path)
		return true
	}
	return false
}
