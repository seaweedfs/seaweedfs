// Package user provides user management functionality for the SFTP server
package user

import (
	"math/rand/v2"
	"path/filepath"

	"golang.org/x/crypto/bcrypt"
)

// User represents an SFTP user with authentication and permission details
type User struct {
	Username       string              `json:"Username"`
	HashedPassword string              `json:"HashedPassword"`          // bcrypt hash
	Password       string              `json:"Password,omitempty"`      // deprecated: plaintext, migrated on next save
	PublicKeys     []string            `json:"PublicKeys,omitempty"`
	HomeDir        string              `json:"HomeDir"`
	Permissions    map[string][]string `json:"Permissions,omitempty"`
	Uid            uint32              `json:"Uid"`
	Gid            uint32              `json:"Gid"`
}

// NewUser creates a new user with default settings
func NewUser(username string) *User {
	// Generate a random UID/GID between 1000 and 60000
	// This range is typically safe for regular users in most systems
	// 0-999 are often reserved for system users
	randomId := 1000 + rand.IntN(59000)

	return &User{
		Username:    username,
		Permissions: make(map[string][]string),
		HomeDir:     filepath.Join("/home", username),
		Uid:         uint32(randomId),
		Gid:         uint32(randomId),
	}
}

// SetPassword hashes and stores the password using bcrypt
func (u *User) SetPassword(password string) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		// bcrypt only errors on passwords > 72 bytes; truncate if needed
		hash, _ = bcrypt.GenerateFromPassword([]byte(password[:72]), bcrypt.DefaultCost)
	}
	u.HashedPassword = string(hash)
	u.Password = "" // clear any legacy plaintext
}

// CheckPassword verifies a password against the stored hash.
// It transparently handles legacy plaintext passwords by upgrading them on match.
func (u *User) CheckPassword(password string) bool {
	if u.HashedPassword != "" {
		return bcrypt.CompareHashAndPassword([]byte(u.HashedPassword), []byte(password)) == nil
	}
	// Legacy plaintext migration path
	if u.Password != "" && u.Password == password {
		u.SetPassword(password) // upgrade to bcrypt
		return true
	}
	return false
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
