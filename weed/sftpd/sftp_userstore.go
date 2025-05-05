package sftpd

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// UserStore interface for user management.
type UserStore interface {
	GetUser(username string) (*User, error)
	ValidatePassword(username string, password []byte) bool
	ValidatePublicKey(username string, keyData string) bool
	GetUserPermissions(username string, path string) []string
}

// User represents an SFTP user with authentication and permission details.
type User struct {
	Username    string
	Password    string              // Plaintext password
	PublicKeys  []string            // Authorized public keys
	HomeDir     string              // User's home directory
	Permissions map[string][]string // path -> permissions (read, write, list, etc.)
	Uid         uint32              // User ID for file ownership
	Gid         uint32              // Group ID for file ownership
}

// FileUserStore implements UserStore using a JSON file.
type FileUserStore struct {
	filePath string
	users    map[string]*User
	mu       sync.RWMutex
}

// NewFileUserStore creates a new user store from a JSON file.
func NewFileUserStore(filePath string) (*FileUserStore, error) {
	store := &FileUserStore{
		filePath: filePath,
		users:    make(map[string]*User),
	}

	if err := store.loadUsers(); err != nil {
		return nil, err
	}

	return store, nil
}

// loadUsers loads users from the JSON file.
func (s *FileUserStore) loadUsers() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return fmt.Errorf("user store file not found: %s", s.filePath)
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to read user store file: %v", err)
	}

	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		return fmt.Errorf("failed to parse user store file: %v", err)
	}

	for _, user := range users {
		s.users[user.Username] = user
	}

	return nil
}

// GetUser returns a user by username.
func (s *FileUserStore) GetUser(username string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[username]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	return user, nil
}

// ValidatePassword checks if the password is valid for the user.
func (s *FileUserStore) ValidatePassword(username string, password []byte) bool {
	user, err := s.GetUser(username)
	if err != nil {
		return false
	}

	// Compare plaintext password using constant time comparison for security
	return subtle.ConstantTimeCompare([]byte(user.Password), password) == 1
}

// ValidatePublicKey checks if the public key is valid for the user.
func (s *FileUserStore) ValidatePublicKey(username string, keyData string) bool {
	user, err := s.GetUser(username)
	if err != nil {
		return false
	}

	for _, key := range user.PublicKeys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(keyData)) == 1 {
			return true
		}
	}

	return false
}

// GetUserPermissions returns the permissions for a user on a path.
func (s *FileUserStore) GetUserPermissions(username string, path string) []string {
	user, err := s.GetUser(username)
	if err != nil {
		return nil
	}

	// Check exact path match first
	if perms, ok := user.Permissions[path]; ok {
		return perms
	}

	// Check parent directories
	var bestMatch string
	var bestPerms []string

	for p, perms := range user.Permissions {
		if strings.HasPrefix(path, p) && len(p) > len(bestMatch) {
			bestMatch = p
			bestPerms = perms
		}
	}

	return bestPerms
}
