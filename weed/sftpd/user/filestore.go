package user

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"golang.org/x/crypto/ssh"
)

// FileStore implements Store using a JSON file
type FileStore struct {
	filePath string
	users    map[string]*User
	mu       sync.RWMutex
}

// Store defines the interface for user storage and retrieval
type Store interface {
	// GetUser retrieves a user by username
	GetUser(username string) (*User, error)

	// ValidatePassword checks if the password is valid for the user
	ValidatePassword(username string, password []byte) bool

	// ValidatePublicKey checks if the public key is valid for the user
	ValidatePublicKey(username string, keyData string) bool

	// GetUserPermissions returns the permissions for a user on a path
	GetUserPermissions(username string, path string) []string

	// SaveUser saves or updates a user
	SaveUser(user *User) error

	// DeleteUser removes a user
	DeleteUser(username string) error

	// ListUsers returns all usernames
	ListUsers() ([]string, error)
}

// UserNotFoundError is returned when a user is not found
type UserNotFoundError struct {
	Username string
}

func (e *UserNotFoundError) Error() string {
	return fmt.Sprintf("user not found: %s", e.Username)
}

// NewFileStore creates a new user store from a JSON file
func NewFileStore(filePath string) (*FileStore, error) {
	store := &FileStore{
		filePath: filePath,
		users:    make(map[string]*User),
	}

	// Create the file if it doesn't exist
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Create an empty users array
		if err := os.WriteFile(filePath, []byte("[]"), 0600); err != nil {
			return nil, fmt.Errorf("failed to create user store file: %v", err)
		}
	}

	if err := store.loadUsers(); err != nil {
		return nil, err
	}

	return store, nil
}

// loadUsers loads users from the JSON file
func (s *FileStore) loadUsers() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to read user store file: %v", err)
	}

	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		return fmt.Errorf("failed to parse user store file: %v", err)
	}

	// Clear existing users and add the loaded ones
	s.users = make(map[string]*User)
	for _, user := range users {
		// Process public keys to ensure they're in the correct format
		for i, keyData := range user.PublicKeys {
			// Try to parse the key as an authorized key format
			pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(keyData))
			if err == nil {
				// If successful, store the marshaled binary format
				user.PublicKeys[i] = string(pubKey.Marshal())
			}
		}
		s.users[user.Username] = user

	}

	return nil
}

// saveUsers saves users to the JSON file
func (s *FileStore) saveUsers() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert map to slice for JSON serialization
	var users []*User
	for _, user := range s.users {
		users = append(users, user)
	}

	data, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize users: %v", err)
	}

	if err := os.WriteFile(s.filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write user store file: %v", err)
	}

	return nil
}

// GetUser returns a user by username
func (s *FileStore) GetUser(username string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[username]
	if !ok {
		return nil, &UserNotFoundError{Username: username}
	}

	return user, nil
}

// ValidatePassword checks if the password is valid for the user
func (s *FileStore) ValidatePassword(username string, password []byte) bool {
	user, err := s.GetUser(username)
	if err != nil {
		return false
	}

	// Compare plaintext password using constant time comparison for security
	return subtle.ConstantTimeCompare([]byte(user.Password), password) == 1
}

// ValidatePublicKey checks if the public key is valid for the user
func (s *FileStore) ValidatePublicKey(username string, keyData string) bool {
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

// GetUserPermissions returns the permissions for a user on a path
func (s *FileStore) GetUserPermissions(username string, path string) []string {
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
		if len(p) > len(bestMatch) && os.IsPathSeparator(p[len(p)-1]) && path[:len(p)] == p {
			bestMatch = p
			bestPerms = perms
		}
	}

	return bestPerms
}

// SaveUser saves or updates a user
func (s *FileStore) SaveUser(user *User) error {
	s.mu.Lock()
	s.users[user.Username] = user
	s.mu.Unlock()

	return s.saveUsers()
}

// DeleteUser removes a user
func (s *FileStore) DeleteUser(username string) error {
	s.mu.Lock()
	_, exists := s.users[username]
	if !exists {
		s.mu.Unlock()
		return &UserNotFoundError{Username: username}
	}

	delete(s.users, username)
	s.mu.Unlock()

	return s.saveUsers()
}

// ListUsers returns all usernames
func (s *FileStore) ListUsers() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	usernames := make([]string, 0, len(s.users))
	for username := range s.users {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

// CreateUser creates a new user with the given username and password
func (s *FileStore) CreateUser(username, password string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if user already exists
	if _, exists := s.users[username]; exists {
		return nil, fmt.Errorf("user already exists: %s", username)
	}

	// Create new user
	user := NewUser(username)

	// Store plaintext password
	user.Password = password

	// Add default permissions
	user.Permissions[user.HomeDir] = []string{"all"}

	// Save the user
	s.users[username] = user
	if err := s.saveUsers(); err != nil {
		return nil, err
	}

	return user, nil
}
