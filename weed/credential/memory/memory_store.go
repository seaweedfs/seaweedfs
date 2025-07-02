package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	credential.Stores = append(credential.Stores, &MemoryStore{})
}

// MemoryStore implements CredentialStore using in-memory storage
// This is primarily intended for testing purposes
type MemoryStore struct {
	mu          sync.RWMutex
	users       map[string]*iam_pb.Identity // username -> identity
	accessKeys  map[string]string           // access_key -> username
	initialized bool
}

func (store *MemoryStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeMemory
}

func (store *MemoryStore) Initialize(configuration util.Configuration, prefix string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.initialized {
		return nil
	}

	store.users = make(map[string]*iam_pb.Identity)
	store.accessKeys = make(map[string]string)
	store.initialized = true

	return nil
}

func (store *MemoryStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	config := &iam_pb.S3ApiConfiguration{}

	// Convert all users to identities
	for _, user := range store.users {
		// Deep copy the identity to avoid mutation issues
		identityCopy := store.deepCopyIdentity(user)
		config.Identities = append(config.Identities, identityCopy)
	}

	return config, nil
}

func (store *MemoryStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	// Clear existing data
	store.users = make(map[string]*iam_pb.Identity)
	store.accessKeys = make(map[string]string)

	// Add all identities
	for _, identity := range config.Identities {
		// Deep copy to avoid mutation issues
		identityCopy := store.deepCopyIdentity(identity)
		store.users[identity.Name] = identityCopy

		// Index access keys
		for _, credential := range identity.Credentials {
			store.accessKeys[credential.AccessKey] = identity.Name
		}
	}

	return nil
}

func (store *MemoryStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	if _, exists := store.users[identity.Name]; exists {
		return credential.ErrUserAlreadyExists
	}

	// Check for duplicate access keys
	for _, cred := range identity.Credentials {
		if _, exists := store.accessKeys[cred.AccessKey]; exists {
			return fmt.Errorf("access key %s already exists", cred.AccessKey)
		}
	}

	// Deep copy to avoid mutation issues
	identityCopy := store.deepCopyIdentity(identity)
	store.users[identity.Name] = identityCopy

	// Index access keys
	for _, cred := range identity.Credentials {
		store.accessKeys[cred.AccessKey] = identity.Name
	}

	return nil
}

func (store *MemoryStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return nil, credential.ErrUserNotFound
	}

	// Return a deep copy to avoid mutation issues
	return store.deepCopyIdentity(user), nil
}

func (store *MemoryStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	existingUser, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Remove old access keys from index
	for _, cred := range existingUser.Credentials {
		delete(store.accessKeys, cred.AccessKey)
	}

	// Check for duplicate access keys (excluding current user)
	for _, cred := range identity.Credentials {
		if existingUsername, exists := store.accessKeys[cred.AccessKey]; exists && existingUsername != username {
			return fmt.Errorf("access key %s already exists", cred.AccessKey)
		}
	}

	// Deep copy to avoid mutation issues
	identityCopy := store.deepCopyIdentity(identity)
	store.users[username] = identityCopy

	// Re-index access keys
	for _, cred := range identity.Credentials {
		store.accessKeys[cred.AccessKey] = username
	}

	return nil
}

func (store *MemoryStore) DeleteUser(ctx context.Context, username string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Remove access keys from index
	for _, cred := range user.Credentials {
		delete(store.accessKeys, cred.AccessKey)
	}

	// Remove user
	delete(store.users, username)

	return nil
}

func (store *MemoryStore) ListUsers(ctx context.Context) ([]string, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	var usernames []string
	for username := range store.users {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

func (store *MemoryStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.initialized {
		return nil, fmt.Errorf("store not initialized")
	}

	username, exists := store.accessKeys[accessKey]
	if !exists {
		return nil, credential.ErrAccessKeyNotFound
	}

	user, exists := store.users[username]
	if !exists {
		// This should not happen, but handle it gracefully
		return nil, credential.ErrUserNotFound
	}

	// Return a deep copy to avoid mutation issues
	return store.deepCopyIdentity(user), nil
}

func (store *MemoryStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Check if access key already exists
	if _, exists := store.accessKeys[cred.AccessKey]; exists {
		return fmt.Errorf("access key %s already exists", cred.AccessKey)
	}

	// Add credential to user
	user.Credentials = append(user.Credentials, &iam_pb.Credential{
		AccessKey: cred.AccessKey,
		SecretKey: cred.SecretKey,
	})

	// Index the access key
	store.accessKeys[cred.AccessKey] = username

	return nil
}

func (store *MemoryStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.initialized {
		return fmt.Errorf("store not initialized")
	}

	user, exists := store.users[username]
	if !exists {
		return credential.ErrUserNotFound
	}

	// Find and remove the credential
	var newCredentials []*iam_pb.Credential
	found := false
	for _, cred := range user.Credentials {
		if cred.AccessKey == accessKey {
			found = true
			// Remove from access key index
			delete(store.accessKeys, accessKey)
		} else {
			newCredentials = append(newCredentials, cred)
		}
	}

	if !found {
		return credential.ErrAccessKeyNotFound
	}

	user.Credentials = newCredentials
	return nil
}

func (store *MemoryStore) Shutdown() {
	store.mu.Lock()
	defer store.mu.Unlock()

	// Clear all data
	store.users = nil
	store.accessKeys = nil
	store.initialized = false
}

// deepCopyIdentity creates a deep copy of an identity to avoid mutation issues
func (store *MemoryStore) deepCopyIdentity(identity *iam_pb.Identity) *iam_pb.Identity {
	if identity == nil {
		return nil
	}

	// Use JSON marshaling/unmarshaling for deep copy
	// This is simple and safe for protobuf messages
	data, err := json.Marshal(identity)
	if err != nil {
		// Fallback to shallow copy if JSON fails
		return &iam_pb.Identity{
			Name:        identity.Name,
			Account:     identity.Account,
			Credentials: identity.Credentials,
			Actions:     identity.Actions,
		}
	}

	var copy iam_pb.Identity
	if err := json.Unmarshal(data, &copy); err != nil {
		// Fallback to shallow copy if JSON fails
		return &iam_pb.Identity{
			Name:        identity.Name,
			Account:     identity.Account,
			Credentials: identity.Credentials,
			Actions:     identity.Actions,
		}
	}

	return &copy
}

// Reset clears all data in the store (useful for testing)
func (store *MemoryStore) Reset() {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.initialized {
		store.users = make(map[string]*iam_pb.Identity)
		store.accessKeys = make(map[string]string)
	}
}

// GetUserCount returns the number of users in the store (useful for testing)
func (store *MemoryStore) GetUserCount() int {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return len(store.users)
}

// GetAccessKeyCount returns the number of access keys in the store (useful for testing)
func (store *MemoryStore) GetAccessKeyCount() int {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return len(store.accessKeys)
}
