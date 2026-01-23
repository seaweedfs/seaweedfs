package credential

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// CredentialManager manages user credentials using a configurable store
type CredentialManager struct {
	store CredentialStore
}

// NewCredentialManager creates a new credential manager with the specified store
func NewCredentialManager(storeName CredentialStoreTypeName, configuration util.Configuration, prefix string) (*CredentialManager, error) {
	var store CredentialStore

	// Find the requested store implementation
	for _, s := range Stores {
		if s.GetName() == storeName {
			store = s
			break
		}
	}

	if store == nil {
		return nil, fmt.Errorf("credential store '%s' not found. Available stores: %s",
			storeName, getAvailableStores())
	}

	// Initialize the store
	if err := store.Initialize(configuration, prefix); err != nil {
		return nil, fmt.Errorf("failed to initialize credential store '%s': %v", storeName, err)
	}

	return &CredentialManager{
		store: store,
	}, nil
}

// GetStore returns the underlying credential store
func (cm *CredentialManager) GetStore() CredentialStore {
	return cm.store
}

// LoadConfiguration loads the S3 API configuration
func (cm *CredentialManager) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	return cm.store.LoadConfiguration(ctx)
}

// SaveConfiguration saves the S3 API configuration
func (cm *CredentialManager) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	return cm.store.SaveConfiguration(ctx, config)
}

// CreateUser creates a new user
func (cm *CredentialManager) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	return cm.store.CreateUser(ctx, identity)
}

// GetUser retrieves a user by username
func (cm *CredentialManager) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	return cm.store.GetUser(ctx, username)
}

// UpdateUser updates an existing user
func (cm *CredentialManager) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return cm.store.UpdateUser(ctx, username, identity)
}

// DeleteUser removes a user
func (cm *CredentialManager) DeleteUser(ctx context.Context, username string) error {
	return cm.store.DeleteUser(ctx, username)
}

// ListUsers returns all usernames
func (cm *CredentialManager) ListUsers(ctx context.Context) ([]string, error) {
	return cm.store.ListUsers(ctx)
}

// GetUserByAccessKey retrieves a user by access key
func (cm *CredentialManager) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	return cm.store.GetUserByAccessKey(ctx, accessKey)
}

// CreateAccessKey creates a new access key for a user
func (cm *CredentialManager) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	return cm.store.CreateAccessKey(ctx, username, credential)
}

// DeleteAccessKey removes an access key for a user
func (cm *CredentialManager) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return cm.store.DeleteAccessKey(ctx, username, accessKey)
}

// CreateServiceAccount creates a new service account
func (cm *CredentialManager) CreateServiceAccount(ctx context.Context, serviceAccount *iam_pb.ServiceAccount) error {
	return cm.store.CreateServiceAccount(ctx, serviceAccount)
}

// GetServiceAccount retrieves a service account by ID
func (cm *CredentialManager) GetServiceAccount(ctx context.Context, serviceAccountId string) (*iam_pb.ServiceAccount, error) {
	return cm.store.GetServiceAccount(ctx, serviceAccountId)
}

// UpdateServiceAccount updates an existing service account
func (cm *CredentialManager) UpdateServiceAccount(ctx context.Context, serviceAccountId string, serviceAccount *iam_pb.ServiceAccount) error {
	return cm.store.UpdateServiceAccount(ctx, serviceAccountId, serviceAccount)
}

// DeleteServiceAccount removes a service account by ID
func (cm *CredentialManager) DeleteServiceAccount(ctx context.Context, serviceAccountId string) error {
	return cm.store.DeleteServiceAccount(ctx, serviceAccountId)
}

// ListServiceAccounts lists service accounts, optionally filtered by parent user
func (cm *CredentialManager) ListServiceAccounts(ctx context.Context, parentUser string) ([]*iam_pb.ServiceAccount, error) {
	return cm.store.ListServiceAccounts(ctx, parentUser)
}

// Shutdown performs cleanup
func (cm *CredentialManager) Shutdown() {
	if cm.store != nil {
		cm.store.Shutdown()
	}
}

// getAvailableStores returns a comma-separated list of available store names
func getAvailableStores() string {
	var storeNames []string
	for _, store := range Stores {
		storeNames = append(storeNames, string(store.GetName()))
	}
	return strings.Join(storeNames, ", ")
}

// GetAvailableStores returns a list of available credential store names
func GetAvailableStores() []CredentialStoreTypeName {
	var storeNames []CredentialStoreTypeName
	for _, store := range Stores {
		storeNames = append(storeNames, store.GetName())
	}
	if storeNames == nil {
		return []CredentialStoreTypeName{}
	}
	return storeNames
}
