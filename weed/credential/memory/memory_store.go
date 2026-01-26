package memory

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	credential.Stores = append(credential.Stores, &MemoryStore{})
}

// MemoryStore implements CredentialStore using in-memory storage
// This is primarily intended for testing purposes
type MemoryStore struct {
	mu              sync.RWMutex
	users           map[string]*iam_pb.Identity             // username -> identity
	accessKeys      map[string]string                       // access_key -> username
	serviceAccounts map[string]*iam_pb.ServiceAccount       // id -> service_account
	policies        map[string]policy_engine.PolicyDocument // policy_name -> policy_document
	initialized     bool
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
	store.serviceAccounts = make(map[string]*iam_pb.ServiceAccount)
	store.policies = make(map[string]policy_engine.PolicyDocument)
	store.initialized = true

	return nil
}

func (store *MemoryStore) Shutdown() {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.users = nil
	store.accessKeys = nil
	store.serviceAccounts = nil
	store.policies = nil
	store.initialized = false
}

// Reset clears all data in the store (useful for testing)
func (store *MemoryStore) Reset() {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.initialized {
		store.users = make(map[string]*iam_pb.Identity)
		store.accessKeys = make(map[string]string)
		store.serviceAccounts = make(map[string]*iam_pb.ServiceAccount)
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
