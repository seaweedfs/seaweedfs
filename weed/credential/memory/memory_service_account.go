package memory

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *MemoryStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.serviceAccounts[sa.Id]; exists {
		return fmt.Errorf("service account already exists")
	}
	store.serviceAccounts[sa.Id] = sa
	if sa.Credential != nil && sa.Credential.AccessKey != "" {
		store.serviceAccountAccessKeys[sa.Credential.AccessKey] = sa.Id
	}
	return nil
}

func (store *MemoryStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	_, exists := store.serviceAccounts[id]
	if !exists {
		return fmt.Errorf("service account does not exist")
	}
	if sa.Id != id {
		return fmt.Errorf("service account ID mismatch")
	}

	// Update access key index: remove any existing keys for this SA
	for k, v := range store.serviceAccountAccessKeys {
		if v == id {
			delete(store.serviceAccountAccessKeys, k)
		}
	}

	store.serviceAccounts[id] = sa

	if sa.Credential != nil && sa.Credential.AccessKey != "" {
		store.serviceAccountAccessKeys[sa.Credential.AccessKey] = sa.Id
	}
	return nil
}

func (store *MemoryStore) DeleteServiceAccount(ctx context.Context, id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if sa, ok := store.serviceAccounts[id]; ok {
		if sa.Credential != nil && sa.Credential.AccessKey != "" {
			delete(store.serviceAccountAccessKeys, sa.Credential.AccessKey)
		}
		delete(store.serviceAccounts, id)
	}
	return nil
}

func (store *MemoryStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if sa, exists := store.serviceAccounts[id]; exists {
		return sa, nil
	}
	return nil, nil // Return nil if not found
}

func (store *MemoryStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var accounts []*iam_pb.ServiceAccount
	for _, sa := range store.serviceAccounts {
		accounts = append(accounts, sa)
	}
	return accounts, nil
}
