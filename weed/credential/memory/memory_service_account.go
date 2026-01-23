package memory

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *MemoryStore) CreateServiceAccount(ctx context.Context, serviceAccount *iam_pb.ServiceAccount) error {
	return fmt.Errorf("service accounts not supported in memory store yet")
}

func (store *MemoryStore) GetServiceAccount(ctx context.Context, serviceAccountId string) (*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("service accounts not supported in memory store yet")
}

func (store *MemoryStore) UpdateServiceAccount(ctx context.Context, serviceAccountId string, serviceAccount *iam_pb.ServiceAccount) error {
	return fmt.Errorf("service accounts not supported in memory store yet")
}

func (store *MemoryStore) DeleteServiceAccount(ctx context.Context, serviceAccountId string) error {
	return fmt.Errorf("service accounts not supported in memory store yet")
}

func (store *MemoryStore) ListServiceAccounts(ctx context.Context, parentUser string) ([]*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("service accounts not supported in memory store yet")
}
