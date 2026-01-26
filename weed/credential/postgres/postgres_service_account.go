package postgres

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *PostgresStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	return fmt.Errorf("not implemented")
}

func (store *PostgresStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	return fmt.Errorf("not implemented")
}

func (store *PostgresStore) DeleteServiceAccount(ctx context.Context, id string) error {
	return fmt.Errorf("not implemented")
}

func (store *PostgresStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("not implemented")
}

func (store *PostgresStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("not implemented")
}
