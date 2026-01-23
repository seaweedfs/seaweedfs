package postgres

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// Service Account methods for PostgreSQL store
// These are stub implementations - full PostgreSQL backend support is pending

func (store *PostgresStore) CreateServiceAccount(ctx context.Context, serviceAccount *iam_pb.ServiceAccount) error {
	return fmt.Errorf("CreateServiceAccount not implemented for PostgreSQL store")
}

func (store *PostgresStore) GetServiceAccount(ctx context.Context, serviceAccountId string) (*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("GetServiceAccount not implemented for PostgreSQL store")
}

func (store *PostgresStore) UpdateServiceAccount(ctx context.Context, serviceAccountId string, serviceAccount *iam_pb.ServiceAccount) error {
	return fmt.Errorf("UpdateServiceAccount not implemented for PostgreSQL store")
}

func (store *PostgresStore) DeleteServiceAccount(ctx context.Context, serviceAccountId string) error {
	return fmt.Errorf("DeleteServiceAccount not implemented for PostgreSQL store")
}

func (store *PostgresStore) ListServiceAccounts(ctx context.Context, parentUser string) ([]*iam_pb.ServiceAccount, error) {
	return nil, fmt.Errorf("ListServiceAccounts not implemented for PostgreSQL store")
}
