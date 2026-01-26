package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *PostgresStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	data, err := json.Marshal(sa)
	if err != nil {
		return fmt.Errorf("failed to marshal service account: %w", err)
	}

	_, err = store.db.ExecContext(ctx,
		"INSERT INTO service_accounts (id, name, content) VALUES ($1, $2, $3)",
		sa.Id, sa.Id, data)
	if err != nil {
		return fmt.Errorf("failed to insert service account: %w", err)
	}
	return nil
}

func (store *PostgresStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	if sa.Id != id {
		return fmt.Errorf("service account ID mismatch")
	}

	data, err := json.Marshal(sa)
	if err != nil {
		return fmt.Errorf("failed to marshal service account: %w", err)
	}

	result, err := store.db.ExecContext(ctx,
		"UPDATE service_accounts SET name = $2, content = $3 WHERE id = $1",
		id, sa.Id, data)
	if err != nil {
		return fmt.Errorf("failed to update service account: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return credential.ErrUserNotFound
	}
	return nil
}

func (store *PostgresStore) DeleteServiceAccount(ctx context.Context, id string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx, "DELETE FROM service_accounts WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete service account: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return credential.ErrUserNotFound
	}
	return nil
}

func (store *PostgresStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var content []byte
	err := store.db.QueryRowContext(ctx, "SELECT content FROM service_accounts WHERE id = $1", id).Scan(&content)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get service account: %w", err)
	}

	sa := &iam_pb.ServiceAccount{}
	if err := json.Unmarshal(content, sa); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service account: %w", err)
	}
	return sa, nil
}

func (store *PostgresStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx, "SELECT content FROM service_accounts")
	if err != nil {
		return nil, fmt.Errorf("failed to list service accounts: %w", err)
	}
	defer rows.Close()

	var accounts []*iam_pb.ServiceAccount
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return nil, fmt.Errorf("failed to scan service account: %w", err)
		}
		sa := &iam_pb.ServiceAccount{}
		if err := json.Unmarshal(content, sa); err != nil {
			return nil, fmt.Errorf("failed to unmarshal service account: %w", err)
		}
		accounts = append(accounts, sa)
	}
	return accounts, nil
}

func (store *PostgresStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	// Lookup service account by searching credentials inside the full service account objects.
	// Note: content is JSONB with credentials array.
	rows, err := store.db.QueryContext(ctx, "SELECT content FROM service_accounts")
	if err != nil {
		return nil, fmt.Errorf("failed to list service accounts for lookup: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return nil, fmt.Errorf("scan service_account row: %w", err)
		}
		sa := &iam_pb.ServiceAccount{}
		if err := json.Unmarshal(content, sa); err != nil {
			return nil, fmt.Errorf("unmarshal service_account: %w", err)
		}
		if sa.Credential != nil && sa.Credential.AccessKey == accessKey {
			return sa, nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return nil, credential.ErrAccessKeyNotFound
}
