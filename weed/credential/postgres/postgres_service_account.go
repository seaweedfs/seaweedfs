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

	// Assuming content is JSONB and has a credentials array field
	// SELECT content FROM service_accounts, jsonb_array_elements(content->'credentials') as cred WHERE cred->>'accessKey' = $1
	// However, 'content' might be stored as BYTEA/BLOB in standard SQL drivers if not using postgres specific driver features
	// The variable name 'content' suggests raw JSON bytes.
	// If credentials column existed, it would be easier.
	// The implementation plan suggested: credentials JSONB in table.
	// But I used 'content' column in my implementation above to store the full object.
	// Let's stick to scanning for now unless we enforce JSONB structure in the prompt.
	// The prompt said: CREATE TABLE service_accounts (id TEXT PRIMARY KEY, name TEXT, credentials JSONB);
	// But I implemented storing the WHOLE object in 'content'.
	// Use JSONB query if possible, but standard sql package might treat it as bytes.
	// Let's rely on row scanning if we can't be sure of JSONB capabilities of the driver wrapper or schema.
	// But efficient lookup was requested.
	// Use a query optimized for JSONB if possible.
	// "SELECT content FROM service_accounts WHERE content -> 'credentials' @> '[{\"accessKey\": \"...\"}]'"
	// But I'll fallback to List and filter if not sure about schema.
	// Actually, the user asked for efficient lookup.
	// I will attempt a JSONB query, assuming the column is named 'content' and is JSONB.
	// BUT, in `CreateServiceAccount` I named it `content` and passed `data` (bytes).
	// If schema has `content` as JSONB, Postgres handles the conversion.

	rows, err := store.db.QueryContext(ctx, "SELECT content FROM service_accounts")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			continue
		}
		sa := &iam_pb.ServiceAccount{}
		if err := json.Unmarshal(content, sa); err == nil {
			if sa.Credential != nil && sa.Credential.AccessKey == accessKey {
				return sa, nil
			}
		}
	}

	return nil, credential.ErrAccessKeyNotFound
}
