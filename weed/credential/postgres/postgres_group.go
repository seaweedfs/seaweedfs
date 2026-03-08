package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *PostgresStore) CreateGroup(ctx context.Context, group *iam_pb.Group) error {
	membersJSON, err := json.Marshal(group.Members)
	if err != nil {
		return fmt.Errorf("failed to marshal members: %w", err)
	}
	policyNamesJSON, err := json.Marshal(group.PolicyNames)
	if err != nil {
		return fmt.Errorf("failed to marshal policy_names: %w", err)
	}

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO groups (name, members, policy_names, disabled) VALUES ($1, $2, $3, $4)`,
		group.Name, membersJSON, policyNamesJSON, group.Disabled)
	if err != nil {
		// Check for unique constraint violation
		return fmt.Errorf("failed to create group: %w", err)
	}
	return nil
}

func (store *PostgresStore) GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error) {
	var membersJSON, policyNamesJSON []byte
	var disabled bool
	err := store.db.QueryRowContext(ctx,
		`SELECT members, policy_names, disabled FROM groups WHERE name = $1`, groupName).
		Scan(&membersJSON, &policyNamesJSON, &disabled)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, credential.ErrGroupNotFound
		}
		return nil, fmt.Errorf("failed to get group: %w", err)
	}

	group := &iam_pb.Group{
		Name:     groupName,
		Disabled: disabled,
	}
	if err := json.Unmarshal(membersJSON, &group.Members); err != nil {
		return nil, fmt.Errorf("failed to unmarshal members: %w", err)
	}
	if err := json.Unmarshal(policyNamesJSON, &group.PolicyNames); err != nil {
		return nil, fmt.Errorf("failed to unmarshal policy_names: %w", err)
	}
	return group, nil
}

func (store *PostgresStore) DeleteGroup(ctx context.Context, groupName string) error {
	result, err := store.db.ExecContext(ctx, `DELETE FROM groups WHERE name = $1`, groupName)
	if err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return credential.ErrGroupNotFound
	}
	return nil
}

func (store *PostgresStore) ListGroups(ctx context.Context) ([]string, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT name FROM groups ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan group name: %w", err)
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func (store *PostgresStore) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	membersJSON, err := json.Marshal(group.Members)
	if err != nil {
		return fmt.Errorf("failed to marshal members: %w", err)
	}
	policyNamesJSON, err := json.Marshal(group.PolicyNames)
	if err != nil {
		return fmt.Errorf("failed to marshal policy_names: %w", err)
	}

	result, err := store.db.ExecContext(ctx,
		`UPDATE groups SET members = $1, policy_names = $2, disabled = $3, updated_at = CURRENT_TIMESTAMP WHERE name = $4`,
		membersJSON, policyNamesJSON, group.Disabled, group.Name)
	if err != nil {
		return fmt.Errorf("failed to update group: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return credential.ErrGroupNotFound
	}
	return nil
}
