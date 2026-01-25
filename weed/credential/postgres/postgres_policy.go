package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// GetPolicies retrieves all IAM policies from PostgreSQL
func (store *PostgresStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	policies := make(map[string]policy_engine.PolicyDocument)

	rows, err := store.db.QueryContext(ctx, "SELECT name, document FROM policies")
	if err != nil {
		return nil, fmt.Errorf("failed to query policies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var documentJSON []byte

		if err := rows.Scan(&name, &documentJSON); err != nil {
			return nil, fmt.Errorf("failed to scan policy row: %w", err)
		}

		var document policy_engine.PolicyDocument
		if err := json.Unmarshal(documentJSON, &document); err != nil {
			return nil, fmt.Errorf("failed to unmarshal policy document for %s: %v", name, err)
		}

		policies[name] = document
	}

	return policies, nil
}

// CreatePolicy creates a new IAM policy in PostgreSQL
func (store *PostgresStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	documentJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	_, err = store.db.ExecContext(ctx,
		"INSERT INTO policies (name, document) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET document = $2, updated_at = CURRENT_TIMESTAMP",
		name, documentJSON)
	if err != nil {
		return fmt.Errorf("failed to insert policy: %w", err)
	}

	return nil
}

// PutPolicy creates or updates an IAM policy in PostgreSQL
func (store *PostgresStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	documentJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	// Use UPSERT
	_, err = store.db.ExecContext(ctx,
		"INSERT INTO policies (name, document) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET document = $2, updated_at = CURRENT_TIMESTAMP",
		name, documentJSON)
	if err != nil {
		return fmt.Errorf("failed to put policy: %w", err)
	}

	return nil
}

// UpdatePolicy updates an existing IAM policy in PostgreSQL
func (store *PostgresStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	documentJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	result, err := store.db.ExecContext(ctx,
		"UPDATE policies SET document = $2, updated_at = CURRENT_TIMESTAMP WHERE name = $1",
		name, documentJSON)
	if err != nil {
		return fmt.Errorf("failed to update policy: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("policy %s not found", name)
	}

	return nil
}

// DeletePolicy deletes an IAM policy from PostgreSQL
func (store *PostgresStore) DeletePolicy(ctx context.Context, name string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx, "DELETE FROM policies WHERE name = $1", name)
	if err != nil {
		return fmt.Errorf("failed to delete policy: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("policy %s not found", name)
	}

	return nil
}

// GetPolicy retrieves a specific IAM policy by name from PostgreSQL
func (store *PostgresStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	policies, err := store.GetPolicies(ctx)
	if err != nil {
		return nil, err
	}

	if policy, exists := policies[name]; exists {
		return &policy, nil
	}

	return nil, nil // Policy not found
}
