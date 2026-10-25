package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func (store *PostgresStore) PutGroupInlinePolicy(ctx context.Context, groupName, policyName string, document policy_engine.PolicyDocument) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	docJSON, err := json.Marshal(document)
	if err != nil {
		glog.Errorf("credential postgres: PutGroupInlinePolicy marshal failed group=%s policy=%s: %v", groupName, policyName, err)
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO group_inline_policies (group_name, policy_name, document)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (group_name, policy_name)
		 DO UPDATE SET document = $3, updated_at = CURRENT_TIMESTAMP`,
		groupName, policyName, jsonbParam(docJSON))
	if err != nil {
		glog.Errorf("credential postgres: PutGroupInlinePolicy failed group=%s policy=%s: %v", groupName, policyName, err)
		return fmt.Errorf("failed to upsert group inline policy: %w", err)
	}

	glog.V(0).Infof("credential postgres: PutGroupInlinePolicy group=%s policy=%s", groupName, policyName)
	return nil
}

func (store *PostgresStore) GetGroupInlinePolicy(ctx context.Context, groupName, policyName string) (*policy_engine.PolicyDocument, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var docJSON []byte
	err := store.db.QueryRowContext(ctx,
		"SELECT document FROM group_inline_policies WHERE group_name = $1 AND policy_name = $2",
		groupName, policyName).Scan(&docJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		glog.Errorf("credential postgres: GetGroupInlinePolicy query failed group=%s policy=%s: %v", groupName, policyName, err)
		return nil, fmt.Errorf("failed to query group inline policy: %w", err)
	}

	var doc policy_engine.PolicyDocument
	if err := json.Unmarshal(docJSON, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group inline policy: %w", err)
	}
	return &doc, nil
}

func (store *PostgresStore) DeleteGroupInlinePolicy(ctx context.Context, groupName, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx,
		"DELETE FROM group_inline_policies WHERE group_name = $1 AND policy_name = $2",
		groupName, policyName)
	if err != nil {
		glog.Errorf("credential postgres: DeleteGroupInlinePolicy failed group=%s policy=%s: %v", groupName, policyName, err)
		return fmt.Errorf("failed to delete group inline policy: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	glog.V(0).Infof("credential postgres: DeleteGroupInlinePolicy group=%s policy=%s deleted=%d", groupName, policyName, rowsAffected)
	return nil
}

func (store *PostgresStore) ListGroupInlinePolicies(ctx context.Context, groupName string) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx,
		"SELECT policy_name FROM group_inline_policies WHERE group_name = $1 ORDER BY policy_name",
		groupName)
	if err != nil {
		glog.Errorf("credential postgres: ListGroupInlinePolicies query failed group=%s: %v", groupName, err)
		return nil, fmt.Errorf("failed to query group inline policies: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan policy name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating group inline policy rows: %w", err)
	}
	return names, nil
}

func (store *PostgresStore) LoadGroupInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx,
		"SELECT group_name, policy_name, document FROM group_inline_policies ORDER BY group_name, policy_name")
	if err != nil {
		glog.Errorf("credential postgres: LoadGroupInlinePolicies query failed: %v", err)
		return nil, fmt.Errorf("failed to query group inline policies: %w", err)
	}
	defer rows.Close()

	result := make(map[string]map[string]policy_engine.PolicyDocument)
	count := 0
	for rows.Next() {
		var groupName, policyName string
		var docJSON []byte
		if err := rows.Scan(&groupName, &policyName, &docJSON); err != nil {
			return nil, fmt.Errorf("failed to scan group inline policy row: %w", err)
		}

		var doc policy_engine.PolicyDocument
		if err := json.Unmarshal(docJSON, &doc); err != nil {
			glog.Warningf("credential postgres: LoadGroupInlinePolicies unmarshal failed group=%s policy=%s: %v", groupName, policyName, err)
			continue
		}

		if result[groupName] == nil {
			result[groupName] = make(map[string]policy_engine.PolicyDocument)
		}
		result[groupName][policyName] = doc
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating group inline policy rows: %w", err)
	}

	glog.V(0).Infof("credential postgres: LoadGroupInlinePolicies loaded %d policies for %d groups", count, len(result))
	return result, nil
}
