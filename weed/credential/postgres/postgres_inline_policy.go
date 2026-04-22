package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func (store *PostgresStore) PutUserInlinePolicy(ctx context.Context, userName, policyName string, document policy_engine.PolicyDocument) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	docJSON, err := json.Marshal(document)
	if err != nil {
		glog.Errorf("credential postgres: PutUserInlinePolicy marshal failed user=%s policy=%s: %v", userName, policyName, err)
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO user_inline_policies (username, policy_name, document)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (username, policy_name)
		 DO UPDATE SET document = $3, updated_at = CURRENT_TIMESTAMP`,
		userName, policyName, string(docJSON))
	if err != nil {
		glog.Errorf("credential postgres: PutUserInlinePolicy failed user=%s policy=%s: %v", userName, policyName, err)
		return fmt.Errorf("failed to upsert inline policy: %w", err)
	}

	glog.V(0).Infof("credential postgres: PutUserInlinePolicy user=%s policy=%s", userName, policyName)
	return nil
}

func (store *PostgresStore) GetUserInlinePolicy(ctx context.Context, userName, policyName string) (*policy_engine.PolicyDocument, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var docJSON []byte
	err := store.db.QueryRowContext(ctx,
		"SELECT document FROM user_inline_policies WHERE username = $1 AND policy_name = $2",
		userName, policyName).Scan(&docJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			glog.V(2).Infof("credential postgres: GetUserInlinePolicy user=%s policy=%s not found", userName, policyName)
			return nil, nil
		}
		glog.Errorf("credential postgres: GetUserInlinePolicy query failed user=%s policy=%s: %v", userName, policyName, err)
		return nil, fmt.Errorf("failed to query inline policy: %w", err)
	}

	var doc policy_engine.PolicyDocument
	if err := json.Unmarshal(docJSON, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inline policy: %w", err)
	}

	glog.V(2).Infof("credential postgres: GetUserInlinePolicy user=%s policy=%s found", userName, policyName)
	return &doc, nil
}

func (store *PostgresStore) DeleteUserInlinePolicy(ctx context.Context, userName, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx,
		"DELETE FROM user_inline_policies WHERE username = $1 AND policy_name = $2",
		userName, policyName)
	if err != nil {
		glog.Errorf("credential postgres: DeleteUserInlinePolicy failed user=%s policy=%s: %v", userName, policyName, err)
		return fmt.Errorf("failed to delete inline policy: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	glog.V(0).Infof("credential postgres: DeleteUserInlinePolicy user=%s policy=%s deleted=%d", userName, policyName, rowsAffected)
	return nil
}

func (store *PostgresStore) ListUserInlinePolicies(ctx context.Context, userName string) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx,
		"SELECT policy_name FROM user_inline_policies WHERE username = $1 ORDER BY policy_name",
		userName)
	if err != nil {
		glog.Errorf("credential postgres: ListUserInlinePolicies query failed user=%s: %v", userName, err)
		return nil, fmt.Errorf("failed to query inline policies: %w", err)
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

	glog.V(1).Infof("credential postgres: ListUserInlinePolicies user=%s count=%d", userName, len(names))
	return names, nil
}

func (store *PostgresStore) LoadInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx,
		"SELECT username, policy_name, document FROM user_inline_policies ORDER BY username, policy_name")
	if err != nil {
		glog.Errorf("credential postgres: LoadInlinePolicies query failed: %v", err)
		return nil, fmt.Errorf("failed to query inline policies: %w", err)
	}
	defer rows.Close()

	result := make(map[string]map[string]policy_engine.PolicyDocument)
	count := 0
	for rows.Next() {
		var username, policyName string
		var docJSON []byte
		if err := rows.Scan(&username, &policyName, &docJSON); err != nil {
			return nil, fmt.Errorf("failed to scan inline policy row: %w", err)
		}

		var doc policy_engine.PolicyDocument
		if err := json.Unmarshal(docJSON, &doc); err != nil {
			glog.Warningf("credential postgres: LoadInlinePolicies unmarshal failed user=%s policy=%s: %v", username, policyName, err)
			continue
		}

		if result[username] == nil {
			result[username] = make(map[string]policy_engine.PolicyDocument)
		}
		result[username][policyName] = doc
		count++
	}

	glog.V(0).Infof("credential postgres: LoadInlinePolicies loaded %d policies for %d users", count, len(result))
	return result, nil
}
