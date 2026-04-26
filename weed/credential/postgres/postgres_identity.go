package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *PostgresStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	config := &iam_pb.S3ApiConfiguration{}

	rows, err := store.db.QueryContext(ctx, "SELECT username, email, account_data, actions, policy_names FROM users")
	if err != nil {
		glog.Errorf("credential postgres: LoadConfiguration query failed: %v", err)
		return nil, fmt.Errorf("failed to query users: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, email string
		var accountDataJSON, actionsJSON, policyNamesJSON []byte

		if err := rows.Scan(&username, &email, &accountDataJSON, &actionsJSON, &policyNamesJSON); err != nil {
			glog.Errorf("credential postgres: LoadConfiguration scan failed: %v", err)
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}

		identity := &iam_pb.Identity{
			Name: username,
		}

		if len(accountDataJSON) > 0 {
			if err := json.Unmarshal(accountDataJSON, &identity.Account); err != nil {
				return nil, fmt.Errorf("failed to unmarshal account data for user %s: %v", username, err)
			}
		}

		if len(actionsJSON) > 0 {
			if err := json.Unmarshal(actionsJSON, &identity.Actions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal actions for user %s: %v", username, err)
			}
		}

		if len(policyNamesJSON) > 0 {
			if err := json.Unmarshal(policyNamesJSON, &identity.PolicyNames); err != nil {
				return nil, fmt.Errorf("failed to unmarshal policy names for user %s: %v", username, err)
			}
		}

		credRows, err := store.db.QueryContext(ctx, "SELECT access_key, secret_key FROM credentials WHERE username = $1", username)
		if err != nil {
			return nil, fmt.Errorf("failed to query credentials for user %s: %v", username, err)
		}

		for credRows.Next() {
			var accessKey, secretKey string
			if err := credRows.Scan(&accessKey, &secretKey); err != nil {
				credRows.Close()
				return nil, fmt.Errorf("failed to scan credential row for user %s: %v", username, err)
			}

			identity.Credentials = append(identity.Credentials, &iam_pb.Credential{
				AccessKey: accessKey,
				SecretKey: secretKey,
			})
		}
		credRows.Close()
		if err := credRows.Err(); err != nil {
			return nil, fmt.Errorf("failed iterating credential rows for user %s: %w", username, err)
		}
		config.Identities = append(config.Identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating user rows: %w", err)
	}

	glog.V(0).Infof("credential postgres: LoadConfiguration loaded %d identities", len(config.Identities))
	return config, nil
}

func (store *PostgresStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Track which usernames are in the incoming config for pruning
	configUsernames := make(map[string]bool, len(config.Identities))

	for _, identity := range config.Identities {
		configUsernames[identity.Name] = true

		var accountDataJSON []byte
		if identity.Account != nil {
			accountDataJSON, err = json.Marshal(identity.Account)
			if err != nil {
				return fmt.Errorf("failed to marshal account data for user %s: %v", identity.Name, err)
			}
		}

		var actionsJSON []byte
		if identity.Actions != nil {
			actionsJSON, err = json.Marshal(identity.Actions)
			if err != nil {
				return fmt.Errorf("failed to marshal actions for user %s: %v", identity.Name, err)
			}
		}

		var policyNamesJSON []byte
		if identity.PolicyNames != nil {
			policyNamesJSON, err = json.Marshal(identity.PolicyNames)
			if err != nil {
				return fmt.Errorf("failed to marshal policy names for user %s: %v", identity.Name, err)
			}
		}

		// Upsert user — preserves the row (and its CASCADE dependents) if it already exists
		_, err = tx.ExecContext(ctx,
			`INSERT INTO users (username, email, account_data, actions, policy_names)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (username) DO UPDATE SET
				email = EXCLUDED.email,
				account_data = EXCLUDED.account_data,
				actions = EXCLUDED.actions,
				policy_names = EXCLUDED.policy_names,
				updated_at = CURRENT_TIMESTAMP`,
			identity.Name, "", string(accountDataJSON), string(actionsJSON), string(policyNamesJSON))
		if err != nil {
			return fmt.Errorf("failed to upsert user %s: %v", identity.Name, err)
		}

		// Replace credentials for this user — credentials carry no independent
		// state worth preserving (unlike inline policies)
		if _, err := tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = $1", identity.Name); err != nil {
			return fmt.Errorf("failed to clear credentials for user %s: %v", identity.Name, err)
		}
		for _, cred := range identity.Credentials {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
				identity.Name, cred.AccessKey, cred.SecretKey)
			if err != nil {
				return fmt.Errorf("failed to insert credential for user %s: %v", identity.Name, err)
			}
		}
	}

	// Prune users no longer in config — CASCADE correctly removes their
	// credentials and inline policies since they were intentionally deleted
	rows, err := tx.QueryContext(ctx, "SELECT username FROM users")
	if err != nil {
		return fmt.Errorf("failed to list existing users for pruning: %w", err)
	}
	var toDelete []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan username for pruning: %w", err)
		}
		if !configUsernames[username] {
			toDelete = append(toDelete, username)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed iterating user rows for pruning: %w", err)
	}

	for _, username := range toDelete {
		if _, err := tx.ExecContext(ctx, "DELETE FROM users WHERE username = $1", username); err != nil {
			return fmt.Errorf("failed to prune user %s: %v", username, err)
		}
	}

	glog.V(0).Infof("credential postgres: SaveConfiguration saved %d identities, pruned %d", len(config.Identities), len(toDelete))
	return tx.Commit()
}

func (store *PostgresStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", identity.Name).Scan(&count)
	if err != nil {
		glog.Errorf("credential postgres: CreateUser check failed user=%s: %v", identity.Name, err)
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count > 0 {
		glog.V(1).Infof("credential postgres: CreateUser user=%s already exists", identity.Name)
		return credential.ErrUserAlreadyExists
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %w", err)
		}
	}

	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %w", err)
		}
	}

	var policyNamesJSON []byte
	if identity.PolicyNames != nil {
		policyNamesJSON, err = json.Marshal(identity.PolicyNames)
		if err != nil {
			return fmt.Errorf("failed to marshal policy names: %w", err)
		}
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO users (username, email, account_data, actions, policy_names) VALUES ($1, $2, $3, $4, $5)",
		identity.Name, "", string(accountDataJSON), string(actionsJSON), string(policyNamesJSON))
	if err != nil {
		glog.Errorf("credential postgres: CreateUser insert failed user=%s: %v", identity.Name, err)
		return fmt.Errorf("failed to insert user: %w", err)
	}

	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			identity.Name, cred.AccessKey, cred.SecretKey)
		if err != nil {
			glog.Errorf("credential postgres: CreateUser insert credential failed user=%s accessKey=%s: %v", identity.Name, cred.AccessKey, err)
			return fmt.Errorf("failed to insert credential: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		glog.Errorf("credential postgres: CreateUser commit failed user=%s: %v", identity.Name, err)
		return fmt.Errorf("failed to commit: %w", err)
	}

	glog.V(0).Infof("credential postgres: CreateUser user=%s credentials=%d actions=%d", identity.Name, len(identity.Credentials), len(identity.Actions))
	return nil
}

func (store *PostgresStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var email string
	var accountDataJSON, actionsJSON, policyNamesJSON []byte

	err := store.db.QueryRowContext(ctx,
		"SELECT email, account_data, actions, policy_names FROM users WHERE username = $1",
		username).Scan(&email, &accountDataJSON, &actionsJSON, &policyNamesJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			glog.V(2).Infof("credential postgres: GetUser user=%s not found", username)
			return nil, credential.ErrUserNotFound
		}
		glog.Errorf("credential postgres: GetUser query failed user=%s: %v", username, err)
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	identity := &iam_pb.Identity{
		Name: username,
	}

	if len(accountDataJSON) > 0 {
		if err := json.Unmarshal(accountDataJSON, &identity.Account); err != nil {
			return nil, fmt.Errorf("failed to unmarshal account data: %w", err)
		}
	}

	if len(actionsJSON) > 0 {
		if err := json.Unmarshal(actionsJSON, &identity.Actions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal actions: %w", err)
		}
	}

	if len(policyNamesJSON) > 0 {
		if err := json.Unmarshal(policyNamesJSON, &identity.PolicyNames); err != nil {
			return nil, fmt.Errorf("failed to unmarshal policy names: %w", err)
		}
	}

	rows, err := store.db.QueryContext(ctx, "SELECT access_key, secret_key FROM credentials WHERE username = $1", username)
	if err != nil {
		return nil, fmt.Errorf("failed to query credentials: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var accessKey, secretKey string
		if err := rows.Scan(&accessKey, &secretKey); err != nil {
			return nil, fmt.Errorf("failed to scan credential: %w", err)
		}
		identity.Credentials = append(identity.Credentials, &iam_pb.Credential{
			AccessKey: accessKey,
			SecretKey: secretKey,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating credential rows: %w", err)
	}

	glog.V(2).Infof("credential postgres: GetUser user=%s credentials=%d actions=%d", username, len(identity.Credentials), len(identity.Actions))
	return identity, nil
}


func (store *PostgresStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %w", err)
		}
	}

	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %w", err)
		}
	}

	var policyNamesJSON []byte
	if identity.PolicyNames != nil {
		policyNamesJSON, err = json.Marshal(identity.PolicyNames)
		if err != nil {
			return fmt.Errorf("failed to marshal policy names: %w", err)
		}
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE users SET email = $2, account_data = $3, actions = $4, policy_names = $5, updated_at = CURRENT_TIMESTAMP WHERE username = $1",
		username, "", string(accountDataJSON), string(actionsJSON), string(policyNamesJSON))
	if err != nil {
		glog.Errorf("credential postgres: UpdateUser failed user=%s: %v", username, err)
		return fmt.Errorf("failed to update user: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = $1", username)
	if err != nil {
		return fmt.Errorf("failed to delete existing credentials: %w", err)
	}

	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			username, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		glog.Errorf("credential postgres: UpdateUser commit failed user=%s: %v", username, err)
		return fmt.Errorf("failed to commit: %w", err)
	}

	glog.V(0).Infof("credential postgres: UpdateUser user=%s credentials=%d", username, len(identity.Credentials))
	return nil
}

func (store *PostgresStore) DeleteUser(ctx context.Context, username string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx, "DELETE FROM users WHERE username = $1", username)
	if err != nil {
		glog.Errorf("credential postgres: DeleteUser failed user=%s: %v", username, err)
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		glog.V(1).Infof("credential postgres: DeleteUser user=%s not found", username)
		return credential.ErrUserNotFound
	}

	glog.V(0).Infof("credential postgres: DeleteUser user=%s", username)
	return nil
}

func (store *PostgresStore) ListUsers(ctx context.Context) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx, "SELECT username FROM users ORDER BY username")
	if err != nil {
		glog.Errorf("credential postgres: ListUsers query failed: %v", err)
		return nil, fmt.Errorf("failed to query users: %w", err)
	}
	defer rows.Close()

	var usernames []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, fmt.Errorf("failed to scan username: %w", err)
		}
		usernames = append(usernames, username)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed iterating user rows: %w", err)
	}

	glog.V(1).Infof("credential postgres: ListUsers count=%d", len(usernames))
	return usernames, nil
}

func (store *PostgresStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var username string
	err := store.db.QueryRowContext(ctx, "SELECT username FROM credentials WHERE access_key = $1", accessKey).Scan(&username)
	if err != nil {
		if err == sql.ErrNoRows {
			glog.V(2).Infof("credential postgres: GetUserByAccessKey accessKey=%s not found", accessKey)
			return nil, credential.ErrAccessKeyNotFound
		}
		glog.Errorf("credential postgres: GetUserByAccessKey query failed accessKey=%s: %v", accessKey, err)
		return nil, fmt.Errorf("failed to query access key: %w", err)
	}

	glog.V(2).Infof("credential postgres: GetUserByAccessKey accessKey=%s resolved to user=%s", accessKey, username)
	return store.GetUser(ctx, username)
}

func (store *PostgresStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	_, err = store.db.ExecContext(ctx,
		"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
		username, cred.AccessKey, cred.SecretKey)
	if err != nil {
		glog.Errorf("credential postgres: CreateAccessKey failed user=%s accessKey=%s: %v", username, cred.AccessKey, err)
		return fmt.Errorf("failed to insert credential: %w", err)
	}

	glog.V(0).Infof("credential postgres: CreateAccessKey user=%s accessKey=%s", username, cred.AccessKey)
	return nil
}

func (store *PostgresStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx,
		"DELETE FROM credentials WHERE username = $1 AND access_key = $2",
		username, accessKey)
	if err != nil {
		glog.Errorf("credential postgres: DeleteAccessKey failed user=%s accessKey=%s: %v", username, accessKey, err)
		return fmt.Errorf("failed to delete access key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		var count int
		err = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check user existence: %w", err)
		}
		if count == 0 {
			return credential.ErrUserNotFound
		}
		return credential.ErrAccessKeyNotFound
	}

	glog.V(0).Infof("credential postgres: DeleteAccessKey user=%s accessKey=%s", username, accessKey)
	return nil
}

func (store *PostgresStore) AttachUserPolicy(ctx context.Context, username string, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	policy, err := store.GetPolicy(ctx, policyName)
	if err != nil {
		return err
	}
	if policy == nil {
		return credential.ErrPolicyNotFound
	}

	for _, p := range identity.PolicyNames {
		if p == policyName {
			return credential.ErrPolicyAlreadyAttached
		}
	}

	identity.PolicyNames = append(identity.PolicyNames, policyName)
	if err := store.UpdateUser(ctx, username, identity); err != nil {
		return err
	}

	glog.V(0).Infof("credential postgres: AttachUserPolicy user=%s policy=%s", username, policyName)
	return nil
}

func (store *PostgresStore) DetachUserPolicy(ctx context.Context, username string, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	found := false
	var newPolicyNames []string
	for _, p := range identity.PolicyNames {
		if p == policyName {
			found = true
		} else {
			newPolicyNames = append(newPolicyNames, p)
		}
	}

	if !found {
		return credential.ErrPolicyNotAttached
	}

	identity.PolicyNames = newPolicyNames
	if err := store.UpdateUser(ctx, username, identity); err != nil {
		return err
	}

	glog.V(0).Infof("credential postgres: DetachUserPolicy user=%s policy=%s", username, policyName)
	return nil
}

func (store *PostgresStore) ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return nil, err
	}

	return identity.PolicyNames, nil
}
