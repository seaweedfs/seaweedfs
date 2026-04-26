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
		credErr := credRows.Err()
		credRows.Close()
		if credErr != nil {
			return nil, fmt.Errorf("failed iterating credential rows for user %s: %w", username, credErr)
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
	}

	// Upsert each user's row.
	for _, identity := range config.Identities {
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
			identity.Name, "", jsonbParam(accountDataJSON), jsonbParam(actionsJSON), jsonbParam(policyNamesJSON))
		if err != nil {
			return fmt.Errorf("failed to upsert user %s: %v", identity.Name, err)
		}
	}

	// Prune users no longer in config BEFORE replacing credentials. The
	// IAM rename path (s3api UpdateUser) renames an identity in place and
	// keeps its access keys: the renamed user shows up in the incoming
	// config, the old name shows up in the pruned set. If we inserted the
	// new credentials first, the renamed user's access keys would still be
	// owned by the old row in this transaction and the INSERT would
	// violate the global UNIQUE constraint on credentials.access_key.
	// CASCADE on the old row releases those keys before the insert.
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
	scanErr := rows.Err()
	rows.Close()
	if scanErr != nil {
		return fmt.Errorf("failed iterating user rows for pruning: %w", scanErr)
	}

	if len(toDelete) > 0 {
		var inlineCount int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM user_inline_policies WHERE username = ANY($1)",
			toDelete).Scan(&inlineCount); err != nil {
			glog.Warningf("credential postgres: SaveConfiguration failed to count inline policies for prune candidates: %v", err)
		} else if inlineCount > 0 {
			glog.Warningf("credential postgres: SaveConfiguration pruning %d users will CASCADE-remove %d inline policies; if this was a rename, re-create them under the new name", len(toDelete), inlineCount)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM users WHERE username = ANY($1)", toDelete); err != nil {
			return fmt.Errorf("failed to prune users: %w", err)
		}
	}

	// Two-pass credential replace: first clear every user we are about to
	// rewrite in a single round-trip, then insert. Doing the per-user delete
	// + insert in a single pass would violate the global UNIQUE constraint
	// on credentials.access_key when an access key gets reassigned from one
	// user to another within the same SaveConfiguration call.
	usernames := make([]string, 0, len(configUsernames))
	for name := range configUsernames {
		usernames = append(usernames, name)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = ANY($1)", usernames); err != nil {
		return fmt.Errorf("failed to clear credentials for incoming users: %w", err)
	}
	for _, identity := range config.Identities {
		for _, cred := range identity.Credentials {
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
				identity.Name, cred.AccessKey, cred.SecretKey); err != nil {
				return fmt.Errorf("failed to insert credential for user %s: %v", identity.Name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		glog.Errorf("credential postgres: SaveConfiguration commit failed: %v", err)
		return fmt.Errorf("failed to commit SaveConfiguration: %w", err)
	}

	if len(toDelete) > 0 {
		glog.Warningf("credential postgres: SaveConfiguration saved %d identities, pruned %d", len(config.Identities), len(toDelete))
	} else {
		glog.V(0).Infof("credential postgres: SaveConfiguration saved %d identities", len(config.Identities))
	}
	return nil
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
		identity.Name, "", jsonbParam(accountDataJSON), jsonbParam(actionsJSON), jsonbParam(policyNamesJSON))
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
		return fmt.Errorf("failed to commit create user %s: %w", identity.Name, err)
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
		username, "", jsonbParam(accountDataJSON), jsonbParam(actionsJSON), jsonbParam(policyNamesJSON))
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
		return fmt.Errorf("failed to commit update user %s: %w", username, err)
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

// RenameUser atomically renames oldName to newName, re-pointing every
// FK-backed dependent row in a single transaction. The schema's foreign
// keys (credentials.username, user_inline_policies.username) reference
// users.username with ON DELETE CASCADE and the default ON UPDATE
// NO ACTION, which means a plain UPDATE users SET username = newName
// would fail because the children still reference the old name. Instead:
//  1. Insert the new users row by copying every non-key column from old.
//  2. Re-point the dependent tables (credentials, user_inline_policies)
//     to the new username — both rows now exist so the FK is satisfied.
//  3. Delete the old users row, which has no remaining dependents.
//
// Implements credential.UserRenamer.
func (store *PostgresStore) RenameUser(ctx context.Context, oldName, newName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}
	if oldName == newName {
		return nil
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Refuse if oldName is missing or newName already exists, so the
	// caller surfaces a recognisable error instead of a constraint
	// violation midway through.
	var oldExists, newExists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)", oldName).Scan(&oldExists); err != nil {
		return fmt.Errorf("failed to check old user %s: %w", oldName, err)
	}
	if !oldExists {
		return credential.ErrUserNotFound
	}
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)", newName).Scan(&newExists); err != nil {
		return fmt.Errorf("failed to check new user %s: %w", newName, err)
	}
	if newExists {
		return credential.ErrUserAlreadyExists
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO users (username, email, account_data, actions, policy_names, created_at, updated_at)
		 SELECT $1, email, account_data, actions, policy_names, created_at, CURRENT_TIMESTAMP
		 FROM users WHERE username = $2`,
		newName, oldName); err != nil {
		return fmt.Errorf("failed to insert renamed user %s: %w", newName, err)
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE credentials SET username = $1 WHERE username = $2", newName, oldName); err != nil {
		return fmt.Errorf("failed to re-point credentials to %s: %w", newName, err)
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE user_inline_policies SET username = $1 WHERE username = $2", newName, oldName); err != nil {
		return fmt.Errorf("failed to re-point inline policies to %s: %w", newName, err)
	}

	if _, err := tx.ExecContext(ctx,
		"DELETE FROM users WHERE username = $1", oldName); err != nil {
		return fmt.Errorf("failed to drop old user %s: %w", oldName, err)
	}

	if err := tx.Commit(); err != nil {
		glog.Errorf("credential postgres: RenameUser commit failed %s -> %s: %v", oldName, newName, err)
		return fmt.Errorf("failed to commit rename %s -> %s: %w", oldName, newName, err)
	}

	glog.V(0).Infof("credential postgres: RenameUser %s -> %s", oldName, newName)
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

	glog.V(1).Infof("credential postgres: AttachUserPolicy user=%s policy=%s", username, policyName)
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

	glog.V(1).Infof("credential postgres: DetachUserPolicy user=%s policy=%s", username, policyName)
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
