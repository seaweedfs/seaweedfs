package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func (store *PostgresStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	config := &iam_pb.S3ApiConfiguration{}

	// Query all users
	rows, err := store.db.QueryContext(ctx, "SELECT username, email, account_data, actions, policy_names FROM users")
	if err != nil {
		return nil, fmt.Errorf("failed to query users: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, email string
		var accountDataJSON, actionsJSON, policyNamesJSON []byte

		if err := rows.Scan(&username, &email, &accountDataJSON, &actionsJSON, &policyNamesJSON); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}

		identity := &iam_pb.Identity{
			Name: username,
		}

		// Parse account data
		if len(accountDataJSON) > 0 {
			if err := json.Unmarshal(accountDataJSON, &identity.Account); err != nil {
				return nil, fmt.Errorf("failed to unmarshal account data for user %s: %v", username, err)
			}
		}

		// Parse actions
		if len(actionsJSON) > 0 {
			if err := json.Unmarshal(actionsJSON, &identity.Actions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal actions for user %s: %v", username, err)
			}
		}

		// Parse policy names
		if len(policyNamesJSON) > 0 {
			if err := json.Unmarshal(policyNamesJSON, &identity.PolicyNames); err != nil {
				return nil, fmt.Errorf("failed to unmarshal policy names for user %s: %v", username, err)
			}
		}

		// Query credentials for this user
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

		config.Identities = append(config.Identities, identity)
	}

	return config, nil
}

func (store *PostgresStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing data
	if _, err := tx.ExecContext(ctx, "DELETE FROM credentials"); err != nil {
		return fmt.Errorf("failed to clear credentials: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM users"); err != nil {
		return fmt.Errorf("failed to clear users: %w", err)
	}

	// Insert all identities
	for _, identity := range config.Identities {
		// Marshal account data
		var accountDataJSON []byte
		if identity.Account != nil {
			accountDataJSON, err = json.Marshal(identity.Account)
			if err != nil {
				return fmt.Errorf("failed to marshal account data for user %s: %v", identity.Name, err)
			}
		}

		// Marshal actions
		var actionsJSON []byte
		if identity.Actions != nil {
			actionsJSON, err = json.Marshal(identity.Actions)
			if err != nil {
				return fmt.Errorf("failed to marshal actions for user %s: %v", identity.Name, err)
			}
		}

		// Marshal policy names
		var policyNamesJSON []byte
		if identity.PolicyNames != nil {
			policyNamesJSON, err = json.Marshal(identity.PolicyNames)
			if err != nil {
				return fmt.Errorf("failed to marshal policy names for user %s: %v", identity.Name, err)
			}
		}

		// Insert user
		_, err := tx.ExecContext(ctx,
			"INSERT INTO users (username, email, account_data, actions, policy_names) VALUES ($1, $2, $3, $4, $5)",
			identity.Name, "", accountDataJSON, actionsJSON, policyNamesJSON)
		if err != nil {
			return fmt.Errorf("failed to insert user %s: %v", identity.Name, err)
		}

		// Insert credentials
		for _, cred := range identity.Credentials {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
				identity.Name, cred.AccessKey, cred.SecretKey)
			if err != nil {
				return fmt.Errorf("failed to insert credential for user %s: %v", identity.Name, err)
			}
		}
	}

	return tx.Commit()
}

func (store *PostgresStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Check if user already exists
	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", identity.Name).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count > 0 {
		return credential.ErrUserAlreadyExists
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Marshal account data
	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %w", err)
		}
	}

	// Marshal actions
	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %w", err)
		}
	}

	// Marshal policy names
	var policyNamesJSON []byte
	if identity.PolicyNames != nil {
		policyNamesJSON, err = json.Marshal(identity.PolicyNames)
		if err != nil {
			return fmt.Errorf("failed to marshal policy names: %w", err)
		}
	}

	// Insert user
	_, err = tx.ExecContext(ctx,
		"INSERT INTO users (username, email, account_data, actions, policy_names) VALUES ($1, $2, $3, $4, $5)",
		identity.Name, "", accountDataJSON, actionsJSON, policyNamesJSON)
	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	// Insert credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			identity.Name, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %w", err)
		}
	}

	return tx.Commit()
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
			return nil, credential.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	identity := &iam_pb.Identity{
		Name: username,
	}

	// Parse account data
	if len(accountDataJSON) > 0 {
		if err := json.Unmarshal(accountDataJSON, &identity.Account); err != nil {
			return nil, fmt.Errorf("failed to unmarshal account data: %w", err)
		}
	}

	// Parse actions
	if len(actionsJSON) > 0 {
		if err := json.Unmarshal(actionsJSON, &identity.Actions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal actions: %w", err)
		}
	}

	// Parse policy names
	if len(policyNamesJSON) > 0 {
		if err := json.Unmarshal(policyNamesJSON, &identity.PolicyNames); err != nil {
			return nil, fmt.Errorf("failed to unmarshal policy names: %w", err)
		}
	}

	// Query credentials
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

	return identity, nil
}

func (store *PostgresStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if user exists
	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Marshal account data
	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %w", err)
		}
	}

	// Marshal actions
	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %w", err)
		}
	}

	// Marshal policy names
	var policyNamesJSON []byte
	if identity.PolicyNames != nil {
		policyNamesJSON, err = json.Marshal(identity.PolicyNames)
		if err != nil {
			return fmt.Errorf("failed to marshal policy names: %w", err)
		}
	}

	// Update user
	_, err = tx.ExecContext(ctx,
		"UPDATE users SET email = $2, account_data = $3, actions = $4, policy_names = $5, updated_at = CURRENT_TIMESTAMP WHERE username = $1",
		username, "", accountDataJSON, actionsJSON, policyNamesJSON)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	// Delete existing credentials
	_, err = tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = $1", username)
	if err != nil {
		return fmt.Errorf("failed to delete existing credentials: %w", err)
	}

	// Insert new credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			username, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %w", err)
		}
	}

	return tx.Commit()
}

func (store *PostgresStore) DeleteUser(ctx context.Context, username string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx, "DELETE FROM users WHERE username = $1", username)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return credential.ErrUserNotFound
	}

	return nil
}

func (store *PostgresStore) ListUsers(ctx context.Context) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx, "SELECT username FROM users ORDER BY username")
	if err != nil {
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
			return nil, credential.ErrAccessKeyNotFound
		}
		return nil, fmt.Errorf("failed to query access key: %w", err)
	}

	return store.GetUser(ctx, username)
}

func (store *PostgresStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Check if user exists
	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Insert credential
	_, err = store.db.ExecContext(ctx,
		"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
		username, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return fmt.Errorf("failed to insert credential: %w", err)
	}

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
		return fmt.Errorf("failed to delete access key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if user exists
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

	return nil
}

// AttachUserPolicy attaches a managed policy to a user by policy name
func (store *PostgresStore) AttachUserPolicy(ctx context.Context, username string, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Get user
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	// Verify policy exists
	policy, err := store.GetPolicy(ctx, policyName)
	if err != nil {
		return err
	}
	if policy == nil {
		return credential.ErrPolicyNotFound
	}

	// Check if already attached
	for _, p := range identity.PolicyNames {
		if p == policyName {
			return credential.ErrPolicyAlreadyAttached
		}
	}

	// Append policy name and update
	identity.PolicyNames = append(identity.PolicyNames, policyName)
	return store.UpdateUser(ctx, username, identity)
}

// DetachUserPolicy detaches a managed policy from a user
func (store *PostgresStore) DetachUserPolicy(ctx context.Context, username string, policyName string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Get user
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		return err
	}

	// Find and remove policy
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
	return store.UpdateUser(ctx, username, identity)
}

// ListAttachedUserPolicies returns the list of policy names attached to a user
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
