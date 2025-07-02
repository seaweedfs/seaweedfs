package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	_ "modernc.org/sqlite"
)

func init() {
	credential.Stores = append(credential.Stores, &SqliteStore{})
}

// SqliteStore implements CredentialStore using SQLite
type SqliteStore struct {
	db         *sql.DB
	configured bool
}

func (store *SqliteStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeSQLite
}

func (store *SqliteStore) Initialize(configuration util.Configuration, prefix string) error {
	if store.configured {
		return nil
	}

	dbFile := configuration.GetString(prefix + "dbFile")
	if dbFile == "" {
		dbFile = "seaweedfs_credentials.db"
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(dbFile)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %v", err)
	}

	store.db = db

	// Create tables if they don't exist
	if err := store.createTables(); err != nil {
		db.Close()
		return fmt.Errorf("failed to create tables: %v", err)
	}

	store.configured = true
	return nil
}

func (store *SqliteStore) createTables() error {
	// Create users table
	usersTable := `
		CREATE TABLE IF NOT EXISTS users (
			username TEXT PRIMARY KEY,
			email TEXT,
			account_data TEXT,
			actions TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	`

	// Create credentials table
	credentialsTable := `
		CREATE TABLE IF NOT EXISTS credentials (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT REFERENCES users(username) ON DELETE CASCADE,
			access_key TEXT UNIQUE NOT NULL,
			secret_key TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_credentials_username ON credentials(username);
		CREATE INDEX IF NOT EXISTS idx_credentials_access_key ON credentials(access_key);
	`

	// Execute table creation
	if _, err := store.db.Exec(usersTable); err != nil {
		return fmt.Errorf("failed to create users table: %v", err)
	}

	if _, err := store.db.Exec(credentialsTable); err != nil {
		return fmt.Errorf("failed to create credentials table: %v", err)
	}

	return nil
}

func (store *SqliteStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	config := &iam_pb.S3ApiConfiguration{}

	// Query all users
	rows, err := store.db.QueryContext(ctx, "SELECT username, email, account_data, actions FROM users")
	if err != nil {
		return nil, fmt.Errorf("failed to query users: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, email, accountDataJSON, actionsJSON string

		if err := rows.Scan(&username, &email, &accountDataJSON, &actionsJSON); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %v", err)
		}

		identity := &iam_pb.Identity{
			Name: username,
		}

		// Parse account data
		if accountDataJSON != "" {
			if err := json.Unmarshal([]byte(accountDataJSON), &identity.Account); err != nil {
				return nil, fmt.Errorf("failed to unmarshal account data for user %s: %v", username, err)
			}
		}

		// Parse actions
		if actionsJSON != "" {
			if err := json.Unmarshal([]byte(actionsJSON), &identity.Actions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal actions for user %s: %v", username, err)
			}
		}

		// Query credentials for this user
		credRows, err := store.db.QueryContext(ctx, "SELECT access_key, secret_key FROM credentials WHERE username = ?", username)
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

func (store *SqliteStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Clear existing data
	if _, err := tx.ExecContext(ctx, "DELETE FROM credentials"); err != nil {
		return fmt.Errorf("failed to clear credentials: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM users"); err != nil {
		return fmt.Errorf("failed to clear users: %v", err)
	}

	// Insert all identities
	for _, identity := range config.Identities {
		// Marshal account data
		var accountDataJSON string
		if identity.Account != nil {
			data, err := json.Marshal(identity.Account)
			if err != nil {
				return fmt.Errorf("failed to marshal account data for user %s: %v", identity.Name, err)
			}
			accountDataJSON = string(data)
		}

		// Marshal actions
		var actionsJSON string
		if identity.Actions != nil {
			data, err := json.Marshal(identity.Actions)
			if err != nil {
				return fmt.Errorf("failed to marshal actions for user %s: %v", identity.Name, err)
			}
			actionsJSON = string(data)
		}

		// Insert user
		_, err := tx.ExecContext(ctx,
			"INSERT INTO users (username, email, account_data, actions) VALUES (?, ?, ?, ?)",
			identity.Name, "", accountDataJSON, actionsJSON)
		if err != nil {
			return fmt.Errorf("failed to insert user %s: %v", identity.Name, err)
		}

		// Insert credentials
		for _, cred := range identity.Credentials {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO credentials (username, access_key, secret_key) VALUES (?, ?, ?)",
				identity.Name, cred.AccessKey, cred.SecretKey)
			if err != nil {
				return fmt.Errorf("failed to insert credential for user %s: %v", identity.Name, err)
			}
		}
	}

	return tx.Commit()
}

func (store *SqliteStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Check if user already exists
	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = ?", identity.Name).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %v", err)
	}
	if count > 0 {
		return credential.ErrUserAlreadyExists
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Marshal account data
	var accountDataJSON string
	if identity.Account != nil {
		data, err := json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %v", err)
		}
		accountDataJSON = string(data)
	}

	// Marshal actions
	var actionsJSON string
	if identity.Actions != nil {
		data, err := json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %v", err)
		}
		actionsJSON = string(data)
	}

	// Insert user
	_, err = tx.ExecContext(ctx,
		"INSERT INTO users (username, email, account_data, actions) VALUES (?, ?, ?, ?)",
		identity.Name, "", accountDataJSON, actionsJSON)
	if err != nil {
		return fmt.Errorf("failed to insert user: %v", err)
	}

	// Insert credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES (?, ?, ?)",
			identity.Name, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %v", err)
		}
	}

	return tx.Commit()
}

func (store *SqliteStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var email, accountDataJSON, actionsJSON string

	err := store.db.QueryRowContext(ctx,
		"SELECT email, account_data, actions FROM users WHERE username = ?",
		username).Scan(&email, &accountDataJSON, &actionsJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, credential.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to query user: %v", err)
	}

	identity := &iam_pb.Identity{
		Name: username,
	}

	// Parse account data
	if accountDataJSON != "" {
		if err := json.Unmarshal([]byte(accountDataJSON), &identity.Account); err != nil {
			return nil, fmt.Errorf("failed to unmarshal account data: %v", err)
		}
	}

	// Parse actions
	if actionsJSON != "" {
		if err := json.Unmarshal([]byte(actionsJSON), &identity.Actions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal actions: %v", err)
		}
	}

	// Query credentials
	rows, err := store.db.QueryContext(ctx, "SELECT access_key, secret_key FROM credentials WHERE username = ?", username)
	if err != nil {
		return nil, fmt.Errorf("failed to query credentials: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var accessKey, secretKey string
		if err := rows.Scan(&accessKey, &secretKey); err != nil {
			return nil, fmt.Errorf("failed to scan credential: %v", err)
		}

		identity.Credentials = append(identity.Credentials, &iam_pb.Credential{
			AccessKey: accessKey,
			SecretKey: secretKey,
		})
	}

	return identity, nil
}

func (store *SqliteStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Start transaction
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Check if user exists
	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = ?", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %v", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Marshal account data
	var accountDataJSON string
	if identity.Account != nil {
		data, err := json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %v", err)
		}
		accountDataJSON = string(data)
	}

	// Marshal actions
	var actionsJSON string
	if identity.Actions != nil {
		data, err := json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %v", err)
		}
		actionsJSON = string(data)
	}

	// Update user
	_, err = tx.ExecContext(ctx,
		"UPDATE users SET email = ?, account_data = ?, actions = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
		"", accountDataJSON, actionsJSON, username)
	if err != nil {
		return fmt.Errorf("failed to update user: %v", err)
	}

	// Delete existing credentials
	_, err = tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = ?", username)
	if err != nil {
		return fmt.Errorf("failed to delete existing credentials: %v", err)
	}

	// Insert new credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES (?, ?, ?)",
			username, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %v", err)
		}
	}

	return tx.Commit()
}

func (store *SqliteStore) DeleteUser(ctx context.Context, username string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx, "DELETE FROM users WHERE username = ?", username)
	if err != nil {
		return fmt.Errorf("failed to delete user: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		return credential.ErrUserNotFound
	}

	return nil
}

func (store *SqliteStore) ListUsers(ctx context.Context) ([]string, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	rows, err := store.db.QueryContext(ctx, "SELECT username FROM users ORDER BY username")
	if err != nil {
		return nil, fmt.Errorf("failed to query users: %v", err)
	}
	defer rows.Close()

	var usernames []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, fmt.Errorf("failed to scan username: %v", err)
		}
		usernames = append(usernames, username)
	}

	return usernames, nil
}

func (store *SqliteStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var username string
	err := store.db.QueryRowContext(ctx, "SELECT username FROM credentials WHERE access_key = ?", accessKey).Scan(&username)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, credential.ErrAccessKeyNotFound
		}
		return nil, fmt.Errorf("failed to query access key: %v", err)
	}

	return store.GetUser(ctx, username)
}

func (store *SqliteStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	// Check if user exists
	var count int
	err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = ?", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %v", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Insert credential
	_, err = store.db.ExecContext(ctx,
		"INSERT INTO credentials (username, access_key, secret_key) VALUES (?, ?, ?)",
		username, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return fmt.Errorf("failed to insert credential: %v", err)
	}

	return nil
}

func (store *SqliteStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	if !store.configured {
		return fmt.Errorf("store not configured")
	}

	result, err := store.db.ExecContext(ctx,
		"DELETE FROM credentials WHERE username = ? AND access_key = ?",
		username, accessKey)
	if err != nil {
		return fmt.Errorf("failed to delete access key: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		// Check if user exists
		var count int
		err = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = ?", username).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check user existence: %v", err)
		}
		if count == 0 {
			return credential.ErrUserNotFound
		}
		return credential.ErrAccessKeyNotFound
	}

	return nil
}

func (store *SqliteStore) Shutdown() {
	if store.db != nil {
		store.db.Close()
		store.db = nil
	}
	store.configured = false
}
