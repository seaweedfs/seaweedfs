package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	_ "github.com/lib/pq"
)

func init() {
	credential.Stores = append(credential.Stores, &PostgresStore{})
}

// PostgresStore implements CredentialStore using PostgreSQL
type PostgresStore struct {
	db         *sql.DB
	configured bool
}

func (store *PostgresStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypePostgres
}

func (store *PostgresStore) Initialize(configuration util.Configuration, prefix string) error {
	if store.configured {
		return nil
	}

	hostname := configuration.GetString(prefix + "hostname")
	port := configuration.GetInt(prefix + "port")
	username := configuration.GetString(prefix + "username")
	password := configuration.GetString(prefix + "password")
	database := configuration.GetString(prefix + "database")
	schema := configuration.GetString(prefix + "schema")
	sslmode := configuration.GetString(prefix + "sslmode")

	// Set defaults
	if hostname == "" {
		hostname = "localhost"
	}
	if port == 0 {
		port = 5432
	}
	if schema == "" {
		schema = "public"
	}
	if sslmode == "" {
		sslmode = "disable"
	}

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s search_path=%s",
		hostname, port, username, password, database, sslmode, schema)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %v", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	store.db = db

	// Create tables if they don't exist
	if err := store.createTables(); err != nil {
		db.Close()
		return fmt.Errorf("failed to create tables: %v", err)
	}

	store.configured = true
	return nil
}

func (store *PostgresStore) createTables() error {
	// Create users table
	usersTable := `
		CREATE TABLE IF NOT EXISTS users (
			username VARCHAR(255) PRIMARY KEY,
			email VARCHAR(255),
			account_data JSONB,
			actions JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	`

	// Create credentials table
	credentialsTable := `
		CREATE TABLE IF NOT EXISTS credentials (
			id SERIAL PRIMARY KEY,
			username VARCHAR(255) REFERENCES users(username) ON DELETE CASCADE,
			access_key VARCHAR(255) UNIQUE NOT NULL,
			secret_key VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

func (store *PostgresStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
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
		var username, email string
		var accountDataJSON, actionsJSON []byte

		if err := rows.Scan(&username, &email, &accountDataJSON, &actionsJSON); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %v", err)
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

		// Insert user
		_, err := tx.ExecContext(ctx,
			"INSERT INTO users (username, email, account_data, actions) VALUES ($1, $2, $3, $4)",
			identity.Name, "", accountDataJSON, actionsJSON)
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
	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %v", err)
		}
	}

	// Marshal actions
	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %v", err)
		}
	}

	// Insert user
	_, err = tx.ExecContext(ctx,
		"INSERT INTO users (username, email, account_data, actions) VALUES ($1, $2, $3, $4)",
		identity.Name, "", accountDataJSON, actionsJSON)
	if err != nil {
		return fmt.Errorf("failed to insert user: %v", err)
	}

	// Insert credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			identity.Name, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %v", err)
		}
	}

	return tx.Commit()
}

func (store *PostgresStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	if !store.configured {
		return nil, fmt.Errorf("store not configured")
	}

	var email string
	var accountDataJSON, actionsJSON []byte

	err := store.db.QueryRowContext(ctx,
		"SELECT email, account_data, actions FROM users WHERE username = $1",
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
	if len(accountDataJSON) > 0 {
		if err := json.Unmarshal(accountDataJSON, &identity.Account); err != nil {
			return nil, fmt.Errorf("failed to unmarshal account data: %v", err)
		}
	}

	// Parse actions
	if len(actionsJSON) > 0 {
		if err := json.Unmarshal(actionsJSON, &identity.Actions); err != nil {
			return nil, fmt.Errorf("failed to unmarshal actions: %v", err)
		}
	}

	// Query credentials
	rows, err := store.db.QueryContext(ctx, "SELECT access_key, secret_key FROM credentials WHERE username = $1", username)
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

func (store *PostgresStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
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
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %v", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Marshal account data
	var accountDataJSON []byte
	if identity.Account != nil {
		accountDataJSON, err = json.Marshal(identity.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account data: %v", err)
		}
	}

	// Marshal actions
	var actionsJSON []byte
	if identity.Actions != nil {
		actionsJSON, err = json.Marshal(identity.Actions)
		if err != nil {
			return fmt.Errorf("failed to marshal actions: %v", err)
		}
	}

	// Update user
	_, err = tx.ExecContext(ctx,
		"UPDATE users SET email = $2, account_data = $3, actions = $4, updated_at = CURRENT_TIMESTAMP WHERE username = $1",
		username, "", accountDataJSON, actionsJSON)
	if err != nil {
		return fmt.Errorf("failed to update user: %v", err)
	}

	// Delete existing credentials
	_, err = tx.ExecContext(ctx, "DELETE FROM credentials WHERE username = $1", username)
	if err != nil {
		return fmt.Errorf("failed to delete existing credentials: %v", err)
	}

	// Insert new credentials
	for _, cred := range identity.Credentials {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
			username, cred.AccessKey, cred.SecretKey)
		if err != nil {
			return fmt.Errorf("failed to insert credential: %v", err)
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

func (store *PostgresStore) ListUsers(ctx context.Context) ([]string, error) {
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
		return nil, fmt.Errorf("failed to query access key: %v", err)
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
		return fmt.Errorf("failed to check user existence: %v", err)
	}
	if count == 0 {
		return credential.ErrUserNotFound
	}

	// Insert credential
	_, err = store.db.ExecContext(ctx,
		"INSERT INTO credentials (username, access_key, secret_key) VALUES ($1, $2, $3)",
		username, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return fmt.Errorf("failed to insert credential: %v", err)
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
		return fmt.Errorf("failed to delete access key: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		// Check if user exists
		var count int
		err = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE username = $1", username).Scan(&count)
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

func (store *PostgresStore) Shutdown() {
	if store.db != nil {
		store.db.Close()
		store.db = nil
	}
	store.configured = false
}
