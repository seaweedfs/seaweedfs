package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
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

	// Create policies table
	policiesTable := `
		CREATE TABLE IF NOT EXISTS policies (
			name VARCHAR(255) PRIMARY KEY,
			document JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_policies_name ON policies(name);
	`

	// Execute table creation
	if _, err := store.db.Exec(usersTable); err != nil {
		return fmt.Errorf("failed to create users table: %v", err)
	}

	if _, err := store.db.Exec(credentialsTable); err != nil {
		return fmt.Errorf("failed to create credentials table: %v", err)
	}

	if _, err := store.db.Exec(policiesTable); err != nil {
		return fmt.Errorf("failed to create policies table: %v", err)
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
