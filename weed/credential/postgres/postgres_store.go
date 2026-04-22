package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	sslcert := configuration.GetString(prefix + "sslcert")
	sslkey := configuration.GetString(prefix + "sslkey")
	sslrootcert := configuration.GetString(prefix + "sslrootcert")
	pgbouncerCompatible := configuration.GetBool(prefix + "pgbouncer_compatible")

	// Set defaults
	if hostname == "" {
		hostname = "localhost"
	}
	if port == 0 {
		port = 5432
	}
	if sslmode == "" {
		sslmode = "disable"
	}

	glog.V(0).Infof("credential postgres: initializing store host=%s port=%d user=%s db=%s sslmode=%s pgbouncer=%v",
		hostname, port, username, database, sslmode, pgbouncerCompatible)

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		hostname, port, username, password, database, sslmode)
	if schema != "" {
		connStr += fmt.Sprintf(" search_path=%s", schema)
	}
	if sslcert != "" {
		connStr += fmt.Sprintf(" sslcert=%s", sslcert)
	}
	if sslkey != "" {
		connStr += fmt.Sprintf(" sslkey=%s", sslkey)
	}
	if sslrootcert != "" {
		connStr += fmt.Sprintf(" sslrootcert=%s", sslrootcert)
	}
	if pgbouncerCompatible {
		connStr += " default_query_exec_mode=simple_protocol"
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		glog.Errorf("credential postgres: failed to open database: %v", err)
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		glog.Errorf("credential postgres: failed to ping database: %v", err)
		return fmt.Errorf("failed to ping database: %w", err)
	}

	glog.V(0).Infof("credential postgres: connection established")

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	store.db = db

	if err := store.createTables(); err != nil {
		db.Close()
		glog.Errorf("credential postgres: failed to create tables: %v", err)
		return fmt.Errorf("failed to create tables: %w", err)
	}

	glog.V(0).Infof("credential postgres: tables verified, store ready")

	store.configured = true
	return nil
}

func (store *PostgresStore) createTables() error {
	usersTable := `
		CREATE TABLE IF NOT EXISTS users (
			username VARCHAR(255) PRIMARY KEY,
			email VARCHAR(255),
			account_data JSONB,
			actions JSONB,
			policy_names JSONB DEFAULT '[]',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	`

	addPolicyNamesColumn := `
		ALTER TABLE users ADD COLUMN IF NOT EXISTS policy_names JSONB DEFAULT '[]';
	`

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

	policiesTable := `
		CREATE TABLE IF NOT EXISTS policies (
			name VARCHAR(255) PRIMARY KEY,
			document JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_policies_name ON policies(name);
	`

	serviceAccountsTable := `
		CREATE TABLE IF NOT EXISTS service_accounts (
			id VARCHAR(255) PRIMARY KEY,
			access_key VARCHAR(255) UNIQUE,
			content JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`

	inlinePoliciesTable := `
		CREATE TABLE IF NOT EXISTS user_inline_policies (
			username VARCHAR(255) REFERENCES users(username) ON DELETE CASCADE,
			policy_name VARCHAR(255) NOT NULL,
			document JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (username, policy_name)
		);
		CREATE INDEX IF NOT EXISTS idx_user_inline_policies_username ON user_inline_policies(username);
	`

	groupsTable := `
		CREATE TABLE IF NOT EXISTS groups (
			name VARCHAR(255) PRIMARY KEY,
			members JSONB DEFAULT '[]',
			policy_names JSONB DEFAULT '[]',
			disabled BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`

	if _, err := store.db.Exec(usersTable); err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	if _, err := store.db.Exec(addPolicyNamesColumn); err != nil {
		return fmt.Errorf("failed to add policy_names column: %w", err)
	}

	if _, err := store.db.Exec(credentialsTable); err != nil {
		return fmt.Errorf("failed to create credentials table: %w", err)
	}

	if _, err := store.db.Exec(policiesTable); err != nil {
		return fmt.Errorf("failed to create policies table: %w", err)
	}

	if _, err := store.db.Exec(serviceAccountsTable); err != nil {
		return fmt.Errorf("failed to create service_accounts table: %w", err)
	}

	if _, err := store.db.Exec(inlinePoliciesTable); err != nil {
		return fmt.Errorf("failed to create user_inline_policies table: %w", err)
	}

	if _, err := store.db.Exec(groupsTable); err != nil {
		return fmt.Errorf("failed to create groups table: %w", err)
	}

	groupsDisabledIndex := `CREATE INDEX IF NOT EXISTS idx_groups_disabled ON groups (disabled);`
	if _, err := store.db.Exec(groupsDisabledIndex); err != nil {
		return fmt.Errorf("failed to create groups disabled index: %w", err)
	}

	groupsMembersIndex := `CREATE INDEX IF NOT EXISTS idx_groups_members_gin ON groups USING GIN (members);`
	if _, err := store.db.Exec(groupsMembersIndex); err != nil {
		return fmt.Errorf("failed to create groups members index: %w", err)
	}

	return nil
}

func (store *PostgresStore) Shutdown() {
	if store.db != nil {
		glog.V(0).Infof("credential postgres: shutting down")
		store.db.Close()
		store.db = nil
	}
	store.configured = false
}
