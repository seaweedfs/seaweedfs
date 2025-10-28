package offset

import (
	"database/sql"
	"fmt"
	"time"
)

// MigrationVersion represents a database migration version
type MigrationVersion struct {
	Version     int
	Description string
	SQL         string
}

// GetMigrations returns all available migrations for offset storage
func GetMigrations() []MigrationVersion {
	return []MigrationVersion{
		{
			Version:     1,
			Description: "Create initial offset storage tables",
			SQL: `
				-- Partition offset checkpoints table
				-- TODO: Add _index as computed column when supported by database
				CREATE TABLE IF NOT EXISTS partition_offset_checkpoints (
					partition_key TEXT PRIMARY KEY,
					ring_size INTEGER NOT NULL,
					range_start INTEGER NOT NULL,
					range_stop INTEGER NOT NULL,
					unix_time_ns INTEGER NOT NULL,
					checkpoint_offset INTEGER NOT NULL,
					updated_at INTEGER NOT NULL
				);
				
				-- Offset mappings table for detailed tracking
				-- TODO: Add _index as computed column when supported by database
				CREATE TABLE IF NOT EXISTS offset_mappings (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					partition_key TEXT NOT NULL,
					kafka_offset INTEGER NOT NULL,
					smq_timestamp INTEGER NOT NULL,
					message_size INTEGER NOT NULL,
					created_at INTEGER NOT NULL,
					UNIQUE(partition_key, kafka_offset)
				);
				
				-- Schema migrations tracking table
				CREATE TABLE IF NOT EXISTS schema_migrations (
					version INTEGER PRIMARY KEY,
					description TEXT NOT NULL,
					applied_at INTEGER NOT NULL
				);
			`,
		},
		{
			Version:     2,
			Description: "Add indexes for performance optimization",
			SQL: `
				-- Indexes for performance
				CREATE INDEX IF NOT EXISTS idx_partition_offset_checkpoints_partition 
				ON partition_offset_checkpoints(partition_key);
				
				CREATE INDEX IF NOT EXISTS idx_offset_mappings_partition_offset 
				ON offset_mappings(partition_key, kafka_offset);
				
				CREATE INDEX IF NOT EXISTS idx_offset_mappings_timestamp 
				ON offset_mappings(partition_key, smq_timestamp);
				
				CREATE INDEX IF NOT EXISTS idx_offset_mappings_created_at 
				ON offset_mappings(created_at);
			`,
		},
		{
			Version:     3,
			Description: "Add partition metadata table for enhanced tracking",
			SQL: `
				-- Partition metadata table
				CREATE TABLE IF NOT EXISTS partition_metadata (
					partition_key TEXT PRIMARY KEY,
					ring_size INTEGER NOT NULL,
					range_start INTEGER NOT NULL,
					range_stop INTEGER NOT NULL,
					unix_time_ns INTEGER NOT NULL,
					created_at INTEGER NOT NULL,
					last_activity_at INTEGER NOT NULL,
					record_count INTEGER DEFAULT 0,
					total_size INTEGER DEFAULT 0
				);
				
				-- Index for partition metadata
				CREATE INDEX IF NOT EXISTS idx_partition_metadata_activity 
				ON partition_metadata(last_activity_at);
			`,
		},
	}
}

// MigrationManager handles database schema migrations
type MigrationManager struct {
	db *sql.DB
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(db *sql.DB) *MigrationManager {
	return &MigrationManager{db: db}
}

// GetCurrentVersion returns the current schema version
func (m *MigrationManager) GetCurrentVersion() (int, error) {
	// First, ensure the migrations table exists
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at INTEGER NOT NULL
		)
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to create migrations table: %w", err)
	}

	var version sql.NullInt64
	err = m.db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	if !version.Valid {
		return 0, nil // No migrations applied yet
	}

	return int(version.Int64), nil
}

// ApplyMigrations applies all pending migrations
func (m *MigrationManager) ApplyMigrations() error {
	currentVersion, err := m.GetCurrentVersion()
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	migrations := GetMigrations()

	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			continue // Already applied
		}

		fmt.Printf("Applying migration %d: %s\n", migration.Version, migration.Description)

		// Begin transaction
		tx, err := m.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Execute migration SQL
		_, err = tx.Exec(migration.SQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute migration %d: %w", migration.Version, err)
		}

		// Record migration as applied
		_, err = tx.Exec(
			"INSERT INTO schema_migrations (version, description, applied_at) VALUES (?, ?, ?)",
			migration.Version,
			migration.Description,
			getCurrentTimestamp(),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", migration.Version, err)
		}

		fmt.Printf("Successfully applied migration %d\n", migration.Version)
	}

	return nil
}

// RollbackMigration rolls back a specific migration (if supported)
func (m *MigrationManager) RollbackMigration(version int) error {
	// TODO: Implement rollback functionality
	// ASSUMPTION: For now, rollbacks are not supported as they require careful planning
	return fmt.Errorf("migration rollbacks not implemented - manual intervention required")
}

// GetAppliedMigrations returns a list of all applied migrations
func (m *MigrationManager) GetAppliedMigrations() ([]AppliedMigration, error) {
	rows, err := m.db.Query(`
		SELECT version, description, applied_at 
		FROM schema_migrations 
		ORDER BY version
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var migrations []AppliedMigration
	for rows.Next() {
		var migration AppliedMigration
		err := rows.Scan(&migration.Version, &migration.Description, &migration.AppliedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan migration: %w", err)
		}
		migrations = append(migrations, migration)
	}

	return migrations, nil
}

// ValidateSchema validates that the database schema is up to date
func (m *MigrationManager) ValidateSchema() error {
	currentVersion, err := m.GetCurrentVersion()
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	migrations := GetMigrations()
	if len(migrations) == 0 {
		return nil
	}

	latestVersion := migrations[len(migrations)-1].Version
	if currentVersion < latestVersion {
		return fmt.Errorf("schema is outdated: current version %d, latest version %d", currentVersion, latestVersion)
	}

	return nil
}

// AppliedMigration represents a migration that has been applied
type AppliedMigration struct {
	Version     int
	Description string
	AppliedAt   int64
}

// getCurrentTimestamp returns the current timestamp in nanoseconds
func getCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}

// CreateDatabase creates and initializes a new offset storage database
func CreateDatabase(dbPath string) (*sql.DB, error) {
	// TODO: Support different database types (PostgreSQL, MySQL, etc.)
	// ASSUMPTION: Using SQLite for now, can be extended for other databases

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure SQLite for better performance
	pragmas := []string{
		"PRAGMA journal_mode=WAL",   // Write-Ahead Logging for better concurrency
		"PRAGMA synchronous=NORMAL", // Balance between safety and performance
		"PRAGMA cache_size=10000",   // Increase cache size
		"PRAGMA foreign_keys=ON",    // Enable foreign key constraints
		"PRAGMA temp_store=MEMORY",  // Store temporary tables in memory
	}

	for _, pragma := range pragmas {
		_, err := db.Exec(pragma)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set pragma %s: %w", pragma, err)
		}
	}

	// Apply migrations
	migrationManager := NewMigrationManager(db)
	err = migrationManager.ApplyMigrations()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	return db, nil
}

// BackupDatabase creates a backup of the offset storage database
func BackupDatabase(sourceDB *sql.DB, backupPath string) error {
	// TODO: Implement database backup functionality
	// ASSUMPTION: This would use database-specific backup mechanisms
	return fmt.Errorf("database backup not implemented yet")
}

// RestoreDatabase restores a database from a backup
func RestoreDatabase(backupPath, targetPath string) error {
	// TODO: Implement database restore functionality
	// ASSUMPTION: This would use database-specific restore mechanisms
	return fmt.Errorf("database restore not implemented yet")
}
