package main

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestSHOWTablesRecovery tests that SHOW TABLES doesn't crash with nil pointer dereference
func TestSHOWTablesRecovery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test SHOW TABLES (this was causing panics before the fix)
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	defer rows.Close()

	tableCount := 0
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Errorf("Error scanning table name: %v", err)
			continue
		}
		tableCount++
		log.Printf("Found table: %s", tableName)
	}

	if tableCount == 0 {
		t.Error("Expected at least one table, got 0")
	}
	log.Printf("✓ SHOW TABLES recovery test passed - found %d tables", tableCount)
}

// TestSHOWTablesFromDatabase tests SHOW TABLES FROM database syntax
func TestSHOWTablesFromDatabase(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test SHOW TABLES FROM specific database
	rows, err := db.Query(`SHOW TABLES FROM "logs"`)
	if err != nil {
		t.Fatalf("SHOW TABLES FROM logs failed: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Errorf("Error scanning table name: %v", err)
			continue
		}
		found = true
		log.Printf("Found table in logs database: %s", tableName)
	}

	if !found {
		t.Error("Expected tables in logs database, found none")
	}
	log.Printf("✓ SHOW TABLES FROM database test passed")
}

// TestSystemQueriesIndividualConnections tests system queries with fresh connections
func TestSystemQueriesIndividualConnections(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{"Version", "SELECT version()"},
		{"Current User", "SELECT current_user"},
		{"Current Database", "SELECT current_database()"},
		{"Server Encoding", "SELECT current_setting('server_encoding')"},
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		getEnv("POSTGRES_HOST", "postgres-server"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "seaweedfs"),
		getEnv("POSTGRES_DB", "logs"))

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			// Create fresh connection for each system query (this was the fix)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Fatalf("Failed to create connection for %s: %v", q.name, err)
			}
			defer db.Close()

			var result string
			err = db.QueryRow(q.query).Scan(&result)
			if err != nil {
				t.Errorf("System query %s failed: %v", q.name, err)
				return
			}

			if result == "" {
				t.Errorf("System query %s returned empty result", q.name)
				return
			}

			log.Printf("✓ %s: %s", q.name, result)
		})
	}
}

// TestDatabaseConnectionSwitching tests connecting to different databases
func TestDatabaseConnectionSwitching(t *testing.T) {
	databases := []string{"analytics", "ecommerce", "logs"}
	
	host := getEnv("POSTGRES_HOST", "postgres-server")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "seaweedfs")

	for _, dbName := range databases {
		t.Run(fmt.Sprintf("Connect to %s", dbName), func(t *testing.T) {
			// Create fresh connection to specific database (this was the fix instead of USE commands)
			connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
				host, port, user, dbName)
			
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Fatalf("Failed to connect to database %s: %v", dbName, err)
			}
			defer db.Close()

			var currentDB string
			err = db.QueryRow("SELECT current_database()").Scan(&currentDB)
			if err != nil {
				t.Errorf("Failed to verify database connection to %s: %v", dbName, err)
				return
			}

			log.Printf("✓ Successfully connected to database: %s", currentDB)
		})
	}
}

// TestCOUNTFunctionParsing tests COUNT(*) parsing that was fixed
func TestCOUNTFunctionParsing(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test COUNT(*) on known table (this was fixed in the parser)
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM application_logs").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT(*) query failed: %v", err)
	}

	if count <= 0 {
		t.Error("Expected COUNT(*) to return positive number")
	}

	log.Printf("✓ COUNT(*) parsing test passed - found %d records", count)
}

// TestParquetLogicalTypesDisplay tests that Parquet logical types are displayed correctly  
func TestParquetLogicalTypesDisplay(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test that timestamp logical types are visible in query results
	rows, err := db.Query("SELECT timestamp, id FROM application_logs LIMIT 2")
	if err != nil {
		t.Fatalf("Failed to query logical types: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var timestamp, id string
		if err := rows.Scan(&timestamp, &id); err != nil {
			t.Errorf("Error scanning logical type data: %v", err)
			continue
		}
		
		// Check that timestamp contains the logical type structure
		if !containsAny(timestamp, []string{"timestamp_value", "timestamp_mic"}) {
			t.Errorf("Expected timestamp to contain logical type structure, got: %s", timestamp)
		}
		
		count++
		log.Printf("Row %d - Timestamp: %s, ID: %s", count, timestamp, id)
	}

	if count == 0 {
		t.Error("Expected to retrieve logical type data, got none")
	}

	log.Printf("✓ Parquet logical types display test passed - %d rows", count)
}

// Helper functions

func setupTestDB(t *testing.T) *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		getEnv("POSTGRES_HOST", "postgres-server"),
		getEnv("POSTGRES_PORT", "5432"), 
		getEnv("POSTGRES_USER", "seaweedfs"),
		getEnv("POSTGRES_DB", "logs"))

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Wait for server to be ready
	for i := 0; i < 30; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Database not ready after 15 seconds: %v", err)
	}

	return db
}

func containsAny(str string, substrings []string) bool {
	for _, sub := range substrings {
		if len(str) >= len(sub) {
			for i := 0; i <= len(str)-len(sub); i++ {
				if str[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
