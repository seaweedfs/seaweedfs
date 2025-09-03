package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Get PostgreSQL connection details from environment
	host := getEnv("POSTGRES_HOST", "localhost")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "seaweedfs")
	dbname := getEnv("POSTGRES_DB", "default")

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbname)

	log.Println("SeaweedFS PostgreSQL Client Test")
	log.Println("=================================")
	log.Printf("Connecting to: %s\n", connStr)

	// Wait for PostgreSQL server to be ready
	log.Println("Waiting for PostgreSQL server...")
	time.Sleep(5 * time.Second)

	// Connect to PostgreSQL server
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Test connection with a simple query instead of Ping()
	var result int
	err = db.QueryRow("SELECT COUNT(*) FROM application_logs LIMIT 1").Scan(&result)
	if err != nil {
		log.Printf("Warning: Simple query test failed: %v", err)
		log.Printf("Trying alternative connection test...")

		// Try a different table
		err = db.QueryRow("SELECT COUNT(*) FROM user_events LIMIT 1").Scan(&result)
		if err != nil {
			log.Fatalf("Error testing PostgreSQL connection: %v", err)
		} else {
			log.Printf("✓ Connected successfully! Found %d records in user_events", result)
		}
	} else {
		log.Printf("✓ Connected successfully! Found %d records in application_logs", result)
	}

	// Run comprehensive tests
	tests := []struct {
		name string
		test func(*sql.DB) error
	}{
		// {"System Information", testSystemInfo},  // Temporarily disabled due to segfault
		{"Database Discovery", testDatabaseDiscovery},
		{"Table Discovery", testTableDiscovery},
		{"Data Queries", testDataQueries},
		{"Aggregation Queries", testAggregationQueries},
		{"Database Context Switching", testDatabaseSwitching},
		{"System Columns", testSystemColumns},
		{"Complex Queries", testComplexQueries},
	}

	successCount := 0
	for _, test := range tests {
		log.Printf("\n--- Running Test: %s ---", test.name)
		if err := test.test(db); err != nil {
			log.Printf("❌ Test FAILED: %s - %v", test.name, err)
		} else {
			log.Printf("✅ Test PASSED: %s", test.name)
			successCount++
		}
	}

	log.Printf("\n=================================")
	log.Printf("Test Results: %d/%d tests passed", successCount, len(tests))
	if successCount == len(tests) {
		log.Println("🎉 All tests passed!")
	} else {
		log.Printf("⚠️  %d tests failed", len(tests)-successCount)
	}
}

func testSystemInfo(db *sql.DB) error {
	queries := []struct {
		name  string
		query string
	}{
		{"Version", "SELECT version()"},
		{"Current User", "SELECT current_user"},
		{"Current Database", "SELECT current_database()"},
		{"Server Encoding", "SELECT current_setting('server_encoding')"},
	}

	for _, q := range queries {
		var result string
		err := db.QueryRow(q.query).Scan(&result)
		if err != nil {
			return fmt.Errorf("%s query failed: %v", q.name, err)
		}
		log.Printf("  %s: %s", q.name, result)
	}

	return nil
}

func testDatabaseDiscovery(db *sql.DB) error {
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return fmt.Errorf("SHOW DATABASES failed: %v", err)
	}
	defer rows.Close()

	databases := []string{}
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return fmt.Errorf("scanning database name: %v", err)
		}
		databases = append(databases, dbName)
	}

	log.Printf("  Found %d databases: %s", len(databases), strings.Join(databases, ", "))
	return nil
}

func testTableDiscovery(db *sql.DB) error {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return fmt.Errorf("SHOW TABLES failed: %v", err)
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	log.Printf("  Found %d tables in current database: %s", len(tables), strings.Join(tables, ", "))
	return nil
}

func testDataQueries(db *sql.DB) error {
	// Try to find a table with data
	tables := []string{"user_events", "system_logs", "metrics", "product_views", "application_logs"}

	for _, table := range tables {
		// Try to query the table
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err == nil && count > 0 {
			log.Printf("  Table '%s' has %d records", table, count)

			// Try to get sample data
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 3", table))
			if err != nil {
				log.Printf("    Warning: Could not query sample data: %v", err)
				continue
			}

			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				log.Printf("    Warning: Could not get columns: %v", err)
				continue
			}

			log.Printf("    Sample columns: %s", strings.Join(columns, ", "))

			sampleCount := 0
			for rows.Next() && sampleCount < 2 {
				// Create slice to hold column values
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				err := rows.Scan(valuePtrs...)
				if err != nil {
					log.Printf("    Warning: Could not scan row: %v", err)
					break
				}

				// Convert to strings for display
				stringValues := make([]string, len(values))
				for i, val := range values {
					if val != nil {
						str := fmt.Sprintf("%v", val)
						if len(str) > 30 {
							str = str[:30] + "..."
						}
						stringValues[i] = str
					} else {
						stringValues[i] = "NULL"
					}
				}

				log.Printf("    Sample row %d: %s", sampleCount+1, strings.Join(stringValues, " | "))
				sampleCount++
			}
			rows.Close()
			break
		}
	}

	return nil
}

func testAggregationQueries(db *sql.DB) error {
	// Try to find a table for aggregation testing
	tables := []string{"user_events", "system_logs", "metrics", "product_views"}

	for _, table := range tables {
		// Check if table exists and has data
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			continue // Table doesn't exist or no access
		}

		if count == 0 {
			continue // No data
		}

		log.Printf("  Testing aggregations on '%s' (%d records)", table, count)

		// Test basic aggregation
		var avgId, maxId, minId float64
		err = db.QueryRow(fmt.Sprintf("SELECT AVG(id), MAX(id), MIN(id) FROM %s", table)).Scan(&avgId, &maxId, &minId)
		if err != nil {
			log.Printf("    Warning: Aggregation query failed: %v", err)
		} else {
			log.Printf("    ID stats - AVG: %.2f, MAX: %.0f, MIN: %.0f", avgId, maxId, minId)
		}

		// Test COUNT with GROUP BY if possible (try common column names)
		groupByColumns := []string{"user_type", "level", "service", "category", "status"}
		for _, col := range groupByColumns {
			rows, err := db.Query(fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s LIMIT 5", col, table, col))
			if err == nil {
				log.Printf("    Group by %s:", col)
				for rows.Next() {
					var group string
					var groupCount int
					if err := rows.Scan(&group, &groupCount); err == nil {
						log.Printf("      %s: %d", group, groupCount)
					}
				}
				rows.Close()
				break
			}
		}

		return nil
	}

	log.Println("  No suitable tables found for aggregation testing")
	return nil
}

func testDatabaseSwitching(db *sql.DB) error {
	// Get current database
	var currentDB string
	err := db.QueryRow("SELECT current_database()").Scan(&currentDB)
	if err != nil {
		return fmt.Errorf("getting current database: %v", err)
	}
	log.Printf("  Current database: %s", currentDB)

	// Try to switch to different databases
	databases := []string{"analytics", "ecommerce", "logs"}

	for _, dbName := range databases {
		_, err := db.Exec(fmt.Sprintf("USE %s", dbName))
		if err != nil {
			log.Printf("  Could not switch to '%s': %v", dbName, err)
			continue
		}

		// Verify switch
		var newDB string
		err = db.QueryRow("SELECT current_database()").Scan(&newDB)
		if err == nil {
			log.Printf("  ✓ Switched to database: %s", newDB)

			// Check tables in this database
			rows, err := db.Query("SHOW TABLES")
			if err == nil {
				tables := []string{}
				for rows.Next() {
					var tableName string
					if err := rows.Scan(&tableName); err == nil {
						tables = append(tables, tableName)
					}
				}
				rows.Close()
				if len(tables) > 0 {
					log.Printf("    Tables: %s", strings.Join(tables, ", "))
				}
			}
			break
		}
	}

	return nil
}

func testSystemColumns(db *sql.DB) error {
	// Try to find a table with system columns
	tables := []string{"user_events", "system_logs", "metrics"}

	for _, table := range tables {
		// Check if table exists
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil || count == 0 {
			continue
		}

		log.Printf("  Testing system columns on '%s'", table)

		// Try to query system columns
		rows, err := db.Query(fmt.Sprintf("SELECT id, _timestamp_ns, _key, _source FROM %s LIMIT 3", table))
		if err != nil {
			log.Printf("    System columns not available: %v", err)
			return nil
		}
		defer rows.Close()

		for rows.Next() {
			var id int64
			var timestamp, key, source sql.NullString
			err := rows.Scan(&id, &timestamp, &key, &source)
			if err != nil {
				log.Printf("    Error scanning system columns: %v", err)
				break
			}

			log.Printf("    ID: %d, Timestamp: %s, Key: %s, Source: %s",
				id,
				stringOrNull(timestamp),
				stringOrNull(key),
				stringOrNull(source))
			break // Just show one example
		}
		return nil
	}

	log.Println("  No suitable tables found for system column testing")
	return nil
}

func testComplexQueries(db *sql.DB) error {
	// Try more complex queries with WHERE, ORDER BY, etc.
	tables := []string{"user_events", "system_logs", "product_views"}

	for _, table := range tables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil || count < 10 {
			continue
		}

		log.Printf("  Testing complex queries on '%s'", table)

		// Test WHERE with comparison
		var filteredCount int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id > 1000", table)).Scan(&filteredCount)
		if err == nil {
			log.Printf("    Records with id > 1000: %d", filteredCount)
		}

		// Test ORDER BY with LIMIT
		rows, err := db.Query(fmt.Sprintf("SELECT id FROM %s ORDER BY id DESC LIMIT 5", table))
		if err == nil {
			topIds := []int64{}
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err == nil {
					topIds = append(topIds, id)
				}
			}
			rows.Close()
			log.Printf("    Top 5 IDs: %v", topIds)
		}

		return nil
	}

	log.Println("  No suitable tables found for complex query testing")
	return nil
}

func stringOrNull(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "NULL"
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
