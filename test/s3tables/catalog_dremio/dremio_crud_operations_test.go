package catalog_dremio

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// setupDremioTest creates a test environment with SeaweedFS and Dremio running.
func setupDremioTest(t *testing.T) *TestEnvironment {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Dremio integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	tableBucket := "iceberg-tables"
	catalogBucket := tableBucket
	createTableBucket(t, env, tableBucket)

	configDir := env.writeDremioConfig(t, catalogBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 120*time.Second)

	return env
}

// TestSchemaCRUD tests schema creation, listing, and deletion operations.
func TestSchemaCRUD(t *testing.T) {
	env := setupDremioTest(t)
	defer env.Cleanup(t)

	schema1 := "crud_test_schema1_" + randomString(6)
	schema2 := "crud_test_schema2_" + randomString(6)

	t.Logf(">>> CREATE: Creating schema %s", schema1)
	runDremioSQL(t, env.dremioContainer, "CREATE SCHEMA "+schema1)
	t.Logf(">>> Schema %s created", schema1)

	t.Logf(">>> CREATE: Creating second schema %s", schema2)
	runDremioSQL(t, env.dremioContainer, "CREATE SCHEMA "+schema2)
	t.Logf(">>> Schema %s created", schema2)

	t.Logf(">>> READ: Listing all schemas")
	output := runDremioSQL(t, env.dremioContainer, "SHOW SCHEMAS")
	if !strings.Contains(output, schema1) {
		t.Fatalf("Expected schema %s in listing, but it was not found.\nOutput:\n%s", schema1, output)
	}

	t.Logf(">>> DELETE: Dropping schema %s", schema1)
	runDremioSQL(t, env.dremioContainer, "DROP SCHEMA "+schema1)
	t.Logf(">>> Schema %s dropped", schema1)

	t.Logf(">>> TestSchemaCRUD PASSED")
}

// TestTableCRUD tests table creation, insertion, listing, and deletion operations.
func TestTableCRUD(t *testing.T) {
	env := setupDremioTest(t)
	defer env.Cleanup(t)

	schemaName := "table_crud_" + randomString(6)
	tableName := "test_table_" + randomString(6)

	t.Logf(">>> CREATE: Creating schema %s", schemaName)
	runDremioSQL(t, env.dremioContainer, "CREATE SCHEMA "+schemaName)

	t.Logf(">>> CREATE: Creating table %s.%s", schemaName, tableName)
	createSQL := fmt.Sprintf(`CREATE TABLE %s.%s (
		id INTEGER,
		name VARCHAR,
		value DOUBLE
	) AS SELECT 1, 'test', 1.5 WHERE FALSE`, schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, createSQL)

	t.Logf(">>> READ: Listing tables in schema")
	output := runDremioSQL(t, env.dremioContainer, fmt.Sprintf("SHOW TABLES IN %s", schemaName))
	if !strings.Contains(output, tableName) {
		t.Fatalf("Expected table %s in listing, but it was not found.\nOutput:\n%s", tableName, output)
	}

	t.Logf(">>> UPDATE: Inserting rows")
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s VALUES (1, 'alice', 10.5), (2, 'bob', 20.3)`, schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, insertSQL)

	t.Logf(">>> READ: Querying table")
	querySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schemaName, tableName)
	output = runDremioSQL(t, env.dremioContainer, querySQL)
	rows := parseDremioResponse(t, output)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected single row with count, got: %v", rows)
	}
	count, ok := rows[0][0].(float64)
	if !ok || count != 2 {
		t.Fatalf("Expected count of 2, got: %v", rows[0][0])
	}

	t.Logf(">>> DELETE: Dropping table %s.%s", schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, fmt.Sprintf("DROP TABLE %s.%s", schemaName, tableName))

	t.Logf(">>> TestTableCRUD PASSED")
}

// TestDataInsertAndQuery tests data insertion and querying with various SQL operations.
func TestDataInsertAndQuery(t *testing.T) {
	env := setupDremioTest(t)
	defer env.Cleanup(t)

	schemaName := "data_test_" + randomString(6)
	tableName := "data_table_" + randomString(6)

	t.Logf(">>> Creating test schema and table")
	runDremioSQL(t, env.dremioContainer, "CREATE SCHEMA "+schemaName)

	createSQL := fmt.Sprintf(`CREATE TABLE %s.%s (
		id INTEGER,
		name VARCHAR,
		category VARCHAR,
		amount DOUBLE
	) AS SELECT 1, 'test', 'cat', 1.5 WHERE FALSE`, schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, createSQL)

	t.Logf(">>> Inserting bulk data")
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s VALUES
		(1, 'alice', 'category_a', 100.50),
		(2, 'bob', 'category_b', 200.75),
		(3, 'charlie', 'category_a', 150.25),
		(4, 'diana', 'category_c', 300.00)
	`, schemaName, tableName)
	runDremioSQL(t, env.dremioContainer, insertSQL)

	t.Logf(">>> Testing COUNT query")
	countSQL := fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s", schemaName, tableName)
	countOutput := runDremioSQL(t, env.dremioContainer, countSQL)
	rows := parseDremioResponse(t, countOutput)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected single row with count, got: %v", rows)
	}
	totalCount, ok := rows[0][0].(float64)
	if !ok || totalCount != 4 {
		t.Fatalf("Expected total count of 4, got: %v", rows[0][0])
	}

	t.Logf(">>> Testing WHERE clause")
	whereSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE category = 'category_a'", schemaName, tableName)
	whereOutput := runDremioSQL(t, env.dremioContainer, whereSQL)
	rows = parseDremioResponse(t, whereOutput)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected single row with count, got: %v", rows)
	}
	filteredCount, ok := rows[0][0].(float64)
	if !ok || filteredCount != 2 {
		t.Fatalf("Expected filtered count of 2 for category_a, got: %v", rows[0][0])
	}

	t.Logf(">>> Testing aggregations")
	aggregateSQL := fmt.Sprintf("SELECT category, SUM(amount) FROM %s.%s GROUP BY category", schemaName, tableName)
	aggregateOutput := runDremioSQL(t, env.dremioContainer, aggregateSQL)
	rows = parseDremioResponse(t, aggregateOutput)
	if len(rows) != 3 {
		t.Fatalf("Expected 3 categories, got %d rows", len(rows))
	}

	t.Logf(">>> TestDataInsertAndQuery PASSED")
}
