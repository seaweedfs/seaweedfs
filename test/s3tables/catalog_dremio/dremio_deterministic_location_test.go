package catalog_dremio

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDeterministicTableLocation tests that explicit table locations are preserved.
func TestDeterministicTableLocation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Dremio integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)

	configDir := env.writeDremioConfig(t, tableBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 120*time.Second)

	namespace := "ns_" + randomString(4)
	tableName := "table_" + randomString(4)
	tableLocation := fmt.Sprintf("s3://%s/%s/%s", tableBucket, namespace, tableName)

	t.Logf(">>> Creating namespace: %s", namespace)
	runDremioSQL(t, env.dremioContainer, "CREATE SCHEMA "+namespace)

	t.Logf(">>> Creating table with explicit location: %s", tableLocation)
	createSQL := fmt.Sprintf(`CREATE TABLE %s.%s (
		id INTEGER,
		event VARCHAR,
		ts TIMESTAMP
	) STORED BY ICEBERG
	LOCATION '%s'
	AS SELECT 1, 'test', CURRENT_TIMESTAMP WHERE FALSE`, namespace, tableName, tableLocation)
	runDremioSQL(t, env.dremioContainer, createSQL)

	t.Logf(">>> Inserting test data")
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s VALUES
		(1, 'click', CURRENT_TIMESTAMP),
		(2, 'view', CURRENT_TIMESTAMP),
		(3, 'click', CURRENT_TIMESTAMP)
	`, namespace, tableName)
	runDremioSQL(t, env.dremioContainer, insertSQL)

	t.Logf(">>> Verifying data insertion")
	querySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", namespace, tableName)
	result := runDremioSQL(t, env.dremioContainer, querySQL)
	rows := parseDremioResponse(t, result)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected single row with count, got: %v", rows)
	}
	count, ok := rows[0][0].(float64)
	if !ok || count != 3 {
		t.Fatalf("Expected 3 rows in table, got: %v", rows[0][0])
	}

	t.Logf(">>> Verifying table location")
	describeSQL := fmt.Sprintf("DESCRIBE FORMATTED %s.%s", namespace, tableName)
	describeResult := runDremioSQL(t, env.dremioContainer, describeSQL)
	if !strings.Contains(describeResult, tableLocation) {
		t.Fatalf("Expected table location %s in DESCRIBE output, but it was not found.\nOutput:\n%s", tableLocation, describeResult)
	}

	t.Logf(">>> TestDeterministicTableLocation PASSED")
}

// TestMultiLevelNamespace tests multi-level namespace (dot-separated) support.
func TestMultiLevelNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Dremio integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)

	configDir := env.writeDremioConfig(t, tableBucket)
	env.startDremioContainer(t, configDir)
	waitForDremio(t, env.dremioContainer, 120*time.Second)

	level1 := "analytics_" + randomString(4)
	level2 := "daily_" + randomString(4)
	tableName := "events_" + randomString(4)

	t.Logf(">>> Creating multi-level namespace: %s.%s", level1, level2)
	runDremioSQL(t, env.dremioContainer, fmt.Sprintf(`CREATE SCHEMA "%s"."%s"`, level1, level2))

	t.Logf(">>> Creating table in multi-level namespace")
	createSQL := fmt.Sprintf(`CREATE TABLE "%s"."%s".%s (
		id INTEGER,
		event VARCHAR,
		ts TIMESTAMP
	) AS SELECT 1, 'test', CURRENT_TIMESTAMP WHERE FALSE`, level1, level2, tableName)
	runDremioSQL(t, env.dremioContainer, createSQL)

	t.Logf(">>> Inserting data into multi-level namespace table")
	insertSQL := fmt.Sprintf(`INSERT INTO "%s"."%s".%s VALUES
		(1, 'click', CURRENT_TIMESTAMP),
		(2, 'view', CURRENT_TIMESTAMP),
		(3, 'click', CURRENT_TIMESTAMP)
	`, level1, level2, tableName)
	runDremioSQL(t, env.dremioContainer, insertSQL)

	t.Logf(">>> Querying data from multi-level namespace table")
	querySQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s".%s`, level1, level2, tableName)
	result := runDremioSQL(t, env.dremioContainer, querySQL)
	rows := parseDremioResponse(t, result)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Expected single row with count, got: %v", rows)
	}
	count, ok := rows[0][0].(float64)
	if !ok || count != 3 {
		t.Fatalf("Expected 3 rows in multi-level namespace table, got: %v", rows[0][0])
	}

	t.Logf(">>> TestMultiLevelNamespace PASSED")
}
