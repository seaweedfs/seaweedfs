package catalog_spark

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// waitForSparkReady polls Spark to verify it's ready by executing a simple query
func waitForSparkReady(t *testing.T, container testcontainers.Container, icebergPort int, s3Port int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output := runSparkPySQL(t, container, `
spark.sql("SELECT 1 as test")
print("Spark ready")
`, icebergPort, s3Port)
		if strings.Contains(output, "Spark ready") {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Spark did not become ready within %v", timeout)
}

// setupSparkTestEnv initializes a test environment with SeaweedFS and Spark containers
func setupSparkTestEnv(t *testing.T) (*TestEnvironment, string, string) {
	t.Helper()

	env := NewTestEnvironment(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Spark integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)
	t.Cleanup(func() { env.Cleanup(t) })

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeSparkConfig(t, catalogBucket)
	env.startSparkContainer(t, configDir)

	// Poll for Spark readiness instead of fixed sleep
	waitForSparkReady(t, env.sparkContainer, env.icebergRestPort, env.s3Port, 30*time.Second)

	return env, catalogBucket, tableBucket
}

// TestSparkCatalogBasicOperations tests basic Spark Iceberg catalog operations
func TestSparkCatalogBasicOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, _ := setupSparkTestEnv(t)

	// Test 1: Create a namespace (database)
	t.Logf(">>> Test 1: Creating namespace")
	namespace := "spark_test_" + randomString(6)
	sparkSQL := fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%s")
print("Namespace created")
`, namespace)
	output := runSparkPySQL(t, env.sparkContainer, sparkSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Namespace created") {
		t.Fatalf("namespace creation failed, output: %s", output)
	}

	// Test 2: Create a table
	t.Logf(">>> Test 2: Creating table")
	tableName := "test_table_" + randomString(6)
	createTableSQL := fmt.Sprintf(`
spark.sql("""
CREATE TABLE iceberg.%s.%s (
    id INT,
    name STRING,
    age INT
)
USING iceberg
""")
print("Table created")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, createTableSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Table created") {
		t.Fatalf("table creation failed, output: %s", output)
	}

	// Test 3: Insert data
	t.Logf(">>> Test 3: Inserting data")
	insertDataSQL := fmt.Sprintf(`
spark.sql("""
INSERT INTO iceberg.%s.%s VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35)
""")
print("Data inserted")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, insertDataSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Data inserted") {
		t.Fatalf("data insertion failed, output: %s", output)
	}

	// Test 4: Query data
	t.Logf(">>> Test 4: Querying data")
	querySQL := fmt.Sprintf(`
result = spark.sql("SELECT COUNT(*) as count FROM iceberg.%s.%s")
result.show()
count = result.collect()[0]['count']
print(f"Row count: {count}")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, querySQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Row count: 3") {
		t.Errorf("expected row count 3, got output: %s", output)
	}

	// Test 5: Update data
	t.Logf(">>> Test 5: Updating data")
	updateSQL := fmt.Sprintf(`
spark.sql("""
UPDATE iceberg.%s.%s SET age = 31 WHERE id = 1
""")
print("Data updated")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, updateSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Data updated") {
		t.Errorf("data update failed, output: %s", output)
	}

	// Test 6: Delete data
	t.Logf(">>> Test 6: Deleting data")
	deleteSQL := fmt.Sprintf(`
spark.sql("""
DELETE FROM iceberg.%s.%s WHERE id = 3
""")
print("Data deleted")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, deleteSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Data deleted") {
		t.Errorf("data delete failed, output: %s", output)
	}

	// Verify final count
	t.Logf(">>> Verifying final data")
	finalCountSQL := fmt.Sprintf(`
result = spark.sql("SELECT COUNT(*) as count FROM iceberg.%s.%s")
result.show()
count = result.collect()[0]['count']
print(f"Final row count: {count}")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, finalCountSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Final row count: 2") {
		t.Errorf("expected final row count 2, got output: %s", output)
	}

	t.Logf(">>> All tests passed")
}

// TestSparkTimeTravel tests Spark Iceberg time travel capabilities
func TestSparkTimeTravel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, _ := setupSparkTestEnv(t)

	namespace := "time_travel_test_" + randomString(6)
	tableName := "tt_table_" + randomString(6)

	// Create namespace and table
	setupSQL := fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%s")
spark.sql("""
CREATE TABLE iceberg.%s.%s (
    id INT,
    value INT
)
USING iceberg
""")
print("Setup complete")
`, namespace, namespace, tableName)
	output := runSparkPySQL(t, env.sparkContainer, setupSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Setup complete") {
		t.Fatalf("setup failed for namespace %s and table %s, output: %s", namespace, tableName, output)
	}

	// Insert initial data
	t.Logf(">>> Inserting initial data")
	insertSQL := fmt.Sprintf(`
spark.sql("""
INSERT INTO iceberg.%s.%s VALUES (1, 10)
""")
snapshot_id = spark.sql("SELECT snapshot_id FROM iceberg.%s.%s.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
print(f"Snapshot ID: {snapshot_id}")
`, namespace, tableName, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, insertSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Snapshot ID:") {
		t.Fatalf("failed to get snapshot ID: %s", output)
	}

	// Extract snapshot ID from output
	var snapshotID string
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "Snapshot ID:") {
			parts := strings.Split(line, ":")
			if len(parts) > 1 {
				snapshotID = strings.TrimSpace(parts[1])
			}
		}
	}

	if snapshotID == "" {
		t.Fatalf("could not extract snapshot ID from output: %s", output)
	}

	// Insert more data
	t.Logf(">>> Inserting more data")
	insertMoreSQL := fmt.Sprintf(`
spark.sql("""
INSERT INTO iceberg.%s.%s VALUES (2, 20)
""")
print("More data inserted")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, insertMoreSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "More data inserted") {
		t.Fatalf("failed to insert more data, output: %s", output)
	}

	// Verify count increased to 2
	t.Logf(">>> Verifying row count after second insert")
	verifySQL := fmt.Sprintf(`
result = spark.sql("SELECT COUNT(*) as count FROM iceberg.%s.%s")
count = result.collect()[0]['count']
print(f"Current row count: {count}")
`, namespace, tableName)
	output = runSparkPySQL(t, env.sparkContainer, verifySQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Current row count: 2") {
		t.Fatalf("expected current row count 2 after second insert, got output: %s", output)
	}

	// Time travel to first snapshot
	t.Logf(">>> Time traveling to first snapshot")
	timeTravelSQL := fmt.Sprintf(`
result = spark.sql("""
SELECT COUNT(*) as count FROM iceberg.%s.%s VERSION AS OF %s
""")
result.show()
count = result.collect()[0]['count']
print(f"Count at snapshot: {count}")
`, namespace, tableName, snapshotID)
	output = runSparkPySQL(t, env.sparkContainer, timeTravelSQL, env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Count at snapshot: 1") {
		t.Errorf("expected count 1 at first snapshot, got: %s", output)
	}

	t.Logf(">>> Time travel test passed")
}
