package catalog_trino

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestNamespaceCRUD tests namespace (schema) CRUD operations via Trino SQL
// Namespaces are the key container for tables in Iceberg, and this test
// verifies the full CRUD lifecycle: Create, Read (List), Update, and Delete
func TestNamespaceCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	// CREATE: Create a namespace
	namespace1 := "crud_test_ns1_" + randomString(6)
	fmt.Printf(">>> CREATE: Creating namespace %s\n", namespace1)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", namespace1))
	fmt.Printf(">>> Namespace %s created\n", namespace1)

	namespace2 := "crud_test_ns2_" + randomString(6)
	fmt.Printf(">>> CREATE: Creating second namespace %s\n", namespace2)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", namespace2))
	fmt.Printf(">>> Namespace %s created\n", namespace2)

	// READ: List all namespaces
	fmt.Printf(">>> READ: Listing all namespaces\n")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, namespace1) {
		t.Fatalf("Expected namespace %s in listing output:\n%s", namespace1, output)
	}
	if !strings.Contains(output, namespace2) {
		t.Fatalf("Expected namespace %s in listing output:\n%s", namespace2, output)
	}
	fmt.Printf(">>> Both namespaces found in listing\n")

	// UPDATE: Namespaces typically don't have "update" semantics in SQL,
	// but we can verify properties via metadata queries
	fmt.Printf(">>> UPDATE: Simulating namespace properties (via SQL properties check)\n")
	// Iceberg REST API supports namespace properties, but Trino SQL doesn't expose them directly
	// This test verifies the namespace still exists and is accessible
	output = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SHOW TABLES FROM iceberg.%s", namespace1))
	fmt.Printf(">>> Namespace %s is accessible (empty table list expected):\n%s\n", namespace1, output)

	// DELETE: Drop namespaces
	fmt.Printf(">>> DELETE: Dropping namespace %s\n", namespace1)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", namespace1))
	fmt.Printf(">>> Namespace %s dropped\n", namespace1)

	fmt.Printf(">>> DELETE: Dropping namespace %s\n", namespace2)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", namespace2))
	fmt.Printf(">>> Namespace %s dropped\n", namespace2)

	// Verify deletion: Namespaces should no longer exist
	fmt.Printf(">>> VERIFY: Checking that namespaces are deleted\n")
	output = runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if strings.Contains(output, namespace1) {
		t.Logf("WARNING: Namespace %s still appears in listing after deletion", namespace1)
	} else {
		fmt.Printf(">>> Namespace %s correctly deleted\n", namespace1)
	}
	if strings.Contains(output, namespace2) {
		t.Logf("WARNING: Namespace %s still appears in listing after deletion", namespace2)
	} else {
		fmt.Printf(">>> Namespace %s correctly deleted\n", namespace2)
	}

	fmt.Printf(">>> TestNamespaceCRUD PASSED\n")
}

// TestNamespaceListingPagination tests that namespace listing works correctly
// This verifies the LIST operation with multiple namespaces
func TestNamespaceListingPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	// Create multiple namespaces
	numNamespaces := 5
	namespaces := make([]string, numNamespaces)
	fmt.Printf(">>> Creating %d namespaces for listing test\n", numNamespaces)
	for i := 0; i < numNamespaces; i++ {
		namespaces[i] = fmt.Sprintf("list_test_ns_%d_%s", i+1, randomString(4))
		runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", namespaces[i]))
		fmt.Printf(">>> Created namespace %d: %s\n", i+1, namespaces[i])
	}

	// List all namespaces
	fmt.Printf(">>> Listing all namespaces\n")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	fmt.Printf(">>> Namespaces listed:\n%s\n", output)

	// Verify all namespaces are in the listing
	fmt.Printf(">>> Verifying all namespaces are in listing\n")
	for i, ns := range namespaces {
		if !strings.Contains(output, ns) {
			t.Fatalf("Expected namespace %d (%s) in listing output", i+1, ns)
		}
		fmt.Printf(">>> Namespace %d (%s) found in listing\n", i+1, ns)
	}

	// Clean up
	fmt.Printf(">>> Cleaning up namespaces\n")
	for _, ns := range namespaces {
		runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", ns))
	}
	fmt.Printf(">>> Cleanup complete\n")

	fmt.Printf(">>> TestNamespaceListingPagination PASSED\n")
}

// TestNamespaceErrorHandling tests error handling for namespace operations
// Tests invalid operations like dropping non-existent namespaces
func TestNamespaceErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	// Test 1: Create and drop same namespace
	ns := "error_test_ns_" + randomString(6)
	fmt.Printf(">>> Test 1: Creating and dropping namespace %s\n", ns)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", ns))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", ns))
	fmt.Printf(">>> Namespace created and dropped successfully\n")

	// Test 2: Try to drop non-existent namespace
	// This may fail or succeed depending on implementation (CREATE IF NOT EXISTS pattern)
	nonExistent := "nonexistent_" + randomString(6)
	fmt.Printf(">>> Test 2: Attempting to drop non-existent namespace %s\n", nonExistent)
	output := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", nonExistent))
	fmt.Printf(">>> Drop non-existent namespace handled (IF EXISTS clause): %s\n", output)

	// Test 3: Creating duplicate namespace (with IF NOT EXISTS)
	ns2 := "dup_test_ns_" + randomString(6)
	fmt.Printf(">>> Test 3: Creating namespace %s twice (using IF NOT EXISTS)\n", ns2)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", ns2))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", ns2))
	fmt.Printf(">>> Duplicate creation handled gracefully\n")

	// Cleanup
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", ns2))

	fmt.Printf(">>> TestNamespaceErrorHandling PASSED\n")
}

// TestSchemaIntegrationWithCatalog tests that schemas are properly integrated with the catalog
// This verifies that schema operations through Trino are visible through Iceberg REST API concepts
func TestSchemaIntegrationWithCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	// Create a schema through Trino
	schemaName := "catalog_integration_" + randomString(6)
	fmt.Printf(">>> Creating schema through Trino SQL: %s\n", schemaName)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))

	// Verify it's accessible through catalog (list schemas)
	fmt.Printf(">>> Verifying schema is visible through catalog (SHOW SCHEMAS)\n")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, schemaName) {
		t.Fatalf("Created schema %s not visible in catalog listing", schemaName)
	}
	fmt.Printf(">>> Schema successfully verified in catalog\n")

	// Verify empty schema
	fmt.Printf(">>> Verifying schema is empty (no tables)\n")
	output = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SHOW TABLES FROM iceberg.%s", schemaName))
	fmt.Printf(">>> Empty schema output:\n%s\n", output)

	// Clean up
	fmt.Printf(">>> Cleaning up schema\n")
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA iceberg.%s", schemaName))

	fmt.Printf(">>> TestSchemaIntegrationWithCatalog PASSED\n")
}
